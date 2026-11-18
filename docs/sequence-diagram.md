# Sequence diagrams: outbound-slot-poc

These diagrams are written in Mermaid. They render in IntelliJ (Markdown preview), on GitHub and at mermaid.live.

## 1. Startup: where the defaults come from

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    participant Boot as Spring Boot
    participant YML as application.yml
    participant Props as SlotPocProperties
    participant Cfg as SlotPocConfig
    participant Reg as SlotRegistry
    participant Del as DeliveryService
    participant PL as PlannerLoop

    Boot->>YML: read slot-poc.*
    YML-->>Props: service-max-slots=1000, default-lane-latency-ms=100,<br/>retry-after-seconds=1, delivery-shutdown-wait-seconds=30,<br/>planner: enabled=true, interval-ms=200, demand-smoothing-up=0.5,<br/>demand-smoothing-down=0.15, shrink-deadband=0.1
    Note over Props: a missing key falls back to the same value<br/>in the record's compact constructor
    Boot->>Cfg: create beans
    Cfg->>Reg: new SlotRegistry(1000, 100, 0.5, 0.15, 0.1)
    Reg->>Reg: service SlotPool = Semaphore(1000)
    opt planner.enabled = true
        Cfg->>PL: new PlannerLoop(registry, 200)
        PL->>PL: platform thread "slot-planner", fixed delay 200 ms
    end
    Cfg->>Del: new DeliveryService(30)
    Del->>Del: virtual-thread executor (delivery-N)
```

## 2. Onboard a connector: POST /api/v1/connectors

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    actor Client
    participant CC as ConnectorController
    participant Reg as SlotRegistry
    participant EH as ApiExceptionHandler

    Client->>CC: POST /connectors {connectorId, slots,<br/>lanes[{laneId, guaranteedSlots, maxSlots?, weight?, latencyMs?}]}
    CC->>Reg: onboard(connectorId, slots, lanes)  [synchronized]
    alt invalid (no lanes, slots < 1, blank id, guaranteed > max,<br/>sum of guaranteed > connector slots)
        Reg-->>EH: IllegalArgumentException
        EH-->>Client: 400
    else connector or lane already exists
        Reg-->>EH: IllegalStateException
        EH-->>Client: 409
    else ok
        Reg->>Reg: connector SlotPool = Semaphore(slots)
        loop each lane
            Reg->>Reg: lane SlotPool = ResizableSemaphore(guaranteedSlots)<br/>maxSlots = given or connector slots, weight = given or 1<br/>latencyMs = given or default (100)
        end
        Reg->>Reg: publish connector, then its lanes
        Reg-->>CC: Connector
        CC-->>Client: 201 {connectorId, slots, lanes[]}
    end
```

## 3. Submit a request: accepted path

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    actor Client
    participant RC as OutboundRequestController
    participant Reg as SlotRegistry
    participant L as Lane semaphore
    participant C as Connector semaphore
    participant S as Service semaphore
    participant Del as DeliveryService
    participant VT as Virtual thread (delivery-N)

    Client->>RC: POST /requests {laneId, latencyMs?}
    RC->>RC: requestId = UUID
    RC->>Reg: admit(laneId)
    Reg->>L: tryAcquire()
    L-->>Reg: true
    Reg->>C: tryAcquire()
    C-->>Reg: true
    Reg->>S: tryAcquire()
    S-->>Reg: true
    Reg-->>RC: Accepted(lane, SlotPermit[L, C, S])
    RC->>Del: dispatch(requestId, accepted, latencyMs)
    Del->>VT: execute (does not block the caller)
    RC-->>Client: 202 {status: ACCEPTED, requestId, laneId}
    Note over VT: try (permit) { sleep(latencyMs) }<br/>simulated downstream call
    VT->>S: release()
    VT->>C: release()
    VT->>L: release()
```

## 4. Submit a request: rejected path (example: connector full)

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    actor Client
    participant RC as OutboundRequestController
    participant Reg as SlotRegistry
    participant L as Lane semaphore
    participant C as Connector semaphore
    participant S as Service semaphore

    Client->>RC: POST /requests {laneId}
    RC->>Reg: admit(laneId)
    alt unknown lane
        Reg-->>Client: 404 (NotFoundException via ApiExceptionHandler)
    else lane full
        Reg->>L: tryAcquire()
        L-->>Reg: false
        Reg-->>RC: Rejected(LANE)
    else connector full
        Reg->>L: tryAcquire()
        L-->>Reg: true
        Reg->>C: tryAcquire()
        C-->>Reg: false
        Reg->>L: release()  (roll back)
        Reg-->>RC: Rejected(CONNECTOR)
    else service full
        Reg->>L: tryAcquire() true
        Reg->>C: tryAcquire() true
        Reg->>S: tryAcquire() false
        Reg->>C: release()  (roll back)
        Reg->>L: release()  (roll back)
        Reg-->>RC: Rejected(SERVICE)
    end
    RC-->>Client: 429 Retry-After: 1<br/>{status: REJECTED, requestId, laneId, rejectedBy, slotPool}
```

## 5. Shared lane slots: planner tick every 200 ms

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    participant PL as PlannerLoop (slot-planner)
    participant Reg as SlotRegistry
    participant Lane as Lane
    participant Plan as SlotPlanner
    participant L as Lane semaphore

    PL->>Reg: rebalance()  [synchronized, same lock as onboard]
    Reg->>Reg: window = time since last tick
    loop each connector
        loop each lane
            Reg->>Lane: updateDemand(window, smoothing)
            Lane->>Lane: arrivals = attempts since last tick (accepted + rejected)<br/>now = max(inFlight, arrivals / window x latency)<br/>a = 0.5 if rising, 0.15 if falling<br/>demand = ceil(a x now + (1 - a) x previous)
        end
        Reg->>Plan: plan(connector slots, [guaranteed, max, weight, demand])
        Plan->>Plan: 1. every lane gets its guaranteed slots<br/>2. lane with demand and no guarantee gets 1<br/>3. spare shared by weighted max-min fairness, capped at min(demand, max)<br/>4. round by largest remainder, total <= connector slots
        Plan-->>Reg: target per lane
        Reg->>Reg: damp: skip a shrink < 10% of target (min 2)<br/>unless it ends at the guarantee or the slots<br/>are needed to stay within connector slots
        loop each lane whose target changed
            Reg->>L: setLimit(target), lane.resizes++
            alt grow
                L->>L: release(extra permits)
            else shrink
                L->>L: reducePermits(n), may go below 0<br/>running work is not interrupted, new requests<br/>are rejected until in flight < target
            end
        end
        Reg->>Reg: DEBUG "Lane slots re-planned" with old -> new per lane
    end
    Note over Reg: connector semaphore stays fixed: it is the hard cap<br/>while a shrunk lane drains (CONNECTOR 429s only then)
```

## 6. Read stats: GET /api/v1/stats and GET /api/v1/connectors/{id}

```mermaid
%%{init: {'theme':'neutral', 'themeVariables': {'fontSize':'18px'}}}%%
sequenceDiagram
    autonumber
    actor Client
    participant CC as ConnectorController
    participant Reg as SlotRegistry

    Client->>CC: GET /stats
    CC->>Reg: servicePool().stats(), connectors()
    Reg-->>CC: capacity, available, inFlight, peakInFlight, rejected<br/>for the service, each connector and each lane<br/>(lane capacity = current planned target, plus guaranteed, max, weight, demand)
    CC-->>Client: 200 {service, connectors[ {slots, lanes[]} ]}
```
