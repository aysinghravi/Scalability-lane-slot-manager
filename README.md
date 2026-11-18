# outbound-slot-poc

POC for **A20-3142** (epic A20-3042, Outbound service scalability). It **acquires or rejects a slot** at three levels, using `java.util.concurrent.Semaphore` and Java 21 virtual threads. Lanes **share their connector's slots**: every 200 ms the planner recalculates each lane's slot count, using weighted fairness and guaranteed minimums. Everything is in memory.

Not done yet:
- sharing service slots across connectors
- a memory budget alongside slots
- priority for starved lanes
- load generation

## Model

```
Service semaphore      x = service max slots (pod-wide, default 1000), fixed
  └─ Connector semaphore   fixed slots, shared by the connector's lanes
       └─ Lane semaphore       resizable: guaranteedSlots <= target <= maxSlots, re-planned every 200 ms
```

A request is accepted only if it gets one slot at **every** level. `SlotRegistry.admit(laneId)` calls `tryAcquire()`, which never blocks, in the order **lane → connector → service**:

- **All three succeed:** returns `202 ACCEPTED`. The simulated delivery (a sleep) runs on a virtual thread. The slots are released in reverse order when it finishes, using try-with-resources, so they can't leak.
- **Any level fails:** the slots already taken are given back, and the call returns `429` with `rejectedBy = LANE | CONNECTOR | SERVICE`.

The lane is checked first so that a request for a full lane never takes a shared connector or service slot, even briefly.

## Shared lane slots

Each lane is onboarded with a slot policy:

| Field | Meaning | Default |
|---|---|---|
| `guaranteedSlots` | Slots reserved for the lane even when it is idle | Required. The total across a connector's lanes must not exceed the connector's slots (otherwise 400) |
| `maxSlots` | The most the lane can ever be given | The connector's slots. Over-subscription is allowed |
| `weight` | The lane's share of the spare slots, relative to its sibling lanes | 1 |

**Demand** (`core/Lane.updateDemand`), measured on each tick:
- `arrivals` is every admission attempt since the last tick, accepted or rejected. A lane that is turning requests away still shows the demand it isn't getting.
- `now = max(inFlight, arrivals / window × latencyMs)`, using Little's law (concurrency = throughput × latency, as on the whiteboard).
- `demand = ceil(α × now + (1 − α) × previous)`. α is 0.5 when demand is rising and 0.15 when it is falling: bursts are picked up within a tick or two, but slots are given back slowly.

**Planning** (`core/SlotPlanner`, a pure function), per connector:
1. Every lane gets its `guaranteedSlots`.
2. A lane with demand but no guarantee gets 1 slot, if any are left.
3. The spare slots go to lanes that still need more, by weighted max-min fairness (water-filling). A lane's need is `min(demand, maxSlots)`, and spare slots no lane needs are left unassigned.
4. Fractions are rounded by largest remainder, so the total never exceeds the connector's slots.

**Applying the plan** (`core/ResizableSemaphore`):
- Growing a lane releases extra permits.
- Shrinking calls `reducePermits`, which can take the available count below zero. Running deliveries are never interrupted; the lane rejects new requests until its in-flight count drops below the new target.
- While a shrunk lane drains, a sibling lane can be under its target while the connector is full. That is the only time a `CONNECTOR` 429 happens, and it is what the fixed connector semaphore is for.

**Damping** (`SlotRegistry.damp`). Without it, a lane under constant but bursty load resized on almost every tick: bucket1 flipped between about 74 and 100 in the load test. Two things stop that:
1. **Asymmetric smoothing** (above): a quiet window can't take a busy lane's slots away.
2. **Shrink deadband**: a shrink smaller than 10% of the lane's target (at least 2 slots) is skipped. There are two exceptions:
   - A shrink that brings the lane back to its guarantee is always applied, so an idle lane always returns there.
   - The skip is allowed only if the kept slots still fit inside the connector next to every other lane's planned target. When a sibling lane needs them, the shrink is applied at once.

Growth is never delayed. Each lane exposes a `resizes` counter in `/stats`.

| Load test (90 s), target changes between samples | bucket1 | bucket2 | topic1 | topic2 |
|---|---|---|---|---|
| Before damping | 120 | 55 | 44 | 34 |
| With damping | 38 | 6 | 4 | 4 |

Under constant load, lanes now make 0 resizes in the second half of every loaded phase.

**Trade-off:** a lane with noisy demand settles near the top of its noise, not the average. In the contention phase, bucket2 settled at 71 and bucket1 at 59, where the planner model predicts 60 and 70. Lower `demand-smoothing-up` to trade reaction speed for a closer fit.

**Worked example:** S3_connector_1 has 130 slots. This is the `sharedSlots` test.

| | bucket1 (guaranteed 18, latency 600 ms) | bucket2 (guaranteed 30, latency 300 ms) |
|---|---|---|
| bucket1 gets 150/s (demand 90), bucket2 idle | 18 + 72 spare = **90** | **30** (guarantee kept) |
| bucket2 rises to 200/s (demand 60) | 18 + 52 = **70** (drains from 90 in flight) | 30 + 30 = **60** |

With `slot-poc.planner.enabled=false`, every lane stays fixed at its `guaranteedSlots`.

## Run

```bash
./gradlew test
./gradlew bootRun          # http://localhost:8085
```

`spring.threads.virtual.enabled=true` makes Tomcat handle requests on virtual threads.

Sequence diagrams for the full flow are in [docs/sequence-diagram.md](docs/sequence-diagram.md): startup, onboarding, the accepted and rejected paths, the planner tick, and stats.

## Configuration and where each default comes from

All defaults live in `src/main/resources/application.yml`. `SlotPocProperties` falls back to the same values only when a key is removed from it.

| Property | Default | Source |
|---|---|---|
| `slot-poc.service-max-slots` | 1000 | Whiteboard: the service slot box (`slots_1 … 1000`) |
| `slot-poc.default-lane-latency-ms` | 100 | POC choice: simulated delivery time for a lane onboarded without `latencyMs` |
| `slot-poc.retry-after-seconds` | 1 | POC choice: `Retry-After` header on a 429 |
| `slot-poc.delivery-shutdown-wait-seconds` | 30 | POC choice: how long shutdown waits for in-flight deliveries |
| `slot-poc.planner.enabled` | true | `false` keeps every lane fixed at its guaranteed slots |
| `slot-poc.planner.interval-ms` | 200 | Lane scheduler reference design (re-plans every 200 ms) |
| `slot-poc.planner.demand-smoothing-up` | 0.5 | POC choice: weight of the newest window when demand rises (react fast) |
| `slot-poc.planner.demand-smoothing-down` | 0.15 | POC choice: weight of the newest window when demand falls (give slots back slowly) |
| `slot-poc.planner.shrink-deadband` | 0.1 | POC choice: skip shrinks smaller than 10% of the target (at least 2 slots); 0 applies every shrink |
| `server.port` | 8085 | POC choice |

Connector `slots` and lane `guaranteedSlots` have **no defaults**: they are required in every onboarding request. `maxSlots` defaults to the connector's slots, and `weight` to 1.

The sample requests use whiteboard values as guarantees:
- S3 connector_1 has 130 slots.
- bucket1 is guaranteed 18 (30/s × 0.6 s).
- bucket2 and bucket3 are guaranteed 30 each (100/s × 0.3 s).

## Logging

SLF4J with Logback (Spring Boot default).

| Level | Logger | What |
|---|---|---|
| INFO | `SlotPocConfig`, `SlotRegistry`, `PlannerLoop` | Config values and service slots at startup; each connector onboarded with its lane policies; planner start and stop |
| DEBUG | `SlotRegistry` | `Lane slots re-planned`: one line per connector, only when a target changes (`old->new`, demand, in flight) |
| ERROR | `PlannerLoop` | A failed re-plan. The previous targets stay in place |
| INFO | `OutboundRequestController` | Every request: `requestId`, lane, and 202, or 429 with `rejectedBy` |
| WARN | `ApiExceptionHandler` | 400, 404 and 409 with the reason |
| DEBUG | `SlotRegistry` | Every slot acquire (in-flight per level) and reject (level, rollback count) |
| DEBUG | `DeliveryService` | Delivery start and finish per `requestId`, on named virtual threads `delivery-N` |

`application.yml` currently sets `com.acuver.aekyam.slotpoc` to DEBUG. To override it from the command line, use for example:

```bash
java -jar build/libs/outbound-slot-poc-0.1.0-SNAPSHOT.jar --logging.level.com.acuver.aekyam.slotpoc=DEBUG
```

Tests use `src/test/resources/logback-test.xml` (INFO), so the 20k-request test doesn't flood the output.

## Sample requests (`src/main/resources/requests/`)

| File | Endpoint |
|---|---|
| `onboard-s3-connector-1.json` | POST `/connectors`: S3_connector_1, 130 slots, bucket1 guaranteed 18 at 600 ms, bucket2 guaranteed 30 at 300 ms |
| `onboard-s3-connector-2.json` | POST `/connectors`: S3_connector_2, 100 slots, bucket3 guaranteed 30 at 300 ms |
| `onboard-kafka-connector-oversubscribed.json` | POST `/connectors`: 20 connector slots, two lanes each guaranteed 5 with `maxSlots` 15 (over-subscribed) and weights 2:1. No `latencyMs`, so the default applies |
| `submit-request.json` | POST `/requests` |
| `submit-request-latency-override.json` | POST `/requests`: holds its slot for 5 s |
| `outbound-slot-poc.postman_collection.json` | **Every endpoint**, including the GETs and a 404 example. Import it into Postman |

```bash
cd src/main/resources/requests
curl -XPOST localhost:8085/api/v1/connectors -H 'Content-Type: application/json' -d @onboard-s3-connector-1.json
curl -XPOST localhost:8085/api/v1/requests   -H 'Content-Type: application/json' -d @submit-request.json
curl localhost:8085/api/v1/connectors/S3_connector_1
curl localhost:8085/api/v1/stats
```

## API (`/api/v1`)

| Method | Path | Purpose |
|---|---|---|
| POST | `/connectors` | Onboard a connector and its lanes, each with `guaranteedSlots` and optional `maxSlots` and `weight` (201; 409 if it already exists) |
| GET | `/connectors/{id}` | Slots for the connector and its lanes. For a lane, `slots.capacity` is its current planned target, and `demand` is what it wanted at the last plan |
| POST | `/requests` | `{"laneId": "...", "latencyMs": 600}`: returns 202 or 429 (`latencyMs` is optional) |
| GET | `/stats` | Capacity, available, in-flight, peak and rejected at every level |

```bash
curl -XPOST localhost:8085/api/v1/connectors -H 'Content-Type: application/json' -d '{
  "connectorId":"S3_connector_1","slots":130,"lanes":[
    {"laneId":"stage-1:bucket1","guaranteedSlots":18,"latencyMs":600},
    {"laneId":"stage-2:bucket2","guaranteedSlots":30,"maxSlots":100,"weight":2,"latencyMs":300}]}'

curl -XPOST localhost:8085/api/v1/requests -H 'Content-Type: application/json' -d '{"laneId":"stage-1:bucket1"}'
# 202 {"status":"ACCEPTED","requestId":"...","laneId":"stage-1:bucket1"}
# 429 {"status":"REJECTED","requestId":"...","laneId":"stage-1:bucket1","rejectedBy":"LANE","slotPool":"stage-1:bucket1"}
```

`latencyMs` on a lane is its simulated delivery time. When it is left out, `slot-poc.default-lane-latency-ms` applies.

## Tests (`./gradlew test`, 22 of 22 pass)

| Test | Checks |
|---|---|
| `SlotPlannerTest` (9) | The worked example exactly (90/30, then 80/50); idle lanes keep their guarantee; `maxSlots` caps; spare splits by weight; a lane with demand gets at least 1 slot; rounding; the same input gives the same plan; guarantees above capacity are rejected; 5,000 random inputs (guarantees met, max respected, total ≤ capacity) |
| Shared slots | bucket1 borrows 90, then shrinks to 70 while bucket2 grows to 60. bucket1's 90 running deliveries are not interrupted, and bucket1 rejects until it drains. bucket2 hits a `CONNECTOR` 429 during the drain |
| Idle shrink | A lane drops back to its guarantee once its demand goes away |
| Damping | Under the same noisy load (alternating 100/s and 60/s), the undamped lane resizes 40 times in 40 ticks and the damped lane 0 times, holding at 95. It still returns to its guarantee when idle |
| Damping and the connector | A small shrink is still applied at once when a sibling lane needs the slots, so the lane targets never add up to more than the connector's slots |
| Lane and service limits | Each full level rejects with its own name. Slots taken at earlier levels are given back |
| Permit closed twice | Slots are released only once |
| 20,000 concurrent requests on virtual threads, re-planning every 5 ms | Connector and service peaks never exceed their caps, and lane peaks never exceed `maxSlots`. All slots are returned. Each rejection is counted at exactly one level |
| Onboarding | Duplicate connector or lane → 409. Empty lanes, 0 slots, guarantees above the connector's slots, guaranteed above max, or a missing `guaranteedSlots` → 400. Unknown lane → 404 |
| REST (planner off) | Lane policy fields, 202 then 429 `rejectedBy: LANE`, stats, 400 for guarantees above the connector's slots |

## Load test (Locust, `loadtest/`)

`loadtest/locustfile.py` runs five load phases against a running POC (planner enabled). It polls `/api/v1/stats` every 250 ms and checks each behaviour from those samples. It onboards its own connectors and lanes, adding a run suffix to their ids, so it can run repeatedly without restarting the server.

| Phase | Load (req/s) | What it shows |
|---|---|---|
| steady | bucket1 10, bucket2 20 | **Processing**: everything is accepted and fits inside the guarantees |
| surge | bucket1 150, bucket2 20 | **Rejection** (`429 LANE`), then an **increase**: bucket1 borrows from 18 up to about 90 |
| contention | bucket1 150, bucket2 200 | bucket2 **increases** from 30 to about 60, and bucket1 **decreases** to about 70 by draining, not interrupting |
| weighted | topic1 150, topic2 150 | The S3 lanes **decrease** back to 18/30. The Kafka connector's spare slots split **2:1 by weight** (12/8) |
| cooldown | none | Every lane returns to its guarantee, and every slot is released |

A **stability** check also requires at most 5 resizes per lane in the second half of each loaded phase. It uses the server-side `resizes` counter.

```bash
pip install -r loadtest/requirements.txt
./gradlew bootRun                     # in another terminal
cd loadtest
locust                                # about 90 s; settings from locust.conf (headless, localhost:8085)
locust --phase-seconds 30             # longer phases
locust --rate-scale 0.5               # half the load, for a slower machine
```

**Output in `loadtest/reports/`** (ignored by git):
- `slot-run-<timestamp>.html`: the run report. It contains:
  - the check results, each with its expected and observed values;
  - the phases;
  - a chart per lane of target, in flight, demand and guarantee over time;
  - connector charts of the lane-target total against in flight and the connector's slots;
  - outcomes per second;
  - a phase × lane table, with the expected target from the same planner algorithm next to the settled and peak targets.
- `slot-run-<timestamp>-samples.csv` and `-summary.json`: the raw samples and the summary.
- `locust-stats.html`: Locust's own request statistics. Its failures table breaks the expected 429s down by `rejectedBy`.

The process exits with 0 when all 12 checks pass and 1 otherwise, so the test can run in CI. It runs in local mode only, not `--master`/`--worker`.

## Code map

| Class | Role |
|---|---|
| `core/SlotPool` | One resizable semaphore plus in-flight, peak, rejected and arrival counters |
| `core/ResizableSemaphore` | A `Semaphore` with `setLimit` (grow with `release`, shrink with `reducePermits`) |
| `core/SlotPlanner` | Pure function: a connector's slots and its lanes' policy and demand in, a target per lane out |
| `core/PlannerLoop` | `slot-planner` thread that calls `SlotRegistry.rebalance()` every `interval-ms` |
| `core/SlotRegistry` | `admit` (acquire or reject), `onboard`, `rebalance` |
| `core/SlotPermit` | AutoCloseable. Releases the held slots once, in reverse order |
| `core/Lane` | Lane policy (guaranteed, max, weight, latency), its slot pool, and its demand estimate |
| `core/Connector`, `core/Admission`, `core/Level` | Model |
| `delivery/DeliveryService` | Virtual-thread executor; a simulated delivery is a sleep |
| `api/*` | REST controllers and error mapping |
