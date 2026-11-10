# outbound-slot-poc

POC for **A20-3142** (epic A20-3042, Outbound service scalability). This is the base version: **acquire or reject a slot** at three levels, using `java.util.concurrent.Semaphore` and Java 21 virtual threads. Everything is in memory.

Rebalancing, sizing formulas (throughput × p99), connector re-onboarding and load generation will be added later.

## Model

```
Service semaphore      x = service max slots (pod-wide, default 1000)
  └─ Connector semaphore   fixed slots, shared by the connector's lanes
       └─ Lane semaphore       fixed slots per lane (stage × destination)
```

A request is accepted only if it gets one slot at **every** level. `SlotRegistry.admit(laneId)` calls `tryAcquire()`, which never blocks, in the order **lane → connector → service**:

- **All three succeed:** returns `202 ACCEPTED`. The simulated delivery (a sleep) runs on a virtual thread. The slots are released in reverse order when it finishes, using try-with-resources, so they can't leak.
- **Any level fails:** the slots already taken are given back, and the call returns `429` with `rejectedBy = LANE | CONNECTOR | SERVICE`.

The lane is checked first so that a request for a full lane never takes a shared connector or service slot, even briefly.

## Run

```bash
./gradlew test
./gradlew bootRun          # http://localhost:8085
```

`spring.threads.virtual.enabled=true` makes Tomcat handle requests on virtual threads.

Sequence diagrams for the full flow are in [docs/sequence-diagram.md](docs/sequence-diagram.md): startup, onboarding, the accepted and rejected paths, and stats.

## Configuration and where each default comes from

All defaults live in `src/main/resources/application.yml`. `SlotPocProperties` falls back to the same values only when a key is removed from it.

| Property | Default | Source |
|---|---|---|
| `slot-poc.service-max-slots` | 1000 | Whiteboard: the service slot box (`slots_1 … 1000`) |
| `slot-poc.default-lane-latency-ms` | 100 | POC choice: simulated delivery time for a lane onboarded without `latencyMs` |
| `slot-poc.retry-after-seconds` | 1 | POC choice: `Retry-After` header on a 429 |
| `slot-poc.delivery-shutdown-wait-seconds` | 30 | POC choice: how long shutdown waits for in-flight deliveries |
| `server.port` | 8085 | POC choice |

Connector and lane slots have **no defaults**: they are required in every onboarding request.

The sample requests use whiteboard values:
- S3 connector_1 has 130 slots.
- bucket1 has 18 slots (30/s × 0.6 s).
- bucket2 and bucket3 have 30 slots each (100/s × 0.3 s).

## Logging

SLF4J with Logback (Spring Boot default).

| Level | Logger | What |
|---|---|---|
| INFO | `SlotPocConfig`, `SlotRegistry` | Config values and service slots at startup; each connector onboarded; over-subscription warning |
| INFO | `OutboundRequestController` | Every request: `requestId`, lane, and 202, or 429 with `rejectedBy` |
| WARN | `ApiExceptionHandler` | 400, 404 and 409 with the reason |
| DEBUG | `SlotRegistry` | Every slot acquire (in-flight per level) and reject (level, rollback count) |
| DEBUG | `DeliveryService` | Delivery start and finish per `requestId`, on named virtual threads `delivery-N` |

`com.acuver.aekyam.slotpoc` logs at INFO by default. To see every slot event:

```bash
java -jar build/libs/outbound-slot-poc-0.1.0-SNAPSHOT.jar --logging.level.com.acuver.aekyam.slotpoc=DEBUG
```

Tests use `src/test/resources/logback-test.xml` (INFO), so the 20k-request test doesn't flood the output.

## Sample requests (`src/main/resources/requests/`)

| File | Endpoint |
|---|---|
| `onboard-s3-connector-1.json` | POST `/connectors`: S3_connector_1, 130 slots, bucket1 18 at 600 ms, bucket2 30 at 300 ms |
| `onboard-s3-connector-2.json` | POST `/connectors`: S3_connector_2, 100 slots, bucket3 30 at 300 ms |
| `onboard-kafka-connector-oversubscribed.json` | POST `/connectors`: 20 connector slots and 15 + 15 lane slots, with no `latencyMs` so the default applies |
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
| POST | `/connectors` | Onboard a connector and its lanes with fixed slots (201; 409 if it already exists) |
| GET | `/connectors/{id}` | Slots for the connector and its lanes |
| POST | `/requests` | `{"laneId": "...", "latencyMs": 600}`: returns 202 or 429 (`latencyMs` is optional) |
| GET | `/stats` | Capacity, available, in-flight, peak and rejected at every level |

```bash
curl -XPOST localhost:8085/api/v1/connectors -H 'Content-Type: application/json' -d '{
  "connectorId":"S3_connector_1","slots":130,"lanes":[
    {"laneId":"stage-1:bucket1","slots":18,"latencyMs":600},
    {"laneId":"stage-2:bucket2","slots":30,"latencyMs":300}]}'

curl -XPOST localhost:8085/api/v1/requests -H 'Content-Type: application/json' -d '{"laneId":"stage-1:bucket1"}'
# 202 {"status":"ACCEPTED","requestId":"...","laneId":"stage-1:bucket1"}
# 429 {"status":"REJECTED","requestId":"...","laneId":"stage-1:bucket1","rejectedBy":"LANE","slotPool":"stage-1:bucket1"}
```

`latencyMs` on a lane is its simulated delivery time. When it is left out, `slot-poc.default-lane-latency-ms` applies.

## Tests (`./gradlew test`, 8 of 8 pass)

| Test | Checks |
|---|---|
| Lane, connector and service limits | Each full level rejects with its own name. Slots taken at earlier levels are given back |
| Permit closed twice | Slots are released only once |
| 20,000 concurrent requests on virtual threads | Peak never goes above the cap at any level. All slots are returned. Each rejection is counted at exactly one level |
| Onboarding | Duplicate connector or lane → 409, empty lanes or 0 slots → 400, unknown lane → 404 |
| REST | 202, then 429 `rejectedBy: LANE`, plus stats |

## Code map

| Class | Role |
|---|---|
| `core/SlotPool` | One semaphore plus in-flight, peak and rejected counters |
| `core/SlotRegistry` | `admit` (acquire or reject) and `onboard` |
| `core/SlotPermit` | AutoCloseable. Releases the held slots once, in reverse order |
| `core/Lane`, `core/Connector`, `core/Admission`, `core/Level` | Model |
| `delivery/DeliveryService` | Virtual-thread executor; a simulated delivery is a sleep |
| `api/*` | REST controllers and error mapping |
