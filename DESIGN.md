# RideHail Backend — Design Document

> A learning-focused Uber/Ola-style ride-hailing backend built with **Java 21 + Spring Boot 3**.
> Goal: implement and be able to *discuss* geo-spatial search, driver matching, Redis, Kafka,
> WebSockets, payments, notifications, rate limiting, a trip state machine, event sourcing, and Kubernetes.

---

## 1. Goals & Non-Goals

### Goals
- Model the **real domain**: drivers stream location, riders request trips, a matcher pairs them, trips move through a lifecycle, payments settle, everyone gets notified.
- Make every "buzzword" a concrete, observable behavior in code.
- Be horizontally scalable in design (partitioning, statelessness, event-driven) even if we run one node locally.
- Be interview-ready: clear trade-offs, especially around **driver–rider matching**.

### Non-Goals
- Real money movement (payment provider is stubbed but the *workflow* is real).
- A mobile client / maps UI (we simulate drivers & riders with scripts and tests).
- Production-grade security hardening (we do auth + rate limiting, not a full security audit).

---

## 2. High-Level Architecture

```
                    ┌─────────────┐
   Rider App ──WS──▶ │ API Gateway │ ◀──WS── Driver App
     (REST)         │  (Spring)   │        (REST)
                    └──────┬──────┘
        ┌──────────┬───────┼────────┬───────────┐
        ▼          ▼       ▼        ▼           ▼
   ┌────────┐ ┌────────┐ ┌──────┐ ┌───────┐ ┌──────────┐
   │  Auth  │ │Location│ │Match │ │ Trip  │ │ Payment  │
   │ Service│ │ Service│ │Service│ │Service│ │ Service  │
   └────────┘ └───┬────┘ └──┬───┘ └───┬───┘ └────┬─────┘
                  │         │         │          │
              ┌───▼─────────▼─────────▼──────────▼───┐
              │  Redis (GEO + cache + rate limit)     │
              └───────────────────┬───────────────────┘
                                  │
              ┌───────────────────▼───────────────────┐
              │   Kafka (event backbone / event store) │
              └───────┬───────────────────────┬────────┘
                      ▼                        ▼
              ┌──────────────┐        ┌────────────────┐
              │ Notification │        │  Read Models /  │
              │   Service    │        │  Projections    │
              └──────────────┘        └────────────────┘
```

### Deployment shape
- **Monorepo, multi-module Maven/Gradle project.** Each service is a Spring Boot module (`:service-trip`, `:service-match`, …) plus a shared `:common` module (events, DTOs, security).
- Start as a **modular monolith** (all modules, one process, easy to run) → split into separate deployables when we containerize. The module boundaries are drawn now so the split is mechanical later.

---

## 3. Technology Stack

| Concern | Choice | Notes |
|---|---|---|
| Language / runtime | Java 21 (virtual threads) | Virtual threads make blocking WebSocket/IO code scale cheaply |
| Framework | Spring Boot 3.x | Web, WebFlux only where needed |
| REST | Spring Web MVC | Simple request/response endpoints |
| Real-time | Spring WebSocket (STOMP optional) | Driver location in, trip updates out |
| Persistence | PostgreSQL + PostGIS | Source of truth; PostGIS for durable geo queries/analytics |
| ORM | Spring Data JPA (+ jOOQ for event store, optional) | JPA for aggregates, raw SQL where perf matters |
| Cache / geo index / locks | Redis 7 (Lettuce client) | `GEOSEARCH`, token buckets, distributed locks, pub/sub |
| Messaging | Apache Kafka (Spring for Apache Kafka) | Event backbone + event store topics |
| Migrations | Flyway | Versioned schema |
| Build | Gradle (Kotlin DSL) | Multi-module |
| Testing | JUnit 5, Testcontainers | Spin up real Postgres/Redis/Kafka in tests |
| Observability | Micrometer + Prometheus, structured logging | Metrics per service |
| Containerization | Docker, docker-compose (local), Kubernetes + Helm (deploy) | |

---

## 4. Domain Model & Data

### 4.1 Core aggregates
- **Rider** — id, name, phone, rating, payment methods.
- **Driver** — id, name, vehicle (type, plate), rating, status (`OFFLINE|AVAILABLE|ON_TRIP`), current location (cached in Redis, snapshotted to DB).
- **Trip** — the central aggregate; event-sourced (see §8).
- **Payment** — one per completed trip; its own saga.

### 4.2 Postgres tables (source of truth)

```sql
-- Drivers (durable profile; live location lives in Redis)
driver(id, name, phone, vehicle_type, plate, rating, status, last_seen_at, home_region)

-- Riders
rider(id, name, phone, rating, default_payment_method_id)

-- Trip current-state projection (rebuilt from events)
trip(id, rider_id, driver_id, status, pickup_lng, pickup_lat, drop_lng, drop_lat,
     requested_at, assigned_at, started_at, completed_at, fare_cents, version)

-- Event store (append-only, the real source of truth for trips)
trip_event(id, trip_id, seq, type, payload_json, occurred_at)
  UNIQUE(trip_id, seq)   -- optimistic concurrency

-- Payments
payment(id, trip_id, rider_id, amount_cents, status, idempotency_key, provider_ref,
        created_at, updated_at)
```

`trip_event` is the event-sourcing spine. The `trip` row is a **projection** we can drop and rebuild by folding events.

---

## 5. Redis Usage (the hot path)

Redis is deliberately used for **four distinct jobs** — good to separate them mentally:

1. **Geo index of available drivers** (per region, to keep sets small):
   - `GEOADD geo:drivers:{region} <lng> <lat> <driverId>`
   - `GEOSEARCH geo:drivers:{region} FROMLONLAT <lng> <lat> BYRADIUS 3 km ASC COUNT 20 WITHDIST`
   - Entry expires if a driver stops pinging (we re-add on each ping; a sweeper removes stale via a companion sorted set keyed by `last_ping_ts`).
2. **Driver live state cache**: `HSET driver:{id} status ... etaModel ...` with short TTL.
3. **Distributed locks / offer holds**: `SET offer:{tripId} {driverId} NX EX 15` so one driver at a time holds an offer and can't be double-assigned.
4. **Rate limiting**: token-bucket via Lua script (see §12).

> Region sharding: we bucket the geo index by city/region (`geo:drivers:blr-koramangala`). Keeps each `GEOSEARCH` over a small set and lets us scale Redis by region later.

---

## 6. Kafka Topology (event backbone + event store)

| Topic | Key | Produced by | Consumed by | Purpose |
|---|---|---|---|---|
| `location.updates` | driverId | Location Service | Match, analytics | Firehose of driver pings |
| `trip.events` | tripId | Trip Service | Payment, Notification, Projection, Match | The trip lifecycle event stream (also the event store) |
| `payment.events` | tripId | Payment Service | Trip, Notification | Auth/capture/refund outcomes |
| `match.commands` | tripId | Trip Service | Match Service | "Find a driver for this trip" |
| `match.results` | tripId | Match Service | Trip Service | "Driver X accepted / no drivers" |

**Partitioning:** key by `tripId` (trip topics) so all events for a trip land on one partition → ordering guaranteed per trip. `location.updates` keyed by `driverId`.

**Ordering & idempotency:** consumers are idempotent (dedupe on `(tripId, seq)` or `idempotencyKey`). We use manual acks / at-least-once and design consumers to tolerate replays.

---

## 7. Trip State Machine

```
                 ┌────────────┐
                 │ REQUESTED  │  rider asked for a ride
                 └─────┬──────┘
                       │ dispatch
                 ┌─────▼──────┐
        ┌────────┤  MATCHING  ├───────┐ no drivers / timeout
        │ driver └─────┬──────┘       ▼
        │ accepted     │        ┌───────────┐
        │        ┌─────▼──────┐ │ NO_DRIVERS│ (terminal)
        │        │ DRIVER_    │ └───────────┘
        └───────▶│ ASSIGNED   │
                 └─────┬──────┘
                       │ driver heading to pickup
                 ┌─────▼──────┐
                 │  EN_ROUTE  │
                 └─────┬──────┘
                       │ driver reached pickup
                 ┌─────▼──────┐
                 │  ARRIVED   │
                 └─────┬──────┘
                       │ rider on board
                 ┌─────▼──────┐
                 │ IN_PROGRESS│
                 └─────┬──────┘
                       │ reached destination
                 ┌─────▼──────┐
                 │ COMPLETED  │ (terminal → triggers payment)
                 └────────────┘

  CANCELLED (terminal) reachable from REQUESTED/MATCHING/DRIVER_ASSIGNED/EN_ROUTE/ARRIVED
```

- Implemented as an **explicit transition table** (`Map<State, Set<State>>`) validated on every command; illegal transitions throw. (We can use Spring StateMachine, but a hand-rolled table is clearer and easier to talk about.)
- Each accepted transition **emits a domain event** appended to the event store and published to `trip.events`.

---

## 8. Event Sourcing (Trip aggregate)

**Why:** audit trail, replay/debugging, and it decouples "what happened" from "current shape". Great interview material.

**Write path (command → events):**
1. Load current state by folding `trip_event` rows for that `tripId` (or read the cached projection + version).
2. Validate the command against the state machine.
3. Append new event(s) with `seq = last_seq + 1`, guarded by `UNIQUE(trip_id, seq)` for optimistic concurrency (retry on conflict).
4. Publish to `trip.events`.

**Events:** `TripRequested`, `MatchingStarted`, `DriverAssigned`, `TripEnRoute`, `DriverArrived`, `TripStarted`, `TripCompleted`, `TripCancelled`, `NoDriversFound`.

**Read path (projections):** a consumer folds `trip.events` into the `trip` table (and any other read model, e.g. driver earnings). Projections are disposable and rebuildable.

```java
// Folding events to derive state (conceptual)
TripState fold(List<TripEvent> events) {
    TripState s = TripState.empty();
    for (TripEvent e : events) s = s.apply(e);   // pure functions
    return s;
}
```

---

## 9. Geo-Spatial Search & Driver Matching  ⭐ (the interview centerpiece)

### 9.1 Location ingestion
- Driver app sends a location ping every ~4s over WebSocket → Location Service.
- Location Service: (a) `GEOADD` to `geo:drivers:{region}`, (b) publishes to `location.updates` (for analytics/replay), (c) periodic snapshot to Postgres.

### 9.2 Candidate search
Given a pickup point:
```
GEOSEARCH geo:drivers:{region} FROMLONLAT <lng> <lat>
          BYRADIUS 3 km ASC COUNT 20 WITHDIST WITHCOORD
```
Redis stores geo as geohash-scored sorted sets, so this is fast. We fetch top ~20 candidates by straight-line distance.

**Trade-off discussion (know these cold):**
- **Geohash (what Redis uses)** — simple, but rectangular buckets distort near poles and have edge-neighbor issues; fine for city-scale.
- **S2 cells (Google/Uber-style)** — hierarchical spherical cells, better neighbor handling, used at scale.
- **Quadtree / R-tree (PostGIS)** — flexible spatial indexing for durable/analytical queries.
- We use **Redis geohash for the hot path**, PostGIS for durable/analytical queries.

### 9.3 Ranking
Straight-line distance ≠ best driver. We score candidates:
```
score = w1 * etaSeconds        (road ETA estimate, not haversine)
      + w2 * (1 - driverRating/5)
      + w3 * fairnessPenalty    (recent assignment count → avoid starving)
      - w4 * acceptanceRate     (prefer reliable acceptors)
```
For learning we approximate ETA with distance × traffic factor; the interface allows swapping in a routing engine (OSRM/Valhalla) later.

### 9.4 Dispatch strategies (the meaty part)
Two classic approaches — we implement **sequential offer** first, then **batch** as an option:

**A) Sequential offer (default)**
1. Rank candidates.
2. Offer to #1: acquire `SET offer:{tripId} {driverId} NX EX 15`, push offer over WebSocket.
3. Wait up to 15s: on accept → assign & emit `DriverAssigned`; on reject/timeout → release lock, offer to #2.
4. Exhaust list or widen radius; if none → `NoDriversFound`.

**B) Batch broadcast**
- Offer to top K simultaneously; first to accept wins (atomic via the Redis `NX` lock — losers get "trip taken"). Lower latency, more driver contention.

**C) (mention only) Batched matching window / global optimization**
- Collect requests + drivers over a short window and solve an assignment (Hungarian algorithm / min-cost bipartite matching) to optimize *global* ETA rather than greedy per-request. This is how large systems reduce total wait time. We note it as the "advanced" answer.

**Consistency guarantees:**
- A driver can hold **one** active offer (Redis lock) and be in **one** trip (driver `status = ON_TRIP` check + lock).
- Offers expire, so a crashed matcher never wedges a driver.

---

## 10. WebSockets (real-time)

Two logical channels:
- **Driver → server**: location pings, accept/reject offer, status changes.
- **Server → rider**: `MATCHING`, `DRIVER_ASSIGNED` (with driver info + live location), ETA updates, `ARRIVED`, `COMPLETED`.
- **Server → driver**: ride offers, trip details.

**Design:**
- Spring WebSocket handler holds sessions in a per-node registry `Map<userId, WebSocketSession>`.
- A Kafka consumer subscribed to `trip.events` looks up the target rider/driver's session and pushes the update.
- **Scaling across nodes:** a user's socket lives on one node. We use Redis pub/sub (or Kafka) so the node holding the socket receives the push. Alternatively, sticky sessions + a shared "which node holds userId" registry in Redis.
- Virtual threads (Java 21) let us handle many blocking WebSocket sessions cheaply.

---

## 11. Payment Workflow (Saga)

Triggered by `TripCompleted`. Modeled as its own state machine:
```
PENDING → AUTHORIZED → CAPTURED           (happy path)
        ↘ AUTH_FAILED
AUTHORIZED → CAPTURE_FAILED → RETRY / MANUAL
CAPTURED → REFUNDED                        (dispute/cancel-after-complete)
```
- **Idempotency:** every charge carries an `idempotency_key` (derived from `tripId`); the provider stub and our DB dedupe on it so retries never double-bill.
- Payment Service consumes `trip.events`, computes fare (base + distance + time + surge), calls the (stubbed) provider, and emits `payment.events`.
- Trip Service/Notification react to `payment.events` (e.g. receipt).
- **Compensation:** if capture fails permanently, emit an event that flags the trip for manual settlement — we don't silently drop money.

---

## 12. Rate Limiting

- At the gateway, per-user and per-IP, protecting `POST /trips` (request ride) and the location-ingest path.
- **Token bucket in Redis via a Lua script** (atomic read-modify-write):
  - Key `rl:{userId}:{route}` holds tokens + last-refill timestamp.
  - Lua refills based on elapsed time, decrements if tokens available, returns allow/deny + retry-after.
- Return `429` with `Retry-After` header when denied.
- Implemented as a Spring `HandlerInterceptor` / filter.

---

## 13. Notifications

- Stateless consumer of `trip.events` and `payment.events`.
- Maps events → user-facing messages (push/SMS/email — stubbed to logs + a `/notifications` inbox endpoint for demoing).
- Demonstrates the pub/sub payoff: adding a new reaction to trip events = a new consumer, zero changes to producers.

---

## 14. Security / Auth (lightweight)

- Auth Service issues **JWTs**; gateway validates them (Spring Security resource server).
- Roles: `RIDER`, `DRIVER`, `ADMIN`.
- WebSocket handshake authenticates via the JWT (query param/header on connect).
- Not a full security build — just enough that requests are attributable (needed for rate limiting and authorship).

---

## 15. Kubernetes / Deployment

**Local:** `docker-compose` brings up Postgres, Redis, Kafka (+ Zookeeper or KRaft), and the app(s).

**K8s:**
- One `Deployment` + `Service` per microservice; `ConfigMap`/`Secret` for config.
- **HPA** on Location & Match services (CPU + custom Kafka-lag metric) — these are the load-sensitive ones.
- Redis, Kafka, Postgres via Helm charts / operators (Bitnami, Strimzi for Kafka).
- `Ingress` (nginx) terminating HTTP + WebSocket (with the right upgrade annotations and sticky sessions for WS).
- Liveness/readiness probes; `PodDisruptionBudget` for the stateful bits.
- Package our services as a **Helm chart** with per-env values.

---

## 16. API Sketch (initial)

```
POST /auth/login                 → JWT
POST /riders/{id}/trips          → request a ride {pickup, drop, carType}   (rate-limited)
GET  /trips/{id}                 → current trip state (from projection)
POST /trips/{id}/cancel          → cancel
WS   /ws/rider                   → trip updates stream
WS   /ws/driver                  → location in, offers out
POST /drivers/{id}/status        → go online/offline
POST /internal/offers/{id}/accept|reject   (driver, via WS or REST)
```

Event schemas live in the shared `:common` module as versioned records (Avro/JSON Schema optional for Kafka).

---

## 17. Build Roadmap (phased — always runnable)

1. **Skeleton + Trip state machine** (in-memory) — domain core, unit-tested transitions.
2. **Postgres + Flyway + event sourcing** for trips (append + fold + projection).
3. **Redis geo + Location Service** — ingest pings, `GEOSEARCH` candidates.
4. **Match Service** — ranking + sequential-offer dispatch with Redis locks.
5. **Kafka** — move trip/payment/notification off in-process calls onto topics.
6. **WebSockets** — real-time driver in / rider out.
7. **Payment saga + rate limiting.**
8. **Notifications** consumer.
9. **Dockerize → docker-compose → K8s + Helm.**
10. **Load test** (simulate N drivers pinging + M ride requests) + metrics dashboards.

Each phase has Testcontainers-backed integration tests so the system is provably working before moving on.

---

## 18. Interview Talking Points (cheat sheet)

- **Matching:** greedy sequential vs. batch broadcast vs. windowed global optimization (Hungarian algorithm); how Redis locks prevent double-assignment; fairness vs. efficiency.
- **Geo:** geohash vs. S2 vs. quadtree; why hot path ≠ analytical store; region sharding.
- **Event sourcing:** why store events not state; projections; optimistic concurrency via `(tripId, seq)`; replay.
- **Kafka:** partition-by-key for per-trip ordering; at-least-once + idempotent consumers; decoupling.
- **WebSockets at scale:** socket-to-node affinity, shared bus for cross-node delivery.
- **Payments:** idempotency keys, saga compensation, never lose money.
- **Reliability:** offer locks expire so crashes self-heal; consumers dedupe.

---

## 19. Open Questions / Decisions to Revisit
- Monolith-first vs. true microservices from day one? (Doc assumes modular monolith → split.)
- Spring StateMachine vs. hand-rolled transition table? (Doc leans hand-rolled for clarity.)
- ETA: haversine approximation vs. real routing engine (OSRM)? (Start approximate.)
- Kafka as event store vs. Postgres `trip_event` as event store + Kafka as bus? (Doc uses Postgres as store of record, Kafka as bus — safest.)
```
