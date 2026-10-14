# 02 · Architecture

## 1. Component overview

```mermaid
flowchart LR
    U["Users<br/>web / mobile"] --> CDN["CDN + WAF + bot protection<br/>static assets, cached catalog"]
    CDN --> LB["Global / regional L7 load balancer"]
    LB --> GW

    subgraph K8S["Kubernetes (3 AZs)"]
        GW["gateway · Node.js<br/>JWT auth · rate limit · load shedding<br/>waiting room · circuit breaker · micro-cache"]
        API["booking-api · Go<br/>catalog · reserve · confirm · cancel"]
        WK["booking-worker · Go<br/>hold expiry · payment reconcile<br/>outbox relay* · gate resync*"]
        NW["notification-worker · Node.js<br/>dedupe · retry · DLQ"]
    end

    GW -->|"waiting room, rate limits"| R
    GW -->|"HTTP + internal token"| API
    API -->|"stock gate (Lua)"| R[("Redis Cluster")]
    API --> PB["PgBouncer / RDS Proxy"]
    PB --> PG[("PostgreSQL primary")]
    PG -. "sync replication" .-> SB[("Standby (other AZ)")]
    PG -. "async" .-> RR[("Read replicas")]
    API -. "catalog reads" .-> RR
    API -->|"charge (idempotency key)"| PSP["Payment provider"]
    WK --> PG
    WK --> R
    WK -->|"acks=all"| KF[("Kafka")]
    KF --> NW
    NW --> PG

    classDef store fill:#eef,stroke:#669;
    class R,PG,SB,RR,KF store;
```

\* leader-elected (one active replica; the others are hot standbys).

| Component | Language | Stateless? | Scales on | Responsibility |
|---|---|---|---|---|
| `services/gateway` | Node.js (Fastify) | yes | CPU / rps | Edge concerns: authN, per-user rate limits, load shedding, **virtual waiting room**, circuit breaker to booking-api, 1 s micro-cache + request collapsing for catalog |
| `services/booking-service` `api` | Go | yes | CPU (bounded by DB) | Business rules and all **write transactions**; Redis stock gate; idempotency |
| `services/booking-service` `worker` | Go | yes | fixed 2–3 | Hold expiry, payment reconciliation, transactional-outbox relay, Redis↔Postgres reconciliation |
| `services/notification-worker` | Node.js | yes | Kafka lag (KEDA) | Consumes domain events, exactly-once effect via de-duplication, retries + DLQ |
| PostgreSQL | — | — | vertical + replicas | **Source of truth** for inventory, reservations, quotas, outbox |
| Redis | — | — | shards | Fast, *disposable* state: stock gate, waiting room, rate limits |
| Kafka | — | — | partitions | Durable async fan-out of domain events |

> The existing `user-service` (root of the repo) issues HS256 JWTs with a `user_id` claim. The gateway verifies exactly that token format, so it plugs in unchanged.

## 2. Request flows

### 2.1 Sale opening: waiting room → admission

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant GW as Gateway
    participant R as Redis
    participant L as Admitter (leader gateway)

    U->>GW: POST /queue/42/join (JWT)
    GW->>R: EVAL join.lua (HGET user / INCR seq / HSET)
    R-->>GW: seq = 731,204 (same seq if user joins twice)
    GW-->>U: signed ticket {sub, eid, seq} + position

    loop every second
        L->>R: EVAL advance.lua: admitted = min(admitted + rate, seq)
        Note over L: rate = 0 while booking-api breaker is OPEN (backpressure)
    end

    loop every poll_after_sec (3–10 s)
        U->>GW: GET /queue/42/status (ticket)
        Note over GW: verify HMAC + compare with locally cached watermark (refreshed 1×/s)<br/>→ no Redis call per request
        GW-->>U: waiting, position N  |  admitted + admission_token (10 min)
    end
```

### 2.2 Reserve (the hot path)

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant GW as Gateway
    participant API as booking-api
    participant R as Redis
    participant PG as Postgres

    U->>GW: POST /reservations (JWT, Idempotency-Key, admission token)
    GW->>GW: auth · token-bucket rate limit · admission check · in-flight limit
    GW->>API: POST /v1/reservations (X-User-Id, Idempotency-Key)
    API->>API: validate · cached catalog lookup (singleflight)
    API->>PG: SELECT by (user_id, idempotency_key) — replay?
    API->>R: EVAL acquire.lua (check-and-DECRBY stock)
    alt sold out in Redis
        R-->>API: -1
        API-->>U: 409 sold out (Postgres never touched)
    else admitted / Redis down (bypass)
        API->>API: bulkhead (bounded DB concurrency, else 503)
        API->>PG: BEGIN
        API->>PG: UPSERT user_event_quota … WHERE reserved+n <= max
        API->>PG: UPDATE inventory_shards … (random shard, FOR UPDATE SKIP LOCKED)
        API->>PG: INSERT reservations (HELD, expires_at = now()+10m)
        API->>PG: INSERT outbox ('reservation.held')
        API->>PG: COMMIT
        alt DB step failed (non sold-out)
            API->>R: INCRBY stock (compensate)
        end
        API-->>U: 201 HELD
    end
```

### 2.3 Confirm (payment)

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant API as booking-api
    participant PG as Postgres
    participant PSP as Payment provider
    participant W as worker (reconciler)

    U->>API: POST /reservations/{id}/confirm
    API->>PG: UPDATE … SET PAYMENT_PENDING WHERE status='HELD' AND expires_at > now()
    Note over API,PG: wins or loses the race with the expiry worker atomically
    API->>PSP: charge(idempotency_key = reservation id) [timeout · retry · breaker]
    alt charged
        API->>PG: UPDATE … CONFIRMED WHERE status='PAYMENT_PENDING' + outbox
        API-->>U: 200 CONFIRMED
    else declined
        API->>PG: PAYMENT_FAILED + release inventory + outbox
        API-->>U: 402
    else provider never called (breaker open)
        API->>PG: back to HELD
        API-->>U: 503 Retry-After
    else outcome unknown (timeout)
        API-->>U: 202 processing
        W->>PSP: lookup(reservation id) after payment window
        W->>PG: CONFIRMED or PAYMENT_FAILED (conditional update)
    end
```

### 2.4 Events: transactional outbox → Kafka → consumers

```mermaid
flowchart LR
    TX["Business TX<br/>(state change + outbox row,<br/>same commit)"] --> OB[("outbox")]
    OB -->|"leader relay: SELECT … ORDER BY id<br/>publish acks=all → mark published"| K[("Kafka<br/>key = reservation id")]
    K --> C["notification-worker"]
    C -->|"BEGIN; INSERT processed_events ON CONFLICT DO NOTHING;<br/>INSERT notifications; COMMIT"| PG[("Postgres")]
    C -->|"after N failures"| DLQ[("booking.events.dlq")]
```

## 3. Design principles

1. **The database is the only source of truth for invariants.** Inventory can't go negative (`CHECK`), idempotency keys are unique (`UNIQUE`), and state transitions are conditional `UPDATE`s. Every other layer (Redis gate, gateway checks, waiting room) only improves efficiency. If one of them fails, correctness still holds.
2. **Shed load early and cheaply.** CDN → waiting room → rate limit → in-flight limit → Redis gate → bulkhead → DB. Each layer is cheaper than the next and rejects with a *fast* `429/503 + Retry-After` instead of queueing.
3. **Every write is idempotent.** Client `Idempotency-Key`, reservation id as payment key, event id for consumers. Retrying is always safe, so every layer may retry.
4. **No distributed transactions.** Use local transactions plus the outbox, compensations (Redis release, refund), and reconcilers that converge state.
5. **Short transactions, no lock waits on hot rows.** Inventory is sharded across rows and taken with `SKIP LOCKED`. Lock order is fixed (quota → shards) to rule out deadlocks.
6. **Stateless compute, managed state.** Pods can be killed at any moment. Leadership for single-writer jobs uses DB advisory locks, which are released automatically if the holder dies.
7. **Degrade, don't die.** Redis down → DB-only mode behind a bulkhead. Kafka down → events buffer in the outbox. Payment provider down → holds are kept, users retry. Read replica down → read from primary.

## 4. Code map

| Concern | File |
|---|---|
| Schema & DB-level invariants | `services/booking-service/internal/db/migrations/0001_init.sql` |
| Reserve / confirm / cancel / expire / reconcile | `services/booking-service/internal/booking/service.go` |
| Redis stock gate (Lua) | `services/booking-service/internal/inventory/gate.go` |
| Catalog cache + singleflight | `services/booking-service/internal/catalog/catalog.go` |
| Payment client (idempotency, retry, breaker) | `services/booking-service/internal/payment/payment.go` |
| Circuit breaker / retry / bulkhead | `services/booking-service/internal/resilience/` |
| Advisory-lock leader election, tx retry | `services/booking-service/internal/db/` |
| Outbox relay & background loops | `services/booking-service/internal/worker/worker.go` |
| Load shedding, graceful drain, error mapping | `services/booking-service/internal/httpapi/server.go` |
| Waiting room | `services/gateway/src/lib/waitingRoom.js` |
| Rate limiting (Redis + local fallback) | `services/gateway/src/lib/rateLimiter.js` |
| Upstream client + breaker | `services/gateway/src/lib/upstream.js`, `circuitBreaker.js` |
| Consumer dedupe / retry / DLQ | `services/notification-worker/src/handler.js` |
| Concurrency proofs (tests) | `services/booking-service/internal/booking/service_integration_test.go` |
