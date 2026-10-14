# High-Concurrency Booking Demo

A ticket-booking platform designed for **10 M users with 1 M concurrent at peak** (flash sales). The business logic is deliberately small; the engineering goes into:

* **correctness under contention:** no oversell, no double-charge, no duplicate bookings, proven with parallel integration tests against real Postgres + Redis;
* **stability under overload:** waiting room, rate limits, load shedding, bulkheads, circuit breakers, backpressure;
* **graceful degradation & failover:** Redis, Kafka, payment-provider, DB-failover and pod/AZ loss all keep the system correct;
* **a cloud deployment plan** for AWS, GCP and Azure, with diagrams and sizing.

```mermaid
flowchart LR
    U["1M users"] --> CDN["CDN + WAF"] --> GW["gateway (Node.js)<br/>auth · rate limit · waiting room"]
    GW --> API["booking-api (Go)"]
    GW --> R[("Redis")]
    API --> R
    API --> PG[("PostgreSQL<br/>source of truth")]
    WK["booking-worker (Go)<br/>expiry · reconcile · outbox"] --> PG
    WK --> K[("Kafka")] --> NW["notification-worker (Node.js)"]
```

## Repository layout

| Path | What |
|---|---|
| [`services/booking-service`](services/booking-service) | **Go**. Booking API + background workers (one binary: `api`, `worker`, `migrate`, `seed`) + `stampede` load generator |
| [`services/gateway`](services/gateway) | **Node.js**. Edge gateway: JWT auth, token-bucket rate limiting, load shedding, **virtual waiting room**, circuit-broken proxy |
| [`services/notification-worker`](services/notification-worker) | **Node.js**. Kafka consumer with exactly-once effect (dedupe), retries and DLQ |
| [`deploy/docker-compose.yml`](deploy/docker-compose.yml) | Full local stack: 2× gateway behind nginx, 2× API, 2× workers, Postgres + PgBouncer, Redis, Kafka |
| [`deploy/k8s`](deploy/k8s) | Production-shaped manifests: HPA, PDB, topology spread, NetworkPolicy, KEDA, migration Job |
| [`docs/`](docs/README.md) | Capacity planning, architecture, data model, **race conditions**, **resilience/failover**, runbook, **AWS / GCP / Azure** |
| [`loadtest/`](loadtest) | k6 flash-sale scenario |
| `cmd/`, `internal/`, `pkg/` (root) | Pre-existing **user-service** (Go): users, roles, permissions, JWT issuing. The gateway accepts its tokens as-is. [Guide](docs/user-service.md) |

## Quick start (Docker)

```bash
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml --profile tools run --rm seed          # creates "Flash Sale Live 2026" (event 1)
docker compose -f deploy/docker-compose.yml --profile tools run --rm stampede -users 3000 -event 1 -tier 1
```

Manual walk-through against `http://localhost:8080`:

```bash
TOKEN=$(curl -s -XPOST localhost:8080/auth/dev-token -H 'content-type: application/json' -d '{"user_id":"alice"}' | jq -r .token)
TICKET=$(curl -s -XPOST localhost:8080/api/v1/queue/1/join -H "authorization: Bearer $TOKEN" | jq -r .ticket)
ADMIT=$(curl -s localhost:8080/api/v1/queue/1/status -H "authorization: Bearer $TOKEN" -H "x-queue-ticket: $TICKET" | jq -r .admission_token)
RES=$(curl -s -XPOST localhost:8080/api/v1/reservations -H "authorization: Bearer $TOKEN" \
  -H "idempotency-key: $(uuidgen)" -H "x-admission-token: $ADMIT" -H 'content-type: application/json' \
  -d '{"event_id":1,"tier_id":1,"quantity":2}' | jq -r .id)
curl -s -XPOST localhost:8080/api/v1/reservations/$RES/confirm -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"payment_token":"tok_visa"}'      # "tok_decline" to test declines
```

## API (via gateway)

| Method & path | Notes |
|---|---|
| `GET /api/v1/events`, `GET /api/v1/events/:id` | Cacheable; includes live availability |
| `POST /api/v1/queue/:eventId/join` | Returns signed queue ticket + position (idempotent per user) |
| `GET /api/v1/queue/:eventId/status` | `X-Queue-Ticket` → `waiting` (position, ETA, poll interval) or `admitted` + `admission_token` |
| `POST /api/v1/reservations` | Requires `Idempotency-Key`; `X-Admission-Token` for waiting-room events. `201` held · `200` replay · `409` sold out · `422` limit · `429/503` retry |
| `POST /api/v1/reservations/:id/confirm` | `{"payment_token"}` → `200` confirmed · `202` processing · `402` declined · `409` expired · `503` retry |
| `POST /api/v1/reservations/:id/cancel` | Idempotent |
| `GET /api/v1/reservations/:id` | |

## Tests

```bash
# Go: unit + integration (needs Postgres 16 + Redis; skipped otherwise)
cd services/booking-service
BOOKING_TEST_DATABASE_URL=postgres://booking:booking@localhost:5432/booking?sslmode=disable \
BOOKING_TEST_REDIS_ADDR=localhost:6379 go test -race -p 1 ./...

# Node
cd services/gateway && TEST_REDIS_URL=redis://localhost:6379/15 npm test
cd services/notification-worker && TEST_DATABASE_URL=postgres://booking:booking@localhost:5432/booking npm test
```

Highlights (all with real parallelism):

| Test | Proves |
|---|---|
| `TestNoOversellUnderContention` | 2,000 simultaneous buyers, 300 tickets → exactly 300 holds, with Redis **and with Redis down** |
| `TestPerUserLimitUnderParallelRequests` | 40 parallel requests from one user → exactly `max_per_user` succeed |
| `TestIdempotentRetriesCreateOneReservation` | 50 identical concurrent retries → 1 reservation, stock decremented once |
| `TestConfirmRacingExpiry` | payments racing the expiry sweepers → each booking ends in exactly one state; charges == confirmations |
| `TestPaymentUnknownOutcomeIsReconciled` | lost PSP responses are resolved correctly by the reconciler |
| `TestGateDriftIsRepaired` | a wildly wrong Redis counter cannot cause oversell and is auto-corrected |
| `TestLeaderElectionFailsOver` / `TestOutboxSurvivesBrokerOutage` | single active relay with failover; no events lost while Kafka is down |
| gateway tests | FIFO/idempotent queue join under stampede, single admitter, backpressure, rate limit fallback, load shedding, request collapsing |
| notification-worker tests | 20 concurrent duplicate deliveries → exactly one notification; DLQ semantics |

Start with **[docs/README.md](docs/README.md)**.
