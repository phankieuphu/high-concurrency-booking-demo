# 06 · Operations Runbook: Sale Day, Observability, Load Testing

## 1. Sale-day timeline

| When | Action |
|---|---|
| T-7 d | Full-scale load test in staging (k6, §3) at **1.5×** expected peak; run the chaos drills from doc 05 |
| T-24 h | Freeze deploys. Confirm cloud quotas (vCPU, LB LCUs, IPs). Warm up the CDN. Check the DB has ≥ 50 % CPU headroom |
| T-2 h | Create the event (`POST /v1/admin/events`), which warms the Redis stock gate. Set the per-event admission rate: `SET wr:{EVENT}:rate 2000` |
| T-1 h | **Pre-scale**: gateway → 60 pods, booking-api → 15, Kafka consumers → partitions. Raise HPA `minReplicas` so it can't scale in. Pre-warm the LB if the provider needs it |
| T-15 m | War room. Dashboards up. Run a synthetic purchase end to end |
| T0 | Sale opens. Watch the **golden signals** (§2). Tune the admission rate up while DB CPU < 60 % and reserve p99 < 1 s |
| Sell-out | Inventory reaches 0 and the gate rejects in O(1). Optionally set rate high to drain the queue quickly ("sold out" answers are cheap) |
| T+1 h | Let holds expire / payments reconcile. Run invariant checks (§4). Scale down |

## 2. Golden signals & alerts

| Signal | Metric | Alert |
|---|---|---|
| Reserve latency | `http_request_duration_seconds{route="POST /v1/reservations"}` p99 | > 1.5 s for 2 min |
| Shed rate | `http_requests_shed_total`, `gateway_shed_total` | sustained > 5 % of requests |
| Outcomes | `booking_reserve_outcomes_total{outcome}` | `error` > 0.5 %; `contention` rising |
| Gate decisions | `booking_gate_decisions_total{decision="bypassed"}` | > 0 means Redis degraded |
| Breakers | `circuit_breaker_state`, `gateway_breaker_open` | any breaker open > 30 s |
| Outbox backlog | `outbox_unpublished_events` | > 10k or growing for 5 min (Kafka problem) |
| Consumer lag | Kafka lag `notification-worker` | > 50k |
| DLQ | messages on `booking.events.dlq` | > 0 → investigate |
| DB | CPU, active connections, lock waits (`log_lock_waits`), replication lag | CPU > 75 %; repl lag > 10 s |
| Redis | CPU per shard, memory, evictions | **evictions > 0 is a P1** (must be `noeviction`) |
| Queue | `wr:{E}:seq - wr:{E}:admitted` | information: queue length |

Tracing: propagate `x-request-id` (gateway → API, logged on both sides). In production, add OpenTelemetry auto-instrumentation (Node + `otelhttp`/`otelpgx` in Go) exporting to X-Ray / Cloud Trace / Application Insights.

## 3. Load testing

| Tool | Use |
|---|---|
| `go run ./cmd/stampede` (in `services/booking-service`) | Functional flash sale against any gateway. Prints outcome counts and latency. Good for local/compose and CI smoke |
| `loadtest/flash-sale.k6.js` | Realistic arrival-rate model with thresholds. Run distributed (k6 Operator / Grafana Cloud k6) for 1 M users |

Local result (laptop-class container, single gateway process, Postgres and Redis on the same host):

```
3000 users in 20.967s           # 1,000-ticket tier, 8 shards, admission 400/s
  confirmed              1000
  sold_out               1999
  auth_0                 1      # client-side connection error at 1,500 parallel sockets
reserve latency  p50=34ms p95=1.233s p99=1.61s
→ DB check: available=0, CONFIRMED=1000, charges=1000, max per user=1, outbox backlog=0
```

The p95 reflects a single Node process and Postgres sharing one small machine. In the cloud, sizing comes from doc 01.

## 4. Invariant checks (must return zero rows)

```sql
-- I1: inventory conservation per tier: shelf + active reservations = total
SELECT t.id, t.total, s.avail, r.active
FROM ticket_tiers t
JOIN (SELECT tier_id, sum(available) avail FROM inventory_shards GROUP BY tier_id) s ON s.tier_id = t.id
LEFT JOIN (SELECT tier_id, sum(quantity) active FROM reservations
           WHERE status IN ('HELD','PAYMENT_PENDING','CONFIRMED') GROUP BY tier_id) r ON r.tier_id = t.id
WHERE s.avail + coalesce(r.active, 0) <> t.total;

-- I2: nobody above the per-user limit
SELECT q.* FROM user_event_quota q JOIN events e ON e.id = q.event_id WHERE q.reserved > e.max_per_user;

-- I3: quota matches active reservations
SELECT q.user_id, q.event_id, q.reserved, coalesce(sum(r.quantity), 0) AS active
FROM user_event_quota q
LEFT JOIN reservations r ON r.user_id = q.user_id AND r.event_id = q.event_id
     AND r.status IN ('HELD','PAYMENT_PENDING','CONFIRMED')
GROUP BY 1, 2, 3 HAVING q.reserved <> coalesce(sum(r.quantity), 0);

-- I4: every charge has a confirmed booking (and vice versa)
SELECT p.idempotency_key FROM mock_payment_provider p
LEFT JOIN reservations r ON r.id::text = p.idempotency_key
WHERE p.status = 'CHARGED' AND (r.status IS DISTINCT FROM 'CONFIRMED');

-- I5: nothing stuck
SELECT id, status, expires_at FROM reservations
WHERE status IN ('HELD','PAYMENT_PENDING') AND expires_at < now() - interval '5 minutes';
```

## 5. Common operations

| Task | How |
|---|---|
| Slow the queue (DB hot) | `redis-cli SET wr:{EVENT}:rate 500` (takes effect within 1 s) |
| Pause admissions | `SET wr:{EVENT}:rate 0` |
| Force stock resync | Restart is not needed. Wait `RECONCILE_INTERVAL`, or `DEL stock:{TIER}` (the next request bypasses to the DB; the resync job rewrites the key) |
| Replay DLQ | Consume `booking.events.dlq`, fix the cause, re-produce to `booking.events` (dedupe makes this safe) |
| Stuck `PAYMENT_PENDING` | Check payment breaker and PSP status; the reconciler retries every ~10 s |
| Rotate secrets | `JWT_SECRET` / `QUEUE_SECRET`: support two keys during rotation (verify old + new, sign with new) |
