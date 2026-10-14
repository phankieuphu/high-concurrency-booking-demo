// Package worker hosts the background loops. Worker replicas are stateless and
// can be scaled/killed freely:
//
//   - hold expiry & payment reconciliation run on EVERY replica
//     (FOR UPDATE SKIP LOCKED / conditional updates make that safe);
//   - outbox relay and Redis resync are single-writer jobs guarded by a
//     Postgres advisory-lock leader election with automatic fail-over.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"booking-service/internal/booking"
	"booking-service/internal/catalog"
	"booking-service/internal/db"
	"booking-service/internal/events"
	"booking-service/internal/metrics"
)

const (
	lockOutboxRelay = 7_242_101
	lockGateResync  = 7_242_102
)

type Config struct {
	ExpiryInterval    time.Duration
	ReconcileInterval time.Duration
	OutboxInterval    time.Duration
	OutboxBatchSize   int
	OutboxRetention   time.Duration
}

type Runner struct {
	pool      *pgxpool.Pool
	svc       *booking.Service
	catalog   *catalog.Catalog
	publisher events.Publisher
	cfg       Config
	log       *slog.Logger
}

func NewRunner(pool *pgxpool.Pool, svc *booking.Service, cat *catalog.Catalog, pub events.Publisher, cfg Config) *Runner {
	return &Runner{pool: pool, svc: svc, catalog: cat, publisher: pub, cfg: cfg, log: slog.With("component", "worker")}
}

// Run blocks until ctx is cancelled and all loops have stopped.
func (r *Runner) Run(ctx context.Context) {
	done := make(chan struct{}, 4)
	run := func(fn func(context.Context)) {
		go func() { fn(ctx); done <- struct{}{} }()
	}
	run(r.expiryLoop)
	run(r.paymentLoop)
	run(func(ctx context.Context) {
		db.RunAsLeader(ctx, r.pool, lockOutboxRelay, "outbox-relay", 2*time.Second, r.RelayOutbox)
	})
	run(func(ctx context.Context) {
		db.RunAsLeader(ctx, r.pool, lockGateResync, "gate-resync", 2*time.Second, r.resyncLoop)
	})
	for i := 0; i < 4; i++ {
		<-done
	}
}

func (r *Runner) expiryLoop(ctx context.Context) {
	every(ctx, r.cfg.ExpiryInterval, func() {
		// Drain in batches so a backlog after an outage clears quickly.
		for ctx.Err() == nil {
			n, err := r.svc.ExpireHolds(ctx, 500)
			if err != nil {
				r.log.Warn("expire holds", "err", err)
				return
			}
			if n > 0 {
				r.log.Info("expired holds", "count", n)
			}
			if n < 500 {
				return
			}
		}
	})
}

func (r *Runner) paymentLoop(ctx context.Context) {
	every(ctx, r.cfg.ExpiryInterval*5, func() {
		if n, err := r.svc.ReconcilePayments(ctx, 100); err != nil {
			r.log.Warn("reconcile payments", "err", err)
		} else if n > 0 {
			r.log.Info("reconciled payments", "count", n)
		}
	})
}

func (r *Runner) resyncLoop(ctx context.Context) {
	every(ctx, r.cfg.ReconcileInterval, func() {
		ids, err := r.catalog.AllActiveTierIDs(ctx)
		if err != nil {
			r.log.Warn("list tiers", "err", err)
			return
		}
		if err := r.svc.SyncGate(ctx, ids, "periodic"); err != nil {
			r.log.Warn("gate resync", "err", err)
		}
	})
}

// RelayOutbox publishes committed outbox rows in id order. Delivery is
// at-least-once: a crash after Publish but before the UPDATE re-sends the
// batch, which consumers absorb via event_id de-duplication. While Kafka is
// down rows simply accumulate; nothing is lost and bookings keep working.
func (r *Runner) RelayOutbox(ctx context.Context) {
	backoff := r.cfg.OutboxInterval
	lastCleanup := time.Time{}
	for ctx.Err() == nil {
		n, err := r.relayBatch(ctx)
		switch {
		case err != nil:
			r.log.Warn("outbox relay", "err", err)
			backoff = min(backoff*2, 10*time.Second)
		case n == r.cfg.OutboxBatchSize:
			backoff = 0 // backlog: keep draining
		default:
			backoff = r.cfg.OutboxInterval
		}
		if time.Since(lastCleanup) > time.Minute {
			lastCleanup = time.Now()
			_, _ = r.pool.Exec(ctx, `DELETE FROM outbox WHERE published_at < now() - make_interval(secs => $1)`,
				r.cfg.OutboxRetention.Seconds())
			var backlog int
			if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&backlog); err == nil {
				metrics.OutboxBacklog.Set(float64(backlog))
			}
		}
		if backoff > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
		}
	}
}

func (r *Runner) relayBatch(ctx context.Context) (int, error) {
	var ids []int64
	var msgs []events.Message
	rows, err := r.pool.Query(ctx, `
		SELECT id, event_id::text, aggregate_id, event_type, payload, created_at
		  FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, r.cfg.OutboxBatchSize)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id int64
		var m events.Message
		if err := rows.Scan(&id, &m.EventID, &m.AggregateID, &m.Type, &m.Data, &m.OccurredAt); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	if err := r.publisher.Publish(ctx, msgs); err != nil {
		return 0, err
	}
	_, err = r.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	metrics.OutboxBacklog.Sub(float64(len(ids)))
	return len(ids), err
}

func every(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		fn()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
