package db

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunAsLeader runs fn only while this process holds the Postgres advisory lock
// `key`. Exactly one replica is leader at a time; if the leader crashes or
// loses its connection, Postgres drops the session lock and a standby takes
// over on its next attempt (fail-over within `retry`).
//
// Used for jobs that must be single-writer (ordered outbox relay, Redis
// reconciliation) while still running N replicas for availability.
func RunAsLeader(ctx context.Context, pool *pgxpool.Pool, key int64, name string, retry time.Duration, fn func(ctx context.Context)) {
	log := slog.With("job", name)
	for ctx.Err() == nil {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			sleep(ctx, retry)
			continue
		}
		var acquired bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil || !acquired {
			conn.Release()
			sleep(ctx, retry)
			continue
		}
		log.Info("became leader")

		leaderCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			fn(leaderCtx)
		}()

		// Keep-alive: if our session dies, so does the lock, so step down.
		ticker := time.NewTicker(retry)
	watch:
		for {
			select {
			case <-ctx.Done():
				break watch
			case <-done:
				break watch
			case <-ticker.C:
				pingCtx, pc := context.WithTimeout(ctx, 2*time.Second)
				err := conn.Ping(pingCtx)
				pc()
				if err != nil {
					log.Warn("lost leader connection, stepping down", "err", err)
					break watch
				}
			}
		}
		ticker.Stop()
		cancel()
		<-done
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
		// Hijack+close rather than returning a possibly-broken conn to the pool.
		_ = conn.Hijack().Close(context.Background())
		log.Info("released leadership")
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
