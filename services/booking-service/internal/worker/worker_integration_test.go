package worker

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"booking-service/internal/db"
	"booking-service/internal/events"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("BOOKING_TEST_DATABASE_URL not set")
	}
	pool, err := db.Connect(context.Background(), url, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestLeaderElectionFailsOver(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var active, maxActive atomic.Int32
	var leaders sync.Map
	job := func(name string) func(context.Context) {
		return func(ctx context.Context) {
			n := active.Add(1)
			defer active.Add(-1)
			for {
				if m := maxActive.Load(); n > m && !maxActive.CompareAndSwap(m, n) {
					continue
				}
				break
			}
			leaders.Store(name, true)
			<-ctx.Done()
		}
	}

	ctxA, killA := context.WithCancel(ctx)
	go RunAsLeaderForTest(ctxA, pool, "A", job("A"))
	time.Sleep(300 * time.Millisecond)
	go RunAsLeaderForTest(ctx, pool, "B", job("B"))
	time.Sleep(300 * time.Millisecond)
	if _, ok := leaders.Load("B"); ok {
		t.Fatal("B must not lead while A holds the lock")
	}

	killA() // leader "crashes"
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := leaders.Load("B"); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, ok := leaders.Load("B"); !ok {
		t.Fatal("standby did not take over")
	}
	if maxActive.Load() != 1 {
		t.Fatalf("split brain: %d leaders at once", maxActive.Load())
	}
}

func RunAsLeaderForTest(ctx context.Context, pool *pgxpool.Pool, name string, fn func(context.Context)) {
	db.RunAsLeader(ctx, pool, 999_001, name, 100*time.Millisecond, fn)
}

type flakyPublisher struct {
	mu       sync.Mutex
	failures int
	seen     map[string]int
}

func (f *flakyPublisher) Publish(_ context.Context, msgs []events.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return errors.New("kafka unavailable")
	}
	for _, m := range msgs {
		f.seen[m.EventID]++
	}
	return nil
}

func (f *flakyPublisher) Close() error { return nil }

func TestOutboxSurvivesBrokerOutage(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1200; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ('r', 'reservation.held', '{}')`); err != nil {
			t.Fatal(err)
		}
	}
	pub := &flakyPublisher{failures: 3, seen: map[string]int{}}
	r := NewRunner(pool, nil, nil, pub, Config{OutboxInterval: 10 * time.Millisecond, OutboxBatchSize: 500, OutboxRetention: time.Hour})

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go r.RelayOutbox(runCtx)
	for runCtx.Err() == nil {
		var left int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&left)
		if left == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.seen) != 1200 {
		t.Fatalf("want all 1200 events delivered after the outage, got %d", len(pub.seen))
	}
}
