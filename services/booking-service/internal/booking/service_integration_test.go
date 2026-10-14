package booking_test

// Integration tests against real Postgres + Redis. They prove the concurrency
// guarantees under genuine parallel load rather than mocking them away.
//
//	BOOKING_TEST_DATABASE_URL=postgres://booking:booking@localhost:5432/booking?sslmode=disable \
//	BOOKING_TEST_REDIS_ADDR=localhost:6379 go test ./... -race
//
// Tests are skipped when the variables are not set.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"booking-service/internal/booking"
	"booking-service/internal/catalog"
	"booking-service/internal/db"
	"booking-service/internal/inventory"
	"booking-service/internal/payment"
	"booking-service/internal/resilience"
)

type env struct {
	pool    *pgxpool.Pool
	rdb     *redis.Client
	cat     *catalog.Catalog
	gate    *inventory.Gate
	svc     *booking.Service
	payProv payment.Provider
}

type envOpts struct {
	redisDown     bool
	holdTTL       time.Duration
	paymentWindow time.Duration
	provider      payment.Provider
	bulkhead      int
}

func setup(t *testing.T, o envOpts) *env {
	t.Helper()
	url := os.Getenv("BOOKING_TEST_DATABASE_URL")
	redisAddr := os.Getenv("BOOKING_TEST_REDIS_ADDR")
	if url == "" || redisAddr == "" {
		t.Skip("BOOKING_TEST_DATABASE_URL / BOOKING_TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url, 80)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE outbox, reservations, user_event_quota, inventory_shards, ticket_tiers,
		events, mock_payment_provider RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}

	if o.redisDown {
		redisAddr = "127.0.0.1:1" // nothing listens here: simulates a Redis outage
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond, PoolSize: 200, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	if !o.redisDown {
		rdb.FlushDB(ctx)
	}

	if o.holdTTL == 0 {
		o.holdTTL = 10 * time.Minute
	}
	if o.paymentWindow == 0 {
		o.paymentWindow = 2 * time.Minute
	}
	if o.provider == nil {
		o.provider = &payment.MockProvider{Pool: pool}
	}
	if o.bulkhead == 0 {
		o.bulkhead = 64
	}
	cat := catalog.New(pool, nil, time.Minute)
	gate := inventory.NewGate(rdb, resilience.NewBreaker("redis", 5, time.Second))
	payments := payment.NewClient(o.provider, resilience.NewBreaker("pay", 100, time.Second), time.Second)
	svc := booking.NewService(pool, cat, gate, payments, resilience.NewBulkhead(o.bulkhead, 5*time.Second),
		booking.Options{HoldTTL: o.holdTTL, PaymentWindow: o.paymentWindow})
	return &env{pool: pool, rdb: rdb, cat: cat, gate: gate, svc: svc, payProv: o.provider}
}

func (e *env) createEvent(t *testing.T, total, shards, maxPerUser int) (*catalog.Event, int64) {
	t.Helper()
	ctx := context.Background()
	ev, err := e.cat.Create(ctx, catalog.NewEvent{
		Name: "test", Venue: "v", StartsAt: time.Now().Add(time.Hour), MaxPerUser: maxPerUser,
		Tiers: []catalog.NewTier{{Name: "GA", PriceCents: 1000, Total: total, Shards: shards}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tierID := ev.Tiers[0].ID
	_ = e.svc.SyncGate(ctx, []int64{tierID}, "test")
	return ev, tierID
}

func (e *env) dbAvailable(t *testing.T, tierID int64) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT sum(available) FROM inventory_shards WHERE tier_id = $1`, tierID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) activeQty(t *testing.T, tierID int64) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), `SELECT coalesce(sum(quantity), 0) FROM reservations
		WHERE tier_id = $1 AND status IN ('HELD', 'PAYMENT_PENDING', 'CONFIRMED')`, tierID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The core invariant: every ticket is either on the shelf or in exactly one
// active reservation. Never both, never neither.
func (e *env) assertConserved(t *testing.T, tierID int64, total int) {
	t.Helper()
	avail, active := e.dbAvailable(t, tierID), e.activeQty(t, tierID)
	if avail+active != total {
		t.Fatalf("inventory not conserved: available=%d + active=%d != total=%d", avail, active, total)
	}
	if avail < 0 {
		t.Fatalf("negative inventory: %d", avail)
	}
}

func TestNoOversellUnderContention(t *testing.T) {
	for _, tc := range []struct {
		name      string
		redisDown bool
	}{
		{"redis gate healthy", false},
		{"redis down / degraded mode", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := setup(t, envOpts{redisDown: tc.redisDown})
			const total, buyers = 300, 2000
			ev, tierID := e.createEvent(t, total, 8, 4)

			var held, soldOut, other, retries atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < buyers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start // release everyone at once
					var err error
					// Behave like a real client: retry 503-style answers with
					// the SAME idempotency key.
					for try := 0; try < 10; try++ {
						_, _, err = e.svc.Reserve(context.Background(), booking.ReserveRequest{
							UserID: fmt.Sprintf("user-%d", i), IdempotencyKey: "k", EventID: ev.ID, TierID: tierID, Quantity: 1,
						})
						if !errors.Is(err, booking.ErrInventoryContention) && !errors.Is(err, booking.ErrOverloaded) {
							break
						}
						retries.Add(1)
						time.Sleep(time.Duration(10*(try+1)) * time.Millisecond)
					}
					switch {
					case err == nil:
						held.Add(1)
					case errors.Is(err, booking.ErrSoldOut):
						soldOut.Add(1)
					default:
						other.Add(1)
						t.Logf("unexpected error: %v", err)
					}
				}(i)
			}
			close(start)
			wg.Wait()

			t.Logf("held=%d sold_out=%d other=%d client_retries=%d", held.Load(), soldOut.Load(), other.Load(), retries.Load())
			if held.Load() != total {
				t.Fatalf("expected exactly %d successful holds, got %d", total, held.Load())
			}
			if other.Load() != 0 {
				t.Fatalf("%d requests failed with unexpected errors", other.Load())
			}
			if got := e.dbAvailable(t, tierID); got != 0 {
				t.Fatalf("expected 0 available, got %d", got)
			}
			e.assertConserved(t, tierID, total)
			if !tc.redisDown {
				if v, _ := e.rdb.Get(context.Background(), inventory.StockKey(tierID)).Int(); v != 0 {
					t.Fatalf("redis stock should be 0, got %d", v)
				}
			}
		})
	}
}

func TestPerUserLimitUnderParallelRequests(t *testing.T) {
	e := setup(t, envOpts{})
	ev, tierID := e.createEvent(t, 1000, 4, 4)

	var ok, limited atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Same user, different idempotency keys: a bot opening 40 tabs.
			_, _, err := e.svc.Reserve(context.Background(), booking.ReserveRequest{
				UserID: "bot", IdempotencyKey: fmt.Sprintf("tab-%d", i), EventID: ev.ID, TierID: tierID, Quantity: 1,
			})
			if err == nil {
				ok.Add(1)
			} else if errors.Is(err, booking.ErrQuotaExceeded) {
				limited.Add(1)
			} else {
				t.Errorf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 4 || limited.Load() != 36 {
		t.Fatalf("want 4 ok / 36 limited, got %d / %d", ok.Load(), limited.Load())
	}
	e.assertConserved(t, tierID, 1000)
	assertRedisMatchesDB(t, e, tierID)
}

func TestIdempotentRetriesCreateOneReservation(t *testing.T) {
	e := setup(t, envOpts{})
	ev, tierID := e.createEvent(t, 100, 2, 4)

	ids := make(chan string, 50)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A flaky mobile network retrying the same request 50 times.
			r, _, err := e.svc.Reserve(context.Background(), booking.ReserveRequest{
				UserID: "alice", IdempotencyKey: "same-key", EventID: ev.ID, TierID: tierID, Quantity: 2,
			})
			if err != nil {
				t.Errorf("retry failed: %v", err)
				return
			}
			ids <- r.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("idempotency broken: got %s and %s", first, id)
		}
	}
	var count int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM reservations WHERE user_id = 'alice'`).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 reservation, got %d", count)
	}
	if got := e.dbAvailable(t, tierID); got != 98 {
		t.Fatalf("inventory decremented more than once: available=%d", got)
	}
	assertRedisMatchesDB(t, e, tierID)
}

func TestConfirmRacingExpiry(t *testing.T) {
	e := setup(t, envOpts{})
	const total, n = 200, 100
	ev, tierID := e.createEvent(t, total, 4, 4)
	ctx := context.Background()

	var res []*booking.Reservation
	for i := 0; i < n; i++ {
		r, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{
			UserID: fmt.Sprintf("u%d", i), IdempotencyKey: "k", EventID: ev.ID, TierID: tierID, Quantity: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, r)
	}

	// Put every hold's deadline ~100ms in the future, then let users pay at
	// random moments around that deadline while two expiry workers sweep.
	// Each reservation must end in exactly one of CONFIRMED or EXPIRED, with
	// inventory accounted for exactly once.
	if _, err := e.pool.Exec(ctx, `UPDATE reservations SET expires_at = now() + interval '100 milliseconds'`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := e.svc.ExpireHolds(ctx, 50); err != nil {
						t.Errorf("expire: %v", err)
					}
				}
			}
		}()
	}
	var confirmed, rejected atomic.Int64
	var cw sync.WaitGroup
	for _, r := range res {
		cw.Add(1)
		go func(r *booking.Reservation, delay time.Duration) {
			defer cw.Done()
			time.Sleep(delay)
			_, err := e.svc.Confirm(ctx, r.UserID, r.ID, "tok_ok")
			switch {
			case err == nil:
				confirmed.Add(1)
			case errors.Is(err, booking.ErrReservationExpired):
				rejected.Add(1)
			default:
				t.Errorf("confirm: %v", err)
			}
		}(r, time.Duration(rand.IntN(200))*time.Millisecond)
	}
	cw.Wait()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	for {
		if n, _ := e.svc.ExpireHolds(ctx, 500); n == 0 {
			break
		}
	}

	t.Logf("confirmed=%d rejected(expired)=%d", confirmed.Load(), rejected.Load())
	if confirmed.Load() == 0 || rejected.Load() == 0 {
		t.Fatalf("test did not exercise the race (confirmed=%d expired=%d)", confirmed.Load(), rejected.Load())
	}
	var nConfirmed, nExpired, nOther int
	_ = e.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status = 'CONFIRMED'),
		count(*) FILTER (WHERE status = 'EXPIRED'),
		count(*) FILTER (WHERE status NOT IN ('CONFIRMED', 'EXPIRED'))
		FROM reservations`).Scan(&nConfirmed, &nExpired, &nOther)
	if nConfirmed != int(confirmed.Load()) || nExpired != int(rejected.Load()) || nOther != 0 {
		t.Fatalf("db: confirmed=%d expired=%d other=%d; callers: confirmed=%d rejected=%d",
			nConfirmed, nExpired, nOther, confirmed.Load(), rejected.Load())
	}
	// No charge without a confirmed booking (no money taken for an expired hold).
	var charged int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM mock_payment_provider WHERE status = 'CHARGED'`).Scan(&charged)
	if charged != nConfirmed {
		t.Fatalf("charged=%d but confirmed=%d", charged, nConfirmed)
	}
	e.assertConserved(t, tierID, total)
	assertRedisMatchesDB(t, e, tierID)
}

// lostResponseProvider charges the card but "loses" the response, or fails
// before charging, simulating network timeouts to a PSP.
type lostResponseProvider struct {
	*payment.MockProvider
	chargeThenFail bool
}

func (p *lostResponseProvider) Charge(ctx context.Context, key string, amount int64, token string) (string, error) {
	if p.chargeThenFail {
		_, _ = p.MockProvider.Charge(ctx, key, amount, token)
	}
	return "", errors.New("timeout talking to PSP")
}

func TestPaymentUnknownOutcomeIsReconciled(t *testing.T) {
	for _, tc := range []struct {
		name           string
		chargeThenFail bool
		want           booking.Status
	}{
		{"charged but response lost -> confirmed", true, booking.StatusConfirmed},
		{"never charged -> failed and released", false, booking.StatusPaymentFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := os.Getenv("BOOKING_TEST_DATABASE_URL")
			if url == "" {
				t.Skip("no db")
			}
			pool, err := db.Connect(context.Background(), url, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			prov := &lostResponseProvider{MockProvider: &payment.MockProvider{Pool: pool}, chargeThenFail: tc.chargeThenFail}
			e := setup(t, envOpts{provider: prov, paymentWindow: 200 * time.Millisecond})
			ev, tierID := e.createEvent(t, 10, 1, 4)
			ctx := context.Background()

			r, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{UserID: "bob", IdempotencyKey: "k", EventID: ev.ID, TierID: tierID, Quantity: 2})
			if err != nil {
				t.Fatal(err)
			}
			got, err := e.svc.Confirm(ctx, "bob", r.ID, "tok_ok")
			if !errors.Is(err, booking.ErrPaymentInProgress) || got.Status != booking.StatusPaymentPending {
				t.Fatalf("want PAYMENT_PENDING/in-progress, got %v %v", got.Status, err)
			}
			// A retry by the user must not start a second charge.
			if _, err := e.svc.Confirm(ctx, "bob", r.ID, "tok_ok"); !errors.Is(err, booking.ErrPaymentInProgress) {
				t.Fatalf("retry during pending: %v", err)
			}

			time.Sleep(250 * time.Millisecond)
			if _, err := e.svc.ReconcilePayments(ctx, 10); err != nil {
				t.Fatal(err)
			}
			final, err := e.svc.Get(ctx, "bob", r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if final.Status != tc.want {
				t.Fatalf("want %s, got %s", tc.want, final.Status)
			}
			e.assertConserved(t, tierID, 10)
			assertRedisMatchesDB(t, e, tierID)
		})
	}
}

func TestCancelIsIdempotentAndReleasesQuota(t *testing.T) {
	e := setup(t, envOpts{})
	ev, tierID := e.createEvent(t, 10, 1, 2)
	ctx := context.Background()

	r, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{UserID: "carol", IdempotencyKey: "a", EventID: ev.ID, TierID: tierID, Quantity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{UserID: "carol", IdempotencyKey: "b", EventID: ev.ID, TierID: tierID, Quantity: 1}); !errors.Is(err, booking.ErrQuotaExceeded) {
		t.Fatalf("want quota exceeded, got %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.svc.Cancel(ctx, "carol", r.ID); err != nil {
				t.Errorf("cancel: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := e.dbAvailable(t, tierID); got != 10 {
		t.Fatalf("double release? available=%d", got)
	}
	if _, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{UserID: "carol", IdempotencyKey: "c", EventID: ev.ID, TierID: tierID, Quantity: 2}); err != nil {
		t.Fatalf("quota should be free again: %v", err)
	}
	if _, err := e.svc.Cancel(ctx, "mallory", r.ID); !errors.Is(err, booking.ErrNotFound) {
		t.Fatalf("other users must not cancel: %v", err)
	}
	e.assertConserved(t, tierID, 10)
	assertRedisMatchesDB(t, e, tierID)
}

func TestGateDriftIsRepaired(t *testing.T) {
	e := setup(t, envOpts{})
	ev, tierID := e.createEvent(t, 5, 1, 4)
	ctx := context.Background()

	// Simulate drift: Redis claims far more stock than exists (e.g. a stale
	// value restored after a Redis failover).
	e.rdb.Set(ctx, inventory.StockKey(tierID), 1000, 0)
	sold := 0
	for i := 0; i < 20; i++ {
		_, _, err := e.svc.Reserve(ctx, booking.ReserveRequest{UserID: fmt.Sprintf("d%d", i), IdempotencyKey: "k", EventID: ev.ID, TierID: tierID, Quantity: 1})
		if err == nil {
			sold++
		}
	}
	if sold != 5 {
		t.Fatalf("Postgres must cap sales at 5 regardless of Redis, sold %d", sold)
	}
	time.Sleep(200 * time.Millisecond) // async resync triggered by the DB sold-out
	if v, _ := e.rdb.Get(ctx, inventory.StockKey(tierID)).Int(); v != 0 {
		t.Fatalf("gate should have been resynced to 0, got %d", v)
	}
}

func assertRedisMatchesDB(t *testing.T, e *env, tierID int64) {
	t.Helper()
	v, err := e.rdb.Get(context.Background(), inventory.StockKey(tierID)).Int()
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	if d := e.dbAvailable(t, tierID); v != d {
		t.Fatalf("redis stock %d != db available %d", v, d)
	}
}
