package booking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"booking-service/internal/catalog"
	"booking-service/internal/db"
	"booking-service/internal/inventory"
	"booking-service/internal/metrics"
	"booking-service/internal/payment"
	"booking-service/internal/resilience"
)

type Service struct {
	pool          *pgxpool.Pool
	catalog       *catalog.Catalog
	gate          *inventory.Gate
	payments      *payment.Client
	bulkhead      *resilience.Bulkhead
	holdTTL       time.Duration
	paymentWindow time.Duration
	log           *slog.Logger
}

type Options struct {
	HoldTTL       time.Duration
	PaymentWindow time.Duration
}

func NewService(pool *pgxpool.Pool, cat *catalog.Catalog, gate *inventory.Gate, payments *payment.Client,
	bulkhead *resilience.Bulkhead, opts Options) *Service {
	return &Service{
		pool: pool, catalog: cat, gate: gate, payments: payments, bulkhead: bulkhead,
		holdTTL: opts.HoldTTL, paymentWindow: opts.PaymentWindow,
		log: slog.With("component", "booking"),
	}
}

func cols(alias string) string {
	c := []string{"id", "user_id", "event_id", "tier_id", "shard_no", "quantity", "amount_cents", "currency",
		"status", "payment_ref", "expires_at", "version", "created_at", "updated_at"}
	if alias != "" {
		for i := range c {
			c[i] = alias + "." + c[i]
		}
	}
	return strings.Join(c, ", ")
}

func scan(row pgx.Row) (*Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.UserID, &r.EventID, &r.TierID, &r.ShardNo, &r.Quantity, &r.AmountCents, &r.Currency,
		&r.Status, &r.PaymentRef, &r.ExpiresAt, &r.Version, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &r, err
}

// ---------------------------------------------------------------------------
// Reserve: the hot path.
// ---------------------------------------------------------------------------

// Reserve places a time-limited hold. It returns replayed=true when the
// idempotency key was already used and the original reservation is returned.
//
//  1. cheap validation + cached catalog lookup (no DB)
//  2. idempotent replay check
//  3. Redis gate: atomic check-and-decrement, rejects sold-out traffic in O(1)
//  4. bulkhead: bounded number of concurrent DB write transactions
//  5. one short DB transaction: quota upsert -> shard decrement -> insert -> outbox
//  6. compensation: give Redis stock back if the DB step failed
func (s *Service) Reserve(ctx context.Context, req ReserveRequest) (*Reservation, bool, error) {
	if req.UserID == "" || req.IdempotencyKey == "" || len(req.IdempotencyKey) > 128 || req.Quantity < 1 {
		return nil, false, ErrInvalidRequest
	}
	ev, tier, err := s.catalog.Tier(ctx, req.EventID, req.TierID)
	if errors.Is(err, catalog.ErrNotFound) {
		return nil, false, ErrNotFound
	} else if err != nil {
		return nil, false, err
	}
	if req.Quantity > ev.MaxPerUser {
		return nil, false, ErrQuotaExceeded
	}
	if time.Now().Before(ev.SaleStartsAt) {
		return nil, false, ErrSaleNotStarted
	}

	if r, err := s.findByKey(ctx, req.UserID, req.IdempotencyKey); err == nil {
		return r, true, nil
	}

	decision := s.gate.TryAcquire(ctx, tier.ID, req.Quantity)
	metrics.GateDecisions.WithLabelValues(decision.String()).Inc()
	if decision == inventory.SoldOut {
		metrics.ReservationOutcomes.WithLabelValues("sold_out_gate").Inc()
		return nil, false, ErrSoldOut
	}

	release, err := s.bulkhead.Acquire(ctx)
	if err != nil {
		if decision == inventory.Admitted {
			s.gate.Release(ctx, tier.ID, req.Quantity)
		}
		metrics.ReservationOutcomes.WithLabelValues("shed").Inc()
		return nil, false, ErrOverloaded
	}
	defer release()

	var res *Reservation
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		res, err = s.reserveTx(ctx, tx, ev, tier, req)
		return err
	})
	if err != nil {
		if decision == inventory.Admitted {
			if errors.Is(err, ErrSoldOut) {
				// Redis said yes, Postgres said no: the counter is too high.
				// Don't give stock back; resync it from the source of truth.
				s.resyncTierAsync(ctx, tier.ID, "db_sold_out")
			} else {
				s.gate.Release(ctx, tier.ID, req.Quantity)
			}
		}
		if db.IsUniqueViolation(err, "reservations_idempotency") {
			// A concurrent request with the same key won the race.
			if r, ferr := s.findByKey(ctx, req.UserID, req.IdempotencyKey); ferr == nil {
				return r, true, nil
			}
		}
		metrics.ReservationOutcomes.WithLabelValues(outcomeLabel(err)).Inc()
		return nil, false, err
	}
	metrics.ReservationOutcomes.WithLabelValues("held").Inc()
	metrics.Transitions.WithLabelValues(string(StatusHeld)).Inc()
	return res, false, nil
}

func (s *Service) reserveTx(ctx context.Context, tx pgx.Tx, ev *catalog.Event, tier *catalog.Tier, req ReserveRequest) (*Reservation, error) {
	// Per-user limit. The conditional upsert is atomic: two parallel requests
	// from the same user serialise on the quota row and the second one sees
	// the first one's increment.
	var reserved int
	err := tx.QueryRow(ctx, `
		INSERT INTO user_event_quota (user_id, event_id, reserved) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, event_id) DO UPDATE
		   SET reserved = user_event_quota.reserved + EXCLUDED.reserved
		 WHERE user_event_quota.reserved + EXCLUDED.reserved <= $4
		RETURNING reserved`, req.UserID, ev.ID, req.Quantity, ev.MaxPerUser).Scan(&reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrQuotaExceeded
	} else if err != nil {
		return nil, err
	}

	shard, err := takeFromShard(ctx, tx, tier.ID, req.Quantity)
	if err != nil {
		return nil, err
	}

	r, err := scan(tx.QueryRow(ctx, `
		INSERT INTO reservations (user_id, event_id, tier_id, shard_no, quantity, amount_cents, currency,
		                          status, idempotency_key, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'HELD', $8, now() + make_interval(secs => $9))
		RETURNING `+cols(""),
		req.UserID, ev.ID, tier.ID, shard, req.Quantity, tier.PriceCents*int64(req.Quantity), tier.Currency,
		req.IdempotencyKey, s.holdTTL.Seconds()))
	if err != nil {
		return nil, err
	}
	return r, insertOutbox(ctx, tx, "reservation.held", r)
}

// Pick a random shard that still has enough stock and lock it, skipping shards
// another transaction is holding right now. N buyers therefore spread over N
// shards instead of queueing on one hot row lock.
const takeSkipLocked = `
	UPDATE inventory_shards SET available = available - $2
	WHERE tier_id = $1 AND shard_no = (
		SELECT shard_no FROM inventory_shards
		WHERE tier_id = $1 AND available >= $2
		ORDER BY random() LIMIT 1
		FOR UPDATE SKIP LOCKED)
	RETURNING shard_no`

// takeFromShard never waits on a row lock. When every shard with stock is
// momentarily locked it backs off for a few milliseconds and tries again; if
// that keeps happening it gives up with ErrInventoryContention (HTTP 503 +
// Retry-After). Blocking lock waits would instead let a crowd pile up behind
// the last few tickets and blow through statement timeouts.
func takeFromShard(ctx context.Context, tx pgx.Tx, tierID int64, qty int) (int, error) {
	const attempts = 25
	for attempt := 0; attempt < attempts; attempt++ {
		var shard int
		err := tx.QueryRow(ctx, takeSkipLocked, tierID, qty).Scan(&shard)
		if err == nil {
			return shard, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		// Nothing unlocked had stock. Is that "sold out" or "all busy"?
		var any bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM inventory_shards WHERE tier_id = $1 AND available >= $2)`,
			tierID, qty).Scan(&any); err != nil {
			return 0, err
		}
		if !any {
			return 0, ErrSoldOut
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Millisecond + rand.N(4*time.Millisecond)):
		}
	}
	return 0, ErrInventoryContention
}

// ---------------------------------------------------------------------------
// Confirm (pay), Cancel, Get
// ---------------------------------------------------------------------------

// Confirm pays for a hold. The reservation is first moved HELD ->
// PAYMENT_PENDING in its own transaction, which (a) wins or loses the race
// against the expiry worker deterministically and (b) leaves a durable record
// BEFORE money moves, so a crash mid-payment is recoverable by the reconciler.
func (s *Service) Confirm(ctx context.Context, userID, id, paymentToken string) (*Reservation, error) {
	if !looksLikeUUID(id) {
		return nil, ErrNotFound
	}
	var r *Reservation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		r, err = scan(tx.QueryRow(ctx, `
			UPDATE reservations
			   SET status = 'PAYMENT_PENDING', expires_at = now() + make_interval(secs => $3),
			       version = version + 1, updated_at = now()
			 WHERE id = $1 AND user_id = $2 AND status = 'HELD' AND expires_at > now()
			RETURNING `+cols(""), id, userID, s.paymentWindow.Seconds()))
		return err
	})
	if errors.Is(err, ErrNotFound) {
		cur, gerr := s.Get(ctx, userID, id)
		if gerr != nil {
			return nil, gerr
		}
		switch cur.Status {
		case StatusConfirmed:
			return cur, nil // idempotent retry
		case StatusPaymentPending:
			return cur, ErrPaymentInProgress
		default:
			return cur, ErrReservationExpired
		}
	}
	if err != nil {
		return nil, err
	}
	metrics.Transitions.WithLabelValues(string(StatusPaymentPending)).Inc()

	// From here on the user may disconnect, but we must still record the
	// outcome of a charge, so detach from the request context.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	// The reservation id is the payment idempotency key: retries (ours, the
	// user's, the reconciler's) can never double-charge.
	ref, err := s.payments.Charge(ctx, r.ID, r.AmountCents, paymentToken)
	switch {
	case err == nil:
		final, ok, ferr := s.markConfirmed(ctx, r.ID, ref)
		if ferr != nil {
			return r, ErrPaymentInProgress // reconciler will finish the job
		}
		if !ok {
			// Lost the race to the payment reconciler, which already failed
			// the reservation and released the seats. Undo the charge.
			if rerr := s.payments.Refund(ctx, r.ID); rerr != nil {
				s.log.Error("refund after lost race failed", "reservation", r.ID, "err", rerr)
			}
			return final, ErrReservationExpired
		}
		return final, nil
	case errors.Is(err, payment.ErrDeclined):
		final, _, ferr := s.markFailed(ctx, r.ID, "declined")
		if ferr != nil {
			return r, ferr
		}
		return final, ErrPaymentDeclined
	case errors.Is(err, payment.ErrUnavailable):
		// Provider was never called: hand the hold back to the user.
		if rerr := s.revertToHeld(ctx, r.ID); rerr != nil {
			s.log.Error("revert to HELD failed", "reservation", r.ID, "err", rerr)
		}
		return r, ErrPaymentUnavailable
	default:
		// Unknown outcome (timeout after the provider may have charged).
		// Leave PAYMENT_PENDING; the reconciler asks the provider later.
		s.log.Warn("payment outcome unknown, deferring to reconciler", "reservation", r.ID, "err", err)
		return r, ErrPaymentInProgress
	}
}

func (s *Service) markConfirmed(ctx context.Context, id, ref string) (*Reservation, bool, error) {
	var r *Reservation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		r, err = scan(tx.QueryRow(ctx, `
			UPDATE reservations SET status = 'CONFIRMED', payment_ref = $2, version = version + 1, updated_at = now()
			 WHERE id = $1 AND status = 'PAYMENT_PENDING'
			RETURNING `+cols(""), id, ref))
		if err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "reservation.confirmed", r)
	})
	if errors.Is(err, ErrNotFound) {
		cur, gerr := s.getByID(ctx, id)
		if gerr != nil {
			return nil, false, gerr
		}
		return cur, cur.Status == StatusConfirmed, nil
	}
	if err != nil {
		return nil, false, err
	}
	metrics.Transitions.WithLabelValues(string(StatusConfirmed)).Inc()
	return r, true, nil
}

func (s *Service) markFailed(ctx context.Context, id, reason string) (*Reservation, bool, error) {
	var r *Reservation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		r, err = scan(tx.QueryRow(ctx, `
			UPDATE reservations SET status = 'PAYMENT_FAILED', version = version + 1, updated_at = now()
			 WHERE id = $1 AND status = 'PAYMENT_PENDING'
			RETURNING `+cols(""), id))
		if err != nil {
			return err
		}
		if err := releaseInventory(ctx, tx, []*Reservation{r}); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "reservation.payment_failed", map[string]any{"reservation": r, "reason": reason})
	})
	if errors.Is(err, ErrNotFound) {
		cur, gerr := s.getByID(ctx, id)
		return cur, false, gerr
	}
	if err != nil {
		return nil, false, err
	}
	s.releaseGate(ctx, []*Reservation{r})
	metrics.Transitions.WithLabelValues(string(StatusPaymentFailed)).Inc()
	return r, true, nil
}

func (s *Service) revertToHeld(ctx context.Context, id string) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE reservations
			   SET status = 'HELD', version = version + 1, updated_at = now(),
			       expires_at = GREATEST(created_at + make_interval(secs => $2), now() + interval '60 seconds')
			 WHERE id = $1 AND status = 'PAYMENT_PENDING'`, id, s.holdTTL.Seconds())
		return err
	})
}

// Cancel releases a hold. Idempotent: cancelling twice returns the same result.
func (s *Service) Cancel(ctx context.Context, userID, id string) (*Reservation, error) {
	if !looksLikeUUID(id) {
		return nil, ErrNotFound
	}
	var r *Reservation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		r, err = scan(tx.QueryRow(ctx, `
			UPDATE reservations SET status = 'CANCELLED', version = version + 1, updated_at = now()
			 WHERE id = $1 AND user_id = $2 AND status = 'HELD'
			RETURNING `+cols(""), id, userID))
		if err != nil {
			return err
		}
		if err := releaseInventory(ctx, tx, []*Reservation{r}); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "reservation.cancelled", r)
	})
	if errors.Is(err, ErrNotFound) {
		cur, gerr := s.Get(ctx, userID, id)
		if gerr != nil {
			return nil, gerr
		}
		if cur.Status == StatusCancelled {
			return cur, nil
		}
		return cur, ErrInvalidState
	}
	if err != nil {
		return nil, err
	}
	s.releaseGate(ctx, []*Reservation{r})
	metrics.Transitions.WithLabelValues(string(StatusCancelled)).Inc()
	return r, nil
}

func (s *Service) Get(ctx context.Context, userID, id string) (*Reservation, error) {
	if !looksLikeUUID(id) {
		return nil, ErrNotFound
	}
	return scan(s.pool.QueryRow(ctx, `SELECT `+cols("")+` FROM reservations WHERE id = $1 AND user_id = $2`, id, userID))
}

func (s *Service) getByID(ctx context.Context, id string) (*Reservation, error) {
	return scan(s.pool.QueryRow(ctx, `SELECT `+cols("")+` FROM reservations WHERE id = $1`, id))
}

func (s *Service) findByKey(ctx context.Context, userID, key string) (*Reservation, error) {
	return scan(s.pool.QueryRow(ctx, `SELECT `+cols("")+` FROM reservations WHERE user_id = $1 AND idempotency_key = $2`, userID, key))
}

// ---------------------------------------------------------------------------
// Background maintenance (run by worker replicas)
// ---------------------------------------------------------------------------

// ExpireHolds expires up to `limit` overdue holds and returns their stock.
// FOR UPDATE SKIP LOCKED lets any number of worker replicas run this
// concurrently: each grabs a disjoint batch, nobody blocks, nothing is
// processed twice. A user confirming at the same instant either locks the
// row first (and the worker skips it) or finds it already EXPIRED.
func (s *Service) ExpireHolds(ctx context.Context, limit int) (int, error) {
	var expired []*Reservation
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		expired = expired[:0]
		rows, err := tx.Query(ctx, `
			WITH due AS (
				SELECT id FROM reservations
				 WHERE status = 'HELD' AND expires_at < now()
				 ORDER BY expires_at LIMIT $1
				   FOR UPDATE SKIP LOCKED)
			UPDATE reservations r SET status = 'EXPIRED', version = r.version + 1, updated_at = now()
			  FROM due WHERE r.id = due.id
			RETURNING `+cols("r"), limit)
		if err != nil {
			return err
		}
		expired, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Reservation, error) { return scan(row) })
		if err != nil || len(expired) == 0 {
			return err
		}
		if err := releaseInventory(ctx, tx, expired); err != nil {
			return err
		}
		for _, r := range expired {
			if err := insertOutbox(ctx, tx, "reservation.expired", r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.releaseGate(ctx, expired)
	metrics.Transitions.WithLabelValues(string(StatusExpired)).Add(float64(len(expired)))
	return len(expired), nil
}

// ReconcilePayments resolves PAYMENT_PENDING reservations whose payment window
// passed (crash mid-payment, provider timeout, lost response) by asking the
// provider what really happened. Transitions are conditional, so racing with
// a late Confirm is safe.
func (s *Service) ReconcilePayments(ctx context.Context, limit int) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM reservations WHERE status = 'PAYMENT_PENDING' AND expires_at < now()
		ORDER BY expires_at LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	done := 0
	for _, id := range ids {
		st, ref, err := s.payments.Lookup(ctx, id)
		switch {
		case err == nil && st == payment.Charged:
			_, _, err = s.markConfirmed(ctx, id, ref)
		case errors.Is(err, payment.ErrNotFound) || (err == nil && st != payment.Charged):
			_, _, err = s.markFailed(ctx, id, "not charged within payment window")
		}
		if err != nil {
			s.log.Warn("payment reconcile deferred", "reservation", id, "err", err)
			continue
		}
		done++
	}
	return done, nil
}

// SyncGate overwrites Redis stock counters with the authoritative Postgres
// values. Fixes drift from crashed processes, failed releases, Redis
// failover/eviction. Transiently over-admitting is harmless: Postgres rejects.
func (s *Service) SyncGate(ctx context.Context, tierIDs []int64, reason string) error {
	avail, err := s.catalog.DBAvailable(ctx, tierIDs)
	if err != nil {
		return err
	}
	for _, id := range tierIDs {
		if err := s.gate.Set(ctx, id, avail[id]); err != nil {
			return err
		}
		metrics.GateDrift.WithLabelValues(reason).Inc()
	}
	return nil
}

func (s *Service) resyncTierAsync(ctx context.Context, tierID int64, reason string) {
	// Debounce across all replicas: one resync per tier per second.
	if !s.gate.TryLock(ctx, fmt.Sprintf("resync:{%d}", tierID), time.Second) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := s.SyncGate(ctx, []int64{tierID}, reason); err != nil {
			s.log.Warn("gate resync failed", "tier", tierID, "err", err)
		}
	}()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// releaseInventory returns stock and quota for reservations leaving an active
// state. Lock order is ALWAYS quota rows (sorted) then shard rows (sorted),
// the same order Reserve uses, so concurrent releases and reserves cannot
// deadlock.
func releaseInventory(ctx context.Context, tx pgx.Tx, rs []*Reservation) error {
	type qk struct {
		user  string
		event int64
	}
	type sk struct {
		tier  int64
		shard int
	}
	quotas := map[qk]int{}
	shards := map[sk]int{}
	for _, r := range rs {
		quotas[qk{r.UserID, r.EventID}] += r.Quantity
		shards[sk{r.TierID, r.ShardNo}] += r.Quantity
	}
	qkeys := make([]qk, 0, len(quotas))
	for k := range quotas {
		qkeys = append(qkeys, k)
	}
	sort.Slice(qkeys, func(i, j int) bool {
		if qkeys[i].user != qkeys[j].user {
			return qkeys[i].user < qkeys[j].user
		}
		return qkeys[i].event < qkeys[j].event
	})
	skeys := make([]sk, 0, len(shards))
	for k := range shards {
		skeys = append(skeys, k)
	}
	sort.Slice(skeys, func(i, j int) bool {
		if skeys[i].tier != skeys[j].tier {
			return skeys[i].tier < skeys[j].tier
		}
		return skeys[i].shard < skeys[j].shard
	})

	b := &pgx.Batch{}
	for _, k := range qkeys {
		b.Queue(`UPDATE user_event_quota SET reserved = reserved - $3 WHERE user_id = $1 AND event_id = $2`, k.user, k.event, quotas[k])
	}
	for _, k := range skeys {
		b.Queue(`UPDATE inventory_shards SET available = available + $3 WHERE tier_id = $1 AND shard_no = $2`, k.tier, k.shard, shards[k])
	}
	return tx.SendBatch(ctx, b).Close()
}

// releaseGate is called only AFTER the DB commit. If we crash in between,
// Redis is temporarily low (we under-sell for a few seconds) and the
// reconciler fixes it; the reverse order could oversell the gate.
func (s *Service) releaseGate(ctx context.Context, rs []*Reservation) {
	perTier := map[int64]int{}
	for _, r := range rs {
		perTier[r.TierID] += r.Quantity
	}
	for tier, qty := range perTier {
		s.gate.Release(ctx, tier, qty)
	}
}

func insertOutbox(ctx context.Context, tx pgx.Tx, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var aggregate string
	switch p := payload.(type) {
	case *Reservation:
		aggregate = p.ID
	case map[string]any:
		if r, ok := p["reservation"].(*Reservation); ok {
			aggregate = r.ID
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`, aggregate, eventType, body)
	return err
}

func outcomeLabel(err error) string {
	switch {
	case errors.Is(err, ErrSoldOut):
		return "sold_out_db"
	case errors.Is(err, ErrQuotaExceeded):
		return "quota_exceeded"
	case errors.Is(err, ErrInventoryContention):
		return "contention"
	default:
		return "error"
	}
}

func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return false
		}
	}
	return true
}
