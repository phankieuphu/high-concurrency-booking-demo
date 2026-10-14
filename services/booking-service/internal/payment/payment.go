// Package payment wraps the (external) payment provider with the protections a
// money-moving call needs: idempotency keys, timeouts, bounded retries and a
// circuit breaker.
package payment

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"booking-service/internal/resilience"
)

var (
	// ErrDeclined is a definitive answer: the card was not charged.
	ErrDeclined = errors.New("payment declined")
	// ErrUnavailable means the provider was not called at all (breaker open):
	// it is safe to tell the user to retry.
	ErrUnavailable = errors.New("payment provider unavailable")
	// ErrNotFound: provider has no record for the idempotency key.
	ErrNotFound = errors.New("payment not found")
	// errTransient: the outcome is UNKNOWN (timeout, 5xx). The charge may or
	// may not have happened; only Lookup can tell.
	errTransient = errors.New("payment provider transient error")
)

type Status string

const (
	Charged  Status = "CHARGED"
	Declined Status = "DECLINED"
	Refunded Status = "REFUNDED"
)

// Provider is the remote API. Every call is idempotent on `key`.
type Provider interface {
	Charge(ctx context.Context, key string, amountCents int64, token string) (ref string, err error)
	Lookup(ctx context.Context, key string) (Status, string, error)
	Refund(ctx context.Context, key string) error
}

type Client struct {
	p       Provider
	breaker *resilience.Breaker
	timeout time.Duration
}

func NewClient(p Provider, breaker *resilience.Breaker, timeout time.Duration) *Client {
	return &Client{p: p, breaker: breaker, timeout: timeout}
}

var chargeRetry = resilience.RetryPolicy{Attempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 500 * time.Millisecond}

func isProviderFailure(err error) bool {
	return err != nil && !errors.Is(err, ErrDeclined) && !errors.Is(err, ErrNotFound)
}

// Charge retries transient failures with the SAME idempotency key, so the
// customer is charged at most once no matter how many attempts we make.
func (c *Client) Charge(ctx context.Context, key string, amountCents int64, token string) (string, error) {
	var ref string
	attempted := false
	err := resilience.Retry(ctx, chargeRetry, func(err error) bool {
		return isProviderFailure(err) && !errors.Is(err, resilience.ErrOpen)
	}, func(ctx context.Context) error {
		return c.breaker.Do(func() error {
			attempted = true
			callCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			var err error
			ref, err = c.p.Charge(callCtx, key, amountCents, token)
			return err
		}, isProviderFailure)
	})
	if errors.Is(err, resilience.ErrOpen) && !attempted {
		return "", ErrUnavailable
	}
	return ref, err
}

func (c *Client) Lookup(ctx context.Context, key string) (Status, string, error) {
	var st Status
	var ref string
	err := c.breaker.Do(func() error {
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		var err error
		st, ref, err = c.p.Lookup(callCtx, key)
		return err
	}, isProviderFailure)
	return st, ref, err
}

func (c *Client) Refund(ctx context.Context, key string) error {
	return resilience.Retry(ctx, chargeRetry, isProviderFailure, func(ctx context.Context) error {
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		return c.p.Refund(callCtx, key)
	})
}

// MockProvider simulates a PSP. State lives in Postgres only so that every
// replica (API and workers) sees the same "remote" ledger in the demo.
//
// FailureRate injects transient errors; half of them happen AFTER the charge
// was recorded ("response lost"), which is the nastiest real-world case and
// is what the payment reconciler exists for.
type MockProvider struct {
	Pool        *pgxpool.Pool
	FailureRate float64
	Latency     time.Duration
}

func (m *MockProvider) Charge(ctx context.Context, key string, amountCents int64, token string) (string, error) {
	if err := m.delay(ctx); err != nil {
		return "", err
	}
	failBefore, failAfter := false, false
	if m.FailureRate > 0 && rand.Float64() < m.FailureRate {
		failBefore = rand.IntN(2) == 0
		failAfter = !failBefore
	}
	if failBefore {
		return "", errTransient
	}
	status := Charged
	if token == "tok_decline" {
		status = Declined
	}
	ref := fmt.Sprintf("pay_%s", key)
	if _, err := m.Pool.Exec(ctx, `
		INSERT INTO mock_payment_provider (idempotency_key, amount_cents, status, payment_ref)
		VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO NOTHING`,
		key, amountCents, status, ref); err != nil {
		return "", fmt.Errorf("%w: %v", errTransient, err)
	}
	if failAfter {
		return "", errTransient
	}
	st, ref, err := m.Lookup(ctx, key)
	if err != nil {
		return "", err
	}
	if st == Declined {
		return "", ErrDeclined
	}
	return ref, nil
}

func (m *MockProvider) Lookup(ctx context.Context, key string) (Status, string, error) {
	var st Status
	var ref string
	err := m.Pool.QueryRow(ctx, `SELECT status, payment_ref FROM mock_payment_provider WHERE idempotency_key = $1`, key).Scan(&st, &ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return st, ref, err
}

func (m *MockProvider) Refund(ctx context.Context, key string) error {
	_, err := m.Pool.Exec(ctx, `UPDATE mock_payment_provider SET status = 'REFUNDED' WHERE idempotency_key = $1 AND status = 'CHARGED'`, key)
	return err
}

func (m *MockProvider) delay(ctx context.Context) error {
	if m.Latency <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.Latency/2 + rand.N(m.Latency)):
		return nil
	}
}
