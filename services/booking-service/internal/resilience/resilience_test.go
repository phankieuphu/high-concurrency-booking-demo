package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBreakerOpensAndRecovers(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBreaker("t", 3, 10*time.Second)
	b.now = func() time.Time { return now }
	boom := errors.New("boom")

	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return boom }, nil)
	}
	if b.State() != Open {
		t.Fatalf("want open, got %s", b.State())
	}
	if err := b.Do(func() error { return nil }, nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("want ErrOpen while cooling down, got %v", err)
	}

	now = now.Add(11 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatalf("probe should be allowed after cooldown: %v", err)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("only one probe at a time, got %v", err)
	}
	b.Success()
	if b.State() != Closed {
		t.Fatalf("want closed after successful probe, got %s", b.State())
	}
}

func TestBreakerIgnoresBusinessErrors(t *testing.T) {
	b := NewBreaker("t", 1, time.Second)
	soldOut := errors.New("sold out")
	_ = b.Do(func() error { return soldOut }, func(err error) bool { return !errors.Is(err, soldOut) })
	if b.State() != Closed {
		t.Fatal("business errors must not trip the breaker")
	}
}

func TestRetryStopsOnNonRetryable(t *testing.T) {
	calls := 0
	fatal := errors.New("fatal")
	err := Retry(context.Background(), RetryPolicy{Attempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		func(err error) bool { return !errors.Is(err, fatal) },
		func(context.Context) error { calls++; return fatal })
	if !errors.Is(err, fatal) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), RetryPolicy{Attempts: 5, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond},
		func(error) bool { return true },
		func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
	if err != nil || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestBulkheadSheds(t *testing.T) {
	b := NewBulkhead(1, 5*time.Millisecond)
	release, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(context.Background()); !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("want ErrBulkheadFull, got %v", err)
	}
	release()
	if _, err := b.Acquire(context.Background()); err != nil {
		t.Fatalf("slot should be free again: %v", err)
	}
}
