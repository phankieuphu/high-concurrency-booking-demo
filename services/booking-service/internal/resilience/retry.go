package resilience

import (
	"context"
	"math/rand/v2"
	"time"
)

type RetryPolicy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// Retry runs fn until it succeeds, returns a non-retryable error, the attempts
// are exhausted or ctx is done. Backoff is exponential with full jitter so a
// fleet of clients that failed together does not retry together.
func Retry(ctx context.Context, p RetryPolicy, retryable func(error) bool, fn func(ctx context.Context) error) error {
	var err error
	for attempt := 0; attempt < p.Attempts; attempt++ {
		if err = fn(ctx); err == nil || !retryable(err) {
			return err
		}
		if attempt == p.Attempts-1 {
			break
		}
		backoff := p.BaseDelay << attempt
		if backoff > p.MaxDelay || backoff <= 0 {
			backoff = p.MaxDelay
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rand.N(backoff) + time.Millisecond):
		}
	}
	return err
}
