package resilience

import (
	"context"
	"errors"
	"time"
)

// ErrBulkheadFull means the protected resource is saturated; callers should
// shed the request (HTTP 503 + Retry-After) rather than wait indefinitely.
var ErrBulkheadFull = errors.New("bulkhead full")

// Bulkhead bounds how many callers use a resource at once (here: write
// transactions against the primary database), with a short wait budget.
type Bulkhead struct {
	slots   chan struct{}
	maxWait time.Duration
}

func NewBulkhead(size int, maxWait time.Duration) *Bulkhead {
	return &Bulkhead{slots: make(chan struct{}, size), maxWait: maxWait}
}

func (b *Bulkhead) Acquire(ctx context.Context) (release func(), err error) {
	select {
	case b.slots <- struct{}{}:
		return b.release, nil
	default:
	}
	t := time.NewTimer(b.maxWait)
	defer t.Stop()
	select {
	case b.slots <- struct{}{}:
		return b.release, nil
	case <-t.C:
		return nil, ErrBulkheadFull
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *Bulkhead) release() { <-b.slots }

func (b *Bulkhead) InUse() int { return len(b.slots) }
