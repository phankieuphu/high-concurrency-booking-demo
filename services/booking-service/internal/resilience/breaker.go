package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned without calling the dependency while the breaker is open.
var ErrOpen = errors.New("circuit breaker open")

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// Breaker is a consecutive-failure circuit breaker.
//
//	closed    --N consecutive failures-->  open
//	open      --cooldown elapsed-------->  half-open (one probe allowed)
//	half-open --probe ok---------------->  closed
//	half-open --probe failed------------>  open
//
// It stops a slow or dead dependency from consuming every goroutine and
// connection we have, and lets callers switch to a fallback immediately.
type Breaker struct {
	name      string
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(name string, from, to State)

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
}

func NewBreaker(name string, threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{name: name, threshold: threshold, cooldown: cooldown, now: time.Now}
}

// OnStateChange registers a hook (metrics/logging). Not safe to call concurrently with use.
func (b *Breaker) OnStateChange(fn func(name string, from, to State)) { b.onChange = fn }

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a call may proceed. Every allowed call must be
// followed by exactly one Success or Failure.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return ErrOpen
		}
		b.transition(HalfOpen)
		b.probing = true
		return nil
	case HalfOpen:
		if b.probing {
			return ErrOpen
		}
		b.probing = true
		return nil
	}
	return nil
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	if b.state != Closed {
		b.transition(Closed)
	}
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.openedAt = b.now()
		if b.state != Open {
			b.transition(Open)
		}
	}
}

// Do runs fn under the breaker. isFailure decides which errors count against
// the dependency (e.g. a "sold out" answer is a healthy response).
func (b *Breaker) Do(fn func() error, isFailure func(error) bool) error {
	if err := b.Allow(); err != nil {
		return err
	}
	err := fn()
	if err != nil && (isFailure == nil || isFailure(err)) {
		b.Failure()
	} else {
		b.Success()
	}
	return err
}

func (b *Breaker) transition(to State) {
	from := b.state
	b.state = to
	if b.onChange != nil {
		go b.onChange(b.name, from, to)
	}
}
