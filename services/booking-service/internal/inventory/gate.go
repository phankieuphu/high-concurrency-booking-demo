// Package inventory implements the Redis "gate": an atomic stock counter in
// front of Postgres. During a flash sale 1M users may hit "buy" for 50k
// tickets; the gate rejects the ~95% that cannot possibly succeed with one
// in-memory Redis operation, so only plausible winners reach the database.
//
// The gate is an OPTIMISATION, never the source of truth. Postgres enforces
// the real invariant (inventory_non_negative), so a stale, lost or
// unreachable Redis can cost efficiency but can never cause an oversell.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"booking-service/internal/resilience"
)

type Decision int

const (
	// Admitted: Redis reserved stock; caller must Release it if the DB step fails.
	Admitted Decision = iota
	// SoldOut: reject immediately without touching the database.
	SoldOut
	// Bypassed: Redis unavailable or counter not initialised. Fall through to
	// the database (degraded mode, protected by the DB bulkhead).
	Bypassed
)

func (d Decision) String() string {
	return [...]string{"admitted", "sold_out", "bypassed"}[d]
}

// Atomic check-and-decrement. Returns remaining stock, -1 if insufficient,
// -2 if the key does not exist (never initialised / evicted).
var acquireScript = redis.NewScript(`
local stock = redis.call('GET', KEYS[1])
if not stock then return -2 end
local n = tonumber(ARGV[1])
if tonumber(stock) < n then return -1 end
return redis.call('DECRBY', KEYS[1], n)
`)

// Only give stock back to a key that exists, otherwise a release racing with
// an eviction would create a counter holding just the released amount.
var releaseScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -2 end
return redis.call('INCRBY', KEYS[1], ARGV[1])
`)

type Gate struct {
	rdb     redis.UniversalClient
	breaker *resilience.Breaker
	log     *slog.Logger
}

func NewGate(rdb redis.UniversalClient, breaker *resilience.Breaker) *Gate {
	return &Gate{rdb: rdb, breaker: breaker, log: slog.With("component", "inventory-gate")}
}

// StockKey uses a hash tag so all keys of a tier land on the same Redis
// Cluster slot. Different tiers spread across the cluster.
func StockKey(tierID int64) string { return fmt.Sprintf("stock:{%d}", tierID) }

func (g *Gate) TryAcquire(ctx context.Context, tierID int64, qty int) Decision {
	var res int64
	err := g.breaker.Do(func() error {
		var err error
		res, err = acquireScript.Run(ctx, g.rdb, []string{StockKey(tierID)}, qty).Int64()
		return err
	}, nil)
	switch {
	case err != nil:
		if !errors.Is(err, resilience.ErrOpen) {
			g.log.Warn("redis acquire failed, bypassing gate", "tier", tierID, "err", err)
		}
		return Bypassed
	case res == -2:
		return Bypassed
	case res == -1:
		return SoldOut
	default:
		return Admitted
	}
}

// Release gives stock back after a failed DB step, a cancellation or an
// expiry. Best effort: if it fails, the reconciler repairs the drift.
func (g *Gate) Release(ctx context.Context, tierID int64, qty int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 200*time.Millisecond)
	defer cancel()
	err := g.breaker.Do(func() error {
		return releaseScript.Run(ctx, g.rdb, []string{StockKey(tierID)}, qty).Err()
	}, nil)
	if err != nil {
		g.log.Warn("redis release failed; reconciler will repair", "tier", tierID, "qty", qty, "err", err)
	}
}

// Set overwrites the counter with the authoritative value from Postgres.
func (g *Gate) Set(ctx context.Context, tierID int64, available int) error {
	return g.breaker.Do(func() error {
		return g.rdb.Set(ctx, StockKey(tierID), available, 0).Err()
	}, nil)
}

// Available returns stock counters for display; missing values are omitted.
func (g *Gate) Available(ctx context.Context, tierIDs []int64) map[int64]int {
	out := make(map[int64]int, len(tierIDs))
	if len(tierIDs) == 0 {
		return out
	}
	_ = g.breaker.Do(func() error {
		// One round trip per tier key; keys live on different slots in a cluster.
		pipe := g.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(tierIDs))
		for i, id := range tierIDs {
			cmds[i] = pipe.Get(ctx, StockKey(id))
		}
		_, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		for i, c := range cmds {
			if v, err := strconv.Atoi(c.Val()); err == nil && c.Err() == nil {
				out[tierIDs[i]] = v
			}
		}
		return nil
	}, nil)
	return out
}

// TryLock is a short-lived Redis lock used to debounce work across replicas.
func (g *Gate) TryLock(ctx context.Context, key string, ttl time.Duration) bool {
	var ok bool
	_ = g.breaker.Do(func() error {
		var err error
		ok, err = g.rdb.SetNX(ctx, key, 1, ttl).Result()
		return err
	}, nil)
	return ok
}
