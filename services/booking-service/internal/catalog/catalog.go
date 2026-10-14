// Package catalog serves event/tier metadata: read-mostly data that every
// booking request needs. It is cached in-process with a short TTL and loaded
// through singleflight, so a cache expiry under 100k RPS causes exactly one
// database query per instance instead of a stampede.
package catalog

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

var ErrNotFound = errors.New("not found")

type Tier struct {
	ID         int64  `json:"id"`
	EventID    int64  `json:"event_id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Currency   string `json:"currency"`
	Total      int    `json:"total"`
	ShardCount int    `json:"shard_count"`
	Available  *int   `json:"available,omitempty"`
}

type Event struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Venue        string    `json:"venue"`
	StartsAt     time.Time `json:"starts_at"`
	SaleStartsAt time.Time `json:"sale_starts_at"`
	MaxPerUser   int       `json:"max_per_user"`
	WaitingRoom  bool      `json:"waiting_room"`
	Tiers        []Tier    `json:"tiers"`
}

type NewTier struct {
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Currency   string `json:"currency"`
	Total      int    `json:"total"`
	// Shards overrides the automatic inventory shard count (0 = auto).
	Shards int `json:"shards,omitempty"`
}

type NewEvent struct {
	Name         string     `json:"name"`
	Venue        string     `json:"venue"`
	StartsAt     time.Time  `json:"starts_at"`
	SaleStartsAt *time.Time `json:"sale_starts_at"`
	MaxPerUser   int        `json:"max_per_user"`
	WaitingRoom  bool       `json:"waiting_room"`
	Tiers        []NewTier  `json:"tiers"`
}

type entry struct {
	event   *Event
	expires time.Time
}

type Catalog struct {
	primary *pgxpool.Pool
	reader  *pgxpool.Pool // read replica when configured, else primary
	ttl     time.Duration

	mu    sync.RWMutex
	cache map[int64]entry
	tiers map[int64]int64 // tier id -> event id
	group singleflight.Group
}

func New(primary, reader *pgxpool.Pool, ttl time.Duration) *Catalog {
	if reader == nil {
		reader = primary
	}
	return &Catalog{primary: primary, reader: reader, ttl: ttl, cache: map[int64]entry{}, tiers: map[int64]int64{}}
}

func (c *Catalog) Event(ctx context.Context, id int64) (*Event, error) {
	c.mu.RLock()
	e, ok := c.cache[id]
	c.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.event, nil
	}

	v, err, _ := c.group.Do(fmt.Sprint(id), func() (any, error) {
		// Detach from the first caller's context so its cancellation does not
		// fail every other request waiting on this flight.
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		ev, err := c.load(loadCtx, id)
		if err != nil {
			// Serve stale on database trouble rather than failing reads.
			if ok && !errors.Is(err, ErrNotFound) {
				return e.event, nil
			}
			return nil, err
		}
		// Jittered TTL so entries loaded together do not expire together.
		ttl := c.ttl + rand.N(c.ttl/2+1)
		c.mu.Lock()
		c.cache[id] = entry{event: ev, expires: time.Now().Add(ttl)}
		for _, t := range ev.Tiers {
			c.tiers[t.ID] = ev.ID
		}
		c.mu.Unlock()
		return ev, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Event), nil
}

// Tier resolves a tier and its event (both cached).
func (c *Catalog) Tier(ctx context.Context, eventID, tierID int64) (*Event, *Tier, error) {
	ev, err := c.Event(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	for i := range ev.Tiers {
		if ev.Tiers[i].ID == tierID {
			return ev, &ev.Tiers[i], nil
		}
	}
	return nil, nil, ErrNotFound
}

func (c *Catalog) load(ctx context.Context, id int64) (*Event, error) {
	ev := &Event{}
	err := c.reader.QueryRow(ctx, `
		SELECT id, name, venue, starts_at, sale_starts_at, max_per_user, waiting_room
		FROM events WHERE id = $1`, id).
		Scan(&ev.ID, &ev.Name, &ev.Venue, &ev.StartsAt, &ev.SaleStartsAt, &ev.MaxPerUser, &ev.WaitingRoom)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := c.reader.Query(ctx, `
		SELECT id, event_id, name, price_cents, currency, total, shard_count
		FROM ticket_tiers WHERE event_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	ev.Tiers, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Tier, error) {
		var t Tier
		err := r.Scan(&t.ID, &t.EventID, &t.Name, &t.PriceCents, &t.Currency, &t.Total, &t.ShardCount)
		return t, err
	})
	return ev, err
}

func (c *Catalog) List(ctx context.Context, limit int) ([]Event, error) {
	rows, err := c.reader.Query(ctx, `
		SELECT id, name, venue, starts_at, sale_starts_at, max_per_user, waiting_room
		FROM events WHERE starts_at > now() ORDER BY starts_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var ev Event
		err := r.Scan(&ev.ID, &ev.Name, &ev.Venue, &ev.StartsAt, &ev.SaleStartsAt, &ev.MaxPerUser, &ev.WaitingRoom)
		return ev, err
	})
}

// DBAvailable sums inventory shards from the primary (authoritative).
func (c *Catalog) DBAvailable(ctx context.Context, tierIDs []int64) (map[int64]int, error) {
	rows, err := c.primary.Query(ctx, `
		SELECT tier_id, sum(available)::int FROM inventory_shards
		WHERE tier_id = ANY($1) GROUP BY tier_id`, tierIDs)
	if err != nil {
		return nil, err
	}
	out := map[int64]int{}
	for rows.Next() {
		var id int64
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// ShardCountFor picks how many inventory rows a tier gets: roughly one shard
// per 500 tickets, so contention spreads out while small tiers keep a single
// row (no fragmentation).
func ShardCountFor(total int) int {
	n := total / 500
	switch {
	case n < 1:
		return 1
	case n > 64:
		return 64
	}
	return n
}

// Create inserts an event with its tiers and pre-split inventory shards.
func (c *Catalog) Create(ctx context.Context, in NewEvent) (*Event, error) {
	if in.Name == "" || len(in.Tiers) == 0 {
		return nil, errors.New("name and at least one tier are required")
	}
	if in.MaxPerUser <= 0 {
		in.MaxPerUser = 4
	}
	saleStarts := time.Now()
	if in.SaleStartsAt != nil {
		saleStarts = *in.SaleStartsAt
	}
	var eventID int64
	err := pgx.BeginFunc(ctx, c.primary, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO events (name, venue, starts_at, sale_starts_at, max_per_user, waiting_room)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			in.Name, in.Venue, in.StartsAt, saleStarts, in.MaxPerUser, in.WaitingRoom).Scan(&eventID); err != nil {
			return err
		}
		for _, t := range in.Tiers {
			if t.Total < 0 || t.Name == "" {
				return fmt.Errorf("invalid tier %q", t.Name)
			}
			if t.Currency == "" {
				t.Currency = "USD"
			}
			shards := ShardCountFor(t.Total)
			if t.Shards > 0 {
				shards = min(t.Shards, 256)
			}
			var tierID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO ticket_tiers (event_id, name, price_cents, currency, total, shard_count)
				VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
				eventID, t.Name, t.PriceCents, t.Currency, t.Total, shards).Scan(&tierID); err != nil {
				return err
			}
			// Spread the total evenly; the first (total % shards) shards get one extra.
			if _, err := tx.Exec(ctx, `
				INSERT INTO inventory_shards (tier_id, shard_no, available)
				SELECT $1, s, $2 / $3 + CASE WHEN s < $2 % $3 THEN 1 ELSE 0 END
				FROM generate_series(0, $3 - 1) AS s`, tierID, t.Total, shards); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Read back from the primary: the replica may not have the rows yet.
	return (&Catalog{reader: c.primary}).load(ctx, eventID)
}

// AllActiveTierIDs lists tiers of events that have not started yet.
func (c *Catalog) AllActiveTierIDs(ctx context.Context) ([]int64, error) {
	rows, err := c.primary.Query(ctx, `
		SELECT t.id FROM ticket_tiers t JOIN events e ON e.id = t.event_id
		WHERE e.starts_at > now()`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
