-- Booking platform schema.
--
-- Every invariant that protects money or inventory is enforced HERE, in the
-- database, not only in application code. Redis and the app are optimisations
-- in front of these guarantees; if they misbehave, the constraints below still
-- make overselling impossible.

CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT        NOT NULL,
    venue           TEXT        NOT NULL,
    starts_at       TIMESTAMPTZ NOT NULL,
    sale_starts_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    max_per_user    INT         NOT NULL DEFAULT 4 CHECK (max_per_user > 0),
    -- When true the gateway forces users through the virtual waiting room.
    waiting_room    BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ticket_tiers (
    id           BIGSERIAL PRIMARY KEY,
    event_id     BIGINT  NOT NULL REFERENCES events (id),
    name         TEXT    NOT NULL,
    price_cents  BIGINT  NOT NULL CHECK (price_cents >= 0),
    currency     TEXT    NOT NULL DEFAULT 'USD',
    total        INT     NOT NULL CHECK (total >= 0),
    shard_count  INT     NOT NULL CHECK (shard_count BETWEEN 1 AND 256),
    UNIQUE (event_id, name)
);
CREATE INDEX IF NOT EXISTS idx_ticket_tiers_event ON ticket_tiers (event_id);

-- Inventory for one tier is split across N rows ("shards") so that thousands
-- of concurrent buyers do not all queue on a single hot row lock.
-- fillfactor < 100 leaves room on each page for HOT updates (no index churn).
CREATE TABLE IF NOT EXISTS inventory_shards (
    tier_id    BIGINT NOT NULL REFERENCES ticket_tiers (id),
    shard_no   INT    NOT NULL,
    available  INT    NOT NULL,
    PRIMARY KEY (tier_id, shard_no),
    -- Last line of defence against overselling.
    CONSTRAINT inventory_non_negative CHECK (available >= 0)
) WITH (fillfactor = 70);

-- Per-user purchase limit, enforced with a conditional upsert.
CREATE TABLE IF NOT EXISTS user_event_quota (
    user_id   TEXT   NOT NULL,
    event_id  BIGINT NOT NULL REFERENCES events (id),
    reserved  INT    NOT NULL CHECK (reserved >= 0),
    PRIMARY KEY (user_id, event_id)
) WITH (fillfactor = 80);

CREATE TABLE IF NOT EXISTS reservations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          TEXT        NOT NULL,
    event_id         BIGINT      NOT NULL REFERENCES events (id),
    tier_id          BIGINT      NOT NULL REFERENCES ticket_tiers (id),
    shard_no         INT         NOT NULL,
    quantity         INT         NOT NULL CHECK (quantity > 0),
    amount_cents     BIGINT      NOT NULL,
    currency         TEXT        NOT NULL,
    status           TEXT        NOT NULL CHECK (status IN
                        ('HELD', 'PAYMENT_PENDING', 'CONFIRMED', 'CANCELLED', 'EXPIRED', 'PAYMENT_FAILED')),
    idempotency_key  TEXT        NOT NULL,
    payment_ref      TEXT,
    -- HELD: hold deadline. PAYMENT_PENDING: deadline after which the payment
    -- reconciler asks the provider what happened.
    expires_at       TIMESTAMPTZ NOT NULL,
    version          INT         NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Client retries with the same key can never create a second reservation.
    CONSTRAINT reservations_idempotency UNIQUE (user_id, idempotency_key)
);
-- Partial indexes stay tiny: only rows the background workers need to scan.
CREATE INDEX IF NOT EXISTS idx_reservations_held_expiry
    ON reservations (expires_at) WHERE status = 'HELD';
CREATE INDEX IF NOT EXISTS idx_reservations_payment_pending
    ON reservations (expires_at) WHERE status = 'PAYMENT_PENDING';
CREATE INDEX IF NOT EXISTS idx_reservations_user
    ON reservations (user_id, created_at DESC);

-- Transactional outbox: state change and the event describing it commit
-- atomically. A relay publishes rows to Kafka afterwards (at-least-once).
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID        NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    aggregate_id  TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;

-- Stand-in for an external payment provider (Stripe/Adyen...). It lives in the
-- same database only so the demo is self-contained; the code talks to it
-- through the payment.Provider interface exactly as it would to a remote API.
CREATE TABLE IF NOT EXISTS mock_payment_provider (
    idempotency_key  TEXT PRIMARY KEY,
    amount_cents     BIGINT      NOT NULL,
    status           TEXT        NOT NULL CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED')),
    payment_ref      TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumer-side de-duplication (used by the Node.js notification-worker).
CREATE TABLE IF NOT EXISTS processed_events (
    consumer      TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    user_id     TEXT        NOT NULL,
    channel     TEXT        NOT NULL,
    template    TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
