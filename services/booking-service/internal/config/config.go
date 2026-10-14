package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
	RequestTimeout  time.Duration
	// MaxInFlight caps concurrent HTTP requests per instance; excess requests
	// are shed with 503 instead of queueing and blowing up latency.
	MaxInFlight int
	// InternalToken, when set, must be presented by the gateway on every call.
	InternalToken string
	AdminKey      string

	DatabaseURL        string
	DatabaseReplicaURL string
	DBMaxConns         int32
	// DBWriteConcurrency is the bulkhead size for write transactions.
	DBWriteConcurrency int
	DBAcquireTimeout   time.Duration

	RedisAddrs    []string
	RedisPassword string

	KafkaBrokers []string
	KafkaTopic   string

	HoldTTL        time.Duration
	PaymentTimeout time.Duration
	PaymentWindow  time.Duration

	ExpiryInterval     time.Duration
	ReconcileInterval  time.Duration
	OutboxInterval     time.Duration
	OutboxBatchSize    int
	OutboxRetention    time.Duration
	PaymentFailureRate float64
}

func Load() Config {
	return Config{
		HTTPAddr:        env("HTTP_ADDR", ":8081"),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 20*time.Second),
		RequestTimeout:  dur("REQUEST_TIMEOUT", 5*time.Second),
		MaxInFlight:     integer("MAX_IN_FLIGHT", 2000),
		InternalToken:   env("INTERNAL_TOKEN", ""),
		AdminKey:        env("ADMIN_KEY", "dev-admin-key"),

		DatabaseURL:        env("DATABASE_URL", "postgres://booking:booking@localhost:5432/booking?sslmode=disable"),
		DatabaseReplicaURL: env("DATABASE_REPLICA_URL", ""),
		DBMaxConns:         int32(integer("DB_MAX_CONNS", 40)),
		DBWriteConcurrency: integer("DB_WRITE_CONCURRENCY", 64),
		DBAcquireTimeout:   dur("DB_ACQUIRE_TIMEOUT", 500*time.Millisecond),

		RedisAddrs:    list("REDIS_ADDRS", "localhost:6379"),
		RedisPassword: env("REDIS_PASSWORD", ""),

		KafkaBrokers: list("KAFKA_BROKERS", ""),
		KafkaTopic:   env("KAFKA_TOPIC", "booking.events"),

		HoldTTL:        dur("HOLD_TTL", 10*time.Minute),
		PaymentTimeout: dur("PAYMENT_TIMEOUT", 5*time.Second),
		PaymentWindow:  dur("PAYMENT_WINDOW", 2*time.Minute),

		ExpiryInterval:     dur("EXPIRY_INTERVAL", 2*time.Second),
		ReconcileInterval:  dur("RECONCILE_INTERVAL", 30*time.Second),
		OutboxInterval:     dur("OUTBOX_INTERVAL", 200*time.Millisecond),
		OutboxBatchSize:    integer("OUTBOX_BATCH_SIZE", 500),
		OutboxRetention:    dur("OUTBOX_RETENTION", 24*time.Hour),
		PaymentFailureRate: float("MOCK_PAYMENT_FAILURE_RATE", 0),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func integer(key string, def int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return def
}

func float(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return def
}

func dur(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil {
		return v
	}
	return def
}

func list(key, def string) []string {
	raw := env(key, def)
	if raw == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
