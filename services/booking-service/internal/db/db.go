package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"booking-service/internal/resilience"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Postgres error codes we react to.
const (
	CodeUniqueViolation      = "23505"
	CodeCheckViolation       = "23514"
	CodeSerializationFailure = "40001"
	CodeDeadlockDetected     = "40P01"
	CodeLockNotAvailable     = "55P03"
	CodeAdminShutdown        = "57P01" // primary restarting / failover in progress
	CodeCannotConnectNow     = "57P03"
	CodeReadOnlyTransaction  = "25006" // we hit a demoted primary during failover
)

func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = maxConns / 4
	// Recycle connections so that after a failover / DNS flip every pod
	// converges on the new primary without a restart.
	cfg.MaxConnLifetime = 5 * time.Minute
	cfg.MaxConnLifetimeJitter = time.Minute
	cfg.MaxConnIdleTime = time.Minute
	cfg.HealthCheckPeriod = 10 * time.Second
	// statement/lock/idle-in-transaction timeouts are role settings (see
	// migrations/0002) so they survive PgBouncer transaction pooling.
	// Compatible with PgBouncer in transaction-pooling mode.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// IsRetryable reports whether a failed transaction can be safely re-run from
// the start: lost races (serialization/deadlock/lock timeout) and transient
// connectivity problems such as a primary failover.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case CodeSerializationFailure, CodeDeadlockDetected, CodeLockNotAvailable,
			CodeAdminShutdown, CodeCannotConnectNow, CodeReadOnlyTransaction:
			return true
		}
		return false
	}
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == CodeUniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

var txRetry = resilience.RetryPolicy{Attempts: 4, BaseDelay: 20 * time.Millisecond, MaxDelay: 300 * time.Millisecond}

// WithTx runs fn in a READ COMMITTED transaction and retries the whole
// transaction on retryable errors. fn must therefore be free of side effects
// outside the transaction.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return resilience.Retry(ctx, txRetry, IsRetryable, func(ctx context.Context) error {
		return pgx.BeginFunc(ctx, pool, fn)
	})
}

// Migrate applies embedded migrations exactly once even when many replicas
// start at the same moment: a session-level advisory lock serialises them.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	const lockKey = 7_242_001
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey) //nolint:errcheck

	// Schema changes may legitimately run longer than the app's 3s limit.
	if _, err := conn.Exec(ctx, `SET statement_timeout = 0; SET lock_timeout = '30s'`); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, f).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrationFS.ReadFile(f)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, f)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", f, err)
		}
	}
	return nil
}
