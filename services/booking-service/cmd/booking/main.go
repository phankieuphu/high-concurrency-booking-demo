// Command booking runs the booking service.
//
//	booking api      HTTP API (stateless, scale horizontally behind the gateway)
//	booking worker   background loops (expiry, payment reconcile, outbox relay, gate resync)
//	booking all      both, in one process (local development)
//	booking migrate  apply DB migrations and exit (run as a k8s Job / init step)
//	booking seed     create a demo flash-sale event and exit
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"booking-service/internal/booking"
	"booking-service/internal/catalog"
	"booking-service/internal/config"
	"booking-service/internal/db"
	"booking-service/internal/events"
	"booking-service/internal/httpapi"
	"booking-service/internal/inventory"
	"booking-service/internal/metrics"
	"booking-service/internal/payment"
	"booking-service/internal/resilience"
	"booking-service/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if err := run(mode); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(mode string) error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connectWithRetry(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	if mode == "migrate" || mode == "all" {
		if err := db.Migrate(ctx, pool); err != nil {
			return err
		}
		slog.Info("migrations applied")
		if mode == "migrate" {
			return nil
		}
	}

	var replica *pgxpool.Pool
	if cfg.DatabaseReplicaURL != "" {
		if replica, err = db.Connect(ctx, cfg.DatabaseReplicaURL, cfg.DBMaxConns); err != nil {
			slog.Warn("read replica unavailable, reading from primary", "err", err)
			replica = nil
		} else {
			defer replica.Close()
		}
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    cfg.RedisAddrs, // 1 addr = standalone/sentinel-proxy, N = cluster
		Password: cfg.RedisPassword,
		// Tight timeouts: Redis is an optimisation, never wait long for it.
		DialTimeout:  300 * time.Millisecond,
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		PoolSize:     200,
		MaxRetries:   1,
	})
	defer rdb.Close()

	observe := func(name string, _, to resilience.State) {
		metrics.BreakerState.WithLabelValues(name).Set(float64(to))
		slog.Warn("circuit breaker state change", "breaker", name, "state", to.String())
	}
	redisBreaker := resilience.NewBreaker("redis", 5, 3*time.Second)
	redisBreaker.OnStateChange(observe)
	paymentBreaker := resilience.NewBreaker("payment", 10, 10*time.Second)
	paymentBreaker.OnStateChange(observe)

	cat := catalog.New(pool, replica, 5*time.Second)
	gate := inventory.NewGate(rdb, redisBreaker)
	payments := payment.NewClient(&payment.MockProvider{
		Pool: pool, FailureRate: cfg.PaymentFailureRate, Latency: 40 * time.Millisecond,
	}, paymentBreaker, cfg.PaymentTimeout)
	svc := booking.NewService(pool, cat, gate, payments,
		resilience.NewBulkhead(cfg.DBWriteConcurrency, cfg.DBAcquireTimeout),
		booking.Options{HoldTTL: cfg.HoldTTL, PaymentWindow: cfg.PaymentWindow})

	if mode == "seed" {
		return seed(ctx, cat, svc)
	}

	var wg sync.WaitGroup
	if mode == "worker" || mode == "all" {
		var pub events.Publisher = events.LogPublisher{}
		if len(cfg.KafkaBrokers) > 0 {
			pub = events.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
		}
		defer pub.Close()
		runner := worker.NewRunner(pool, svc, cat, pub, worker.Config{
			ExpiryInterval: cfg.ExpiryInterval, ReconcileInterval: cfg.ReconcileInterval,
			OutboxInterval: cfg.OutboxInterval, OutboxBatchSize: cfg.OutboxBatchSize,
			OutboxRetention: cfg.OutboxRetention,
		})
		wg.Add(1)
		go func() { defer wg.Done(); runner.Run(ctx) }()
		if mode == "worker" {
			// Workers still expose health + metrics.
			go serveOps(ctx, cfg.HTTPAddr)
		}
	}

	if mode == "api" || mode == "all" {
		api := httpapi.New(svc, cat, gate, pool, httpapi.Options{
			InternalToken: cfg.InternalToken, AdminKey: cfg.AdminKey,
			RequestTimeout: cfg.RequestTimeout, MaxInFlight: cfg.MaxInFlight,
		})
		srv := &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           api.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
			IdleTimeout:       90 * time.Second,
		}
		errCh := make(chan error, 1)
		go func() { errCh <- srv.ListenAndServe() }()
		slog.Info("booking api listening", "addr", cfg.HTTPAddr, "mode", mode)

		select {
		case err := <-errCh:
			if !errors.Is(err, http.ErrServerClosed) {
				return err
			}
		case <-ctx.Done():
		}
		// Graceful shutdown: fail readiness, give the LB time to notice, then
		// stop accepting and wait for in-flight requests to finish.
		api.Drain()
		time.Sleep(drainDelay())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("shutdown", "err", err)
		}
	}
	<-ctx.Done()
	wg.Wait()
	slog.Info("stopped")
	return nil
}

func drainDelay() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("DRAIN_DELAY")); err == nil {
		return d
	}
	return 5 * time.Second
}

// connectWithRetry tolerates the database starting after us (compose, k8s)
// or being mid-failover at boot.
func connectWithRetry(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		pool, err := db.Connect(ctx, url, maxConns)
		if err == nil {
			return pool, nil
		}
		lastErr = err
		slog.Warn("database not ready, retrying", "attempt", attempt+1, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("connect database: %w", lastErr)
}

func serveOps(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	_ = srv.ListenAndServe()
}

func seed(ctx context.Context, cat *catalog.Catalog, svc *booking.Service) error {
	ev, err := cat.Create(ctx, catalog.NewEvent{
		Name:        "Flash Sale Live 2026",
		Venue:       "National Stadium",
		StartsAt:    time.Now().Add(30 * 24 * time.Hour),
		MaxPerUser:  4,
		WaitingRoom: true,
		Tiers: []catalog.NewTier{
			{Name: "VIP", PriceCents: 25000, Total: 2000},
			{Name: "GA", PriceCents: 8000, Total: 48000},
		},
	})
	if err != nil {
		return err
	}
	ids := make([]int64, len(ev.Tiers))
	for i, t := range ev.Tiers {
		ids[i] = t.ID
	}
	if err := svc.SyncGate(ctx, ids, "seed"); err != nil {
		slog.Warn("warming gate failed", "err", err)
	}
	out, _ := json.MarshalIndent(ev, "", "  ")
	fmt.Println(string(out))
	return nil
}
