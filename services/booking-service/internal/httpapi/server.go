package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"booking-service/internal/booking"
	"booking-service/internal/catalog"
	"booking-service/internal/inventory"
	"booking-service/internal/metrics"
)

type Server struct {
	svc           *booking.Service
	catalog       *catalog.Catalog
	gate          *inventory.Gate
	pool          *pgxpool.Pool
	internalToken string
	adminKey      string
	timeout       time.Duration
	maxInFlight   int64

	inFlight atomic.Int64
	draining atomic.Bool
}

type Options struct {
	InternalToken  string
	AdminKey       string
	RequestTimeout time.Duration
	MaxInFlight    int
}

func New(svc *booking.Service, cat *catalog.Catalog, gate *inventory.Gate, pool *pgxpool.Pool, o Options) *Server {
	return &Server{svc: svc, catalog: cat, gate: gate, pool: pool, internalToken: o.InternalToken,
		adminKey: o.AdminKey, timeout: o.RequestTimeout, maxInFlight: int64(o.MaxInFlight)}
}

// Drain flips readiness to false so the load balancer stops routing new
// traffic before we stop accepting connections (graceful shutdown).
func (s *Server) Drain() { s.draining.Store(true) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())

	api := http.NewServeMux()
	api.HandleFunc("GET /v1/events", s.listEvents)
	api.HandleFunc("GET /v1/events/{id}", s.getEvent)
	api.HandleFunc("POST /v1/reservations", s.reserve)
	api.HandleFunc("GET /v1/reservations/{id}", s.getReservation)
	api.HandleFunc("POST /v1/reservations/{id}/confirm", s.confirm)
	api.HandleFunc("POST /v1/reservations/{id}/cancel", s.cancel)
	api.HandleFunc("POST /v1/admin/events", s.createEvent)
	mux.Handle("/v1/", s.middleware(api))
	return mux
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic", "err", rec, "path", r.URL.Path)
				writeErr(w, http.StatusInternalServerError, "internal error")
			}
		}()

		// Load shedding: past the in-flight limit, answer 503 immediately.
		// A fast "no" keeps latency flat for admitted requests and lets the
		// gateway/clients back off, instead of every request timing out.
		if n := s.inFlight.Add(1); n > s.maxInFlight {
			s.inFlight.Add(-1)
			metrics.HTTPShed.Inc()
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusServiceUnavailable, "overloaded")
			return
		}
		defer s.inFlight.Add(-1)

		if s.internalToken != "" &&
			subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Token")), []byte(s.internalToken)) != 1 {
			writeErr(w, http.StatusUnauthorized, "missing internal token")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
		rw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		req := r.WithContext(ctx)
		next.ServeHTTP(rw, req)

		route := req.Pattern // set by the inner mux on the request it received
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rw.code)).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeErr(w, http.StatusServiceUnavailable, "draining")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	// Only the primary DB gates readiness. Redis being down is a degraded
	// mode we tolerate, not a reason to pull every pod out of rotation.
	if err := s.pool.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "in_flight": s.inFlight.Load()})
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	evs, err := s.catalog.List(r.Context(), 100)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "catalog unavailable")
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=5, stale-while-revalidate=30")
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusNotFound, "event not found")
		return
	}
	ev, err := s.catalog.Event(r.Context(), id)
	if errors.Is(err, catalog.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "event not found")
		return
	} else if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "catalog unavailable")
		return
	}
	// Copy, then attach near-real-time availability from the Redis gate.
	out := *ev
	out.Tiers = append([]catalog.Tier(nil), ev.Tiers...)
	ids := make([]int64, len(out.Tiers))
	for i, t := range out.Tiers {
		ids[i] = t.ID
	}
	avail := s.gate.Available(r.Context(), ids)
	for i := range out.Tiers {
		if n, ok := avail[out.Tiers[i].ID]; ok {
			n := max(n, 0)
			out.Tiers[i].Available = &n
		}
	}
	// Short CDN TTL: availability is advisory, the reserve call is the truth.
	w.Header().Set("Cache-Control", "public, max-age=2, stale-while-revalidate=10")
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	var req booking.ReserveRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.UserID = r.Header.Get("X-User-Id")
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	if req.UserID == "" {
		writeErr(w, http.StatusUnauthorized, "missing user")
		return
	}
	if req.IdempotencyKey == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	res, replayed, err := s.svc.Reserve(r.Context(), req)
	if err != nil {
		writeDomainErr(w, err, nil)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, res)
		return
	}
	writeJSON(w, http.StatusCreated, res)
}

func (s *Server) getReservation(w http.ResponseWriter, r *http.Request) {
	res, err := s.svc.Get(r.Context(), r.Header.Get("X-User-Id"), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) confirm(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PaymentToken string `json:"payment_token"`
	}
	if err := decode(r, &body); err != nil || body.PaymentToken == "" {
		writeErr(w, http.StatusBadRequest, "payment_token is required")
		return
	}
	res, err := s.svc.Confirm(r.Context(), r.Header.Get("X-User-Id"), r.PathValue("id"), body.PaymentToken)
	if err != nil {
		writeDomainErr(w, err, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	res, err := s.svc.Cancel(r.Context(), r.Header.Get("X-User-Id"), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) createEvent(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(s.adminKey)) != 1 {
		writeErr(w, http.StatusForbidden, "forbidden")
		return
	}
	var in catalog.NewEvent
	if err := decode(r, &in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ev, err := s.catalog.Create(r.Context(), in)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ids := make([]int64, len(ev.Tiers))
	for i, t := range ev.Tiers {
		ids[i] = t.ID
	}
	// Warm the Redis gate before the sale opens.
	if err := s.svc.SyncGate(r.Context(), ids, "create"); err != nil {
		slog.Warn("warming gate failed; reconciler will retry", "err", err)
	}
	writeJSON(w, http.StatusCreated, ev)
}

// writeDomainErr maps business errors to HTTP semantics. Retryable conditions
// (overload, contention, provider down) get 503/429 + Retry-After so
// well-behaved clients back off instead of hammering.
func writeDomainErr(w http.ResponseWriter, err error, res *booking.Reservation) {
	type body struct {
		Error       string               `json:"error"`
		Reservation *booking.Reservation `json:"reservation,omitempty"`
	}
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, booking.ErrInvalidRequest):
		code = http.StatusBadRequest
	case errors.Is(err, booking.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, booking.ErrSoldOut), errors.Is(err, booking.ErrReservationExpired),
		errors.Is(err, booking.ErrInvalidState), errors.Is(err, booking.ErrSaleNotStarted):
		code = http.StatusConflict
	case errors.Is(err, booking.ErrQuotaExceeded):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, booking.ErrPaymentDeclined):
		code = http.StatusPaymentRequired
	case errors.Is(err, booking.ErrPaymentInProgress):
		code = http.StatusAccepted
	case errors.Is(err, booking.ErrOverloaded), errors.Is(err, booking.ErrInventoryContention),
		errors.Is(err, booking.ErrPaymentUnavailable), errors.Is(err, context.DeadlineExceeded):
		code = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
	}
	msg := err.Error()
	if code == http.StatusInternalServerError {
		slog.Error("request failed", "err", err)
		msg = "internal error"
	}
	writeJSON(w, code, body{Error: msg, Reservation: res})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 64<<10))
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}
