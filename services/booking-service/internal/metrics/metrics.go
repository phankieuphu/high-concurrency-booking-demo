package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route and status.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"route", "code"})

	HTTPShed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "http_requests_shed_total",
		Help: "Requests rejected by the in-flight limiter (load shedding).",
	})

	ReservationOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "booking_reserve_outcomes_total",
		Help: "Reservation attempts by outcome.",
	}, []string{"outcome"})

	GateDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "booking_gate_decisions_total",
		Help: "Redis inventory gate decisions (admitted / sold_out / bypassed).",
	}, []string{"decision"})

	Transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "booking_state_transitions_total",
		Help: "Reservation state transitions.",
	}, []string{"to"})

	BreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "0=closed 1=open 2=half-open.",
	}, []string{"name"})

	OutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_unpublished_events",
		Help: "Events waiting in the outbox (grows while Kafka is unavailable).",
	})

	GateDrift = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "inventory_gate_resync_total",
		Help: "Redis stock counters rewritten from Postgres.",
	}, []string{"reason"})
)
