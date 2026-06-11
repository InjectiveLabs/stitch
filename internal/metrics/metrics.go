// Package metrics defines the Prometheus collectors that every later phase
// will populate. Phase 0 registers the schema; the values stay zero until
// the corresponding code path lands.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const Namespace = "stitch"

var (
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "requests_total",
			Help:      "Number of requests handled, partitioned by protocol/method/backend/status.",
		},
		[]string{"protocol", "method_class", "backend", "status"},
	)
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "request_duration_seconds",
			Help:      "Request latency in seconds.",
			Buckets:   prometheus.ExponentialBucketsRange(0.001, 30, 14),
		},
		[]string{"protocol", "method_class", "backend"},
	)
	BackendHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backend_health",
			Help:      "1 if the backend is healthy for the protocol, else 0.",
		},
		[]string{"backend", "protocol"},
	)
	BackendLagBlocks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backend_lag_blocks",
			Help:      "Difference between max-known head and this backend's latest height.",
		},
		[]string{"backend"},
	)
	BackendLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  Namespace,
			Name:       "backend_latency_seconds",
			Help:       "Per-backend per-protocol request latency.",
			Objectives: map[float64]float64{0.5: 0.05, 0.95: 0.01, 0.99: 0.001},
			MaxAge:     prometheus.DefMaxAge,
		},
		[]string{"backend", "protocol"},
	)
	CircuitState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "circuit_state",
			Help:      "Circuit breaker state: 0=closed, 1=half-open, 2=open.",
		},
		[]string{"backend", "protocol"},
	)
	FailoverAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "failover_attempts_total",
			Help:      "Number of failover attempts, partitioned by from/to backend and reason.",
		},
		[]string{"from", "to", "reason"},
	)
	CacheTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "cache_total",
			Help:      "Cache lookups by layer and result.",
		},
		[]string{"layer", "result"},
	)
	SubscriptionsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "subscriptions_active",
			Help:      "Number of active subscriptions per protocol/kind.",
		},
		[]string{"protocol", "kind"},
	)
	SubscriptionResumes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "subscription_resumes_total",
			Help:      "Subscription resumption events by reason.",
		},
		[]string{"reason"},
	)
	SubscriptionGaps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "subscription_gaps_total",
			Help:      "Synthetic gap events emitted to clients.",
		},
		[]string{"protocol"},
	)
	SubscriptionDroppedNotifs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "subscription_dropped_notifications_total",
			Help:      "Upstream notifications dropped instead of forwarded, by protocol and reason.",
		},
		[]string{"protocol", "reason"},
	)
	InflightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "inflight_requests",
			Help:      "In-flight requests per protocol.",
		},
		[]string{"protocol"},
	)
	BroadcastFanout = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "broadcast_fanout_total",
			Help:      "Broadcast fan-out outcomes.",
		},
		[]string{"result"},
	)
	HedgeWins = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "hedge_wins_total",
			Help:      "Hedged-request wins by which candidate won.",
		},
		[]string{"method", "winner_index"},
	)
	RelayTruncated = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "relay_truncated_total",
			Help:      "Relayed responses whose upstream body failed mid-copy after headers were sent.",
		},
		[]string{"backend", "protocol"},
	)
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "build_info",
			Help:      "Always 1 — labels expose build metadata.",
		},
		[]string{"version", "commit", "goversion"},
	)
)

// Registry is the package-private registry; expose via Handler.
var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(
		RequestsTotal,
		RequestDuration,
		BackendHealth,
		BackendLagBlocks,
		BackendLatency,
		CircuitState,
		FailoverAttempts,
		CacheTotal,
		SubscriptionsActive,
		SubscriptionResumes,
		SubscriptionGaps,
		SubscriptionDroppedNotifs,
		InflightRequests,
		BroadcastFanout,
		HedgeWins,
		RelayTruncated,
		BuildInfo,
	)
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
}

// Handler returns an http.Handler that serves /metrics from the package
// registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Registry exposes the underlying registry for tests.
func Registry() *prometheus.Registry { return registry }

// SetBuildInfo records the build metadata gauge.
func SetBuildInfo(version, commit, goversion string) {
	BuildInfo.WithLabelValues(version, commit, goversion).Set(1)
}
