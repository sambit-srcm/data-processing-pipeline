package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	// Built-in Go runtime + process stats (memory, GC, goroutines, CPU).
	registerCollector(collectors.NewGoCollector())
	registerCollector(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

func registerCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		// Safe if tests or another init already added the same collector.
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			panic(err)
		}
	}
}

// App metrics we bump ourselves.
var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "How many HTTP requests the API handled.",
	}, []string{"method", "path", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "How long each HTTP request took.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	jobsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_jobs_created_total",
		Help: "How many pipeline jobs were created.",
	})

	jobsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_jobs_finished_total",
		Help: "How many pipeline jobs finished, by final status.",
	}, []string{"status"})

	jobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pipeline_job_duration_seconds",
		Help:    "How long a pipeline job ran from start to finish.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"status"})

	jobsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipeline_jobs_running",
		Help: "How many pipeline jobs are running right now.",
	})
)

// Handler serves Prometheus scrape output at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}

// ObserveHTTP records one finished HTTP request.
func ObserveHTTP(method, path string, status int, d time.Duration) {
	httpRequests.WithLabelValues(method, path, strconv.Itoa(status)).Inc()
	httpDuration.WithLabelValues(method, path).Observe(d.Seconds())
}

// JobCreated records that a new job was accepted.
func JobCreated() {
	jobsCreated.Inc()
}

// JobStarted marks a job as currently running.
func JobStarted() {
	jobsRunning.Inc()
}

// JobFinished records a terminal job status and how long it ran.
func JobFinished(status string, d time.Duration) {
	jobsRunning.Dec()
	jobsFinished.WithLabelValues(status).Inc()
	jobDuration.WithLabelValues(status).Observe(d.Seconds())
}
