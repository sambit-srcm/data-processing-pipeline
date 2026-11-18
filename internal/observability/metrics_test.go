package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsHandler(t *testing.T) {
	JobCreated()
	JobStarted()
	ObserveHTTP(http.MethodGet, "/health", http.StatusOK, 5*time.Millisecond)
	JobFinished("succeeded", 20*time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"pipeline_jobs_created_total",
		"pipeline_jobs_finished_total",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}
