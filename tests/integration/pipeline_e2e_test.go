package integration_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/api"
	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	httpsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/http"
)

func TestE2E_HealthReadyMetrics(t *testing.T) {
	env := startEnv(t)

	var body struct {
		Status string `json:"status"`
	}
	if status := doJSON(t, http.MethodGet, env.URL+"/health", nil, &body); status != http.StatusOK || body.Status == "" {
		t.Fatalf("health failed: status=%d body=%+v", status, body)
	}
	if status := doJSON(t, http.MethodGet, env.URL+"/ready", nil, &body); status != http.StatusOK {
		t.Fatalf("ready status=%d", status)
	}

	resp, err := http.Get(env.URL + "/metrics") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d", resp.StatusCode)
	}
	text := string(raw)
	for _, want := range []string{"go_goroutines", "http_requests_total"} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}

func TestE2E_CSVJobProgressResultsErrors(t *testing.T) {
	env := startEnv(t)
	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
	})
	done := waitJob(t, env, job.ID, domain.StatusSucceeded, domain.StatusPartial)

	var progress api.ProgressView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/progress", nil, &progress); status != http.StatusOK {
		t.Fatalf("progress status=%d", status)
	}
	if progress.Processed == 0 || progress.PercentComplete == nil || progress.StartedAt == nil {
		t.Fatalf("bad progress: %+v", progress)
	}

	var results api.ResultsView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/results", nil, &results); status != http.StatusOK {
		t.Fatalf("results status=%d", status)
	}
	if results.RecordCount == 0 || results.Aggregations == nil {
		t.Fatalf("bad results: %+v", results)
	}

	var errs api.ErrorsView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/errors", nil, &errs); status != http.StatusOK {
		t.Fatalf("errors status=%d", status)
	}
	if errs.Errors == nil {
		t.Fatal("errors must be a list")
	}
}

func TestE2E_MultiSourceCSVJSONHTTP(t *testing.T) {
	env := startEnv(t)
	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{
			{ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv")},
			{ID: "crypto", Kind: domain.SourceJSON, Path: samplePath(t, "coingecko.json")},
			{ID: "weather", Kind: domain.SourceHTTP, URL: env.WeatherURL},
		},
		Aggregations: []domain.AggregationConfig{
			{Kind: domain.AggregationStats, GroupBy: "auto", ValueField: "auto"},
			{Kind: domain.AggregationGlobalDaily},
		},
	})
	done := waitJob(t, env, job.ID, domain.StatusSucceeded, domain.StatusPartial)

	var progress api.ProgressView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/progress", nil, &progress); status != http.StatusOK {
		t.Fatalf("progress status=%d", status)
	}
	if progress.Valid < 3 {
		t.Fatalf("expected multi-source valid records, got %d", progress.Valid)
	}

	var results api.ResultsView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/results", nil, &results); status != http.StatusOK {
		t.Fatalf("results status=%d", status)
	}
	if len(results.GlobalDaily) == 0 || results.GlobalDaily[0].TotalRecords == 0 {
		t.Fatalf("expected global_daily report, got %+v", results.GlobalDaily)
	}
	if len(results.GlobalDaily[0].Kinds) < 2 {
		t.Fatalf("expected multi-kind global daily, got %+v", results.GlobalDaily[0].Kinds)
	}
}

func TestE2E_ExportCSVAndJSON(t *testing.T) {
	env := startEnv(t)
	dir := t.TempDir()
	csvOut := filepath.Join(dir, "out.csv")
	jsonOut := filepath.Join(dir, "out.json")

	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
		Exports: []domain.ExportConfig{
			{Kind: domain.ExportCSV, Path: csvOut},
			{Kind: domain.ExportJSON, Path: jsonOut},
		},
	})
	done := waitJob(t, env, job.ID, domain.StatusSucceeded, domain.StatusPartial)
	if done.Status == domain.StatusFailed {
		t.Fatal("export job failed")
	}

	if _, err := os.Stat(csvOut); err != nil {
		t.Fatalf("csv export missing: %v", err)
	}
	if _, err := os.Stat(jsonOut); err != nil {
		t.Fatalf("json export missing: %v", err)
	}

	var results api.ResultsView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/results", nil, &results); status != http.StatusOK {
		t.Fatalf("results status=%d", status)
	}
	if len(results.Exports) != 2 {
		t.Fatalf("exports = %d", len(results.Exports))
	}
}

func TestE2E_CancelRunningJob(t *testing.T) {
	env := startEnv(t)
	env.Runner.Sleep = time.Hour

	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
	})
	waitJob(t, env, job.ID, domain.StatusRunning)

	var cancelled domain.Job
	status := doJSON(t, http.MethodPatch, env.URL+"/api/v1/pipelines/"+string(job.ID)+"/cancel", nil, &cancelled)
	if status != http.StatusOK {
		t.Fatalf("cancel status=%d", status)
	}
	if cancelled.Status != domain.StatusCancelled {
		t.Fatalf("status=%s", cancelled.Status)
	}
}

func TestE2E_DeleteJob(t *testing.T) {
	env := startEnv(t)
	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
	})
	done := waitJob(t, env, job.ID, domain.StatusSucceeded, domain.StatusPartial)

	status := doJSON(t, http.MethodDelete, env.URL+"/api/v1/pipelines/"+string(done.ID), nil, nil)
	if status != http.StatusNoContent {
		t.Fatalf("delete status=%d", status)
	}

	var missing api.ErrorBody
	status = doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID), nil, &missing)
	if status != http.StatusNotFound {
		t.Fatalf("get after delete status=%d", status)
	}
}

func TestE2E_PartialWhenHTTPSourceFails(t *testing.T) {
	env := startEnv(t)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)
	env.Runner.Weather = httpsources.Client{HTTP: bad.Client()}

	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{
			{ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv")},
			{ID: "weather", Kind: domain.SourceHTTP, URL: bad.URL},
		},
	})
	done := waitJob(t, env, job.ID, domain.StatusPartial)
	if done.Status != domain.StatusPartial {
		t.Fatalf("status=%s", done.Status)
	}
}

func TestE2E_ListAndGetJob(t *testing.T) {
	env := startEnv(t)
	created := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
	})
	waitJob(t, env, created.ID, domain.StatusSucceeded, domain.StatusPartial)

	var list []domain.Job
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines", nil, &list); status != http.StatusOK {
		t.Fatalf("list status=%d", status)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("list=%+v", list)
	}

	var got domain.Job
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(created.ID), nil, &got); status != http.StatusOK {
		t.Fatalf("get status=%d", status)
	}
	if got.ID != created.ID {
		t.Fatalf("got=%+v", got)
	}
}

func TestE2E_RejectBadConfig(t *testing.T) {
	env := startEnv(t)
	var body api.ErrorBody
	status := doJSON(t, http.MethodPost, env.URL+"/api/v1/pipelines", []byte(`{
		"workers": {"validation": 0, "transformation": 1},
		"sources": [{"id": "hw", "kind": "csv", "path": "x.csv"}]
	}`), &body)
	if status != http.StatusBadRequest || body.Error == "" {
		t.Fatalf("status=%d body=%+v", status, body)
	}
}
