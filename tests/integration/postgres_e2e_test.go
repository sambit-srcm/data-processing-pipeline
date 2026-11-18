package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/api"
	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/postgres"
	httpsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/http"
)

func TestE2E_PostgresJobReports(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	weather := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"latitude":1,"longitude":2,"current":{"temperature_2m":10,"time":"2026-11-18T12:00"}}`))
	}))
	t.Cleanup(weather.Close)

	jobs := postgres.NewJobStore(db)
	arts := postgres.NewArtifactStore(db)
	runner := pipeline.NewRunner(jobs)
	runner.Artifacts = arts
	runner.Sleep = time.Millisecond
	runner.Weather = httpsources.Client{HTTP: weather.Client()}

	srv := httptest.NewServer(api.Handler(jobs, runner, arts))
	t.Cleanup(func() {
		runner.Shutdown(context.Background())
		srv.Close()
	})

	env := &testEnv{URL: srv.URL, Runner: runner}
	job := postJob(t, env, domain.PipelineConfig{
		Workers: defaultWorkers(),
		Buffers: defaultBuffers(),
		Sources: []domain.SourceConfig{{
			ID: "hw", Kind: domain.SourceCSV, Path: samplePath(t, "hw_sample.csv"),
		}},
	})
	done := waitJob(t, env, job.ID, domain.StatusSucceeded, domain.StatusPartial)
	t.Cleanup(func() { _ = jobs.Delete(context.Background(), done.ID) })

	var progress api.ProgressView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/progress", nil, &progress); status != http.StatusOK {
		t.Fatalf("progress status=%d", status)
	}
	if progress.Processed == 0 {
		t.Fatal("expected persisted progress")
	}

	var results api.ResultsView
	if status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(done.ID)+"/results", nil, &results); status != http.StatusOK {
		t.Fatalf("results status=%d", status)
	}
	if results.RecordCount == 0 {
		t.Fatal("expected persisted results")
	}
}
