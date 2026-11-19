package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
	httpsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/http"
)

func TestRunnerPartialWhenOneSourceFails(t *testing.T) {
	badWeather := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(badWeather.Close)

	store := memory.NewJobStore()
	arts := memory.NewArtifactStore()
	runner := NewRunner(store)
	runner.Sleep = 0
	runner.Weather = httpsources.Client{HTTP: badWeather.Client()}
	runner.Artifacts = arts

	job, err := store.Create(context.Background(), domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Buffers: domain.Buffers{Records: 8, Validated: 8, Transformed: 8, Errors: 8, Progress: 8},
			Sources: []domain.SourceConfig{
				{ID: "hw", Kind: domain.SourceCSV, Path: "samples/input/hw_sample.csv"},
				{ID: "weather", Kind: domain.SourceHTTP, URL: badWeather.URL},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runner.Start(job)
	waitJobStatus(t, store, job.ID, domain.StatusPartial)
	runner.Shutdown(context.Background())

	errs, err := arts.ListErrors(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range errs {
		if e.Stage == domain.StageIngestion && e.SourceID == "weather" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ingestion error for weather source, got %+v", errs)
	}
}

func waitJobStatus(t *testing.T, store repository.JobRepository, id domain.JobID, want domain.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s, want %s", got.Status, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
