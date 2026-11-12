package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
)

func TestRunnerCompletes(t *testing.T) {
	store := memory.NewJobStore()
	runner := NewRunner(store)
	runner.Sleep = 10 * time.Millisecond

	job, err := store.Create(context.Background(), domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "a", Kind: domain.SourceCSV, Path: "a.csv"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runner.Start(job)

	deadline := time.Now().Add(time.Second)
	for {
		got, err := store.Get(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == domain.StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s", got.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}

	runner.Shutdown(context.Background())
}

func TestRunnerCancel(t *testing.T) {
	store := memory.NewJobStore()
	runner := NewRunner(store)
	runner.Sleep = time.Hour

	job, err := store.Create(context.Background(), domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "a", Kind: domain.SourceCSV, Path: "a.csv"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runner.Start(job)

	deadline := time.Now().Add(time.Second)
	for {
		got, err := store.Get(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == domain.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never running: %s", got.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := store.UpdateStatus(context.Background(), job.ID, domain.StatusCancelling); err != nil {
		t.Fatal(err)
	}
	runner.Cancel(job.ID)

	deadline = time.Now().Add(time.Second)
	for {
		got, err := store.Get(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == domain.StatusCancelled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s", got.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}

	runner.Shutdown(context.Background())
}
