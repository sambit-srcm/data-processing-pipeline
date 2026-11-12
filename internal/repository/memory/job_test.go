package memory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

func newTestJob() domain.Job {
	return domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "covid", Kind: domain.SourceCSV, Path: "a.csv"}},
		},
	}
}

func TestCreateGetDelete(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()

	job, err := store.Create(ctx, newTestJob())
	if err != nil {
		t.Fatal(err)
	}
	if job.ID == "" || job.Status != domain.StatusPending {
		t.Fatalf("bad create: %+v", job)
	}

	got, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID {
		t.Fatalf("got %s", got.ID)
	}

	if err := store.Delete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, job.ID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestUpdateStatus(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()
	job, err := store.Create(ctx, newTestJob())
	if err != nil {
		t.Fatal(err)
	}

	running, err := store.UpdateStatus(ctx, job.ID, domain.StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != domain.StatusRunning || running.StartedAt == nil {
		t.Fatalf("%+v", running)
	}

	if _, err := store.UpdateStatus(ctx, job.ID, domain.StatusPending); !errors.Is(err, repository.ErrInvalidTransition) {
		t.Fatalf("want invalid transition, got %v", err)
	}

	done, err := store.UpdateStatus(ctx, job.ID, domain.StatusSucceeded)
	if err != nil {
		t.Fatal(err)
	}
	if done.FinishedAt == nil {
		t.Fatal("expected FinishedAt")
	}
}

func TestConcurrentUpdates(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()

	const n = 20
	ids := make([]domain.JobID, n)
	for i := 0; i < n; i++ {
		job, err := store.Create(ctx, newTestJob())
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = job.ID
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id domain.JobID) {
			defer wg.Done()
			if _, err := store.UpdateStatus(ctx, id, domain.StatusRunning); err != nil {
				t.Errorf("%v", err)
			}
		}(ids[i])
	}
	wg.Wait()
}
