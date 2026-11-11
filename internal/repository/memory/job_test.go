package memory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

func sampleJob() domain.Job {
	return domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "covid", Kind: domain.SourceCSV, Path: "samples/input/covid.csv"}},
		},
	}
}

func TestCreateGetListDelete(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()

	created, err := store.Create(ctx, sampleJob())
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Status != domain.StatusPending {
		t.Fatalf("created = %+v", created)
	}

	got, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID {
		t.Fatalf("get id = %s, want %s", got.ID, created.ID)
	}

	list, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("list len = %d", len(list))
	}

	if err := store.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, created.ID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("get after delete err = %v", err)
	}
}

func TestUpdateStatusTransitions(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()
	created, err := store.Create(ctx, sampleJob())
	if err != nil {
		t.Fatal(err)
	}

	running, err := store.UpdateStatus(ctx, created.ID, domain.StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != domain.StatusRunning || running.StartedAt == nil {
		t.Fatalf("running = %+v", running)
	}

	if _, err := store.UpdateStatus(ctx, created.ID, domain.StatusPending); !errors.Is(err, repository.ErrInvalidTransition) {
		t.Fatalf("invalid transition err = %v", err)
	}

	done, err := store.UpdateStatus(ctx, created.ID, domain.StatusSucceeded)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != domain.StatusSucceeded || done.FinishedAt == nil {
		t.Fatalf("done = %+v", done)
	}
}

func TestUpdateStatusNotFound(t *testing.T) {
	_, err := NewJobStore().UpdateStatus(context.Background(), "missing", domain.StatusRunning)
	if !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	ctx := context.Background()
	store := NewJobStore()

	const n = 50
	ids := make([]domain.JobID, n)
	for i := range n {
		job, err := store.Create(ctx, sampleJob())
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = job.ID
	}

	var wg sync.WaitGroup
	wg.Add(n * 3)
	for i := range n {
		id := ids[i]
		go func() {
			defer wg.Done()
			if _, err := store.UpdateStatus(ctx, id, domain.StatusRunning); err != nil {
				t.Errorf("running %s: %v", id, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := store.Get(ctx, id); err != nil {
				t.Errorf("get %s: %v", id, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := store.List(ctx); err != nil {
				t.Errorf("list: %v", err)
			}
		}()
	}
	wg.Wait()

	for _, id := range ids {
		job, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != domain.StatusRunning {
			t.Fatalf("job %s status = %s", id, job.Status)
		}
		if _, err := store.UpdateStatus(ctx, id, domain.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
}
