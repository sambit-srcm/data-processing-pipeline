package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestJobStore(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	db, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store := NewJobStore(db)
	ctx := context.Background()
	job, err := store.Create(ctx, domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "c", Kind: domain.SourceCSV, Path: "x.csv"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), job.ID) })

	running, err := store.UpdateStatus(ctx, job.ID, domain.StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != domain.StatusRunning {
		t.Fatalf("status = %s", running.Status)
	}
	got, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID {
		t.Fatalf("id = %s", got.ID)
	}
	list, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, j := range list {
		if j.ID == job.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("created job missing from List")
	}
}
