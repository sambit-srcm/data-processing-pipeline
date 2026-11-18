package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestArtifactStore(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	db, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	jobs := NewJobStore(db)
	arts := NewArtifactStore(db)
	ctx := context.Background()

	job, err := jobs.Create(ctx, domain.Job{
		Config: domain.PipelineConfig{
			Workers: domain.Workers{Validation: 1, Transformation: 1},
			Sources: []domain.SourceConfig{{ID: "c", Kind: domain.SourceCSV, Path: "x.csv"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Delete(context.Background(), job.ID) })

	now := time.Now().UTC()
	if err := arts.SaveErrors(ctx, []domain.PipelineError{{
		JobID: job.ID, SourceID: "c", RecordID: "r1", Stage: domain.StageValidation, Message: "bad", At: now,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := arts.SaveAggregations(ctx, []domain.Aggregation{{
		JobID: job.ID, SourceID: "c", GroupKey: "all", Count: 3, Sum: 6, Min: 1, Max: 3,
	}}); err != nil {
		t.Fatal(err)
	}

	var errCount, aggCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_errors WHERE job_id = $1`, job.ID).Scan(&errCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_aggregations WHERE job_id = $1`, job.ID).Scan(&aggCount); err != nil {
		t.Fatal(err)
	}
	if errCount != 1 || aggCount != 1 {
		t.Fatalf("errors=%d aggs=%d", errCount, aggCount)
	}

	if err := jobs.Delete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_errors WHERE job_id = $1`, job.ID).Scan(&errCount); err != nil {
		t.Fatal(err)
	}
	if errCount != 0 {
		t.Fatalf("cascade failed, errors left=%d", errCount)
	}
}
