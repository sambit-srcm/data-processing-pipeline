package postgres

import (
	"context"
	"database/sql"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// ArtifactStore saves pipeline errors and aggregations.
type ArtifactStore struct {
	db *sql.DB
}

// NewArtifactStore returns a Postgres artifact store.
func NewArtifactStore(db *sql.DB) *ArtifactStore {
	return &ArtifactStore{db: db}
}

// SaveErrors inserts pipeline errors for a job.
func (s *ArtifactStore) SaveErrors(ctx context.Context, errs []domain.PipelineError) error {
	for _, e := range errs {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO job_errors (job_id, source_id, record_id, stage, message, at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			e.JobID, e.SourceID, e.RecordID, e.Stage, e.Message, e.At.UTC(),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveAggregations inserts aggregation rows for a job.
func (s *ArtifactStore) SaveAggregations(ctx context.Context, aggs []domain.Aggregation) error {
	for _, a := range aggs {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO job_aggregations (job_id, source_id, group_key, count, sum, min, max)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			a.JobID, a.SourceID, a.GroupKey, a.Count, a.Sum, a.Min, a.Max,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
