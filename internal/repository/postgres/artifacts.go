package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// ArtifactStore saves and loads pipeline progress, errors, and aggregations.
type ArtifactStore struct {
	db *sql.DB
}

// NewArtifactStore returns a Postgres artifact store.
func NewArtifactStore(db *sql.DB) *ArtifactStore {
	return &ArtifactStore{db: db}
}

// SaveProgress upserts the latest progress for a job.
func (s *ArtifactStore) SaveProgress(ctx context.Context, id domain.JobID, p domain.Progress) error {
	var total any
	if p.Total != nil {
		total = *p.Total
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO job_progress (job_id, total, processed, valid, failed, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (job_id) DO UPDATE SET
			total = EXCLUDED.total,
			processed = EXCLUDED.processed,
			valid = EXCLUDED.valid,
			failed = EXCLUDED.failed,
			started_at = EXCLUDED.started_at,
			finished_at = EXCLUDED.finished_at`,
		id, total, p.Processed, p.Valid, p.Failed, p.StartedAt.UTC(), p.FinishedAt,
	)
	return err
}

// GetProgress returns saved progress for a job.
func (s *ArtifactStore) GetProgress(ctx context.Context, id domain.JobID) (domain.Progress, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT total, processed, valid, failed, started_at, finished_at
		FROM job_progress WHERE job_id = $1`, id)

	var (
		p     domain.Progress
		total sql.NullInt64
	)
	err := row.Scan(&total, &p.Processed, &p.Valid, &p.Failed, &p.StartedAt, &p.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Progress{}, repository.ErrNotFound
	}
	if err != nil {
		return domain.Progress{}, err
	}
	if total.Valid {
		v := total.Int64
		p.Total = &v
	}
	return p, nil
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

// ListErrors returns saved errors for a job.
func (s *ArtifactStore) ListErrors(ctx context.Context, id domain.JobID) ([]domain.PipelineError, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, source_id, record_id, stage, message, at
		FROM job_errors WHERE job_id = $1 ORDER BY at, id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []domain.PipelineError
	for rows.Next() {
		var e domain.PipelineError
		if err := rows.Scan(&e.JobID, &e.SourceID, &e.RecordID, &e.Stage, &e.Message, &e.At); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
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

// ListAggregations returns saved aggregations for a job.
func (s *ArtifactStore) ListAggregations(ctx context.Context, id domain.JobID) ([]domain.Aggregation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, source_id, group_key, count, sum, min, max
		FROM job_aggregations WHERE job_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Aggregation
	for rows.Next() {
		var a domain.Aggregation
		if err := rows.Scan(&a.JobID, &a.SourceID, &a.GroupKey, &a.Count, &a.Sum, &a.Min, &a.Max); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteJob removes artifact rows for a job (also covered by FK cascade).
func (s *ArtifactStore) DeleteJob(ctx context.Context, id domain.JobID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM job_progress WHERE job_id = $1`, id)
	return err
}
