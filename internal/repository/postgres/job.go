package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// JobStore stores jobs in Postgres.
type JobStore struct {
	db *sql.DB
}

// NewJobStore returns a Postgres job repository.
func NewJobStore(db *sql.DB) *JobStore {
	return &JobStore{db: db}
}

var _ repository.JobRepository = (*JobStore)(nil)

// Create inserts a pending job.
func (s *JobStore) Create(ctx context.Context, job domain.Job) (domain.Job, error) {
	if job.ID == "" {
		job.ID = domain.JobID(fmt.Sprintf("job-%d", time.Now().UnixNano()))
	}
	if job.Status == "" {
		job.Status = domain.StatusPending
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	raw, err := json.Marshal(job.Config)
	if err != nil {
		return domain.Job{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, status, config_json, created_at, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		job.ID, job.Status, raw, job.CreatedAt.UTC(), job.StartedAt, job.FinishedAt,
	)
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Get returns one job.
func (s *JobStore) Get(ctx context.Context, id domain.JobID) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, status, config_json, created_at, started_at, finished_at
		FROM jobs WHERE id = $1`, id)
	return scanJob(row)
}

// List returns all jobs.
func (s *JobStore) List(ctx context.Context) ([]domain.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, status, config_json, created_at, started_at, finished_at
		FROM jobs
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

// UpdateStatus applies a legal status transition.
func (s *JobStore) UpdateStatus(ctx context.Context, id domain.JobID, status domain.JobStatus) (domain.Job, error) {
	job, err := s.Get(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	if !job.Status.CanTransitionTo(status) {
		return domain.Job{}, repository.ErrInvalidTransition
	}
	now := time.Now().UTC()
	job.Status = status
	if status == domain.StatusRunning && job.StartedAt == nil {
		job.StartedAt = &now
	}
	if status == domain.StatusCancelled || status == domain.StatusSucceeded || status == domain.StatusFailed || status == domain.StatusPartial {
		job.FinishedAt = &now
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE jobs SET status = $1, started_at = $2, finished_at = $3 WHERE id = $4`,
		job.Status, job.StartedAt, job.FinishedAt, job.ID,
	)
	if err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// Delete removes a job and cascaded child rows.
func (s *JobStore) Delete(ctx context.Context, id domain.JobID) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return repository.ErrNotFound
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (domain.Job, error) {
	var (
		job domain.Job
		raw []byte
	)
	err := row.Scan(&job.ID, &job.Status, &raw, &job.CreatedAt, &job.StartedAt, &job.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, repository.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	if err := json.Unmarshal(raw, &job.Config); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}
