// Package repository defines persistence contracts for pipeline jobs.
package repository

import (
	"context"
	"errors"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

var (
	// ErrNotFound means the job id does not exist.
	ErrNotFound = errors.New("job not found")
	// ErrInvalidTransition means the status change is not allowed.
	ErrInvalidTransition = errors.New("invalid job status transition")
)

// JobRepository stores and updates pipeline jobs.
type JobRepository interface {
	Create(ctx context.Context, job domain.Job) (domain.Job, error)
	Get(ctx context.Context, id domain.JobID) (domain.Job, error)
	List(ctx context.Context) ([]domain.Job, error)
	UpdateStatus(ctx context.Context, id domain.JobID, status domain.JobStatus) (domain.Job, error)
	Delete(ctx context.Context, id domain.JobID) error
}
