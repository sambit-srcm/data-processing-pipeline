// Package memory is an in-memory JobRepository.
package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// JobStore is a mutex-guarded in-memory job repository.
type JobStore struct {
	mu   sync.RWMutex
	jobs map[domain.JobID]domain.Job
}

// NewJobStore returns an empty in-memory job store.
func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[domain.JobID]domain.Job)}
}

// Create stores a new pending job and assigns an id when missing.
func (s *JobStore) Create(_ context.Context, job domain.Job) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.ID == "" {
		id, err := newJobID()
		if err != nil {
			return domain.Job{}, err
		}
		job.ID = id
	}
	if job.Status == "" {
		job.Status = domain.StatusPending
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}

	s.jobs[job.ID] = cloneJob(job)
	return cloneJob(job), nil
}

// Get returns one job by id.
func (s *JobStore) Get(_ context.Context, id domain.JobID) (domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, repository.ErrNotFound
	}
	return cloneJob(job), nil
}

// List returns all jobs in insertion order is not guaranteed.
func (s *JobStore) List(_ context.Context) ([]domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]domain.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		out = append(out, cloneJob(job))
	}
	return out, nil
}

// UpdateStatus applies a legal status transition.
func (s *JobStore) UpdateStatus(_ context.Context, id domain.JobID, status domain.JobStatus) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, repository.ErrNotFound
	}
	if !job.Status.CanTransitionTo(status) {
		return domain.Job{}, repository.ErrInvalidTransition
	}

	now := time.Now().UTC()
	job.Status = status
	switch status {
	case domain.StatusRunning:
		if job.StartedAt == nil {
			job.StartedAt = &now
		}
	case domain.StatusCancelled, domain.StatusSucceeded, domain.StatusFailed, domain.StatusPartial:
		job.FinishedAt = &now
	}

	s.jobs[id] = cloneJob(job)
	return cloneJob(job), nil
}

// Delete removes a job by id.
func (s *JobStore) Delete(_ context.Context, id domain.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; !ok {
		return repository.ErrNotFound
	}
	delete(s.jobs, id)
	return nil
}

func newJobID() (domain.JobID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return domain.JobID(hex.EncodeToString(b[:])), nil
}

func cloneJob(job domain.Job) domain.Job {
	out := job
	if job.StartedAt != nil {
		t := *job.StartedAt
		out.StartedAt = &t
	}
	if job.FinishedAt != nil {
		t := *job.FinishedAt
		out.FinishedAt = &t
	}
	if job.Config.Sources != nil {
		out.Config.Sources = append([]domain.SourceConfig(nil), job.Config.Sources...)
	}
	if job.Config.Exports != nil {
		out.Config.Exports = append([]domain.ExportConfig(nil), job.Config.Exports...)
	}
	return out
}
