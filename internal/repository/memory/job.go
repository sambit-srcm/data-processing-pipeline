// Package memory is an in-memory job store for the POC.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// JobStore keeps jobs in memory.
type JobStore struct {
	mu   sync.Mutex
	seq  int
	jobs map[domain.JobID]domain.Job
}

// NewJobStore returns an empty store.
func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[domain.JobID]domain.Job)}
}

// Create adds a pending job.
func (s *JobStore) Create(_ context.Context, job domain.Job) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	if job.ID == "" {
		job.ID = domain.JobID(fmt.Sprintf("job-%d", s.seq))
	}
	if job.Status == "" {
		job.Status = domain.StatusPending
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}

	s.jobs[job.ID] = job
	return job, nil
}

// Get returns one job.
func (s *JobStore) Get(_ context.Context, id domain.JobID) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, repository.ErrNotFound
	}
	return job, nil
}

// List returns all jobs.
func (s *JobStore) List(_ context.Context) ([]domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]domain.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		out = append(out, job)
	}
	return out, nil
}

// UpdateStatus changes status when the transition is allowed.
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
	if status == domain.StatusRunning && job.StartedAt == nil {
		job.StartedAt = &now
	}
	if status == domain.StatusCancelled || status == domain.StatusSucceeded || status == domain.StatusFailed || status == domain.StatusPartial {
		job.FinishedAt = &now
	}

	s.jobs[id] = job
	return job, nil
}

// Delete removes a job.
func (s *JobStore) Delete(_ context.Context, id domain.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; !ok {
		return repository.ErrNotFound
	}
	delete(s.jobs, id)
	return nil
}
