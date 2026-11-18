package memory

import (
	"context"
	"sync"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// ArtifactStore keeps progress, errors, and aggregations in memory.
type ArtifactStore struct {
	mu       sync.Mutex
	progress map[domain.JobID]domain.Progress
	errors   map[domain.JobID][]domain.PipelineError
	aggs     map[domain.JobID][]domain.Aggregation
}

// NewArtifactStore returns an empty in-memory artifact store.
func NewArtifactStore() *ArtifactStore {
	return &ArtifactStore{
		progress: make(map[domain.JobID]domain.Progress),
		errors:   make(map[domain.JobID][]domain.PipelineError),
		aggs:     make(map[domain.JobID][]domain.Aggregation),
	}
}

// SaveProgress stores the latest progress for a job.
func (s *ArtifactStore) SaveProgress(_ context.Context, id domain.JobID, p domain.Progress) error {
	s.mu.Lock()
	s.progress[id] = p
	s.mu.Unlock()
	return nil
}

// GetProgress returns saved progress for a job.
func (s *ArtifactStore) GetProgress(_ context.Context, id domain.JobID) (domain.Progress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.progress[id]
	if !ok {
		return domain.Progress{}, repository.ErrNotFound
	}
	return p, nil
}

// SaveErrors replaces the error list for a job.
func (s *ArtifactStore) SaveErrors(_ context.Context, errs []domain.PipelineError) error {
	if len(errs) == 0 {
		return nil
	}
	id := errs[0].JobID
	s.mu.Lock()
	s.errors[id] = append([]domain.PipelineError(nil), errs...)
	s.mu.Unlock()
	return nil
}

// ListErrors returns saved errors for a job.
func (s *ArtifactStore) ListErrors(_ context.Context, id domain.JobID) ([]domain.PipelineError, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]domain.PipelineError(nil), s.errors[id]...)
	return out, nil
}

// SaveAggregations replaces aggregation rows for a job.
func (s *ArtifactStore) SaveAggregations(_ context.Context, aggs []domain.Aggregation) error {
	if len(aggs) == 0 {
		return nil
	}
	id := aggs[0].JobID
	s.mu.Lock()
	s.aggs[id] = append([]domain.Aggregation(nil), aggs...)
	s.mu.Unlock()
	return nil
}

// ListAggregations returns saved aggregations for a job.
func (s *ArtifactStore) ListAggregations(_ context.Context, id domain.JobID) ([]domain.Aggregation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]domain.Aggregation(nil), s.aggs[id]...)
	return out, nil
}

// DeleteJob clears all artifacts for a job.
func (s *ArtifactStore) DeleteJob(_ context.Context, id domain.JobID) error {
	s.mu.Lock()
	delete(s.progress, id)
	delete(s.errors, id)
	delete(s.aggs, id)
	s.mu.Unlock()
	return nil
}
