package pipeline

import (
	"sync"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// ProgressHub keeps the latest progress for running (and finished) jobs in memory.
type ProgressHub struct {
	mu   sync.RWMutex
	byID map[domain.JobID]domain.Progress
}

// NewProgressHub returns an empty hub.
func NewProgressHub() *ProgressHub {
	return &ProgressHub{byID: make(map[domain.JobID]domain.Progress)}
}

// Set stores the latest progress snapshot for a job.
func (h *ProgressHub) Set(id domain.JobID, p domain.Progress) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.byID[id] = p
	h.mu.Unlock()
}

// Get returns the latest progress if we have one.
func (h *ProgressHub) Get(id domain.JobID) (domain.Progress, bool) {
	if h == nil {
		return domain.Progress{}, false
	}
	h.mu.RLock()
	p, ok := h.byID[id]
	h.mu.RUnlock()
	return p, ok
}

// Delete removes a job's progress entry.
func (h *ProgressHub) Delete(id domain.JobID) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.byID, id)
	h.mu.Unlock()
}
