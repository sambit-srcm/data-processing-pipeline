package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

const defaultSleep = 50 * time.Millisecond

// Runner starts stub jobs in goroutines and supports cancellation.
type Runner struct {
	jobs repository.JobRepository
	// Sleep is how long a stub job runs before succeeding.
	Sleep time.Duration

	mu      sync.Mutex
	cancels map[domain.JobID]context.CancelFunc
	wg      sync.WaitGroup
}

// NewRunner returns a stub job runner.
func NewRunner(jobs repository.JobRepository) *Runner {
	return &Runner{
		jobs:    jobs,
		Sleep:   defaultSleep,
		cancels: make(map[domain.JobID]context.CancelFunc),
	}
}

// Start runs a job until sleep completes or the job is cancelled.
func (r *Runner) Start(job domain.Job) {
	ctx, cancel := context.WithCancel(context.Background())

	r.mu.Lock()
	r.cancels[job.ID] = cancel
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.clearCancel(job.ID)
		defer cancel()

		if _, err := r.jobs.UpdateStatus(ctx, job.ID, domain.StatusRunning); err != nil {
			slog.Error("job start", "id", job.ID, "err", err)
			return
		}

		sleep := r.Sleep
		if sleep <= 0 {
			sleep = defaultSleep
		}
		timer := time.NewTimer(sleep)
		defer timer.Stop()

		select {
		case <-timer.C:
			if _, err := r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusSucceeded); err != nil {
				slog.Debug("job finish", "id", job.ID, "err", err)
			}
		case <-ctx.Done():
			if _, err := r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusCancelled); err != nil {
				slog.Debug("job cancel finish", "id", job.ID, "err", err)
			}
		}
	}()
}

// Cancel requests cancellation of a running job.
func (r *Runner) Cancel(id domain.JobID) {
	r.mu.Lock()
	cancel, ok := r.cancels[id]
	r.mu.Unlock()
	if ok {
		cancel()
	}
}

// Shutdown cancels all running jobs and waits for their goroutines.
func (r *Runner) Shutdown(ctx context.Context) {
	r.mu.Lock()
	for _, cancel := range r.cancels {
		cancel()
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (r *Runner) clearCancel(id domain.JobID) {
	r.mu.Lock()
	delete(r.cancels, id)
	r.mu.Unlock()
}
