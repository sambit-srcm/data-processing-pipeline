package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

const defaultSleep = 50 * time.Millisecond

// Runner starts each job in a goroutine and supports cancellation.
type Runner struct {
	jobs repository.JobRepository
	// Sleep slows the stub coordinator step (also used by cancel tests).
	Sleep time.Duration

	mu      sync.Mutex
	cancels map[domain.JobID]context.CancelFunc
	wg      sync.WaitGroup
}

// NewRunner returns a job runner.
func NewRunner(jobs repository.JobRepository) *Runner {
	return &Runner{
		jobs:    jobs,
		Sleep:   defaultSleep,
		cancels: make(map[domain.JobID]context.CancelFunc),
	}
}

// Start marks the job running and executes the coordinator stub.
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

		// Day 4: one stub record until real sources exist (Day 5).
		recs := []domain.Record{{
			ID:       "stub-1",
			JobID:    job.ID,
			SourceID: "stub",
			Payload:  domain.COVIDCase{Date: "1970-01-01", Region: "stub"},
		}}
		coord := Coordinator{Sleep: sleep}
		_, err := coord.Run(ctx, job.Config, recs)

		status := domain.StatusSucceeded
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				status = domain.StatusCancelled
			} else {
				status = domain.StatusFailed
				slog.Error("job run", "id", job.ID, "err", err)
			}
		}
		if _, err := r.jobs.UpdateStatus(context.Background(), job.ID, status); err != nil {
			slog.Debug("job finish", "id", job.ID, "err", err)
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
