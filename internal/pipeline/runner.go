package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/validation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
	csvsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/csv"
)

const defaultSleep = 50 * time.Millisecond

// Runner starts each job in a goroutine and supports cancellation.
type Runner struct {
	jobs repository.JobRepository
	// Sleep slows validation (used by cancel tests).
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

// Start marks the job running and executes the coordinator.
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

		recs, err := loadRecords(job)
		if err != nil {
			slog.Error("load records", "id", job.ID, "err", err)
			_, _ = r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusFailed)
			return
		}

		sleep := r.Sleep
		if sleep <= 0 {
			sleep = defaultSleep
		}
		coord := Coordinator{Sleep: sleep, Validator: validation.COVID{}}
		result, err := coord.Run(ctx, job.Config, recs)

		status := domain.StatusSucceeded
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				status = domain.StatusCancelled
			} else {
				status = domain.StatusFailed
				slog.Error("job run", "id", job.ID, "err", err)
			}
		} else if result.Progress.Failed > 0 && result.Progress.Valid > 0 {
			status = domain.StatusPartial
		} else if result.Progress.Valid == 0 && result.Progress.Failed > 0 {
			status = domain.StatusFailed
		}

		if _, err := r.jobs.UpdateStatus(context.Background(), job.ID, status); err != nil {
			slog.Debug("job finish", "id", job.ID, "err", err)
		}
	}()
}

func loadRecords(job domain.Job) ([]domain.Record, error) {
	var out []domain.Record
	for _, src := range job.Config.Sources {
		switch src.Kind {
		case domain.SourceCSV:
			path, err := resolveDataPath(src.Path)
			if err != nil {
				return nil, err
			}
			recs, err := csvsources.ReadFile(path, job.ID, src.ID)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		default:
			return nil, errors.New("unsupported source kind: " + string(src.Kind))
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no records loaded")
	}
	return out, nil
}

func resolveDataPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, path)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return "", fmt.Errorf("file not found: %s", path)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("file not found: %s", path)
		}
		dir = parent
	}
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
