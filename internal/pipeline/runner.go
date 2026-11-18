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
	"github.com/sambit-srcm/data-processing-pipeline/internal/observability"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/aggregation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/export"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/transformation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/validation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
	csvsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/csv"
	httpsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/http"
	jsonsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/json"
)

const defaultSleep = 50 * time.Millisecond

// ArtifactStore saves and loads pipeline outputs for a job.
type ArtifactStore interface {
	SaveProgress(ctx context.Context, id domain.JobID, p domain.Progress) error
	GetProgress(ctx context.Context, id domain.JobID) (domain.Progress, error)
	SaveErrors(ctx context.Context, errs []domain.PipelineError) error
	ListErrors(ctx context.Context, id domain.JobID) ([]domain.PipelineError, error)
	SaveAggregations(ctx context.Context, aggs []domain.Aggregation) error
	ListAggregations(ctx context.Context, id domain.JobID) ([]domain.Aggregation, error)
	DeleteJob(ctx context.Context, id domain.JobID) error
}

// Runner starts each job in a goroutine and supports cancellation.
type Runner struct {
	jobs repository.JobRepository
	// Sleep slows validation (used by cancel tests).
	Sleep time.Duration
	// Weather fetches HTTP weather sources. Optional in tests.
	Weather httpsources.Client
	// Artifacts optionally persists progress, errors, and aggregations.
	Artifacts ArtifactStore
	// Progress is the live in-memory progress view used by the API.
	Progress *ProgressHub

	mu      sync.Mutex
	cancels map[domain.JobID]context.CancelFunc
	wg      sync.WaitGroup
}

// NewRunner returns a job runner.
func NewRunner(jobs repository.JobRepository) *Runner {
	return &Runner{
		jobs:     jobs,
		Sleep:    defaultSleep,
		Progress: NewProgressHub(),
		cancels:  make(map[domain.JobID]context.CancelFunc),
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

		started := time.Now()
		observability.JobStarted()

		if _, err := r.jobs.UpdateStatus(ctx, job.ID, domain.StatusRunning); err != nil {
			slog.Error("job start", "id", job.ID, "err", err)
			observability.JobFinished(string(domain.StatusFailed), time.Since(started))
			return
		}

		recs, sourceFails, err := r.loadRecords(ctx, job)
		if err != nil {
			slog.Error("load records", "id", job.ID, "err", err)
			_, _ = r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusFailed)
			observability.JobFinished(string(domain.StatusFailed), time.Since(started))
			return
		}

		sleep := r.Sleep
		if sleep <= 0 {
			sleep = defaultSleep
		}
		coord := Coordinator{
			Sleep:       sleep,
			Validator:   validation.Router{},
			Transformer: transformation.Normalize{},
			Aggregator:  aggregation.NewMemory(),
			OnProgress: func(p domain.Progress) {
				r.Progress.Set(job.ID, p)
			},
		}
		result, err := coord.Run(ctx, job.Config, recs)
		r.finish(ctx, job, sourceFails, result, err, started)
	}()
}

func (r *Runner) finish(ctx context.Context, job domain.Job, sourceFails int, result Result, runErr error, started time.Time) {
	id := job.ID
	status := domain.StatusSucceeded
	switch {
	case runErr != nil && (errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)):
		status = domain.StatusCancelled
	case runErr != nil:
		status = domain.StatusFailed
		slog.Error("job run", "id", id, "err", runErr)
	case sourceFails > 0 || (result.Progress.Failed > 0 && result.Progress.Valid > 0):
		status = domain.StatusPartial
	case result.Progress.Valid == 0 && result.Progress.Failed > 0:
		status = domain.StatusFailed
	}

	if status == domain.StatusSucceeded || status == domain.StatusPartial {
		if err := export.WriteAll(context.Background(), job.Config.Exports, result.Records, result.Aggregations); err != nil {
			slog.Error("export", "id", id, "err", err)
			status = domain.StatusFailed
		}
	}

	r.Progress.Set(id, result.Progress)

	if r.Artifacts != nil {
		if err := r.Artifacts.SaveProgress(context.Background(), id, result.Progress); err != nil {
			slog.Error("save progress", "id", id, "err", err)
		}
		if err := r.Artifacts.SaveErrors(context.Background(), result.Errors); err != nil {
			slog.Error("save errors", "id", id, "err", err)
		}
		if err := r.Artifacts.SaveAggregations(context.Background(), result.Aggregations); err != nil {
			slog.Error("save aggregations", "id", id, "err", err)
		}
	}

	observability.JobFinished(string(status), time.Since(started))

	if _, err := r.jobs.UpdateStatus(context.Background(), id, status); err != nil {
		slog.Debug("job finish", "id", id, "err", err)
	}
}

// LatestProgress returns live progress when available.
func (r *Runner) LatestProgress(id domain.JobID) (domain.Progress, bool) {
	return r.Progress.Get(id)
}

// ClearProgress drops a job's live progress entry.
func (r *Runner) ClearProgress(id domain.JobID) {
	r.Progress.Delete(id)
}

func (r *Runner) loadRecords(ctx context.Context, job domain.Job) ([]domain.Record, int, error) {
	var out []domain.Record
	fails := 0
	for _, src := range job.Config.Sources {
		recs, err := r.loadOne(ctx, job, src)
		if err != nil {
			slog.Error("source failed", "job", job.ID, "source", src.ID, "err", err)
			fails++
			continue
		}
		out = append(out, recs...)
	}
	if len(out) == 0 {
		return nil, fails, errors.New("no records loaded")
	}
	return out, fails, nil
}

func (r *Runner) loadOne(ctx context.Context, job domain.Job, src domain.SourceConfig) ([]domain.Record, error) {
	switch src.Kind {
	case domain.SourceCSV:
		path, err := resolveDataPath(src.Path)
		if err != nil {
			return nil, err
		}
		return csvsources.ReadFile(path, job.ID, src.ID)
	case domain.SourceJSON:
		path, err := resolveDataPath(src.Path)
		if err != nil {
			return nil, err
		}
		return jsonsources.ReadFile(path, job.ID, src.ID)
	case domain.SourceHTTP:
		if src.URL == "" {
			return nil, errors.New("http source url is required")
		}
		return r.Weather.Fetch(ctx, src.URL, job.ID, src.ID)
	default:
		return nil, fmt.Errorf("unsupported source kind: %s", src.Kind)
	}
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
