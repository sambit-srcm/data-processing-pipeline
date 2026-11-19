package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

		if len(job.Config.Sources) == 0 {
			slog.Error("load records", "id", job.ID, "err", "no sources")
			_, _ = r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusFailed)
			observability.JobFinished(string(domain.StatusFailed), time.Since(started))
			return
		}

		sleep := r.Sleep
		if sleep <= 0 {
			sleep = defaultSleep
		}
		transformer, err := transformation.FromConfig(job.Config.Transformations)
		if err != nil {
			slog.Error("transform config", "id", job.ID, "err", err)
			_, _ = r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusFailed)
			observability.JobFinished(string(domain.StatusFailed), time.Since(started))
			return
		}
		aggregator, err := aggregation.FromConfigs(job.Config.Aggregations...)
		if err != nil {
			slog.Error("aggregation config", "id", job.ID, "err", err)
			_, _ = r.jobs.UpdateStatus(context.Background(), job.ID, domain.StatusFailed)
			observability.JobFinished(string(domain.StatusFailed), time.Since(started))
			return
		}
		coord := Coordinator{
			Sleep:       sleep,
			Validator:   validation.Router{},
			Transformer: transformer,
			Aggregator:  aggregator,
			OnProgress: func(p domain.Progress) {
				r.Progress.Set(job.ID, p)
			},
		}

		var sourceFails atomic.Int64
		result, err := coord.RunIngest(ctx, job.Config, func(ctx context.Context, recordsCh chan<- domain.Record, errorsCh chan<- domain.PipelineError) {
			r.ingestSources(ctx, job, recordsCh, errorsCh, &sourceFails)
		})
		fails := int(sourceFails.Load())
		if err == nil && result.Progress.Processed == 0 && result.Progress.Failed == 0 && fails > 0 {
			err = errors.New("no records loaded")
		}
		r.finish(ctx, job, fails, result, err, started)
	}()
}

func (r *Runner) finish(ctx context.Context, job domain.Job, sourceFails int, result Result, runErr error, started time.Time) {
	id := job.ID
	status := finishStatus(ctx, id, sourceFails, result, runErr)
	persistCtx := context.WithoutCancel(ctx)

	exportDone := r.startExport(persistCtx, status, job, result)
	r.Progress.Set(id, result.Progress)
	r.saveArtifacts(persistCtx, id, result)

	if err := <-exportDone; err != nil {
		slog.Error("export", "id", id, "err", err)
		status = domain.StatusFailed
	}

	observability.JobFinished(string(status), time.Since(started))
	if _, err := r.jobs.UpdateStatus(persistCtx, id, status); err != nil {
		slog.Debug("job finish", "id", id, "err", err)
	}
}

func finishStatus(ctx context.Context, id domain.JobID, sourceFails int, result Result, runErr error) domain.JobStatus {
	switch {
	case runErr != nil && (errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)):
		return domain.StatusCancelled
	case runErr != nil:
		slog.Error("job run", "id", id, "err", runErr)
		return domain.StatusFailed
	case sourceFails > 0 || (result.Progress.Failed > 0 && result.Progress.Valid > 0):
		return domain.StatusPartial
	case result.Progress.Valid == 0 && result.Progress.Failed > 0:
		return domain.StatusFailed
	default:
		return domain.StatusSucceeded
	}
}

func (r *Runner) startExport(ctx context.Context, status domain.JobStatus, job domain.Job, result Result) <-chan error {
	done := make(chan error, 1)
	if status != domain.StatusSucceeded && status != domain.StatusPartial {
		close(done)
		return done
	}
	go func() {
		done <- export.WriteAll(ctx, job.Config.Exports, result.Records, result.Aggregations, result.GlobalDaily)
		close(done)
	}()
	return done
}

func (r *Runner) saveArtifacts(ctx context.Context, id domain.JobID, result Result) {
	if r.Artifacts == nil {
		return
	}
	if err := r.Artifacts.SaveProgress(ctx, id, result.Progress); err != nil {
		slog.Error("save progress", "id", id, "err", err)
	}
	if err := r.Artifacts.SaveErrors(ctx, result.Errors); err != nil {
		slog.Error("save errors", "id", id, "err", err)
	}
	if err := r.Artifacts.SaveAggregations(ctx, result.Aggregations); err != nil {
		slog.Error("save aggregations", "id", id, "err", err)
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

// ingestSources loads each source in its own goroutine and fans records into recordsCh.
// Source failures are reported on errorsCh as StageIngestion pipeline errors.
func (r *Runner) ingestSources(ctx context.Context, job domain.Job, recordsCh chan<- domain.Record, errorsCh chan<- domain.PipelineError, sourceFails *atomic.Int64) {
	sources := job.Config.Sources
	var wg sync.WaitGroup
	for _, src := range sources {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				sourceFails.Add(1)
				_ = send(ctx, errorsCh, domain.PipelineError{
					JobID:    job.ID,
					SourceID: src.ID,
					Stage:    domain.StageIngestion,
					Message:  err.Error(),
					At:       time.Now().UTC(),
				})
				return
			}
			recs, err := r.loadOne(ctx, job, src)
			if err != nil {
				slog.Error("source failed", "job", job.ID, "source", src.ID, "err", err)
				sourceFails.Add(1)
				_ = send(ctx, errorsCh, domain.PipelineError{
					JobID:    job.ID,
					SourceID: src.ID,
					Stage:    domain.StageIngestion,
					Message:  err.Error(),
					At:       time.Now().UTC(),
				})
				return
			}
			for _, rec := range recs {
				if !send(ctx, recordsCh, rec) {
					return
				}
			}
		}()
	}
	wg.Wait()
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
