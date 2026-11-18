package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/aggregation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/transformation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/validation"
)

// Result is the output of one coordinator run.
type Result struct {
	Records      []domain.Record
	Errors       []domain.PipelineError
	Progress     domain.Progress
	Aggregations []domain.Aggregation
}

// Coordinator runs validation and transformation pools.
type Coordinator struct {
	Sleep       time.Duration
	Validator   validation.Validator       // nil means accept every record
	Transformer transformation.Transformer // nil means pass-through
	Aggregator  aggregation.Aggregator     // nil means skip aggregation
}

// Run moves records through the stage channels and returns what came out.
func (c *Coordinator) Run(ctx context.Context, cfg domain.PipelineConfig, records []domain.Record) (Result, error) {
	ch := newChannels(cfg.Buffers)
	total := int64(len(records))
	startedAt := time.Now().UTC()

	var errMu sync.Mutex
	var errs []domain.PipelineError
	var progMu sync.Mutex
	prog := domain.Progress{Total: &total, StartedAt: startedAt}

	var collectors sync.WaitGroup
	collectors.Add(2)
	go collectErrors(ch.errors, &errMu, &errs, &collectors)
	go collectProgress(ch.progress, &progMu, &prog, &collectors)

	var processed, valid, failed atomic.Int64

	validateWG := c.startValidation(ctx, cfg.Workers.Validation, ch, total, startedAt, &processed, &valid, &failed)
	transformWG := c.startTransform(ctx, cfg.Workers.Transformation, ch)

	go func() {
		defer close(ch.results)
		forward(ctx, ch.transformed, ch.results)
	}()
	go feed(ctx, ch.records, records)

	out := drain(ctx, ch.results, len(records))

	validateWG.Wait()
	transformWG.Wait()
	finishProgress(ctx, ch, &progMu, &prog, &processed, &valid, &failed, total, startedAt)

	close(ch.errors)
	close(ch.progress)
	collectors.Wait()

	aggs, err := c.aggregate(ctx, out)
	if err != nil {
		return Result{}, err
	}

	errMu.Lock()
	progMu.Lock()
	result := Result{Records: out, Errors: errs, Progress: prog, Aggregations: aggs}
	progMu.Unlock()
	errMu.Unlock()

	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func collectErrors(ch <-chan domain.PipelineError, mu *sync.Mutex, out *[]domain.PipelineError, wg *sync.WaitGroup) {
	defer wg.Done()
	for e := range ch {
		mu.Lock()
		*out = append(*out, e)
		mu.Unlock()
	}
}

func collectProgress(ch <-chan domain.Progress, mu *sync.Mutex, out *domain.Progress, wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range ch {
		mu.Lock()
		*out = p
		mu.Unlock()
	}
}

func (c *Coordinator) startValidation(
	ctx context.Context,
	workers int,
	ch *channels,
	total int64,
	startedAt time.Time,
	processed, valid, failed *atomic.Int64,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.validateLoop(ctx, ch, total, startedAt, processed, valid, failed)
		}()
	}
	go func() {
		wg.Wait()
		close(ch.validated)
	}()
	return &wg
}

func (c *Coordinator) validateLoop(
	ctx context.Context,
	ch *channels,
	total int64,
	startedAt time.Time,
	processed, valid, failed *atomic.Int64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch.records:
			if !ok {
				return
			}
			if c.Sleep > 0 {
				select {
				case <-time.After(c.Sleep):
				case <-ctx.Done():
					return
				}
			}

			if c.Validator != nil {
				var err error
				rec, err = c.Validator.Validate(ctx, rec)
				if err != nil {
					n := processed.Add(1)
					f := failed.Add(1)
					_ = send(ctx, ch.errors, domain.PipelineError{
						JobID:    rec.JobID,
						SourceID: rec.SourceID,
						RecordID: rec.ID,
						Stage:    domain.StageValidation,
						Message:  err.Error(),
						At:       time.Now().UTC(),
					})
					_ = send(ctx, ch.progress, domain.Progress{
						Total:     &total,
						Processed: n,
						Failed:    f,
						Valid:     valid.Load(),
						StartedAt: startedAt,
					})
					continue
				}
			}

			if !send(ctx, ch.validated, rec) {
				return
			}
			n := processed.Add(1)
			v := valid.Add(1)
			_ = send(ctx, ch.progress, domain.Progress{
				Total:     &total,
				Processed: n,
				Valid:     v,
				Failed:    failed.Load(),
				StartedAt: startedAt,
			})
		}
	}
}

func (c *Coordinator) startTransform(ctx context.Context, workers int, ch *channels) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.transformLoop(ctx, ch)
		}()
	}
	go func() {
		wg.Wait()
		close(ch.transformed)
	}()
	return &wg
}

func (c *Coordinator) transformLoop(ctx context.Context, ch *channels) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch.validated:
			if !ok {
				return
			}
			if c.Transformer != nil {
				var err error
				rec, err = c.Transformer.Transform(ctx, rec)
				if err != nil {
					_ = send(ctx, ch.errors, domain.PipelineError{
						JobID:    rec.JobID,
						SourceID: rec.SourceID,
						RecordID: rec.ID,
						Stage:    domain.StageTransformation,
						Message:  err.Error(),
						At:       time.Now().UTC(),
					})
					continue
				}
			}
			if !send(ctx, ch.transformed, rec) {
				return
			}
		}
	}
}

func (c *Coordinator) aggregate(ctx context.Context, records []domain.Record) ([]domain.Aggregation, error) {
	if c.Aggregator == nil || ctx.Err() != nil {
		return nil, nil
	}
	for _, rec := range records {
		if err := c.Aggregator.Add(ctx, rec); err != nil {
			return nil, err
		}
	}
	return c.Aggregator.Snapshot(ctx)
}

func feed(ctx context.Context, ch chan<- domain.Record, records []domain.Record) {
	defer close(ch)
	for _, rec := range records {
		if !send(ctx, ch, rec) {
			return
		}
	}
}

func drain(ctx context.Context, results <-chan domain.Record, sizeHint int) []domain.Record {
	out := make([]domain.Record, 0, sizeHint)
	for {
		select {
		case <-ctx.Done():
			for range results {
			}
			return out
		case rec, ok := <-results:
			if !ok {
				return out
			}
			out = append(out, rec)
		}
	}
}

func finishProgress(
	ctx context.Context,
	ch *channels,
	progMu *sync.Mutex,
	prog *domain.Progress,
	processed, valid, failed *atomic.Int64,
	total int64,
	startedAt time.Time,
) {
	if ctx.Err() != nil {
		return
	}
	finished := time.Now().UTC()
	final := domain.Progress{
		Total:      &total,
		Processed:  processed.Load(),
		Valid:      valid.Load(),
		Failed:     failed.Load(),
		StartedAt:  startedAt,
		FinishedAt: &finished,
	}
	progMu.Lock()
	*prog = final
	progMu.Unlock()
	_ = send(ctx, ch.progress, final)
}

func forward(ctx context.Context, in <-chan domain.Record, out chan<- domain.Record) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-in:
			if !ok {
				return
			}
			if !send(ctx, out, rec) {
				return
			}
		}
	}
}
