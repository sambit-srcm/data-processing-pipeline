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
	GlobalDaily  []domain.GlobalDailyReport
}

// Coordinator runs validation and transformation pools.
type Coordinator struct {
	Sleep       time.Duration
	Validator   validation.Validator       // nil means accept every record
	Transformer transformation.Transformer // nil means pass-through
	Aggregator  aggregation.Aggregator     // nil means skip aggregation
	// OnProgress is called whenever a new progress snapshot is collected.
	OnProgress func(domain.Progress)
}

// Run moves preloaded records through the stage channels and returns what came out.
func (c *Coordinator) Run(ctx context.Context, cfg domain.PipelineConfig, records []domain.Record) (Result, error) {
	total := int64(len(records))
	return c.run(ctx, cfg, &total, len(records), func(ctx context.Context, recordsCh chan<- domain.Record, _ chan<- domain.PipelineError) {
		for _, rec := range records {
			if !send(ctx, recordsCh, rec) {
				return
			}
		}
	})
}

// RunIngest starts ingestFn (which should fan-in onto recordsCh and may report errors), then runs the pipeline.
func (c *Coordinator) RunIngest(ctx context.Context, cfg domain.PipelineConfig, ingestFn func(context.Context, chan<- domain.Record, chan<- domain.PipelineError)) (Result, error) {
	return c.run(ctx, cfg, nil, 0, ingestFn)
}

func (c *Coordinator) run(
	ctx context.Context,
	cfg domain.PipelineConfig,
	total *int64,
	sizeHint int,
	ingestFn func(context.Context, chan<- domain.Record, chan<- domain.PipelineError),
) (Result, error) {
	ch := newChannels(cfg.Buffers)
	startedAt := time.Now().UTC()

	var errMu sync.Mutex
	var errs []domain.PipelineError
	var progMu sync.Mutex
	prog := domain.Progress{Total: total, StartedAt: startedAt}

	var collectors sync.WaitGroup
	collectors.Add(2)
	go collectErrors(ch.errors, &errMu, &errs, &collectors)
	go collectProgress(ch.progress, &progMu, &prog, c.OnProgress, &collectors)

	var processed, valid, failed atomic.Int64
	totalVal := int64(0)
	if total != nil {
		totalVal = *total
	}

	validateWG := c.startValidation(ctx, cfg.Workers.Validation, ch, totalVal, total != nil, startedAt, &processed, &valid, &failed)
	transformWG := c.startTransform(ctx, cfg.Workers.Transformation, ch)

	go func() {
		defer close(ch.results)
		forward(ctx, ch.transformed, ch.results)
	}()
	// Ingest owns recordsCh: callers fan-in source goroutines here, then we close.
	go func() {
		defer close(ch.records)
		ingestFn(ctx, ch.records, ch.errors)
	}()

	// Fan-in: one consumer folds records into the aggregator as they arrive on resultsCh.
	out, aggs, daily := c.fanInAggregate(ctx, ch, sizeHint)

	validateWG.Wait()
	transformWG.Wait()
	finishTotal := totalVal
	if total == nil {
		finishTotal = processed.Load()
	}
	finishProgress(ctx, ch, &progMu, &prog, &processed, &valid, &failed, finishTotal, startedAt)

	close(ch.errors)
	close(ch.progress)
	collectors.Wait()

	errMu.Lock()
	progMu.Lock()
	result := Result{Records: out, Errors: errs, Progress: prog, Aggregations: aggs, GlobalDaily: daily}
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

func collectProgress(ch <-chan domain.Progress, mu *sync.Mutex, out *domain.Progress, onProgress func(domain.Progress), wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range ch {
		mu.Lock()
		*out = p
		mu.Unlock()
		if onProgress != nil {
			onProgress(p)
		}
	}
}

func (c *Coordinator) startValidation(
	ctx context.Context,
	workers int,
	ch *channels,
	total int64,
	totalKnown bool,
	startedAt time.Time,
	processed, valid, failed *atomic.Int64,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.validateLoop(ctx, ch, total, totalKnown, startedAt, processed, valid, failed)
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
	totalKnown bool,
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
						Total:     progressTotal(total, totalKnown),
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
				Total:     progressTotal(total, totalKnown),
				Processed: n,
				Valid:     v,
				Failed:    failed.Load(),
				StartedAt: startedAt,
			})
		}
	}
}

func progressTotal(total int64, known bool) *int64 {
	if !known {
		return nil
	}
	v := total
	return &v
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

// fanInAggregate consumes resultsCh, folding each record into the aggregator as it arrives.
func (c *Coordinator) fanInAggregate(ctx context.Context, ch *channels, sizeHint int) ([]domain.Record, []domain.Aggregation, []domain.GlobalDailyReport) {
	out := make([]domain.Record, 0, sizeHint)
	for {
		select {
		case <-ctx.Done():
			for range ch.results {
			}
			return out, c.snapshotAggregations(ctx), c.snapshotGlobalDaily()
		case rec, ok := <-ch.results:
			if !ok {
				return out, c.snapshotAggregations(ctx), c.snapshotGlobalDaily()
			}
			out = append(out, rec)
			if c.Aggregator == nil {
				continue
			}
			if err := c.Aggregator.Add(ctx, rec); err != nil {
				_ = send(ctx, ch.errors, domain.PipelineError{
					JobID:    rec.JobID,
					SourceID: rec.SourceID,
					RecordID: rec.ID,
					Stage:    domain.StageAggregation,
					Message:  err.Error(),
					At:       time.Now().UTC(),
				})
			}
		}
	}
}

func (c *Coordinator) snapshotAggregations(ctx context.Context) []domain.Aggregation {
	if c.Aggregator == nil {
		return nil
	}
	aggs, err := c.Aggregator.Snapshot(ctx)
	if err != nil {
		return nil
	}
	return aggs
}

func (c *Coordinator) snapshotGlobalDaily() []domain.GlobalDailyReport {
	return aggregation.ReportsOf(c.Aggregator)
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
