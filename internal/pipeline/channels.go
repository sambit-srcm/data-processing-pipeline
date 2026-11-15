package pipeline

import (
	"context"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// channels are the typed belts between stages.
type channels struct {
	records     chan domain.Record
	validated   chan domain.Record
	transformed chan domain.Record
	results     chan domain.Record
	errors      chan domain.PipelineError
	progress    chan domain.Progress
}

func newChannels(buffers domain.Buffers) *channels {
	return &channels{
		records:     make(chan domain.Record, buffers.Records),
		validated:   make(chan domain.Record, buffers.Validated),
		transformed: make(chan domain.Record, buffers.Transformed),
		results:     make(chan domain.Record, buffers.Transformed),
		errors:      make(chan domain.PipelineError, buffers.Errors),
		progress:    make(chan domain.Progress, buffers.Progress),
	}
}

// send returns false if ctx is cancelled before the value is sent.
func send[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}
