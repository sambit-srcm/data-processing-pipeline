package transformation

import (
	"context"
	"fmt"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Chain runs transformers in order.
type Chain []Transformer

// Transform applies each step.
func (c Chain) Transform(ctx context.Context, record domain.Record) (domain.Record, error) {
	var err error
	for _, step := range c {
		record, err = step.Transform(ctx, record)
		if err != nil {
			return record, err
		}
	}
	return record, nil
}

// FromConfig builds a transformer from the job specification.
func FromConfig(cfgs []domain.TransformConfig) (Transformer, error) {
	if len(cfgs) == 0 {
		return Normalize{}, nil
	}
	steps := make(Chain, 0, len(cfgs))
	for _, cfg := range cfgs {
		switch cfg.Kind {
		case domain.TransformNormalize:
			steps = append(steps, Normalize{})
		default:
			return nil, fmt.Errorf("unsupported transform kind: %s", cfg.Kind)
		}
	}
	if len(steps) == 1 {
		return steps[0], nil
	}
	return steps, nil
}
