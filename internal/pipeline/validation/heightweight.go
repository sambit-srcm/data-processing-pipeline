package validation

import (
	"context"
	"fmt"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// HeightWeight validates HeightWeight payloads.
type HeightWeight struct{}

// Validate checks index and plausible height/weight ranges.
func (HeightWeight) Validate(_ context.Context, record domain.Record) (domain.Record, error) {
	h, ok := record.Payload.(domain.HeightWeight)
	if !ok {
		return record, fmt.Errorf("expected HeightWeight payload")
	}
	if h.Index < 1 {
		return record, fmt.Errorf("index must be >= 1")
	}
	if h.Height < 50 || h.Height > 90 {
		return record, fmt.Errorf("height must be between 50 and 90 inches")
	}
	if h.Weight < 50 || h.Weight > 400 {
		return record, fmt.Errorf("weight must be between 50 and 400 pounds")
	}
	return record, nil
}
