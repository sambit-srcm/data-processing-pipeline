package validation

import (
	"context"
	"fmt"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// COVID validates COVIDCase payloads.
type COVID struct{}

// Validate checks required fields and non-negative counts.
func (COVID) Validate(_ context.Context, record domain.Record) (domain.Record, error) {
	c, ok := record.Payload.(domain.COVIDCase)
	if !ok {
		return record, fmt.Errorf("expected COVIDCase payload")
	}
	if c.Date == "" {
		return record, fmt.Errorf("date is required")
	}
	if c.Region == "" {
		return record, fmt.Errorf("region is required")
	}
	if c.Cases < 0 || c.Deaths < 0 {
		return record, fmt.Errorf("cases and deaths must be >= 0")
	}
	return record, nil
}
