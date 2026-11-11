// Package validation defines the validation stage contract.
package validation

import (
	"context"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Validator checks one record.
// A non-nil error means the record is rejected and processing continues.
type Validator interface {
	Validate(ctx context.Context, record domain.Record) (domain.Record, error)
}
