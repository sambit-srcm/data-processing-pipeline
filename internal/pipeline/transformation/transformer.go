// Package transformation defines the transformation stage contract.
package transformation

import (
	"context"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Transformer applies one source-specific change to a valid record.
type Transformer interface {
	Transform(ctx context.Context, record domain.Record) (domain.Record, error)
}
