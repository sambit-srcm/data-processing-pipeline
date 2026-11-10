// Package aggregation defines the aggregation stage contract.
package aggregation

import (
	"context"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Aggregator folds transformed records into per-source group results.
// Implementations own their state. Callers use one aggregator at a time.
type Aggregator interface {
	Add(ctx context.Context, record domain.Record) error
	Snapshot(ctx context.Context) ([]domain.Aggregation, error)
}
