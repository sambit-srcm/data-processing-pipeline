package validation

import (
	"context"
	"fmt"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Router picks a simple validator based on payload type.
type Router struct{}

// Validate routes to the matching source rules.
func (Router) Validate(ctx context.Context, record domain.Record) (domain.Record, error) {
	switch p := record.Payload.(type) {
	case domain.HeightWeight:
		return HeightWeight{}.Validate(ctx, record)
	case domain.CryptoPrice:
		if p.Symbol == "" {
			return record, fmt.Errorf("symbol is required")
		}
		if p.Price < 0 {
			return record, fmt.Errorf("price must be >= 0")
		}
		return record, nil
	case domain.WeatherObservation:
		return record, nil
	default:
		return record, fmt.Errorf("unsupported payload")
	}
}
