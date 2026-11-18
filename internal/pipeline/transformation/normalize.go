package transformation

import (
	"context"
	"strings"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Normalize cleans source-specific fields before aggregation/export.
type Normalize struct{}

// Transform applies light normalization by payload type.
func (Normalize) Transform(_ context.Context, record domain.Record) (domain.Record, error) {
	switch p := record.Payload.(type) {
	case domain.HeightWeight:
		// Numeric payload has no string fields to normalize.
		record.Payload = p
	case domain.CryptoPrice:
		p.Symbol = strings.ToUpper(strings.TrimSpace(p.Symbol))
		record.Payload = p
	case domain.WeatherObservation:
		record.Payload = p
	}
	return record, nil
}
