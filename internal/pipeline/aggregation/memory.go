package aggregation

import (
	"context"
	"fmt"
	"sync"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Memory aggregates records in process memory.
type Memory struct {
	mu    sync.Mutex
	byKey map[string]domain.Aggregation
}

// NewMemory returns an empty aggregator.
func NewMemory() *Memory {
	return &Memory{byKey: make(map[string]domain.Aggregation)}
}

// Add folds one record into its group.
func (m *Memory) Add(_ context.Context, record domain.Record) error {
	key, value, err := groupValue(record)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	agg, ok := m.byKey[key]
	if !ok {
		agg = domain.Aggregation{
			JobID:    record.JobID,
			SourceID: record.SourceID,
			GroupKey: key,
			Min:      value,
			Max:      value,
		}
	}
	agg.Count++
	agg.Sum += value
	if value < agg.Min {
		agg.Min = value
	}
	if value > agg.Max {
		agg.Max = value
	}
	m.byKey[key] = agg
	return nil
}

// Snapshot returns current aggregates.
func (m *Memory) Snapshot(_ context.Context) ([]domain.Aggregation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Aggregation, 0, len(m.byKey))
	for _, agg := range m.byKey {
		out = append(out, agg)
	}
	return out, nil
}

func groupValue(record domain.Record) (string, float64, error) {
	switch p := record.Payload.(type) {
	case domain.HeightWeight:
		return fmt.Sprintf("%s|hw", record.SourceID), p.Weight, nil
	case domain.CryptoPrice:
		return fmt.Sprintf("%s|%s", record.SourceID, p.Symbol), p.Price, nil
	case domain.WeatherObservation:
		return fmt.Sprintf("%s|weather", record.SourceID), p.TempC, nil
	default:
		return "", 0, fmt.Errorf("unsupported payload for aggregation")
	}
}
