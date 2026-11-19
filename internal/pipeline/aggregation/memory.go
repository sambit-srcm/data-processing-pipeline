package aggregation

import (
	"context"
	"fmt"
	"sync"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Memory aggregates records in process memory using the job's aggregation config.
type Memory struct {
	mu    sync.Mutex
	byKey map[string]domain.Aggregation
	cfg   domain.AggregationConfig
}

// NewMemory returns a stats aggregator (auto defaults). Prefer FromConfigs for job specs.
func NewMemory(cfgs ...domain.AggregationConfig) *Memory {
	cfg := domain.AggregationConfig{
		Kind:       domain.AggregationStats,
		GroupBy:    "auto",
		ValueField: "auto",
	}
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	return newMemory(cfg)
}

func newMemory(cfg domain.AggregationConfig) *Memory {
	if cfg.GroupBy == "" {
		cfg.GroupBy = "auto"
	}
	if cfg.ValueField == "" {
		cfg.ValueField = "auto"
	}
	if cfg.Kind == "" {
		cfg.Kind = domain.AggregationStats
	}
	return &Memory{byKey: make(map[string]domain.Aggregation), cfg: cfg}
}

// Add folds one record into its group.
func (m *Memory) Add(_ context.Context, record domain.Record) error {
	key, value, err := groupValue(record, m.cfg)
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

func groupValue(record domain.Record, cfg domain.AggregationConfig) (string, float64, error) {
	switch p := record.Payload.(type) {
	case domain.HeightWeight:
		key, err := groupKey(record, cfg.GroupBy, "hw", "")
		if err != nil {
			return "", 0, err
		}
		value, err := numericField(cfg.ValueField, "weight", map[string]float64{
			"weight": p.Weight,
			"height": p.Height,
		})
		return key, value, err
	case domain.CryptoPrice:
		key, err := groupKey(record, cfg.GroupBy, p.Symbol, p.Symbol)
		if err != nil {
			return "", 0, err
		}
		value, err := numericField(cfg.ValueField, "price", map[string]float64{
			"price": p.Price,
		})
		return key, value, err
	case domain.WeatherObservation:
		key, err := groupKey(record, cfg.GroupBy, "weather", "")
		if err != nil {
			return "", 0, err
		}
		value, err := numericField(cfg.ValueField, "temp_c", map[string]float64{
			"temp_c": p.TempC,
		})
		return key, value, err
	default:
		return "", 0, fmt.Errorf("unsupported payload for aggregation")
	}
}

func groupKey(record domain.Record, groupBy, autoSuffix, symbol string) (string, error) {
	switch groupBy {
	case "", "auto":
		return fmt.Sprintf("%s|%s", record.SourceID, autoSuffix), nil
	case "source":
		return string(record.SourceID), nil
	case "symbol":
		if symbol == "" {
			return "", fmt.Errorf("group_by symbol is only valid for crypto payloads")
		}
		return fmt.Sprintf("%s|%s", record.SourceID, symbol), nil
	default:
		return "", fmt.Errorf("unsupported group_by: %s", groupBy)
	}
}

func numericField(field, autoDefault string, values map[string]float64) (float64, error) {
	if field == "" || field == "auto" {
		field = autoDefault
	}
	v, ok := values[field]
	if !ok {
		return 0, fmt.Errorf("value_field %q not available for payload", field)
	}
	return v, nil
}
