package aggregation

import (
	"context"
	"fmt"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// Multi fans each record into every configured aggregation kind.
type Multi struct {
	parts []Aggregator
	daily *GlobalDaily
}

// FromConfigs builds aggregators for each AggregationConfig entry.
func FromConfigs(cfgs ...domain.AggregationConfig) (Aggregator, error) {
	if len(cfgs) == 0 {
		return NewMemory(), nil
	}

	m := &Multi{}
	for _, cfg := range cfgs {
		switch cfg.Kind {
		case domain.AggregationStats, "":
			m.parts = append(m.parts, newMemory(cfg))
		case domain.AggregationGlobalDaily:
			if m.daily == nil {
				m.daily = NewGlobalDaily()
				m.parts = append(m.parts, m.daily)
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation kind: %s", cfg.Kind)
		}
	}
	if len(m.parts) == 1 {
		return m.parts[0], nil
	}
	return m, nil
}

// Add forwards to every part.
func (m *Multi) Add(ctx context.Context, record domain.Record) error {
	for _, part := range m.parts {
		if err := part.Add(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot merges all part snapshots.
func (m *Multi) Snapshot(ctx context.Context) ([]domain.Aggregation, error) {
	var out []domain.Aggregation
	for _, part := range m.parts {
		aggs, err := part.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, aggs...)
	}
	return out, nil
}

// Reports returns global daily reports when that kind is configured.
func (m *Multi) Reports() []domain.GlobalDailyReport {
	if m.daily == nil {
		return nil
	}
	return m.daily.Reports()
}

// ReportsOf returns global daily reports from any aggregator that supports them.
func ReportsOf(a Aggregator) []domain.GlobalDailyReport {
	switch v := a.(type) {
	case *GlobalDaily:
		return v.Reports()
	case *Multi:
		return v.Reports()
	default:
		return nil
	}
}
