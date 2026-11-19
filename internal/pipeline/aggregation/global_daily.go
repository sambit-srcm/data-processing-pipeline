package aggregation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// GlobalDaily folds every record into a cross-source report for the pipeline run day (UTC)
// and payload kind — one “global daily report” combining all source types.
type GlobalDaily struct {
	mu     sync.Mutex
	runDay string
	byKey  map[string]domain.Aggregation // key: date|kind
}

// NewGlobalDaily returns an empty global daily aggregator.
func NewGlobalDaily() *GlobalDaily {
	return &GlobalDaily{byKey: make(map[string]domain.Aggregation)}
}

// Add folds one record into the run-day + kind bucket.
func (g *GlobalDaily) Add(_ context.Context, record domain.Record) error {
	kind := domain.SourceKindOf(record.Payload)
	if kind == "" {
		return fmt.Errorf("unsupported payload for global daily aggregation")
	}
	value, err := autoValue(record)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.runDay == "" {
		g.runDay = time.Now().UTC().Format("2006-01-02")
	}
	date := g.runDay
	key := date + "|" + string(kind)

	agg, ok := g.byKey[key]
	if !ok {
		agg = domain.Aggregation{
			JobID:    record.JobID,
			SourceID: SourceIDGlobal,
			GroupKey: "global_daily|" + key,
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
	g.byKey[key] = agg
	return nil
}

// SourceIDGlobal marks aggregation rows produced by the global_daily kind.
const SourceIDGlobal domain.SourceID = "global"

// Snapshot returns flat aggregation rows for persistence (group_key global_daily|<date>|<kind>).
func (g *GlobalDaily) Snapshot(_ context.Context) ([]domain.Aggregation, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]domain.Aggregation, 0, len(g.byKey))
	for _, agg := range g.byKey {
		out = append(out, agg)
	}
	return out, nil
}

// Reports builds one GlobalDailyReport per calendar day.
func (g *GlobalDaily) Reports() []domain.GlobalDailyReport {
	g.mu.Lock()
	defer g.mu.Unlock()

	byDate := map[string]*domain.GlobalDailyReport{}
	for key, agg := range g.byKey {
		date, kind, ok := splitDailyKey(key)
		if !ok {
			continue
		}
		rep, exists := byDate[date]
		if !exists {
			rep = &domain.GlobalDailyReport{JobID: agg.JobID, Date: date}
			byDate[date] = rep
		}
		rep.TotalRecords += agg.Count
		rep.Kinds = append(rep.Kinds, domain.GlobalDailyKindStat{
			Kind:  domain.SourceKind(kind),
			Count: agg.Count,
			Sum:   agg.Sum,
			Min:   agg.Min,
			Max:   agg.Max,
		})
	}

	out := make([]domain.GlobalDailyReport, 0, len(byDate))
	for _, rep := range byDate {
		sort.Slice(rep.Kinds, func(i, j int) bool {
			return rep.Kinds[i].Kind < rep.Kinds[j].Kind
		})
		out = append(out, *rep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date < out[j].Date })
	return out
}

func splitDailyKey(key string) (date, kind string, ok bool) {
	// key format: YYYY-MM-DD|kind
	if len(key) < 12 || key[10] != '|' {
		return "", "", false
	}
	return key[:10], key[11:], true
}

func autoValue(record domain.Record) (float64, error) {
	switch p := record.Payload.(type) {
	case domain.HeightWeight:
		return p.Weight, nil
	case domain.CryptoPrice:
		return p.Price, nil
	case domain.WeatherObservation:
		return p.TempC, nil
	default:
		return 0, fmt.Errorf("unsupported payload for global daily aggregation")
	}
}
