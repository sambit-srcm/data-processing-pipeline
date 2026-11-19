package aggregation

import (
	"context"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestGlobalDailyMultiKind(t *testing.T) {
	day := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	agg, err := FromConfigs(
		domain.AggregationConfig{Kind: domain.AggregationStats},
		domain.AggregationConfig{Kind: domain.AggregationGlobalDaily},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	recs := []domain.Record{
		{JobID: "j1", SourceID: "hw", ObservedAt: day, Payload: domain.HeightWeight{Weight: 100}},
		{JobID: "j1", SourceID: "crypto", ObservedAt: day, Payload: domain.CryptoPrice{Symbol: "BTC", Price: 50}},
		{JobID: "j1", SourceID: "weather", ObservedAt: day, Payload: domain.WeatherObservation{TempC: 20}},
	}
	for _, rec := range recs {
		if err := agg.Add(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	aggs, err := agg.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) < 4 { // 3 stats groups + 3 global_daily kind rows (approx)
		t.Fatalf("expected stats + global rows, got %d", len(aggs))
	}

	reports := ReportsOf(agg)
	if len(reports) != 1 || reports[0].TotalRecords != 3 {
		t.Fatalf("reports = %+v", reports)
	}
	if len(reports[0].Kinds) != 3 {
		t.Fatalf("kinds = %+v", reports[0].Kinds)
	}
}

func TestGlobalDailyFromAggregations(t *testing.T) {
	got := domain.GlobalDailyFromAggregations([]domain.Aggregation{
		{JobID: "j1", GroupKey: "global_daily|2026-11-19|csv", Count: 2, Sum: 10, Min: 4, Max: 6},
		{JobID: "j1", GroupKey: "hw|weight", Count: 2, Sum: 10, Min: 4, Max: 6},
		{JobID: "j1", GroupKey: "global_daily|2026-11-19|json", Count: 1, Sum: 5, Min: 5, Max: 5},
	})
	if len(got) != 1 || got[0].TotalRecords != 3 || len(got[0].Kinds) != 2 {
		t.Fatalf("%+v", got)
	}
}
