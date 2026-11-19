package aggregation

import (
	"context"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestMemoryAggregate(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	_ = m.Add(ctx, domain.Record{JobID: "j1", SourceID: "hw", Payload: domain.HeightWeight{Index: 1, Height: 65, Weight: 110}})
	_ = m.Add(ctx, domain.Record{JobID: "j1", SourceID: "hw", Payload: domain.HeightWeight{Index: 2, Height: 70, Weight: 130}})
	aggs, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 1 {
		t.Fatalf("got %d groups", len(aggs))
	}
	if aggs[0].Count != 2 || aggs[0].Sum != 240 || aggs[0].Min != 110 || aggs[0].Max != 130 {
		t.Fatalf("%+v", aggs[0])
	}
}

func TestMemoryAggregateByHeight(t *testing.T) {
	m := NewMemory(domain.AggregationConfig{
		Kind: domain.AggregationStats, GroupBy: "source", ValueField: "height",
	})
	ctx := context.Background()
	_ = m.Add(ctx, domain.Record{JobID: "j1", SourceID: "hw", Payload: domain.HeightWeight{Index: 1, Height: 65, Weight: 110}})
	_ = m.Add(ctx, domain.Record{JobID: "j1", SourceID: "hw", Payload: domain.HeightWeight{Index: 2, Height: 70, Weight: 130}})
	aggs, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 1 || aggs[0].GroupKey != "hw" || aggs[0].Sum != 135 {
		t.Fatalf("%+v", aggs)
	}
}
