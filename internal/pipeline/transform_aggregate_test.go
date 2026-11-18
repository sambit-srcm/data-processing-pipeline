package pipeline

import (
	"context"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/aggregation"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/transformation"
)

func TestCoordinatorTransformAndAggregate(t *testing.T) {
	recs := []domain.Record{
		{ID: "1", JobID: "j1", SourceID: "crypto", Payload: domain.CryptoPrice{Symbol: "btc", Price: 10}},
		{ID: "2", JobID: "j1", SourceID: "crypto", Payload: domain.CryptoPrice{Symbol: "btc", Price: 5}},
	}
	c := Coordinator{
		Transformer: transformation.Normalize{},
		Aggregator:  aggregation.NewMemory(),
	}
	res, err := c.Run(context.Background(), testConfig(), recs)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := res.Records[0].Payload.(domain.CryptoPrice)
	if !ok || got.Symbol != "BTC" {
		t.Fatalf("expected normalized symbol, got %#v", res.Records[0].Payload)
	}
	if len(res.Aggregations) != 1 {
		t.Fatalf("aggs = %d", len(res.Aggregations))
	}
	if res.Aggregations[0].Sum != 15 {
		t.Fatalf("sum = %v", res.Aggregations[0].Sum)
	}
}
