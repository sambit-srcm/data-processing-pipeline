package transformation

import (
	"context"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestNormalize(t *testing.T) {
	rec, err := (Normalize{}).Transform(context.Background(), domain.Record{
		Payload: domain.CryptoPrice{Symbol: " btc ", Price: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := rec.Payload.(domain.CryptoPrice)
	if !ok {
		t.Fatalf("payload %#v", rec.Payload)
	}
	if got.Symbol != "BTC" {
		t.Fatalf("symbol = %q", got.Symbol)
	}
}
