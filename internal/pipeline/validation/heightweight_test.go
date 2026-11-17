package validation

import (
	"context"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestHeightWeightValidate(t *testing.T) {
	v := HeightWeight{}
	ok := domain.Record{Payload: domain.HeightWeight{Index: 1, Height: 65.78, Weight: 112.99}}
	if _, err := v.Validate(context.Background(), ok); err != nil {
		t.Fatalf("valid row: %v", err)
	}
	bad := domain.Record{Payload: domain.HeightWeight{Index: 1, Height: -5, Weight: 140}}
	if _, err := v.Validate(context.Background(), bad); err == nil {
		t.Fatal("expected error for negative height")
	}
}
