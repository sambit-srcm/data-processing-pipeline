package validation

import (
	"context"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestCOVIDValidate(t *testing.T) {
	v := COVID{}
	ok := domain.Record{Payload: domain.COVIDCase{Date: "2020-01-01", Region: "A", Cases: 1, Deaths: 0}}
	if _, err := v.Validate(context.Background(), ok); err != nil {
		t.Fatal(err)
	}

	bad := domain.Record{Payload: domain.COVIDCase{Date: "2020-01-01", Region: "A", Cases: -1}}
	if _, err := v.Validate(context.Background(), bad); err == nil {
		t.Fatal("expected error for negative cases")
	}
}
