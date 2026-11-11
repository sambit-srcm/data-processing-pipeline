package domain

import "testing"

func TestJobStatusTransitions(t *testing.T) {
	cases := []struct {
		from JobStatus
		to   JobStatus
		ok   bool
	}{
		{StatusPending, StatusRunning, true},
		{StatusPending, StatusSucceeded, false},
		{StatusRunning, StatusPartial, true},
		{StatusRunning, StatusCancelling, true},
		{StatusCancelling, StatusCancelled, true},
		{StatusSucceeded, StatusRunning, false},
	}
	for _, tc := range cases {
		if got := tc.from.CanTransitionTo(tc.to); got != tc.ok {
			t.Errorf("%s -> %s = %v, want %v", tc.from, tc.to, got, tc.ok)
		}
	}
}

func TestProgressPercentageUnknownTotal(t *testing.T) {
	if _, ok := (Progress{Processed: 3}).Percentage(); ok {
		t.Fatal("percentage must be unset when total is unknown")
	}
	total := int64(4)
	got, ok := (Progress{Total: &total, Processed: 1}).Percentage()
	if !ok || got != 25 {
		t.Fatalf("percentage = %v, %v", got, ok)
	}
}

func TestAggregationAverage(t *testing.T) {
	if _, ok := (Aggregation{}).Average(); ok {
		t.Fatal("average must be unset when count is zero")
	}
	got, ok := (Aggregation{Count: 2, Sum: 10}).Average()
	if !ok || got != 5 {
		t.Fatalf("average = %v, %v", got, ok)
	}
}

func TestPipelineConfigValidate(t *testing.T) {
	cfg := PipelineConfig{
		Workers: Workers{Validation: 5, Transformation: 5},
		Sources: []SourceConfig{{ID: "covid", Kind: SourceCSV, Path: "samples/input/covid.csv"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Sources = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected an error with no sources")
	}
}
