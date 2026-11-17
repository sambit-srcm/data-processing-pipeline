package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func testConfig() domain.PipelineConfig {
	return domain.PipelineConfig{
		Workers: domain.Workers{Validation: 2, Transformation: 2},
		Buffers: domain.Buffers{
			Records:     4,
			Validated:   4,
			Transformed: 4,
			Errors:      4,
			Progress:    4,
		},
		Sources: []domain.SourceConfig{{ID: "fake", Kind: domain.SourceCSV, Path: "fake.csv"}},
	}
}

func fakeRecords(n int) []domain.Record {
	out := make([]domain.Record, n)
	for i := 0; i < n; i++ {
		out[i] = domain.Record{
			ID:       domain.RecordID(fmt.Sprintf("r-%d", i)),
			JobID:    "job-1",
			SourceID: "fake",
			Payload:  domain.HeightWeight{Index: int64(i + 1), Height: 68.0, Weight: 130.0},
		}
	}
	return out
}

func TestCoordinatorEndToEnd(t *testing.T) {
	var c Coordinator
	res, err := c.Run(context.Background(), testConfig(), fakeRecords(20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Records) != 20 {
		t.Fatalf("got %d records, want 20", len(res.Records))
	}
	if res.Progress.Processed != 20 {
		t.Fatalf("processed = %d", res.Progress.Processed)
	}
	if res.Progress.FinishedAt == nil {
		t.Fatal("expected FinishedAt")
	}
}

func TestCoordinatorCancelHalfway(t *testing.T) {
	cfg := testConfig()
	cfg.Buffers = domain.Buffers{}
	cfg.Workers = domain.Workers{Validation: 1, Transformation: 1}

	c := Coordinator{Sleep: 30 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	var (
		res Result
		err error
	)
	go func() {
		defer close(done)
		res, err = c.Run(ctx, cfg, fakeRecords(50))
	}()

	time.Sleep(40 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(res.Records) >= 50 {
		t.Fatalf("expected partial results, got %d", len(res.Records))
	}
}
