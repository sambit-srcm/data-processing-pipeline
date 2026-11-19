package export

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestWriteCSVAndJSON(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	recs := []domain.Record{{
		ID: "r1", JobID: "j1", SourceID: "hw", ObservedAt: now,
		Payload: domain.HeightWeight{Index: 1, Height: 70, Weight: 160},
	}}
	aggs := []domain.Aggregation{{
		JobID: "j1", SourceID: "hw", GroupKey: "all", Count: 1, Sum: 160, Min: 160, Max: 160,
	}}

	csvPath := filepath.Join(dir, "out.csv")
	jsonPath := filepath.Join(dir, "out.json")
	err := WriteAll(context.Background(), []domain.ExportConfig{
		{Kind: domain.ExportCSV, Path: csvPath},
		{Kind: domain.ExportJSON, Path: jsonPath},
	}, recs, aggs, nil)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("csv rows = %d", len(rows))
	}

	aggRows, err := os.ReadFile(filepath.Join(dir, "out.aggs.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(aggRows) == 0 {
		t.Fatal("empty aggs csv")
	}

	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	var got jsonExport
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 1 || len(got.Aggregations) != 1 {
		t.Fatalf("json records=%d aggs=%d", len(got.Records), len(got.Aggregations))
	}
}
