package export

import (
	"encoding/json"
	"os"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

type jsonExport struct {
	Records      []jsonRecord      `json:"records"`
	Aggregations []jsonAggregation `json:"aggregations"`
}

type jsonRecord struct {
	ID         string `json:"id"`
	JobID      string `json:"job_id"`
	SourceID   string `json:"source_id"`
	ObservedAt string `json:"observed_at"`
	Kind       string `json:"kind"`
	Payload    any    `json:"payload"`
}

type jsonAggregation struct {
	JobID    string  `json:"job_id"`
	SourceID string  `json:"source_id"`
	GroupKey string  `json:"group_key"`
	Count    int64   `json:"count"`
	Sum      float64 `json:"sum"`
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
}

func writeJSON(path string, records []domain.Record, aggs []domain.Aggregation) error {
	out := jsonExport{
		Records:      make([]jsonRecord, 0, len(records)),
		Aggregations: make([]jsonAggregation, 0, len(aggs)),
	}
	for _, rec := range records {
		out.Records = append(out.Records, jsonRecord{
			ID:         string(rec.ID),
			JobID:      string(rec.JobID),
			SourceID:   string(rec.SourceID),
			ObservedAt: rec.ObservedAt.UTC().Format(time.RFC3339Nano),
			Kind:       string(domain.SourceKindOf(rec.Payload)),
			Payload:    rec.Payload,
		})
	}
	for _, a := range aggs {
		out.Aggregations = append(out.Aggregations, jsonAggregation{
			JobID:    string(a.JobID),
			SourceID: string(a.SourceID),
			GroupKey: a.GroupKey,
			Count:    a.Count,
			Sum:      a.Sum,
			Min:      a.Min,
			Max:      a.Max,
		})
	}

	raw, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644) //nolint:gosec
}
