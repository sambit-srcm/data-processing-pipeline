package csv

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// ReadFile loads height/weight rows from a CSV file into records.
func ReadFile(path string, jobID domain.JobID, sourceID domain.SourceID) ([]domain.Record, error) {
	// Path comes from job config supplied by the operator.
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("csv %s: no data rows", path)
	}

	out := make([]domain.Record, 0, len(rows)-1)
	for i, row := range rows[1:] {
		if len(row) < 3 {
			return nil, fmt.Errorf("csv %s: line %d: want 3 columns", path, i+2)
		}
		idx, _ := strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
		height, _ := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		weight, _ := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		out = append(out, domain.Record{
			ID:       domain.RecordID(fmt.Sprintf("%s-%d", sourceID, i+1)),
			JobID:    jobID,
			SourceID: sourceID,
			Payload: domain.HeightWeight{
				Index:  idx,
				Height: height,
				Weight: weight,
			},
		})
	}
	return out, nil
}
