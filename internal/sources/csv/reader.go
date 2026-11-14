package csv

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// ReadFile loads COVID rows from a CSV file into records.
func ReadFile(path string, jobID domain.JobID, sourceID domain.SourceID) ([]domain.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("csv %s: no data rows", path)
	}

	out := make([]domain.Record, 0, len(rows)-1)
	for i, row := range rows[1:] {
		if len(row) < 4 {
			return nil, fmt.Errorf("csv %s: line %d: want 4 columns", path, i+2)
		}
		cases, _ := strconv.ParseInt(row[2], 10, 64)
		deaths, _ := strconv.ParseInt(row[3], 10, 64)
		out = append(out, domain.Record{
			ID:       domain.RecordID(fmt.Sprintf("%s-%d", sourceID, i+1)),
			JobID:    jobID,
			SourceID: sourceID,
			Payload: domain.COVIDCase{
				Date:   row[0],
				Region: row[1],
				Cases:  cases,
				Deaths: deaths,
			},
		})
	}
	return out, nil
}
