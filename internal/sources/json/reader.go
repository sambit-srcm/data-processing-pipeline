package json

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

type coinPrice struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"current_price"`
}

// ReadFile loads CoinGecko-style price rows from a JSON file.
func ReadFile(path string, jobID domain.JobID, sourceID domain.SourceID) ([]domain.Record, error) {
	raw, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	var rows []coinPrice
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("json %s: no rows", path)
	}
	now := time.Now().UTC()
	out := make([]domain.Record, 0, len(rows))
	for i, row := range rows {
		out = append(out, domain.Record{
			ID:         domain.RecordID(fmt.Sprintf("%s-%d", sourceID, i+1)),
			JobID:      jobID,
			SourceID:   sourceID,
			ObservedAt: now,
			Payload: domain.CryptoPrice{
				Symbol: row.Symbol,
				Price:  row.Price,
				At:     now,
			},
		})
	}
	return out, nil
}
