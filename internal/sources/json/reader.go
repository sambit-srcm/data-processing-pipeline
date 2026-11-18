package json

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// coinPrice accepts both the small API-style snapshot and scraper JSONL fields.
type coinPrice struct {
	Symbol         string   `json:"symbol"`
	CurrentPrice   *float64 `json:"current_price"`
	CurrentPriceCL *float64 `json:"currentPrice"`
}

func (c coinPrice) price() (float64, bool) {
	if c.CurrentPriceCL != nil {
		return *c.CurrentPriceCL, true
	}
	if c.CurrentPrice != nil {
		return *c.CurrentPrice, true
	}
	return 0, false
}

// ReadFile loads CoinGecko-style price rows from a JSON array or JSONL file.
func ReadFile(path string, jobID domain.JobID, sourceID domain.SourceID) ([]domain.Record, error) {
	raw, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, fmt.Errorf("json %s: empty file", path)
	}

	now := time.Now().UTC()
	var rows []coinPrice
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
	} else {
		rows, err = decodeJSONL(raw)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("json %s: no rows", path)
	}

	out := make([]domain.Record, 0, len(rows))
	for i, row := range rows {
		price, ok := row.price()
		if !ok {
			continue
		}
		symbol := strings.TrimSpace(row.Symbol)
		out = append(out, domain.Record{
			ID:         domain.RecordID(fmt.Sprintf("%s-%d", sourceID, i+1)),
			JobID:      jobID,
			SourceID:   sourceID,
			ObservedAt: now,
			Payload: domain.CryptoPrice{
				Symbol: symbol,
				Price:  price,
				At:     now,
			},
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("json %s: no usable price rows", path)
	}
	return out, nil
}

func decodeJSONL(raw []byte) ([]coinPrice, error) {
	sc := bufio.NewScanner(bytes.NewReader(raw))
	// Scraper lines are large; raise the token limit (default 64KiB).
	buf := make([]byte, 0, 1024*1024)
	sc.Buffer(buf, 4*1024*1024)

	var rows []coinPrice
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var row coinPrice
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("jsonl line %d: %w", lineNo, err)
		}
		rows = append(rows, row)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}
