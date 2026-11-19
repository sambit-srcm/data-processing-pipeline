package export

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func writeCSV(path string, records []domain.Record, aggs []domain.Aggregation, daily []domain.GlobalDailyReport) error {
	if err := writeRecordCSV(path, records); err != nil {
		return err
	}
	if len(aggs) > 0 {
		if err := writeAggCSV(aggPath(path), aggs); err != nil {
			return err
		}
	}
	if len(daily) == 0 {
		return nil
	}
	return writeGlobalDailyCSV(globalDailyPath(path), daily)
}

func writeRecordCSV(path string, records []domain.Record) error {
	f, err := os.Create(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	header := []string{
		"id", "job_id", "source_id", "observed_at", "kind",
		"index", "height", "weight",
		"symbol", "price",
		"latitude", "longitude", "temp_c", "at",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, rec := range records {
		row := []string{
			string(rec.ID),
			string(rec.JobID),
			string(rec.SourceID),
			rec.ObservedAt.UTC().Format(time.RFC3339Nano),
			string(domain.SourceKindOf(rec.Payload)),
			"", "", "",
			"", "",
			"", "", "", "",
		}
		switch p := rec.Payload.(type) {
		case domain.HeightWeight:
			row[5] = strconv.FormatInt(p.Index, 10)
			row[6] = formatFloat(p.Height)
			row[7] = formatFloat(p.Weight)
		case domain.CryptoPrice:
			row[8] = p.Symbol
			row[9] = formatFloat(p.Price)
			row[13] = p.At.UTC().Format(time.RFC3339Nano)
		case domain.WeatherObservation:
			row[10] = formatFloat(p.Latitude)
			row[11] = formatFloat(p.Longitude)
			row[12] = formatFloat(p.TempC)
			row[13] = p.At.UTC().Format(time.RFC3339Nano)
		default:
			return fmt.Errorf("unsupported payload type %T", rec.Payload)
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeAggCSV(path string, aggs []domain.Aggregation) error {
	f, err := os.Create(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"job_id", "source_id", "group_key", "count", "sum", "min", "max"}); err != nil {
		return err
	}
	for _, a := range aggs {
		if err := w.Write([]string{
			string(a.JobID),
			string(a.SourceID),
			a.GroupKey,
			strconv.FormatInt(a.Count, 10),
			formatFloat(a.Sum),
			formatFloat(a.Min),
			formatFloat(a.Max),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeGlobalDailyCSV(path string, reports []domain.GlobalDailyReport) error {
	f, err := os.Create(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"job_id", "date", "kind", "count", "sum", "min", "max", "total_records"}); err != nil {
		return err
	}
	for _, rep := range reports {
		for _, k := range rep.Kinds {
			if err := w.Write([]string{
				string(rep.JobID),
				rep.Date,
				string(k.Kind),
				strconv.FormatInt(k.Count, 10),
				formatFloat(k.Sum),
				formatFloat(k.Min),
				formatFloat(k.Max),
				strconv.FormatInt(rep.TotalRecords, 10),
			}); err != nil {
				return err
			}
		}
	}
	w.Flush()
	return w.Error()
}

func aggPath(path string) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	if ext == "" {
		return path + ".aggs.csv"
	}
	return base + ".aggs" + ext
}

func globalDailyPath(path string) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	if ext == "" {
		return path + ".global_daily.csv"
	}
	return base + ".global_daily" + ext
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
