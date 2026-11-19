package export

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// WriteAll writes each configured export target.
func WriteAll(ctx context.Context, exports []domain.ExportConfig, records []domain.Record, aggs []domain.Aggregation, daily []domain.GlobalDailyReport) error {
	for _, cfg := range exports {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writeOne(cfg, records, aggs, daily); err != nil {
			return err
		}
	}
	return nil
}

func writeOne(cfg domain.ExportConfig, records []domain.Record, aggs []domain.Aggregation, daily []domain.GlobalDailyReport) error {
	if cfg.Path == "" {
		return fmt.Errorf("export path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return err
	}
	switch cfg.Kind {
	case domain.ExportCSV:
		return writeCSV(cfg.Path, records, aggs, daily)
	case domain.ExportJSON:
		return writeJSON(cfg.Path, records, aggs, daily)
	default:
		return fmt.Errorf("unsupported export kind: %s", cfg.Kind)
	}
}
