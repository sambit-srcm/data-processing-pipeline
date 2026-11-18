package api

import (
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

// ProgressView is the API response for GET .../progress.
type ProgressView struct {
	JobID           domain.JobID     `json:"job_id"`
	Status          domain.JobStatus `json:"status"`
	Total           *int64           `json:"total,omitempty"`
	Processed       int64            `json:"processed"`
	Valid           int64            `json:"valid"`
	Failed          int64            `json:"failed"`
	Pending         *int64           `json:"pending,omitempty"`
	PercentComplete *float64         `json:"percent_complete,omitempty"`
	RecordsPerSec   float64          `json:"records_per_sec"`
	StartedAt       *time.Time       `json:"started_at,omitempty"`
	FinishedAt      *time.Time       `json:"finished_at,omitempty"`
}

// ResultsView is the API response for GET .../results.
type ResultsView struct {
	JobID        domain.JobID          `json:"job_id"`
	Status       domain.JobStatus      `json:"status"`
	RecordCount  int64                 `json:"record_count"`
	Aggregations []domain.Aggregation  `json:"aggregations"`
	Exports      []domain.ExportConfig `json:"exports"`
}

// ErrorsView is the API response for GET .../errors.
type ErrorsView struct {
	JobID  domain.JobID           `json:"job_id"`
	Errors []domain.PipelineError `json:"errors"`
}

func buildProgressView(job domain.Job, p domain.Progress) ProgressView {
	view := ProgressView{
		JobID:      job.ID,
		Status:     job.Status,
		Total:      p.Total,
		Processed:  p.Processed,
		Valid:      p.Valid,
		Failed:     p.Failed,
		FinishedAt: p.FinishedAt,
	}
	if !p.StartedAt.IsZero() {
		started := p.StartedAt
		view.StartedAt = &started
	} else if job.StartedAt != nil {
		view.StartedAt = job.StartedAt
	}
	if job.FinishedAt != nil && view.FinishedAt == nil {
		view.FinishedAt = job.FinishedAt
	}
	if pct, ok := p.Percentage(); ok {
		view.PercentComplete = &pct
	}
	if pending, ok := p.Pending(); ok {
		view.Pending = &pending
	}
	view.RecordsPerSec = recordsPerSec(p, view.StartedAt, view.FinishedAt)
	return view
}

func recordsPerSec(p domain.Progress, started, finished *time.Time) float64 {
	if started == nil || p.Processed == 0 {
		return 0
	}
	end := time.Now().UTC()
	if finished != nil {
		end = finished.UTC()
	}
	sec := end.Sub(started.UTC()).Seconds()
	if sec <= 0 {
		return 0
	}
	return float64(p.Processed) / sec
}
