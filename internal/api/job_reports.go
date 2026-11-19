package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

// ProgressSource can return live progress for a running job.
type ProgressSource interface {
	LatestProgress(id domain.JobID) (domain.Progress, bool)
	ClearProgress(id domain.JobID)
}

func getProgress(jobs repository.JobRepository, progress ProgressSource, artifacts pipeline.ArtifactStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		job, err := jobs.Get(r.Context(), id)
		if err != nil {
			writeJobError(w, err, id)
			return
		}

		p, ok := progress.LatestProgress(id)
		if !ok && artifacts != nil {
			p, err = artifacts.GetProgress(r.Context(), id)
			if err != nil && !errors.Is(err, repository.ErrNotFound) {
				writeJobError(w, err, id)
				return
			}
			if errors.Is(err, repository.ErrNotFound) {
				p = domain.Progress{}
				if job.StartedAt != nil {
					p.StartedAt = *job.StartedAt
				}
				p.FinishedAt = job.FinishedAt
			}
		}

		writeJSON(w, http.StatusOK, buildProgressView(job, p))
	}
}

func getResults(jobs repository.JobRepository, artifacts pipeline.ArtifactStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		job, err := jobs.Get(r.Context(), id)
		if err != nil {
			writeJobError(w, err, id)
			return
		}

		aggs := []domain.Aggregation{}
		var recordCount int64
		if artifacts != nil {
			aggs, err = artifacts.ListAggregations(r.Context(), id)
			if err != nil {
				writeJobError(w, err, id)
				return
			}
			if p, perr := artifacts.GetProgress(r.Context(), id); perr == nil {
				recordCount = p.Valid
			}
		}
		if aggs == nil {
			aggs = []domain.Aggregation{}
		}
		exports := job.Config.Exports
		if exports == nil {
			exports = []domain.ExportConfig{}
		}

		writeJSON(w, http.StatusOK, ResultsView{
			JobID:        job.ID,
			Status:       job.Status,
			RecordCount:  recordCount,
			Aggregations: aggs,
			GlobalDaily:  domain.GlobalDailyFromAggregations(aggs),
			Exports:      exports,
		})
	}
}

func getErrors(jobs repository.JobRepository, artifacts pipeline.ArtifactStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		if _, err := jobs.Get(r.Context(), id); err != nil {
			writeJobError(w, err, id)
			return
		}

		errs := []domain.PipelineError{}
		if artifacts != nil {
			var err error
			errs, err = artifacts.ListErrors(r.Context(), id)
			if err != nil {
				writeJobError(w, err, id)
				return
			}
		}
		if errs == nil {
			errs = []domain.PipelineError{}
		}
		writeJSON(w, http.StatusOK, ErrorsView{JobID: id, Errors: errs})
	}
}

func deleteJobArtifacts(ctx context.Context, artifacts pipeline.ArtifactStore, progress ProgressSource, id domain.JobID) {
	if artifacts != nil {
		_ = artifacts.DeleteJob(ctx, id)
	}
	if progress != nil {
		progress.ClearProgress(id)
	}
}
