// Package api is the HTTP adapter for the pipeline service.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

const defaultAddr = ":8080"

// Runner starts and cancels pipeline jobs.
type Runner interface {
	Start(job domain.Job)
	Cancel(id domain.JobID)
}

// New returns an HTTP server bound to addr.
// An empty addr uses :8080.
func New(addr string, jobs repository.JobRepository, runner Runner) *http.Server {
	if addr == "" {
		addr = defaultAddr
	}
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(jobs, runner),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// Handler serves health, readiness, and job routes.
func Handler(jobs repository.JobRepository, runner Runner) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", ready)
	mux.HandleFunc("POST /api/v1/pipelines", createPipeline(jobs, runner))
	mux.HandleFunc("GET /api/v1/pipelines", listPipelines(jobs))
	mux.HandleFunc("GET /api/v1/pipelines/{id}", getPipeline(jobs))
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", cancelPipeline(jobs, runner))
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", deletePipeline(jobs, runner))
	return withMiddleware(mux)
}

type statusBody struct {
	Status string `json:"status"`
}

// ErrorBody is a typed JSON error response.
type ErrorBody struct {
	Error string `json:"error"`
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
}

func ready(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ready"})
}

func createPipeline(jobs repository.JobRepository, runner Runner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var cfg domain.PipelineConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorBody{Error: "invalid JSON body"})
			return
		}
		if err := cfg.Validate(); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorBody{Error: err.Error()})
			return
		}

		job, err := jobs.Create(r.Context(), domain.Job{Config: cfg})
		if err != nil {
			slog.Error("create job", "err", err)
			writeJSON(w, http.StatusInternalServerError, ErrorBody{Error: "internal server error"})
			return
		}
		runner.Start(job)
		writeJSON(w, http.StatusCreated, job)
	}
}

func listPipelines(jobs repository.JobRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := jobs.List(r.Context())
		if err != nil {
			slog.Error("list jobs", "err", err)
			writeJSON(w, http.StatusInternalServerError, ErrorBody{Error: "internal server error"})
			return
		}
		if list == nil {
			list = []domain.Job{}
		}
		writeJSON(w, http.StatusOK, list)
	}
}

func getPipeline(jobs repository.JobRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		job, err := jobs.Get(r.Context(), id)
		if err != nil {
			writeJobError(w, err, id)
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func cancelPipeline(jobs repository.JobRepository, runner Runner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		job, err := cancelJob(r, jobs, runner, id)
		if err != nil {
			writeJobError(w, err, id)
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func deletePipeline(jobs repository.JobRepository, runner Runner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := domain.JobID(r.PathValue("id"))
		job, err := jobs.Get(r.Context(), id)
		if err != nil {
			writeJobError(w, err, id)
			return
		}

		if job.Status == domain.StatusRunning || job.Status == domain.StatusCancelling {
			if _, err := cancelJob(r, jobs, runner, id); err != nil && !errors.Is(err, repository.ErrInvalidTransition) {
				writeJobError(w, err, id)
				return
			}
		}

		if err := jobs.Delete(r.Context(), id); err != nil {
			writeJobError(w, err, id)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func cancelJob(r *http.Request, jobs repository.JobRepository, runner Runner, id domain.JobID) (domain.Job, error) {
	job, err := jobs.Get(r.Context(), id)
	if err != nil {
		return domain.Job{}, err
	}

	switch job.Status {
	case domain.StatusPending:
		return jobs.UpdateStatus(r.Context(), id, domain.StatusCancelled)
	case domain.StatusRunning:
		job, err = jobs.UpdateStatus(r.Context(), id, domain.StatusCancelling)
		if err != nil {
			return domain.Job{}, err
		}
		runner.Cancel(id)
		return waitCancelled(r.Context(), jobs, id, job)
	case domain.StatusCancelling:
		runner.Cancel(id)
		return waitCancelled(r.Context(), jobs, id, job)
	default:
		return domain.Job{}, repository.ErrInvalidTransition
	}
}

func waitCancelled(ctx context.Context, jobs repository.JobRepository, id domain.JobID, fallback domain.Job) (domain.Job, error) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := jobs.Get(ctx, id)
		if err != nil {
			return domain.Job{}, err
		}
		if job.Status == domain.StatusCancelled || job.Status == domain.StatusFailed {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return fallback, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return jobs.Get(ctx, id)
}

func writeJobError(w http.ResponseWriter, err error, id domain.JobID) {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, ErrorBody{Error: "job not found"})
	case errors.Is(err, repository.ErrInvalidTransition):
		writeJSON(w, http.StatusConflict, ErrorBody{Error: "invalid job status transition"})
	default:
		slog.Error("job request", "err", err, "id", id)
		writeJSON(w, http.StatusInternalServerError, ErrorBody{Error: "internal server error"})
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write json response", "err", err)
	}
}
