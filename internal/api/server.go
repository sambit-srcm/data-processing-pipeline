// Package api is the HTTP adapter for the pipeline service.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
)

const defaultAddr = ":8080"

// New returns an HTTP server bound to addr.
// An empty addr uses :8080.
func New(addr string, jobs repository.JobRepository) *http.Server {
	if addr == "" {
		addr = defaultAddr
	}
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(jobs),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// Handler serves health, readiness, and job routes.
func Handler(jobs repository.JobRepository) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", ready)
	mux.HandleFunc("POST /api/v1/pipelines", createPipeline(jobs))
	mux.HandleFunc("GET /api/v1/pipelines", listPipelines(jobs))
	mux.HandleFunc("GET /api/v1/pipelines/{id}", getPipeline(jobs))
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

func createPipeline(jobs repository.JobRepository) http.HandlerFunc {
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
			if errors.Is(err, repository.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, ErrorBody{Error: "job not found"})
				return
			}
			slog.Error("get job", "err", err, "id", id)
			writeJSON(w, http.StatusInternalServerError, ErrorBody{Error: "internal server error"})
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write json response", "err", err)
	}
}
