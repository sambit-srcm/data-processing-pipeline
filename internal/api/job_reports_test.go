package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

func TestPipelineProgressResultsErrors(t *testing.T) {
	srv, _, runner := newRunnerServer(t)
	runner.Sleep = time.Millisecond

	created := createJob(t, srv)
	waitStatus(t, srv, created.ID, domain.StatusSucceeded)

	var progress ProgressView
	status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(created.ID)+"/progress", nil, &progress)
	if status != http.StatusOK {
		t.Fatalf("progress status = %d", status)
	}
	if progress.JobID != created.ID {
		t.Fatalf("progress job = %s", progress.JobID)
	}
	if progress.Processed == 0 {
		t.Fatal("expected processed > 0")
	}
	if progress.PercentComplete == nil {
		t.Fatal("expected percent_complete")
	}

	var results ResultsView
	status = requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(created.ID)+"/results", nil, &results)
	if status != http.StatusOK {
		t.Fatalf("results status = %d", status)
	}
	if results.JobID != created.ID {
		t.Fatalf("results job = %s", results.JobID)
	}
	if results.Aggregations == nil {
		t.Fatal("aggregations should be a list")
	}

	var errs ErrorsView
	status = requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(created.ID)+"/errors", nil, &errs)
	if status != http.StatusOK {
		t.Fatalf("errors status = %d", status)
	}
	if errs.Errors == nil {
		t.Fatal("errors should be a list")
	}
}

func TestPipelineReportsNotFound(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/progress", "/results", "/errors"} {
		var body ErrorBody
		status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/missing"+path, nil, &body)
		if status != http.StatusNotFound {
			t.Fatalf("%s status = %d", path, status)
		}
	}
}
