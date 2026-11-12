package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
)

func TestHealthAndReady(t *testing.T) {
	srv := newTestServer(t)

	for _, path := range []string{"/health", "/ready"} {
		var body statusBody
		status := requestJSON(t, http.MethodGet, srv.URL+path, nil, &body)
		if status != http.StatusOK {
			t.Fatalf("%s status = %d", path, status)
		}
		if body.Status == "" {
			t.Fatalf("%s returned an empty status", path)
		}
	}
}

func TestCreatePipeline(t *testing.T) {
	srv := newTestServer(t)
	job := createJob(t, srv)
	if job.ID == "" || job.Status != domain.StatusPending {
		t.Fatalf("bad job: %+v", job)
	}
}

func TestListPipelines(t *testing.T) {
	srv := newTestServer(t)
	created := createJob(t, srv)

	var list []domain.Job
	status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines", nil, &list)
	if status != http.StatusOK {
		t.Fatalf("list status = %d", status)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("list = %+v", list)
	}
}

func TestGetPipeline(t *testing.T) {
	srv := newTestServer(t)
	created := createJob(t, srv)

	var got domain.Job
	status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(created.ID), nil, &got)
	if status != http.StatusOK {
		t.Fatalf("get status = %d", status)
	}
	if got.ID != created.ID {
		t.Fatalf("got %+v", got)
	}
}

func TestCreatePipelineBadRequest(t *testing.T) {
	srv := newTestServer(t)

	cases := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "invalid config", body: `{"workers":{"validation":0,"transformation":1},"sources":[{"id":"a","kind":"csv","path":"a.csv"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body ErrorBody
			status := requestJSON(t, http.MethodPost, srv.URL+"/api/v1/pipelines", []byte(tc.body), &body)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d", status)
			}
			if body.Error == "" {
				t.Fatal("expected error message")
			}
		})
	}
}

func TestGetPipelineNotFound(t *testing.T) {
	srv := newTestServer(t)

	var body ErrorBody
	status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/missing", nil, &body)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d", status)
	}
	if body.Error != "job not found" {
		t.Fatalf("error = %q", body.Error)
	}
}

func TestCancelPipeline(t *testing.T) {
	srv, _, runner := newRunnerServer(t)
	runner.Sleep = time.Hour

	created := createJob(t, srv)
	waitStatus(t, srv, created.ID, domain.StatusRunning)

	var job domain.Job
	status := requestJSON(t, http.MethodPatch, srv.URL+"/api/v1/pipelines/"+string(created.ID)+"/cancel", nil, &job)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if job.Status != domain.StatusCancelled {
		t.Fatalf("status = %s", job.Status)
	}
}

func TestCancelPipelineNotFound(t *testing.T) {
	srv := newTestServer(t)
	var body ErrorBody
	status := requestJSON(t, http.MethodPatch, srv.URL+"/api/v1/pipelines/missing/cancel", nil, &body)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d", status)
	}
}

func TestDeletePipeline(t *testing.T) {
	srv := newTestServer(t)
	created := createJob(t, srv)

	status := requestNoContent(t, http.MethodDelete, srv.URL+"/api/v1/pipelines/"+string(created.ID))
	if status != http.StatusNoContent {
		t.Fatalf("status = %d", status)
	}

	var body ErrorBody
	status = requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(created.ID), nil, &body)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d", status)
	}
}

func TestDeleteRunningPipelineCancelsFirst(t *testing.T) {
	srv, store, runner := newRunnerServer(t)
	runner.Sleep = time.Hour

	created := createJob(t, srv)
	waitStatus(t, srv, created.ID, domain.StatusRunning)

	status := requestNoContent(t, http.MethodDelete, srv.URL+"/api/v1/pipelines/"+string(created.ID))
	if status != http.StatusNoContent {
		t.Fatalf("status = %d", status)
	}
	if _, err := store.Get(context.Background(), created.ID); err == nil {
		t.Fatal("expected job deleted")
	}
}

type noopRunner struct{}

func (noopRunner) Start(domain.Job)    {}
func (noopRunner) Cancel(domain.JobID) {}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(Handler(memory.NewJobStore(), noopRunner{}))
	t.Cleanup(srv.Close)
	return srv
}

func newRunnerServer(t *testing.T) (*httptest.Server, *memory.JobStore, *pipeline.Runner) {
	t.Helper()
	store := memory.NewJobStore()
	runner := pipeline.NewRunner(store)
	srv := httptest.NewServer(Handler(store, runner))
	t.Cleanup(func() {
		runner.Shutdown(context.Background())
		srv.Close()
	})
	return srv, store, runner
}

func waitStatus(t *testing.T, srv *httptest.Server, id domain.JobID, want domain.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var job domain.Job
		status := requestJSON(t, http.MethodGet, srv.URL+"/api/v1/pipelines/"+string(id), nil, &job)
		if status == http.StatusOK && job.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s, want %s", job.Status, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func requestNoContent(t *testing.T, method, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	return resp.StatusCode
}

func validConfigJSON() []byte {
	return []byte(`{
		"workers": {"validation": 1, "transformation": 1},
		"buffers": {},
		"sources": [{"id": "covid", "kind": "csv", "path": "a.csv"}]
	}`)
}

func createJob(t *testing.T, srv *httptest.Server) domain.Job {
	t.Helper()
	var job domain.Job
	status := requestJSON(t, http.MethodPost, srv.URL+"/api/v1/pipelines", validConfigJSON(), &job)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d", status)
	}
	return job
}

func requestJSON(t *testing.T, method, url string, body []byte, dst any) int {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode
}
