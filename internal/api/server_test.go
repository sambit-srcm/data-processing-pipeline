package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
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

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(Handler(memory.NewJobStore()))
	t.Cleanup(srv.Close)
	return srv
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
