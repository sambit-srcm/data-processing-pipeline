package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
)

func TestMiddlewareStatusCodes(t *testing.T) {
	store := memory.NewJobStore()
	srv := httptest.NewServer(Handler(store))
	t.Cleanup(srv.Close)

	assertStatus := func(t *testing.T, method, path string, body []byte, want int, dst any) {
		t.Helper()
		status, reqID := doJSON(t, method, srv.URL+path, body, dst)
		if status != want {
			t.Fatalf("status = %d, want %d", status, want)
		}
		if reqID == "" {
			t.Fatal("expected X-Request-ID")
		}
	}

	var health statusBody
	assertStatus(t, http.MethodGet, "/health", nil, http.StatusOK, &health)

	var ready statusBody
	assertStatus(t, http.MethodGet, "/ready", nil, http.StatusOK, &ready)

	var created domain.Job
	assertStatus(t, http.MethodPost, "/api/v1/pipelines", validConfigJSON(), http.StatusCreated, &created)

	var list []domain.Job
	assertStatus(t, http.MethodGet, "/api/v1/pipelines", nil, http.StatusOK, &list)

	var got domain.Job
	assertStatus(t, http.MethodGet, "/api/v1/pipelines/"+string(created.ID), nil, http.StatusOK, &got)

	var bad ErrorBody
	assertStatus(t, http.MethodPost, "/api/v1/pipelines", []byte(`{`), http.StatusBadRequest, &bad)

	var missing ErrorBody
	assertStatus(t, http.MethodGet, "/api/v1/pipelines/missing", nil, http.StatusNotFound, &missing)

	srv500 := httptest.NewServer(Handler(errStore{createErr: errors.New("boom")}))
	t.Cleanup(srv500.Close)
	var internal ErrorBody
	status, reqID := doJSON(t, http.MethodPost, srv500.URL+"/api/v1/pipelines", validConfigJSON(), &internal)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d", status)
	}
	if reqID == "" {
		t.Fatal("expected X-Request-ID")
	}
}

func TestRequestIDPropagated(t *testing.T) {
	srv := newTestServer(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(requestIDHeader, "fixed-id-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if got := resp.Header.Get(requestIDHeader); got != "fixed-id-123" {
		t.Fatalf("request id = %q", got)
	}
}

func TestRecoverMiddleware(t *testing.T) {
	h := withMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Header().Get(requestIDHeader) == "" {
		t.Fatal("expected X-Request-ID")
	}
	var body ErrorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "internal server error" {
		t.Fatalf("error = %q", body.Error)
	}
}

func doJSON(t *testing.T, method, url string, body []byte, dst any) (int, string) {
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
	return resp.StatusCode, resp.Header.Get(requestIDHeader)
}

type errStore struct {
	createErr error
}

func (e errStore) Create(context.Context, domain.Job) (domain.Job, error) {
	return domain.Job{}, e.createErr
}
func (e errStore) Get(context.Context, domain.JobID) (domain.Job, error) {
	return domain.Job{}, repository.ErrNotFound
}
func (e errStore) List(context.Context) ([]domain.Job, error) { return nil, nil }
func (e errStore) UpdateStatus(context.Context, domain.JobID, domain.JobStatus) (domain.Job, error) {
	return domain.Job{}, repository.ErrNotFound
}
func (e errStore) Delete(context.Context, domain.JobID) error { return repository.ErrNotFound }
