package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/api"
	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
	httpsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/http"
)

type testEnv struct {
	URL        string
	Runner     *pipeline.Runner
	Jobs       *memory.JobStore
	Arts       *memory.ArtifactStore
	WeatherURL string
}

func startEnv(t *testing.T) *testEnv {
	t.Helper()

	weather := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"latitude": 20.0,
			"longitude": 85.0,
			"current": {"temperature_2m": 29.5, "time": "2026-11-18T12:00"}
		}`))
	}))
	t.Cleanup(weather.Close)

	jobs := memory.NewJobStore()
	arts := memory.NewArtifactStore()
	runner := pipeline.NewRunner(jobs)
	runner.Artifacts = arts
	runner.Sleep = time.Millisecond
	runner.Weather = httpsources.Client{HTTP: weather.Client()}

	srv := httptest.NewServer(api.Handler(jobs, runner, arts))
	t.Cleanup(func() {
		runner.Shutdown(context.Background())
		srv.Close()
	})

	return &testEnv{
		URL:        srv.URL,
		Runner:     runner,
		Jobs:       jobs,
		Arts:       arts,
		WeatherURL: weather.URL,
	}
}

func samplePath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidate := filepath.Join(dir, "samples", "input", name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			t.Fatalf("sample not found: %s", name)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("sample not found: %s", name)
		}
		dir = parent
	}
}

func defaultWorkers() domain.Workers {
	return domain.Workers{Validation: 2, Transformation: 2}
}

func defaultBuffers() domain.Buffers {
	return domain.Buffers{Records: 32, Validated: 32, Transformed: 32, Errors: 32, Progress: 32}
}

func postJob(t *testing.T, env *testEnv, cfg domain.PipelineConfig) domain.Job {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var job domain.Job
	status := doJSON(t, http.MethodPost, env.URL+"/api/v1/pipelines", raw, &job)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d", status)
	}
	return job
}

func waitJob(t *testing.T, env *testEnv, id domain.JobID, want ...domain.JobStatus) domain.Job {
	t.Helper()
	wanted := map[domain.JobStatus]bool{}
	for _, s := range want {
		wanted[s] = true
	}
	deadline := time.Now().Add(5 * time.Second)
	var job domain.Job
	for {
		status := doJSON(t, http.MethodGet, env.URL+"/api/v1/pipelines/"+string(id), nil, &job)
		if status == http.StatusOK && wanted[job.Status] {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s status = %s, want one of %v", id, job.Status, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func doJSON(t *testing.T, method, url string, body []byte, dst any) int {
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
	defer func() { _ = resp.Body.Close() }()
	if dst != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}
