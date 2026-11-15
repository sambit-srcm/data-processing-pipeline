package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline/validation"
	csvsources "github.com/sambit-srcm/data-processing-pipeline/internal/sources/csv"
)

func TestCoordinatorCSVValidation(t *testing.T) {
	path := findRepoFile(t, "samples", "input", "covid.csv")
	recs, err := csvsources.ReadFile(path, "job-1", "covid")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	c := Coordinator{Validator: validation.COVID{}}
	res, err := c.Run(context.Background(), cfg, recs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Progress.Valid != 3 {
		t.Fatalf("valid = %d, want 3", res.Progress.Valid)
	}
	if res.Progress.Failed != 2 {
		t.Fatalf("failed = %d, want 2", res.Progress.Failed)
	}
	if len(res.Errors) != 2 {
		t.Fatalf("errors = %d, want 2", len(res.Errors))
	}
	if len(res.Records) != 3 {
		t.Fatalf("records = %d, want 3", len(res.Records))
	}
}

func findRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidate := filepath.Join(append([]string{dir}, parts...)...)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			t.Fatalf("missing %s", filepath.Join(parts...))
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
