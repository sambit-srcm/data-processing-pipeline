package csv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadFile(t *testing.T) {
	path := findRepoFile(t, "samples", "input", "hw_200.csv")
	recs, err := ReadFile(path, "job-1", "hw")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 202 {
		t.Fatalf("got %d rows, want 202", len(recs))
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
