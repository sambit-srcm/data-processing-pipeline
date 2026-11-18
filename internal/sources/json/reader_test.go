package json

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadFileJSONArray(t *testing.T) {
	path := findRepoFile(t, "samples", "input", "coingecko.json")
	recs, err := ReadFile(path, "job-1", "crypto")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d", len(recs))
	}
}

func TestReadFileJSONL(t *testing.T) {
	path := findRepoFile(t, "samples", "input", "coingecko.jsonl")
	recs, err := ReadFile(path, "job-1", "crypto")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) < 100 {
		t.Fatalf("got %d rows, want a large scraper dump", len(recs))
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
