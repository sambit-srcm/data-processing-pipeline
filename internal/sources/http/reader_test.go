package http

import (
	"context"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchOK(t *testing.T) {
	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		_, _ = w.Write([]byte(`{"latitude":1.0,"longitude":2.0,"current":{"temperature_2m":21.5,"time":"2024-01-01T12:00"}}`))
	}))
	t.Cleanup(srv.Close)

	c := Client{HTTP: srv.Client()}
	recs, err := c.Fetch(context.Background(), srv.URL, "job-1", "weather")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d", len(recs))
	}
}

func TestFetchHTTPError(t *testing.T) {
	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := Client{HTTP: srv.Client()}
	if _, err := c.Fetch(context.Background(), srv.URL, "job-1", "weather"); err == nil {
		t.Fatal("expected error")
	}
}
