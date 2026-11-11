// Package api is the HTTP adapter for the pipeline service.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

const defaultAddr = ":8080"

// New returns an HTTP server bound to addr.
// An empty addr uses :8080.
func New(addr string) *http.Server {
	if addr == "" {
		addr = defaultAddr
	}
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// Handler serves health and readiness routes.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", ready)
	return mux
}

type statusBody struct {
	Status string `json:"status"`
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
}

func ready(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ready"})
}

func writeJSON(w http.ResponseWriter, code int, body statusBody) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write json response", "err", err)
	}
}
