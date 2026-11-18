// Command api starts the pipeline HTTP server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/api"
	"github.com/sambit-srcm/data-processing-pipeline/internal/config"
	"github.com/sambit-srcm/data-processing-pipeline/internal/pipeline"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/memory"
	"github.com/sambit-srcm/data-processing-pipeline/internal/repository/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	jobs, artifacts, cleanup, err := newStores(cfg)
	if err != nil {
		slog.Error("store", "err", err)
		os.Exit(1)
	}
	defer cleanup()

	runner := pipeline.NewRunner(jobs)
	runner.Artifacts = artifacts
	srv := api.New(cfg.HTTPAddr, jobs, runner)
	slog.Info("http server listening",
		"addr", srv.Addr,
		"postgres", cfg.DatabaseURL != "",
		"validation_workers", cfg.Workers.Validation,
		"transform_workers", cfg.Workers.Transformation,
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server stopped", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		runner.Shutdown(shutdownCtx)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http server shutdown", "err", err)
			os.Exit(1)
		}
	}
}

func newStores(cfg config.Config) (repository.JobRepository, pipeline.ArtifactStore, func(), error) {
	if cfg.DatabaseURL == "" {
		return memory.NewJobStore(), nil, func() {}, nil
	}
	db, err := postgres.Open(cfg.DatabaseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	return postgres.NewJobStore(db), postgres.NewArtifactStore(db), func() { _ = db.Close() }, nil
}
