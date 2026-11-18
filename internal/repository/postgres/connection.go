package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  config_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS job_errors (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  source_id TEXT,
  record_id TEXT,
  stage TEXT,
  message TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS job_aggregations (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  source_id TEXT NOT NULL,
  group_key TEXT NOT NULL,
  count BIGINT NOT NULL,
  sum DOUBLE PRECISION NOT NULL,
  min DOUBLE PRECISION NOT NULL,
  max DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS job_errors_job_id_idx ON job_errors(job_id);
CREATE INDEX IF NOT EXISTS job_aggregations_job_id_idx ON job_aggregations(job_id);
`

// Open connects to Postgres, pings, and applies schema.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}
