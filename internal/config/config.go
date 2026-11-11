package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config is process settings loaded at startup.
type Config struct {
	HTTPAddr string
	Workers  Workers
	Buffers  Buffers
}

// Workers is the default size of each worker pool.
type Workers struct {
	Validation     int
	Transformation int
}

// Buffers is the default capacity of each pipeline channel.
type Buffers struct {
	Records     int
	Validated   int
	Transformed int
	Errors      int
	Progress    int
}

// Default returns the built-in process settings.
func Default() Config {
	return Config{
		HTTPAddr: ":8080",
		Workers: Workers{
			Validation:     5,
			Transformation: 5,
		},
		Buffers: Buffers{
			Records:     100,
			Validated:   100,
			Transformed: 100,
			Errors:      100,
			Progress:    100,
		},
	}
}

// Load reads environment variables over Default and validates the result.
func Load() (Config, error) {
	cfg := Default()

	if v, ok := os.LookupEnv("HTTP_ADDR"); ok {
		cfg.HTTPAddr = v
	}

	var err error
	if cfg.Workers.Validation, err = intFromEnv("VALIDATION_WORKERS", cfg.Workers.Validation); err != nil {
		return Config{}, err
	}
	if cfg.Workers.Transformation, err = intFromEnv("TRANSFORM_WORKERS", cfg.Workers.Transformation); err != nil {
		return Config{}, err
	}
	if cfg.Buffers.Records, err = intFromEnv("RECORDS_BUFFER", cfg.Buffers.Records); err != nil {
		return Config{}, err
	}
	if cfg.Buffers.Validated, err = intFromEnv("VALIDATED_BUFFER", cfg.Buffers.Validated); err != nil {
		return Config{}, err
	}
	if cfg.Buffers.Transformed, err = intFromEnv("TRANSFORMED_BUFFER", cfg.Buffers.Transformed); err != nil {
		return Config{}, err
	}
	if cfg.Buffers.Errors, err = intFromEnv("ERRORS_BUFFER", cfg.Buffers.Errors); err != nil {
		return Config{}, err
	}
	if cfg.Buffers.Progress, err = intFromEnv("PROGRESS_BUFFER", cfg.Buffers.Progress); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks process settings.
func (c Config) Validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("HTTP_ADDR must not be empty")
	}
	if c.Workers.Validation < 1 || c.Workers.Transformation < 1 {
		return fmt.Errorf("worker counts must be at least 1")
	}
	if c.Buffers.Records < 0 || c.Buffers.Validated < 0 || c.Buffers.Transformed < 0 || c.Buffers.Errors < 0 || c.Buffers.Progress < 0 {
		return fmt.Errorf("buffer sizes must be zero or greater")
	}
	return nil
}

func intFromEnv(key string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
