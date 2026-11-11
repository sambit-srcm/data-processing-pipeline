package config

import "testing"

func TestDefaultValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := Default()
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadEnvOverlay(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("VALIDATION_WORKERS", "3")
	t.Setenv("TRANSFORM_WORKERS", "4")
	t.Setenv("RECORDS_BUFFER", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Fatalf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if cfg.Workers.Validation != 3 || cfg.Workers.Transformation != 4 {
		t.Fatalf("workers = %+v", cfg.Workers)
	}
	if cfg.Buffers.Records != 10 {
		t.Fatalf("Records = %d", cfg.Buffers.Records)
	}
}

func TestLoadInvalidWorker(t *testing.T) {
	t.Setenv("VALIDATION_WORKERS", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for VALIDATION_WORKERS=0")
	}
}

func TestLoadInvalidInt(t *testing.T) {
	t.Setenv("RECORDS_BUFFER", "abc")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for RECORDS_BUFFER=abc")
	}
}

func TestValidateEmptyAddr(t *testing.T) {
	cfg := Default()
	cfg.HTTPAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty HTTPAddr")
	}
}
