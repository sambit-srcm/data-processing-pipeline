package domain

import "errors"

// PipelineConfig is the input that starts a job.
type PipelineConfig struct {
	Workers Workers        `json:"workers"`
	Buffers Buffers        `json:"buffers"`
	Sources []SourceConfig `json:"sources"`
	Exports []ExportConfig `json:"exports,omitempty"`
}

// Workers is how many goroutines each pool runs.
type Workers struct {
	Validation     int `json:"validation"`
	Transformation int `json:"transformation"`
}

// Buffers is the capacity of each pipeline channel.
// Zero is an unbuffered channel.
type Buffers struct {
	Records     int `json:"records"`
	Validated   int `json:"validated"`
	Transformed int `json:"transformed"`
	Errors      int `json:"errors"`
	Progress    int `json:"progress"`
}

// SourceConfig describes one input.
type SourceConfig struct {
	ID   SourceID   `json:"id"`
	Kind SourceKind `json:"kind"`
	Path string     `json:"path,omitempty"`
	URL  string     `json:"url,omitempty"`
}

// ExportConfig describes one output.
type ExportConfig struct {
	Kind ExportKind `json:"kind"`
	Path string     `json:"path,omitempty"`
}

// Validate checks counts and that the job has something to read.
func (c PipelineConfig) Validate() error {
	if c.Workers.Validation < 1 || c.Workers.Transformation < 1 {
		return errors.New("worker counts must be at least 1")
	}
	if c.Buffers.Records < 0 || c.Buffers.Validated < 0 || c.Buffers.Transformed < 0 || c.Buffers.Errors < 0 || c.Buffers.Progress < 0 {
		return errors.New("buffer sizes must be zero or greater")
	}
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	return nil
}
