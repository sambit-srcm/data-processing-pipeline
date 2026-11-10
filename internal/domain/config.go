package domain

import "errors"

// PipelineConfig is the input that starts a job.
type PipelineConfig struct {
	Workers Workers
	Buffers Buffers
	Sources []SourceConfig
	Exports []ExportConfig
}

// Workers is how many goroutines each pool runs.
type Workers struct {
	Validation     int
	Transformation int
}

// Buffers is the capacity of each pipeline channel.
// Zero is an unbuffered channel.
type Buffers struct {
	Records     int
	Validated   int
	Transformed int
	Errors      int
	Progress    int
}

// SourceConfig describes one input.
type SourceConfig struct {
	ID   SourceID
	Kind SourceKind
	Path string
	URL  string
}

// ExportConfig describes one output.
type ExportConfig struct {
	Kind ExportKind
	Path string
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
