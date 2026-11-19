package domain

import (
	"errors"
	"fmt"
)

// PipelineConfig is the input that starts a job.
type PipelineConfig struct {
	Workers         Workers             `json:"workers"`
	Buffers         Buffers             `json:"buffers"`
	Sources         []SourceConfig      `json:"sources"`
	Transformations []TransformConfig   `json:"transformations,omitempty"`
	Aggregations    []AggregationConfig `json:"aggregations,omitempty"`
	Exports         []ExportConfig      `json:"exports,omitempty"`
}

// TransformKind names a built-in transformation step.
type TransformKind string

const (
	// TransformNormalize trims/uppercases source-specific fields.
	TransformNormalize TransformKind = "normalize"
)

// TransformConfig is one transformation applied in order.
type TransformConfig struct {
	Kind TransformKind `json:"kind"`
}

// AggregationKind names a built-in aggregation.
type AggregationKind string

const (
	// AggregationStats folds records into per-group count/sum/min/max.
	AggregationStats AggregationKind = "stats"
	// AggregationGlobalDaily builds a cross-source report for the run day.
	AggregationGlobalDaily AggregationKind = "global_daily"
)

// AggregationConfig defines grouping and which numeric field is folded (count/sum/min/max).
// Kind "global_daily" builds a cross-source daily report; group_by/value_field are ignored.
type AggregationConfig struct {
	Kind       AggregationKind `json:"kind"`
	GroupBy    string          `json:"group_by,omitempty"`    // auto | source | symbol (stats only)
	ValueField string          `json:"value_field,omitempty"` // auto | weight | height | price | temp_c (stats only)
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

// ApplyDefaults fills missing transformation and aggregation definitions.
func (c *PipelineConfig) ApplyDefaults() {
	if len(c.Transformations) == 0 {
		c.Transformations = []TransformConfig{{Kind: TransformNormalize}}
	}
	if len(c.Aggregations) == 0 {
		c.Aggregations = []AggregationConfig{{
			Kind:       AggregationStats,
			GroupBy:    "auto",
			ValueField: "auto",
		}}
	}
}

// Validate checks counts and that the job has something to read.
func (c PipelineConfig) Validate() error {
	if c.Workers.Validation < 1 || c.Workers.Transformation < 1 {
		return errors.New("worker counts must be at least 1")
	}
	if err := c.Buffers.validate(); err != nil {
		return err
	}
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	if err := validateTransforms(c.Transformations); err != nil {
		return err
	}
	if err := validateAggregations(c.Aggregations); err != nil {
		return err
	}
	return validateExports(c.Exports)
}

func (b Buffers) validate() error {
	if b.Records < 0 || b.Validated < 0 || b.Transformed < 0 || b.Errors < 0 || b.Progress < 0 {
		return errors.New("buffer sizes must be zero or greater")
	}
	return nil
}

func validateTransforms(items []TransformConfig) error {
	for _, t := range items {
		if t.Kind != TransformNormalize {
			return fmt.Errorf("unsupported transform kind: %s", t.Kind)
		}
	}
	return nil
}

func validateAggregations(items []AggregationConfig) error {
	for _, a := range items {
		switch a.Kind {
		case AggregationStats:
			if err := validateStatsAggregation(a); err != nil {
				return err
			}
		case AggregationGlobalDaily:
		default:
			return fmt.Errorf("unsupported aggregation kind: %s", a.Kind)
		}
	}
	return nil
}

func validateStatsAggregation(a AggregationConfig) error {
	switch a.GroupBy {
	case "", "auto", "source", "symbol":
	default:
		return fmt.Errorf("unsupported aggregation group_by: %s", a.GroupBy)
	}
	switch a.ValueField {
	case "", "auto", "weight", "height", "price", "temp_c":
	default:
		return fmt.Errorf("unsupported aggregation value_field: %s", a.ValueField)
	}
	return nil
}

func validateExports(items []ExportConfig) error {
	for _, e := range items {
		if e.Path == "" {
			return errors.New("export path is required")
		}
		switch e.Kind {
		case ExportCSV, ExportJSON:
		default:
			return fmt.Errorf("unsupported export kind: %s", e.Kind)
		}
	}
	return nil
}
