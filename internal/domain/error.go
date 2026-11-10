package domain

import "time"

// PipelineError is one failure collected from any stage.
type PipelineError struct {
	JobID    JobID
	SourceID SourceID
	RecordID RecordID
	Stage    Stage
	Message  string
	At       time.Time
}
