package domain

import "time"

// PipelineError is one failure collected from any stage.
type PipelineError struct {
	JobID    JobID     `json:"job_id"`
	SourceID SourceID  `json:"source_id,omitempty"`
	RecordID RecordID  `json:"record_id,omitempty"`
	Stage    Stage     `json:"stage,omitempty"`
	Message  string    `json:"message"`
	At       time.Time `json:"at"`
}
