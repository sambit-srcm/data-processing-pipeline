package domain

import "time"

// JobStatus is the lifecycle state of a job.
type JobStatus string

// Job lifecycle statuses.
const (
	StatusPending    JobStatus = "pending"
	StatusRunning    JobStatus = "running"
	StatusCancelling JobStatus = "cancelling"
	StatusCancelled  JobStatus = "cancelled"
	StatusSucceeded  JobStatus = "succeeded"
	StatusFailed     JobStatus = "failed"
	StatusPartial    JobStatus = "partial"
)

// Job is one pipeline execution.
type Job struct {
	ID         JobID          `json:"id"`
	Status     JobStatus      `json:"status"`
	Config     PipelineConfig `json:"config"`
	CreatedAt  time.Time      `json:"created_at"`
	StartedAt  *time.Time     `json:"started_at,omitempty"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
}

// CanTransitionTo reports whether next is a legal status change.
func (s JobStatus) CanTransitionTo(next JobStatus) bool {
	switch s {
	case StatusPending:
		return next == StatusRunning || next == StatusCancelled
	case StatusRunning:
		return next == StatusCancelling || next == StatusSucceeded || next == StatusFailed || next == StatusPartial
	case StatusCancelling:
		return next == StatusCancelled || next == StatusFailed
	default:
		return false
	}
}
