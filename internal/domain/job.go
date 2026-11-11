package domain

import "time"

// JobStatus is the lifecycle state of a job.
type JobStatus string

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
	ID         JobID
	Status     JobStatus
	Config     PipelineConfig
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
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
