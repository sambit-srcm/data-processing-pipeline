package domain

import "time"

// Progress is a point-in-time view of one job.
// Total is unset when the source cannot know the count ahead of time.
type Progress struct {
	Total      *int64
	Processed  int64
	Valid      int64
	Failed     int64
	StartedAt  time.Time
	FinishedAt *time.Time
}

// Percentage returns how much of Total has been processed.
// The boolean is false when Total is unknown or zero.
func (p Progress) Percentage() (float64, bool) {
	if p.Total == nil || *p.Total <= 0 {
		return 0, false
	}
	return float64(p.Processed) / float64(*p.Total) * 100, true
}

// Pending returns how many records are still outstanding.
// The boolean is false when Total is unknown.
func (p Progress) Pending() (int64, bool) {
	if p.Total == nil {
		return 0, false
	}
	left := *p.Total - p.Processed
	if left < 0 {
		return 0, true
	}
	return left, true
}
