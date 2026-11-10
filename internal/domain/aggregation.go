package domain

// Aggregation is one group-by result for a single source.
type Aggregation struct {
	JobID    JobID
	SourceID SourceID
	GroupKey string
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
}

// Average returns Sum / Count.
// The boolean is false when Count is zero.
func (a Aggregation) Average() (float64, bool) {
	if a.Count == 0 {
		return 0, false
	}
	return a.Sum / float64(a.Count), true
}
