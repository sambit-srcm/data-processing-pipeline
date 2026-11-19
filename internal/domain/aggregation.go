package domain

// Aggregation is one group-by result for a single source.
type Aggregation struct {
	JobID    JobID    `json:"job_id"`
	SourceID SourceID `json:"source_id"`
	GroupKey string   `json:"group_key"`
	Count    int64    `json:"count"`
	Sum      float64  `json:"sum"`
	Min      float64  `json:"min"`
	Max      float64  `json:"max"`
}

// Average returns Sum / Count.
// The boolean is false when Count is zero.
func (a Aggregation) Average() (float64, bool) {
	if a.Count == 0 {
		return 0, false
	}
	return a.Sum / float64(a.Count), true
}

// GlobalDailyReport combines all source kinds for one calendar day (UTC).
type GlobalDailyReport struct {
	JobID        JobID                 `json:"job_id"`
	Date         string                `json:"date"`
	TotalRecords int64                 `json:"total_records"`
	Kinds        []GlobalDailyKindStat `json:"kinds"`
}

// GlobalDailyKindStat is one source-kind rollup inside a global daily report.
type GlobalDailyKindStat struct {
	Kind  SourceKind `json:"kind"`
	Count int64      `json:"count"`
	Sum   float64    `json:"sum"`
	Min   float64    `json:"min"`
	Max   float64    `json:"max"`
}
