package domain

import (
	"sort"
	"strings"
)

// GlobalDailyFromAggregations rebuilds reports from persisted aggregation rows
// whose group_key is "global_daily|<date>|<kind>".
func GlobalDailyFromAggregations(aggs []Aggregation) []GlobalDailyReport {
	byDate := map[string]*GlobalDailyReport{}
	for _, a := range aggs {
		date, kind, ok := parseGlobalDailyGroupKey(a.GroupKey)
		if !ok {
			continue
		}
		rep, exists := byDate[date]
		if !exists {
			rep = &GlobalDailyReport{JobID: a.JobID, Date: date}
			byDate[date] = rep
		}
		rep.TotalRecords += a.Count
		rep.Kinds = append(rep.Kinds, GlobalDailyKindStat{
			Kind:  SourceKind(kind),
			Count: a.Count,
			Sum:   a.Sum,
			Min:   a.Min,
			Max:   a.Max,
		})
	}
	out := make([]GlobalDailyReport, 0, len(byDate))
	for _, rep := range byDate {
		sort.Slice(rep.Kinds, func(i, j int) bool {
			return rep.Kinds[i].Kind < rep.Kinds[j].Kind
		})
		out = append(out, *rep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date < out[j].Date })
	return out
}

func parseGlobalDailyGroupKey(key string) (date, kind string, ok bool) {
	const prefix = "global_daily|"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	date, kind, ok = strings.Cut(rest, "|")
	if !ok || len(date) != 10 || kind == "" {
		return "", "", false
	}
	return date, kind, true
}
