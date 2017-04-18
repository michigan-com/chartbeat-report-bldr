package chartbeatreportbldr

import (
	"strings"
)

type Dimension string

const (
	Path         Dimension = "path"
	TZDay                  = "tz_day"
	TZHour                 = "tz_hour"
	TZMinute               = "tz_minute"
	UTCDay                 = "utc_day"
	UTCHour                = "utc_hour"
	UTCMinute              = "utc_minute"
	ClientDay              = "client_day"
	ClientHour             = "client_hour"
	ClientMinute           = "client_minute"
	ReferrerType           = "referrer_type" // The page referrer type (social, search, direct, internal, etc).
	City                   = "city"
	Section                = "section"
	Author                 = "author"
)

type TimeGroup struct {
	DayDimension    Dimension
	HourDimension   Dimension
	MinuteDimension Dimension

	TimeCol string
}

const (
	TZTimeCol     = "tz_time"
	UTCTimeCol    = "utc_time"
	ClientTimeCol = "client_time"
)

var (
	TZTimeGroup = TimeGroup{
		DayDimension:    TZDay,
		HourDimension:   TZHour,
		MinuteDimension: TZMinute,
		TimeCol:         TZTimeCol,
	}
	UTCTimeGroup = TimeGroup{
		DayDimension:    UTCDay,
		HourDimension:   UTCHour,
		MinuteDimension: UTCMinute,
		TimeCol:         UTCTimeCol,
	}
	ClientTimeGroup = TimeGroup{
		DayDimension:    ClientDay,
		HourDimension:   ClientHour,
		MinuteDimension: ClientMinute,
		TimeCol:         ClientTimeCol,
	}
)

func joinDimensions(dims []Dimension, sep string) string {
	var ss []string
	for _, dim := range dims {
		ss = append(ss, string(dim))
	}
	return strings.Join(ss, sep)
}
