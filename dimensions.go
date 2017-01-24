package chartbeatreportbldr

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
