package chartbeatreportbldr

type TimeResolution int

const (
	TimeResolutionMinute TimeResolution = iota
	TimeResolutionHour
	TimeResolutionDay
	TimeResolutionNone
)

func (tr TimeResolution) PickDimensions(timegr TimeGroup) []Dimension {
	switch tr {
	case TimeResolutionMinute:
		return []Dimension{timegr.DayDimension, timegr.HourDimension, timegr.MinuteDimension}
	case TimeResolutionHour:
		return []Dimension{timegr.DayDimension, timegr.HourDimension}
	case TimeResolutionDay:
		return []Dimension{timegr.DayDimension}
	case TimeResolutionNone:
		return []Dimension{}
	default:
		panic("invalid TimeResolution")
	}
}
