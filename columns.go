package chartbeatreportbldr

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

const timeFmt = "2006-01-02 15:04"

type columnFunc func(v string, row *Row) error

var (
	errMissingData = errors.New("missing")
)

func newColumnProcessor(domain string, column string) columnFunc {
	switch column {
	case string(Path):
		return func(v string, row *Row) error {
			if len(v) == 0 {
				return errMissingData
			}
			row.URL = "http://" + domain + v
			return nil
		}

	case string(TZTimeCol):
		return columnFunc(parseTZTimeCol)
	case string(UTCTimeCol):
		return columnFunc(parseUTCTimeCol)

	case string(PageViews):
		return columnFunc(parseVisitsCol)
	case string(Uniques):
		return columnFunc(parseUniquesCol)
	case string(LoyalVisitorPageViews):
		return columnFunc(parseLoyalVisitorPageViewsCol)
	case string(QualityPageViews):
		return columnFunc(parseQualityPageViewsCol)
	case string(TotalEngagedTime):
		return columnFunc(parseTotalEngagedTimeCol)

	default:
		return func(v string, row *Row) error {
			return fmt.Errorf("unknown column %#v, value %#v", column, v)
		}
	}
}

func parseTZTimeCol(v string, row *Row) error {
	tm, err := time.ParseInLocation(timeFmt, v, BaseLocation)
	if err != nil {
		return err
	}
	row.Time = tm
	return nil
}

func parseUTCTimeCol(v string, row *Row) error {
	tm, err := time.ParseInLocation(timeFmt, v, time.UTC)
	if err != nil {
		return err
	}
	row.Time = tm
	return nil
}

func parseVisitsCol(v string, row *Row) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	row.Visits = n
	return nil
}
func parseUniquesCol(v string, row *Row) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	row.Uniques = n
	return nil
}
func parseLoyalVisitorPageViewsCol(v string, row *Row) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	row.LoyalVisitorPageViews = n
	return nil
}
func parseQualityPageViewsCol(v string, row *Row) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	row.QualityPageViews = n
	return nil
}
func parseTotalEngagedTimeCol(v string, row *Row) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	row.TotalEngagedTime = n
	return nil
}
