package chartbeatreportbldr

import (
	"time"
)

var BaseLocation *time.Location

func init() {
	var err error
	BaseLocation, err = time.LoadLocation("EST")
	if err != nil {
		panic(err)
	}
}
