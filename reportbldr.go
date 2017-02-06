package chartbeatreportbldr

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"

	"github.com/michigan-com/sentient/httputil"
)

var errResultsNotReady = errors.New("results not ready yet")
var ErrResultSetTooLarge = errors.New("result set too large")

type Row struct {
	Time time.Time
	URL  string

	Visits                int
	Uniques               int
	QualityPageViews      int
	LoyalVisitorPageViews int
	TotalEngagedTime      int
}

type Client struct {
	APIKey string
}

type Filter struct {
	StartDay string
	EndDay   string
	Limit    int
	Metrics  []Metric
}

func (cbc *Client) Load(domain string, filt Filter, maxLoadTime time.Duration, f func(row Row), logger *log.Logger) error {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 100 * time.Millisecond
	boff.MaxElapsedTime = maxLoadTime

	if len(filt.Metrics) == 0 {
		filt.Metrics = []Metric{PageViews}
	}

	var queryID string
	attempt := 1
	boff.Reset()
	err := backoff.RetryNotify(func() error {
		logger.Printf("Submitting report builder request for %s (attempt %d)...", domain, attempt)
		var err error
		queryID, err = cbc.submit(domain, filt)
		return err
	}, boff, func(err error, delay time.Duration) {
		logger.Printf("WARNING: Submit op failed, sleeping for %.1f: %v", delay.Seconds(), err)
	})
	if err != nil {
		return errors.Wrap(err, "submit op")
	}

	attempt = 1
	boff.Reset()
	var finalErr error
	err = backoff.RetryNotify(func() error {
		logger.Printf("Checking status of report builder query %s for %s (attempt %d)...", queryID, domain, attempt)
		err := cbc.query(domain, queryID, len(filt.Metrics), f)
		if err == ErrResultSetTooLarge {
			finalErr = err
			return nil
		}
		return err
	}, boff, func(err error, delay time.Duration) {
		if err == errResultsNotReady {
			logger.Printf("Results not ready yet, sleeping for %.1f sec.", delay.Seconds())
		} else {
			logger.Printf("WARNING: Status check failed, sleeping for %.1f: %v", delay.Seconds(), err)
		}
		attempt++
	})
	if err != nil {
		return errors.Wrap(err, "fetch op")
	}

	return finalErr
}

func (cbc *Client) submit(domain string, filt Filter) (string, error) {
	var mm []string
	for _, m := range filt.Metrics {
		mm = append(mm, string(m))
	}

	params := make(url.Values)
	params.Set("host", domain)
	params.Set("apikey", cbc.APIKey)
	params.Set("metrics", strings.Join(mm, ","))
	params.Set("dimensions", "path,tz_day,tz_hour,tz_minute")
	params.Set("sort_column", "page_views")
	params.Set("sort_order", "desc")
	params.Set("pagetype", "Article")
	params.Set("start", filt.StartDay)
	params.Set("end", filt.EndDay)
	if filt.Limit > 0 {
		params.Set("limit", strconv.Itoa(filt.Limit))
	}

	var resp submitResponse
	err := httputil.GetJSON("http://chartbeat.com/query/v2/submit/page/", params, nil, &resp)
	if err != nil {
		return "", err
	}

	if resp.QueryID == "" {
		return "", errors.New("missing query_id in response")
	}

	return resp.QueryID, nil
}

func (cbc *Client) query(domain string, queryID string, nmetrics int, f func(row Row)) error {
	params := make(url.Values)
	params.Set("host", domain)
	params.Set("apikey", cbc.APIKey)
	params.Set("query_id", queryID)

	resp, err := httputil.GetRaw("http://chartbeat.com/query/v2/fetch/", params, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusInternalServerError {
		return errResultsNotReady
	}
	if resp.StatusCode == http.StatusGatewayTimeout {
		return ErrResultSetTooLarge
	}

	err = httputil.VerifyResponse(resp, httputil.CSVContentType)
	if err != nil {
		return err
	}

	// handle rows
	rdr := csv.NewReader(resp.Body)
	numFields := 2 + nmetrics
	recno := 0

	var columns = make([]columnFunc, 0, numFields)
	var colNames = make([]string, 0, numFields)

	for {
		record, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to decode CSV on data row %d", recno)
		}

		if recno == 0 {
			if len(record) == 2 && record[0] == "Error" {
				msg := record[1]
				if msg == "No results found" {
					break
				} else {
					return errors.Errorf("chartbeat error: %s", msg)
				}
			} else if len(record) != numFields {
				return errors.Errorf("wrong number of fields in response, got %d, wanted %d", len(record), numFields)
			}

			// log.Printf("columns = %#v", record)
			colNames = record
			for _, column := range record {
				proc := newColumnProcessor(domain, column)
				columns = append(columns, proc)
			}
		} else {
			if len(record) != numFields {
				panic("len(record) mismatch")
			}

			var row Row
			for i, proc := range columns {
				v := record[i]
				err = proc(v, &row)
				if err != nil {
					return errors.Wrapf(err, "record %d, column %d %v, value %#v", recno, i+1, colNames[i], v)
				}
			}

			f(row)
		}

		recno++
	}

	return nil
}

type submitResponse struct {
	QueryID string `json:"query_id"`
}
