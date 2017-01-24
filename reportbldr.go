package chartbeatreportbldr

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"

	"github.com/michigan-com/sentient/httputil"
)

var errResultsNotReady = errors.New("results not ready yet")

const timeFmt = "2006-01-02 15:04"

type Row struct {
	Time   time.Time
	URL    string
	Visits int
}

type Client struct {
	APIKey string
}

func (cbc *Client) Load(domain string, startDay, endDay string, limit int, maxLoadTime time.Duration, f func(row Row), logger *log.Logger) error {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 100 * time.Millisecond
	boff.MaxElapsedTime = maxLoadTime

	var queryID string
	attempt := 1
	boff.Reset()
	err := backoff.RetryNotify(func() error {
		logger.Printf("Submitting report builder request for %s (attempt %d)...", domain, attempt)
		var err error
		queryID, err = cbc.submit(domain, startDay, endDay, limit)
		return err
	}, boff, func(err error, delay time.Duration) {
		logger.Printf("WARNING: Submit op failed, sleeping for %.1f: %v", delay.Seconds(), err)
	})
	if err != nil {
		return errors.Wrap(err, "submit op")
	}

	decodeRow := func(rec []string) error {
		if len(rec) != 3 {
			panic("expected 3 columns")
		}
		tm, err := time.ParseInLocation(timeFmt, rec[0], time.UTC)
		if err != nil {
			return errors.Errorf("cannot parse time %#v", rec[0])
		}
		url := "http://" + domain + rec[1]
		visits, err := strconv.Atoi(rec[2])
		if err != nil {
			return errors.Errorf("cannot parse visits count %#v", rec[2])
		}

		f(Row{tm, url, visits})
		return nil
	}

	attempt = 1
	boff.Reset()
	err = backoff.RetryNotify(func() error {
		logger.Printf("Checking status of report builder query %s for %s (attempt %d)...", queryID, domain, attempt)
		return cbc.query(domain, queryID, decodeRow)
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

	return nil
}

func (cbc *Client) submit(domain string, startDay, endDay string, limit int) (string, error) {
	params := make(url.Values)
	params.Set("host", domain)
	params.Set("apikey", cbc.APIKey)
	params.Set("metrics", "page_views")
	params.Set("dimensions", "path,tz_day,tz_hour,tz_minute")
	params.Set("sort_column", "page_views")
	params.Set("sort_order", "desc")
	params.Set("pagetype", "Article")
	params.Set("start", startDay)
	params.Set("end", endDay)
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
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

func (cbc *Client) query(domain string, queryID string, f func([]string) error) error {
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

	err = httputil.VerifyResponse(resp, httputil.CSVContentType)
	if err != nil {
		return err
	}

	// handle rows
	rdr := csv.NewReader(resp.Body)
	rdr.FieldsPerRecord = 3
	recno := 0
	for {
		record, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to decode CSV on data row %d", recno)
		}

		if recno > 0 {
			err = f(record)
			if err != nil {
				return errors.Wrapf(err, "record %d", recno)
			}
		}

		recno++
	}

	return nil
}

type submitResponse struct {
	QueryID string `json:"query_id"`
}
