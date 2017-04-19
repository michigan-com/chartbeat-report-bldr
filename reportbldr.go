package chartbeatreportbldr

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"

	"github.com/andreyvit/date"
	"github.com/michigan-com/sentient/httputil"
)

var errResultsNotReady = errors.New("results not ready yet")
var ErrResultSetTooLarge = errors.New("result set too large")

type Printfable interface {
	Printf(format string, v ...interface{})
}

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
	Start date.Date
	End   date.Date

	Limit   int
	Metrics []Metric

	TimeResolution TimeResolution

	Path string
}

type Chunk struct {
	Start date.Date
	End   date.Date

	// Ordinal is a number of this chunk, from 1 to EstimatedCount
	Ordinal int

	// EstimatedCount is total expected number of chunks if the chunk size does not change
	EstimatedCount int

	// Size is the current chunk size
	Size int
}

func (c Chunk) String() string {
	return fmt.Sprintf("chunk %d of %d (from %s to %s, chunk size %d)", c.Ordinal, c.EstimatedCount, c.Start.String(), c.End.String(), c.Size)
}

type LoadOptions struct {
	MaxLoadTime time.Duration

	Log func(s string)

	InitialChunkSize     int
	ChunkSizeAfterNoData int
	OptimalRows          int
}

func (cbc *Client) Load(domain string, filt Filter, options LoadOptions, f func(row Row) error) error {
	if filt.Start.IsZero() || filt.End.IsZero() || options.InitialChunkSize < 0 {
		// cannot do chunked load if the date range isn't specified
		err, _ := cbc.loadSingleChunk(domain, filt, options, f)
		return err
	}

	daysPerChunk := options.InitialChunkSize
	if daysPerChunk == 0 {
		daysPerChunk = 1
	}
	if options.OptimalRows == 0 {
		options.OptimalRows = 800000
	}
	if options.ChunkSizeAfterNoData == 0 {
		options.ChunkSizeAfterNoData = 7
	}

	if len(filt.Metrics) == 0 {
		filt.Metrics = []Metric{PageViews}
	}

	chunkIdx := 0
	chunkStart := filt.Start
	end := filt.End
	for !chunkStart.After(filt.End) {
		chunk, nextChunkStart, remChunks := date.NextChunk(chunkStart, end, daysPerChunk)

		chunkInfo := Chunk{
			Start:          chunk.Start,
			End:            chunk.End,
			Size:           daysPerChunk,
			Ordinal:        chunkIdx + 1,
			EstimatedCount: chunkIdx + 1 + remChunks,
		}
		chunkInfoStr := chunkInfo.String()

		var log func(s string)
		if options.Log != nil {
			log = func(s string) {
				options.Log(chunkInfoStr + ": " + s)
			}
		}

		chunkFilt := filt
		chunkFilt.Start = chunk.Start
		chunkFilt.End = chunk.End

		err, rows := cbc.loadSingleChunk(domain, chunkFilt, LoadOptions{MaxLoadTime: options.MaxLoadTime, Log: log}, f)
		if err == ErrResultSetTooLarge {
			if log != nil {
				log("result set too large, retrying with chunk size of 1")
			}
			daysPerChunk = 1
			continue
		} else if err != nil {
			return err
		}

		if rows == 0 {
			daysPerChunk = options.ChunkSizeAfterNoData
		} else if rows < options.OptimalRows/2 {
			daysPerChunk = daysPerChunk * 2
		}

		chunkIdx++
		chunkStart = nextChunkStart
	}

	return nil
}

func (cbc *Client) loadSingleChunk(domain string, filt Filter, options LoadOptions, f func(row Row) error) (error, int) {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 100 * time.Millisecond
	boff.MaxElapsedTime = options.MaxLoadTime

	var queryID string
	attempt := 1
	boff.Reset()
	err := backoff.RetryNotify(func() error {
		if options.Log != nil {
			options.Log(fmt.Sprintf("submitting request (attempt %d)", attempt))
		}
		var err error
		queryID, err = cbc.submit(domain, filt)
		return err
	}, boff, func(err error, delay time.Duration) {
		if options.Log != nil {
			options.Log(fmt.Sprintf("submit failed, sleeping for %.1f: %v", delay.Seconds(), err))
		}
	})
	if err != nil {
		return errors.Wrap(err, "submit op"), 0
	}

	attempt = 1
	boff.Reset()
	var finalErr error
	var finalRows int
	err = backoff.RetryNotify(func() error {
		if options.Log != nil {
			options.Log(fmt.Sprintf("loading results (attempt %d)", attempt))
		}
		err, rows, isBreak := cbc.query(domain, queryID, filt, f)
		finalRows = rows
		if err == ErrResultSetTooLarge {
			finalErr = err
			return nil
		}
		if isBreak {
			finalErr = err
			return nil
		}
		return err
	}, boff, func(err error, delay time.Duration) {
		if err == errResultsNotReady {
			if options.Log != nil {
				options.Log(fmt.Sprintf("results not ready yet (attempt %d), sleeping for %.1f sec.", attempt, delay.Seconds()))
			}
		} else {
			if options.Log != nil {
				options.Log(fmt.Sprintf("loading failed (attempt %d), sleeping for %.1f: %v", attempt, delay.Seconds(), err))
			}
		}
		attempt++
	})
	if err != nil {
		return errors.Wrap(err, "fetch op"), finalRows
	}

	return finalErr, finalRows
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

	dimensions := []Dimension{Path}
	dimensions = append(dimensions, filt.TimeResolution.PickDimensions(UTCTimeGroup)...)
	params.Set("dimensions", joinDimensions(dimensions, ","))
	params.Set("sort_column", "page_views")
	params.Set("sort_order", "desc")
	params.Set("pagetype", "Article")
	if !filt.Start.IsZero() {
		params.Set("start", filt.Start.String())
	}
	if !filt.End.IsZero() {
		params.Set("end", filt.End.String())
	}
	if filt.Limit > 0 {
		params.Set("limit", strconv.Itoa(filt.Limit))
	}
	if filt.Path != "" {
		params.Set("path", filt.Path)
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

func (cbc *Client) query(domain string, queryID string, filt Filter, f func(row Row) error) (error, int, bool) {
	params := make(url.Values)
	params.Set("host", domain)
	params.Set("apikey", cbc.APIKey)
	params.Set("query_id", queryID)

	resp, err := httputil.GetRaw("http://chartbeat.com/query/v2/fetch/", params, nil)
	if err != nil {
		return err, 0, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusInternalServerError || resp.StatusCode == http.StatusServiceUnavailable {
		return errResultsNotReady, 0, false
	}
	if resp.StatusCode == http.StatusGatewayTimeout {
		return ErrResultSetTooLarge, 0, false
	}

	err = httputil.VerifyResponse(resp, httputil.CSVContentType)
	if err != nil {
		return err, 0, false
	}

	// handle rows
	rdr := csv.NewReader(resp.Body)
	numFields := 1 + len(filt.Metrics)
	if filt.TimeResolution != TimeResolutionNone {
		numFields++
	}
	recno := 0

	var columns = make([]columnFunc, 0, numFields)
	var colNames = make([]string, 0, numFields)

	var rows int
	for {
		record, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to decode CSV on data row %d", recno), rows, false
		}

		if recno == 0 {
			if len(record) == 2 && record[0] == "Error" {
				msg := record[1]
				if msg == "No results found" {
					break
				} else {
					return errors.Errorf("chartbeat error: %s", msg), rows, false
				}
			} else if len(record) != numFields {
				return errors.Errorf("wrong number of fields in response, got %d, wanted %d", len(record), numFields), rows, false
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
					return errors.Wrapf(err, "record %d, column %d %v, value %#v", recno, i+1, colNames[i], v), rows, false
				}
			}

			rows++
			err := f(row)
			if err != nil {
				return err, rows, true
			}
		}

		recno++
	}

	return nil, rows, false
}

type submitResponse struct {
	QueryID string `json:"query_id"`
}
