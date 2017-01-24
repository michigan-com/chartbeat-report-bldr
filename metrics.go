package chartbeatreportbldr

type Metric string

const (
	PageViews             Metric = "page_views"
	Uniques                      = "page_uniques"
	LoyalVisitorPageViews        = "page_views_loyal"   // The number of pageviews from visitors who visit your site an average of every other day
	QualityPageViews             = "page_views_quality" // The number of pageviews that received at least 15 seconds of engaged time.
	TotalEngagedTime             = "page_total_time"    // The total amount of time in seconds spent actively on a page, across all visitors.
	AverageScroll                = "page_avg_scroll"    // The average maximum depth that visitors have scrolled to in pixels from top of page.
)
