package server

import (
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/sirupsen/logrus"
	"go.opencensus.io/stats"
	view "go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

// Exporter exports stats to Prometheus, users need
// to register the exporter as an http.Handler to be
// able to export.
type Converter struct {
	opts     Options
	measures map[string]*stats.Float64Measure
	viewsMu  sync.Mutex
	e        view.Exporter
}

// Options contains options for configuring the exporter.
type Options struct {
	Namespace string
	Exporter  view.Exporter
}

func NewConverter(o Options) (*Converter, error) {
	e := &Converter{
		opts:     o,
		e:        o.Exporter,
		measures: make(map[string]*stats.Float64Measure),
	}
	return e, nil
}

// ExportView exports to the Prometheus if view data has one or more rows.
// Each OpenCensus AggregationData will be converted to
// corresponding Prometheus Metric: SumData will be converted
// to Untyped Metric, CountData will be a Counter Metric,
// DistributionData will be a Histogram Metric.
func (c *Converter) ExportSpan(sd *trace.SpanData) {
	m := c.getMeasure(sd)

	spanTimeNanos := sd.EndTime.Sub(sd.StartTime)
	spanTimeMillis := float64(int64(spanTimeNanos / time.Millisecond))

	m.M(spanTimeMillis)
}

func (c *Converter) getMeasure(span *trace.SpanData) *stats.Float64Measure {
	sig := spanName(c.opts.Namespace, span)
	c.viewsMu.Lock()
	m, ok := c.measures[sig]
	c.viewsMu.Unlock()

	if !ok {
		logrus.Info("Creating Measure: %s", sig)
		m = stats.Float64("span length", "The span length in milliseconds", "ms")
		v := &view.View{
			Name:        spanName(c.opts.Namespace, span),
			Description: spanName(c.opts.Namespace, span),
			Measure:     m,
			Aggregation: view.Distribution(0, 1<<32, 2<<32, 3<<32),
		}
		// Buckets: []float64{1,
		// 	10,
		// 	50,
		// 	100,
		// 	250,
		// 	500,
		// 	1000,
		// 	10000,
		// 	60000,
		// 	120000},
		c.viewsMu.Lock()
		c.measures[sig] = m
		view.Register(v)
		c.viewsMu.Unlock()
	}

	return m
}

func spanName(namespace string, s *trace.SpanData) string {
	var name string
	if namespace != "" {
		name = namespace + "_"
	}
	return name + sanitize(s.Name)
}

const labelKeySizeLimit = 100

// sanitize returns a string that is trunacated to 100 characters if it's too
// long, and replaces non-alphanumeric characters to underscores.
func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}
	if len(s) > labelKeySizeLimit {
		s = s[:labelKeySizeLimit]
	}
	s = strings.Map(sanitizeRune, s)
	if unicode.IsDigit(rune(s[0])) {
		s = "key_" + s
	}
	if s[0] == '_' {
		s = "key" + s
	}
	return s
}

// converts anything that is not a letter or digit to an underscore
func sanitizeRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return r
	}
	// Everything else turns into an underscore
	return '_'
}

//Gin creates spans for all paths, containing ID values.
//We can safely discard these, as other histograms are being created for them.
func urlName(s string) bool {
	return strings.HasPrefix(s, "/")
}
