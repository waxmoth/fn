package spanexporter

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"

	"go.opencensus.io/trace"
)

// Exporter exports stats to Prometheus, users need
// to register the exporter as an http.Handler to be
// able to export.
type Exporter struct {
	opts    Options
	g       prometheus.Gatherer
	c       *collector
	handler http.Handler
}

var _ trace.Exporter = (*Exporter)(nil)

// Options contains options for configuring the exporter.
type Options struct {
	Namespace string
	Registry  *prometheus.Registry
	OnError   func(err error)
}

// NewExporter returns an exporter that exports stats to Prometheus.
func NewExporter(o Options) (*Exporter, error) {
	if o.Registry == nil {
		o.Registry = prometheus.NewRegistry()
	}
	collector := newCollector(o, o.Registry)
	e := &Exporter{
		opts: o,
		g:    o.Registry,
		c:    collector,
	}
	return e, nil
}

// ExportSpan exports to the Prometheus
// Each OpenCensus AggregationData will be converted to
// corresponding Prometheus Metric: SumData will be converted
// to Untyped Metric, CountData will be a Counter Metric,
// DistributionData will be a Histogram Metric.
func (e *Exporter) ExportSpan(sd *trace.SpanData) {
	if urlName(sd.Name) {
		return
	}
	histo := e.c.getHistogram(sd)

	spanTimeSpanNanos := sd.EndTime.Sub(sd.StartTime)
	spanTimeSpanMillis := float64(int64(spanTimeSpanNanos / time.Millisecond))

	histo.Observe(spanTimeSpanMillis)
}

var _ trace.Exporter = (*Exporter)(nil)

func (c *collector) getHistogram(span *trace.SpanData) prometheus.Histogram {
	sig := spanName(c.opts.Namespace, span)
	c.registeredHistosMu.Lock()
	histogram, ok := c.registeredHistograms[sig]
	c.registeredHistosMu.Unlock()

	if !ok {
		histogram = prometheus.NewHistogram(
			prometheus.HistogramOpts{Namespace: c.opts.Namespace,
				Name: sanitize(span.Name),
				Help: sanitize(span.Name),
				Buckets: []float64{1,
					10,
					50,
					100,
					250,
					500,
					1000,
					10000,
					60000,
					120000},
			})
		c.registeredHistosMu.Lock()
		c.registeredHistograms[sig] = histogram
		c.registeredHistosMu.Unlock()
	}

	c.ensureRegisteredOnce()

	return histogram
}

// ensureRegisteredOnce invokes reg.Register on the collector itself
// exactly once to ensure that we don't get errors such as
//  cannot register the collector: descriptor Desc{fqName: *}
//  already exists with the same fully-qualified name and const label values
// which is documented by Prometheus at
//  https://github.com/prometheus/client_golang/blob/fcc130e101e76c5d303513d0e28f4b6d732845c7/prometheus/registry.go#L89-L101
func (c *collector) ensureRegisteredOnce() {
	c.registerOnce.Do(func() {
		if err := c.reg.Register(c); err != nil {
			c.opts.onError(fmt.Errorf("cannot register the collector: %v", err))
		}
	})
}

func (o *Options) onError(err error) {
	if o.OnError != nil {
		o.OnError(err)
	} else {
		log.Printf("Failed to export spans to Prometheus: %v", err)
	}
}

// collector implements prometheus.Collector
type collector struct {
	opts Options
	mu   sync.Mutex // mu guards all the fields.

	registerOnce sync.Once

	// reg helps collector register views dynamically.
	reg *prometheus.Registry

	registeredHistosMu sync.Mutex

	registeredHistograms map[string]prometheus.Histogram
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	c.registeredHistosMu.Lock()
	registered := make(map[string]*prometheus.Desc)
	for k, histo := range c.registeredHistograms {
		registered[k] = histo.Desc()
	}
	c.registeredHistosMu.Unlock()

	for _, desc := range registered {
		ch <- desc
	}
}

// Collect fetches the statistics from OpenCensus
// and delivers them as Prometheus Metrics.
// Collect is invoked everytime a prometheus.Gatherer is run
// for example when the HTTP endpoint is invoked by Prometheus.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for _, histo := range c.registeredHistograms {
		ch <- histo
	}
}

func newCollector(opts Options, registrar *prometheus.Registry) *collector {
	return &collector{
		reg:                  registrar,
		opts:                 opts,
		registeredHistograms: make(map[string]prometheus.Histogram),
	}
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
