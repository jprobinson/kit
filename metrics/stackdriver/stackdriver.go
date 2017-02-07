package stackdriver

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	monitoring "google.golang.org/api/monitoring/v3"
)

type Stackdriver struct {
	resourceName string
	resourceType string

	svc *monitoring.Service

	mtx      sync.RWMutex
	counters map[string]*Counter
	//	gauges     map[string]*Gauge
	//	histograms map[string]*Histogram
	logger log.Logger
}

func New(client *http.Client, resourceName, resourceType string, logger log.Logger) (*Stackdriver, error) {
	svc, err := monitoring.New(client)
	if err != nil {
		return nil, err
	}
	return &Stackdriver{
		svc:          svc,
		resourceName: resourceName,
		resourceType: resourceType,
	}, nil
}

// WriteLoop is a helper method that invokes WriteTo to the passed writer every
// time the passed channel fires. This method blocks until the channel is
// closed, so clients probably want to run it in its own goroutine. For typical
// usage, create a time.Ticker and pass its C channel to this method.
func (g *Stackdriver) WriteLoop(c <-chan time.Time, w io.Writer) {
	for range c {
		if _, err := g.WriteTo(w); err != nil {
			g.logger.Log("during", "WriteTo", "err", err)
		}
	}
}

// SendLoop is a helper method that wraps WriteLoop, passing a managed
// connection to the network and address. Like WriteLoop, this method blocks
// until the channel is closed, so clients probably want to start it in its own
// goroutine. For typical usage, create a time.Ticker and pass its C channel to
// this method.
func (g *Stackdriver) SendLoop(c <-chan time.Time, network, address string) {
	g.WriteLoop(c)
}

// WriteTo flushes the buffered content of the metrics to the writer, in
// Stackdriver plaintext format. WriteTo abides best-effort semantics, so
// observations are lost if there is a problem with the write. Clients should be
// sure to call WriteTo regularly, ideally through the WriteLoop or SendLoop
// helper methods.
func (g *Stackdriver) WriteTo(w io.Writer) (count int64, err error) {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	now := time.Now().Unix()

	for name, c := range g.counters {
		// EMIT METRICS TO STACKDRIVER!
		count += int64(n)

		timeseries := monitoring.TimeSeries{
			Metric: &monitoring.Metric{
				Type: "custom.googleapis.com/" + name,
			},
			Resource: &monitoring.MonitoredResource{
				// TODO: add actual instance data
				Labels: map[string]string{},
				Type:   "gce_instance",
			},
			Points: []*monitoring.Point{
				{
					Interval: &monitoring.TimeInterval{
						StartTime: c.lastSync,
						EndTime:   now,
					},
					Value: &monitoring.TypedValue{
						DoubleValue: &c.ValueReset(),
					},
				},
			},
		}

		// set time of latest sync
		c.lastSync = now

		createTimeseriesRequest := monitoring.CreateTimeSeriesRequest{
			TimeSeries: []*monitoring.TimeSeries{&timeseries},
		}

		_, err := g.svc.Projects.TimeSeries.Create(projectResource(projectID), &createTimeseriesRequest).Do()
		if err != nil {
			return fmt.Errorf("Could not write time series value, %v ", err)
		}
	}

	return count, err
}

func (s *Stackdriver) NewCounter(name string) *Counter {
	c := NewCounter(name)
	g.mtx.Lock()
	g.counters[name] = c
	g.mtx.Unlock()
}

// Counter is a Stackdriver counter metric.
type Counter struct {
	c        *generic.Counter
	lastSync int64
}

// With is a no-op.
func (c *Counter) With(...string) metrics.Counter { return c }

// Add implements counter.
func (c *Counter) Add(delta float64) { c.c.Add(delta) }

// NewCounter returns a new usable counter metric.
func NewCounter(name string) *Counter {
	return &Counter{generic.NewCounter(name)}
}
