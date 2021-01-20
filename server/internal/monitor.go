package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/metrics"
)

type Meter struct {
	Metrics        metrics.Metrics
	ConnTimeSeries metrics.TimeSeries
	Connections    metrics.Counter
	Subscriptions  metrics.Counter
	InMsgs         metrics.Counter
	OutMsgs        metrics.Counter
	InBytes        metrics.Counter
	OutBytes       metrics.Counter
}

func NewMeter() *Meter {
	Metrics := metrics.NewMetrics()
	c := &Meter{
		Metrics:        Metrics,
		ConnTimeSeries: metrics.GetOrRegisterTimeSeries("conn_timeseries_ns", Metrics),
		Connections:    metrics.NewCounter(),
		Subscriptions:  metrics.NewCounter(),
		InMsgs:         metrics.NewCounter(),
		OutMsgs:        metrics.NewCounter(),
		InBytes:        metrics.NewCounter(),
		OutBytes:       metrics.NewCounter(),
	}

	c.ConnTimeSeries.Time(func() {})
	Metrics.GetOrRegister("Connections", c.Connections)
	Metrics.GetOrRegister("Subscriptions", c.Subscriptions)
	Metrics.GetOrRegister("InMsgs", c.InMsgs)
	Metrics.GetOrRegister("OutMsgs", c.OutMsgs)
	Metrics.GetOrRegister("InBytes", c.InBytes)
	Metrics.GetOrRegister("Connections", c.Connections)

	return c
}

func (m *Meter) UnregisterAll() {
	m.Metrics.UnregisterAll()
}

// Stats will output server information on the monitoring port at /varz.
type Varz struct {
	Start         time.Time `json:"start"`
	Now           time.Time `json:"now"`
	Uptime        string    `json:"uptime"`
	Connections   int64     `json:"connections"`
	InMsgs        int64     `json:"in_msgs"`
	OutMsgs       int64     `json:"out_msgs"`
	InBytes       int64     `json:"in_bytes"`
	OutBytes      int64     `json:"out_bytes"`
	Subscriptions int64     `json:"subscriptions"`
	HMean         float64   `json:"hmean"` // Event duration harmonic mean.
	P50           float64   `json:"p50"`   // Event duration nth percentiles ..
	P75           float64   `json:"p75"`
	P95           float64   `json:"p95"`
	P99           float64   `json:"p99"`
	P999          float64   `json:"p999"`
	Long5p        float64   `json:"long_5p"`  // Average of the longest 5% event durations.
	Short5p       float64   `json:"short_5p"` // Average of the shortest 5% event durations.
	Max           float64   `json:"max"`      // Highest event duration.
	Min           float64   `json:"min"`      // Lowest event duration.
	StdDev        float64   `json:"stddev"`   // Standard deviation.
	// Range     		 time.Duration `json:"range"`    // Event duration range (Max-Min).
	// // Per-second rate based on event duration avg. via Metrics.Cumulative / Metrics.Samples.
	// Rate 			float64 `json:"rate"`
}

func uptime(d time.Duration) string {
	// Just use total seconds for uptime, and display days / years
	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}
	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}
	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}
	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}
	return fmt.Sprintf("%ds", tsecs)
}

// Varz returns a Varz struct containing the server information.
func (s *Service) Varz() (*Varz, error) {
	// Snapshot server options.

	v := &Varz{Start: s.start}
	v.Now = time.Now()
	v.Uptime = uptime(time.Since(s.start))
	v.Connections = s.meter.Connections.Count()
	v.InMsgs = s.meter.InMsgs.Count()
	v.OutMsgs = s.meter.OutMsgs.Count()
	v.InBytes = s.meter.InBytes.Count()
	v.OutBytes = s.meter.OutBytes.Count()
	v.Subscriptions = s.meter.Subscriptions.Count()
	ts := s.meter.ConnTimeSeries.Snapshot()
	v.HMean = float64(ts.HMean())
	v.P50 = float64(ts.P50())
	v.P75 = float64(ts.P75())
	v.P95 = float64(ts.P95())
	v.P99 = float64(ts.P99())
	v.P999 = float64(ts.P999())
	v.Long5p = float64(ts.Long5p())
	v.Short5p = float64(ts.Short5p())
	v.Max = float64(ts.Max())
	v.Min = float64(ts.Min())
	v.StdDev = float64(ts.StdDev())

	return v, nil
}

// HandleVarz will process HTTP requests for conn stats information.
func (m *Service) HandleVarz(w http.ResponseWriter, r *http.Request) {
	// As of now, no error is ever returned
	v, _ := m.Varz()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Error("metrics", "Error marshaling response to /varz request: "+err.Error())
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// ResponseHandler handles responses for monitoring routes
func ResponseHandler(w http.ResponseWriter, r *http.Request, data []byte) {
	// Get callback from request
	callback := r.URL.Query().Get("callback")
	// If callback is not empty then
	if callback != "" {
		// Response for JSONP
		w.Header().Set("Content-Type", "application/javascript")
		fmt.Fprintf(w, "%s(%s)", callback, data)
	} else {
		// Otherwise JSON
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}
