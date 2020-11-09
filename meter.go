/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/unit-io/unitdb/metrics"
)

// Meter meter provides various db statistics.
type Meter struct {
	Metrics    metrics.Metrics
	TimeSeries metrics.TimeSeries
	Gets       metrics.Counter
	Puts       metrics.Counter
	Leases     metrics.Counter
	Syncs      metrics.Counter
	Recovers   metrics.Counter
	Aborts     metrics.Counter
	Dels       metrics.Counter
	InMsgs     metrics.Counter
	OutMsgs    metrics.Counter
	InBytes    metrics.Counter
	OutBytes   metrics.Counter
}

// NewMeter provide meter to capture statistics.
func NewMeter() *Meter {
	Metrics := metrics.NewMetrics()
	c := &Meter{
		Metrics:    Metrics,
		TimeSeries: metrics.GetOrRegisterTimeSeries("timeseries_ns", Metrics),
		Gets:       metrics.NewCounter(),
		Puts:       metrics.NewCounter(),
		Leases:     metrics.NewCounter(),
		Syncs:      metrics.NewCounter(),
		Recovers:   metrics.NewCounter(),
		Aborts:     metrics.NewCounter(),
		Dels:       metrics.NewCounter(),
		InMsgs:     metrics.NewCounter(),
		OutMsgs:    metrics.NewCounter(),
		InBytes:    metrics.NewCounter(),
		OutBytes:   metrics.NewCounter(),
	}

	c.TimeSeries.Time(func() {})
	Metrics.GetOrRegister("Gets", c.Gets)
	Metrics.GetOrRegister("Puts", c.Puts)
	Metrics.GetOrRegister("leases", c.Leases)
	Metrics.GetOrRegister("Syncs", c.Syncs)
	Metrics.GetOrRegister("Recovers", c.Recovers)
	Metrics.GetOrRegister("Aborts", c.Aborts)
	Metrics.GetOrRegister("Dels", c.Dels)
	Metrics.GetOrRegister("InMsgs", c.InMsgs)
	Metrics.GetOrRegister("OutMsgs", c.OutMsgs)
	Metrics.GetOrRegister("InBytes", c.InBytes)

	return c
}

// UnregisterAll unregister all metrics from meter.
func (m *Meter) UnregisterAll() {
	m.Metrics.UnregisterAll()
}

// Varz outputs unitdb stats on the monitoring port at /varz.
type Varz struct {
	Start    time.Time `json:"start"`
	Now      time.Time `json:"now"`
	Uptime   string    `json:"uptime"`
	Seq      int64     `json:"seq"`
	Count    int64     `json:"count"`
	Blocks   int64     `json:"blocks"`
	Gets     int64     `json:"gets"`
	Puts     int64     `json:"puts"`
	Leases   int64     `json:"leases"`
	Syncs    int64     `json:"syncs"`
	Recovers int64     `json:"recovers"`
	Aborts   int64     `json:"aborts"`
	Dels     int64     `json:"Dels"`
	InMsgs   int64     `json:"in_msgs"`
	OutMsgs  int64     `json:"out_msgs"`
	InBytes  int64     `json:"in_bytes"`
	OutBytes int64     `json:"out_bytes"`
	HMean    float64   `json:"hmean"` // Event duration harmonic mean.
	P50      float64   `json:"p50"`   // Event duration nth percentiles.
	P75      float64   `json:"p75"`
	P95      float64   `json:"p95"`
	P99      float64   `json:"p99"`
	P999     float64   `json:"p999"`
	Long5p   float64   `json:"long_5p"`  // Average of the longest 5% event durations.
	Short5p  float64   `json:"short_5p"` // Average of the shortest 5% event durations.
	Max      float64   `json:"max"`      // Highest event duration.
	Min      float64   `json:"min"`      // Lowest event duration.
	StdDev   float64   `json:"stddev"`   // Standard deviation.
	// Range     		 time.Duration `json:"range"`    // Event duration range (Max-Min).
	// // Per-second rate based on event duration avg. via Metrics.Cumulative / Metrics.Samples.
	// Rate 			float64 `json:"rate"`
}

func uptime(d time.Duration) string {
	// Just use total seconds for uptime, and display days / years.
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

// Varz returns a Varz struct containing the unitdb information.
func (db *DB) Varz() (*Varz, error) {
	v := &Varz{Start: db.internal.start}
	v.Now = time.Now()
	v.Uptime = uptime(time.Since(db.internal.start))
	v.Seq = int64(db.internal.dbInfo.sequence)
	v.Count = int64(db.Count())
	v.Blocks = int64(db.blocks())
	v.Gets = db.internal.meter.Gets.Count()
	v.Puts = db.internal.meter.Puts.Count()
	v.Leases = db.internal.meter.Leases.Count()
	v.Syncs = db.internal.meter.Syncs.Count()
	v.Recovers = db.internal.meter.Recovers.Count()
	v.Aborts = db.internal.meter.Aborts.Count()
	v.Dels = db.internal.meter.Dels.Count()
	v.InMsgs = db.internal.meter.InMsgs.Count()
	v.OutMsgs = db.internal.meter.OutMsgs.Count()
	v.InBytes = db.internal.meter.InBytes.Count()
	v.OutBytes = db.internal.meter.OutBytes.Count()
	ts := db.internal.meter.TimeSeries.Snapshot()
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

// HandleVarz will process HTTP requests for unitdb stats information.
func (db *DB) HandleVarz(w http.ResponseWriter, r *http.Request) {
	// As of now, no error is ever returned.
	v, _ := db.Varz()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		logger.Error().Msg("metrics: Error marshaling response to /varz request: " + err.Error())
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// ResponseHandler handles responses for monitoring routes.
func ResponseHandler(w http.ResponseWriter, r *http.Request, data []byte) {
	// Get callback from request.
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
