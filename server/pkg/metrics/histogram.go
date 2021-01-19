package metrics

import "time"

// Histograms calculate distribution statistics from a series of int64 values.
type Histogram interface {
	Reset()
	Cumulative() time.Duration // Cumulative time of all sampled events.
	HMean() time.Duration      // Event duration harmonic mean.
	Avg() time.Duration        // Event duration average.
	P50() time.Duration        // Event duration nth percentiles ..
	P75() time.Duration
	P95() time.Duration
	P99() time.Duration
	P999() time.Duration
	Long5p() time.Duration  // Average of the longest 5% event durations.
	Short5p() time.Duration // Average of the shortest 5% event durations.
	Max() time.Duration     // Highest event duration.
	Min() time.Duration     // Lowest event duration.
	StdDev() time.Duration  // Standard deviation.
	Range() time.Duration   // Event duration range (Max-Min).
	AddTime(time.Duration)
	SetWallTime(time.Duration)
	Snapshot() Histogram
}

// GetOrRegisterHistogram returns an existing Histogram or constructs and
// registers a new StandardHistogram.
func GetOrRegisterHistogram(name string, r Metrics, s Sample) Histogram {
	return r.GetOrRegister(name, func() Histogram { return NewHistogram(s) }).(Histogram)
}

// NewHistogram constructs a new StandardHistogram from a Sample.
func NewHistogram(s Sample) Histogram {
	return &histogram{sample: s}
}

// HistogramSnapshot is a read-only copy of another Histogram.
type HistogramSnapshot struct {
	sample *SampleSnapshot
}

// Reset clears the histogram and its sample.
func (h *HistogramSnapshot) Reset() { h.sample.Reset() }

// Cumulative returns cumulative time of all sampled events.
func (h *HistogramSnapshot) Cumulative() time.Duration { return h.sample.Cumulative() }

// HMean returns event duration harmonic mean.
func (h *HistogramSnapshot) HMean() time.Duration { return h.sample.HMean() }

// Avg returns average of number of events recorded.
func (h *HistogramSnapshot) Avg() time.Duration { return h.sample.Avg() }

// P50 returns event duration nth percentiles ..
func (h *HistogramSnapshot) P50() time.Duration { return h.sample.P50() }

// P75 returns event duration nth percentiles ..
func (h *HistogramSnapshot) P75() time.Duration { return h.sample.P75() }

// P95 returns event duration nth percentiles ..
func (h *HistogramSnapshot) P95() time.Duration { return h.sample.P95() }

// P99 returns event duration nth percentiles ..
func (h *HistogramSnapshot) P99() time.Duration { return h.sample.P99() }

// P999 returns event duration nth percentiles ..
func (h *HistogramSnapshot) P999() time.Duration { return h.sample.P999() }

// StdDev returns standard deviation.
func (h *HistogramSnapshot) StdDev() time.Duration { return h.sample.StdDev() }

// Long5p returns average of the longest 5% event durations.
func (h *HistogramSnapshot) Long5p() time.Duration { return h.sample.Long5p() }

// Short5p returns average of the shortest 5% event durations.
func (h *HistogramSnapshot) Short5p() time.Duration { return h.sample.Short5p() }

// Min returns lowest event duration.
func (h *HistogramSnapshot) Min() time.Duration { return h.sample.Min() }

// Max returns highest event duration.
func (h *HistogramSnapshot) Max() time.Duration { return h.sample.Max() }

//  Range returns event duration range (Max-Min).
func (h *HistogramSnapshot) Range() time.Duration { return h.sample.Range() }

// AddTime panics
func (h *HistogramSnapshot) AddTime(t time.Duration) { panic("AddTime called on a HistogramSnapshot") }

// SetWallTime panics
func (h *HistogramSnapshot) SetWallTime(t time.Duration) {
	panic("SetWallTime  called on a HistogramSnapshot")
}

// Snapshot returns the snapshot.
func (h *HistogramSnapshot) Snapshot() Histogram { return h }

// StandardHistogram is the standard implementation of a Histogram and uses a
// Sample to bound its memory use.
type histogram struct {
	sample Sample
}

// Reset clears the histogram and its sample.
func (h *histogram) Reset() { h.sample.Reset() }

// Cumulative returns cumulative time of all sampled events.
func (h *histogram) Cumulative() time.Duration { return h.sample.Cumulative() }

// HMean returns event duration harmonic mean.
func (h *histogram) HMean() time.Duration { return h.sample.HMean() }

// Avg returns average of number of events recorded.
func (h *histogram) Avg() time.Duration { return h.sample.Avg() }

// P50 returns event duration nth percentiles ..
func (h *histogram) P50() time.Duration { return h.sample.P50() }

// P75 returns event duration nth percentiles ..
func (h *histogram) P75() time.Duration { return h.sample.P75() }

// P95 returns event duration nth percentiles ..
func (h *histogram) P95() time.Duration { return h.sample.P95() }

// P99 returns event duration nth percentiles ..
func (h *histogram) P99() time.Duration { return h.sample.P99() }

// P999 returns event duration nth percentiles ..
func (h *histogram) P999() time.Duration { return h.sample.P999() }

// StdDev returns standard deviation.
func (h *histogram) StdDev() time.Duration { return h.sample.StdDev() }

// Long5p returns average of the longest 5% event durations.
func (h *histogram) Long5p() time.Duration { return h.sample.Long5p() }

// Short5p returns average of the shortest 5% event durations.
func (h *histogram) Short5p() time.Duration { return h.sample.Short5p() }

// Min returns lowest event duration.
func (h *histogram) Min() time.Duration { return h.sample.Min() }

// Max returns highest event duration.
func (h *histogram) Max() time.Duration { return h.sample.Max() }

//  Range returns event duration range (Max-Min).
func (h *histogram) Range() time.Duration { return h.sample.Range() }

// AddTime adds a time.Duration to metrics
func (h *histogram) AddTime(t time.Duration) { h.sample.AddTime(t) }

// SetWallTime optionally sets an elapsed wall time duration.
// This affects rate output by using total events counted over time.
// This is useful for concurrent/parallelized events that overlap
// in wall time and are writing to a shared metrics instance.
func (h *histogram) SetWallTime(t time.Duration) { h.sample.SetWallTime(t) }

// Snapshot returns a read-only copy of the histogram.
func (h *histogram) Snapshot() Histogram {
	return &HistogramSnapshot{sample: h.sample.Snapshot().(*SampleSnapshot)}
}
