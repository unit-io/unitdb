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

package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// a stream.
type Sample interface {
	Reset()
	// Count() uint64
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
	Snapshot() Sample
}

// timeslice holds time.Duration values.
type timeSlice []time.Duration

// Satisfy sort for timeSlice.
func (p timeSlice) Len() int           { return len(p) }
func (p timeSlice) Less(i, j int) bool { return int64(p[i]) < int64(p[j]) }
func (p timeSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Config struct {
	Size int
}

type sample struct {
	sync.Mutex
	Size     uint64
	Times    timeSlice
	Count    uint64
	Samples  int
	WallTime time.Duration
}

// New initializes a new metric sample.
func NewSample(c *Config) *sample {
	return &sample{
		Size:  uint64(c.Size),
		Times: make([]time.Duration, c.Size),
	}
}

// Reset resets sample
func (s *sample) Reset() {
	s.Lock()
	defer s.Unlock()
	s.Count = 0
}

// The statistics of a live sample are computed on a snapshot: it holds only
// the recorded samples, sorted, and is taken under the sample's lock.

// Cumulative returns cumulative time of all sampled events.
func (s *sample) Cumulative() time.Duration { return s.Snapshot().Cumulative() }

// HMean returns event duration harmonic mean.
func (s *sample) HMean() time.Duration { return s.Snapshot().HMean() }

// Avg returns average of number of events recorded.
func (s *sample) Avg() time.Duration { return s.Snapshot().Avg() }

// P50 returns event duration nth percentiles ..
func (s *sample) P50() time.Duration { return s.Snapshot().P50() }

// P75 returns event duration nth percentiles ..
func (s *sample) P75() time.Duration { return s.Snapshot().P75() }

// P95 returns event duration nth percentiles ..
func (s *sample) P95() time.Duration { return s.Snapshot().P95() }

// P99 returns event duration nth percentiles ..
func (s *sample) P99() time.Duration { return s.Snapshot().P99() }

// P999 returns event duration nth percentiles ..
func (s *sample) P999() time.Duration { return s.Snapshot().P999() }

// StdDev returns standard deviation.
func (s *sample) StdDev() time.Duration { return s.Snapshot().StdDev() }

// Long5p returns average of the longest 5% event durations.
func (s *sample) Long5p() time.Duration { return s.Snapshot().Long5p() }

// Short5p returns average of the shortest 5% event durations.
func (s *sample) Short5p() time.Duration { return s.Snapshot().Short5p() }

// Min returns lowest event duration.
func (s *sample) Min() time.Duration { return s.Snapshot().Min() }

// Max returns highest event duration.
func (s *sample) Max() time.Duration { return s.Snapshot().Max() }

//  Range returns event duration range (Max-Min).
func (s *sample) Range() time.Duration { return s.Snapshot().Range() }

// AddTime adds a time.Duration to metrics.
func (s *sample) AddTime(t time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.Times[s.Count%s.Size] = t
	s.Count++
}

// SetWallTime optionally sets an elapsed wall time duration.
// This affects rate output by using total events counted over time.
// This is useful for concurrent/parallelized events that overlap
// in wall time and are writing to a shared metrics instance.
func (s *sample) SetWallTime(t time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.WallTime = t
}

// Snapshot returns a read-only copy of the sample.
func (s *sample) Snapshot() Sample {
	s.Lock()
	defer s.Unlock()
	samples := s.Count
	if samples > s.Size {
		samples = s.Size
	}
	times := make(timeSlice, samples)
	copy(times, s.Times[:samples])
	sort.Sort(times)

	return &SampleSnapshot{
		count:     s.Count,
		timeSlice: times,
	}
}

// SampleSnapshot is a read-only copy of another Sample.
type SampleSnapshot struct {
	count     uint64
	timeSlice timeSlice
}

func NewSampleSnapshot(count uint64, samples int) *SampleSnapshot {
	return &SampleSnapshot{
		count:     count,
		timeSlice: make(timeSlice, samples),
	}
}

// Reset panics.
func (*SampleSnapshot) Reset() {
	panic("Reset called on a SampleSnapshot")
}

// Cumulative returns cumulative time of all sampled events.
func (s *SampleSnapshot) Cumulative() time.Duration { return s.timeSlice.cumulative() }

// HMean returns event duration harmonic mean.
func (s *SampleSnapshot) HMean() time.Duration { return s.timeSlice.hMean() }

// Avg returns average of number of events recorded.
func (s *SampleSnapshot) Avg() time.Duration { return s.timeSlice.avg() }

// P50 returns event duration nth percentiles ..
func (s *SampleSnapshot) P50() time.Duration {
	if len(s.timeSlice) == 0 {
		return 0
	}
	return s.timeSlice[s.timeSlice.Len()/2]
}

// P75 returns event duration nth percentiles ..
func (s *SampleSnapshot) P75() time.Duration { return s.timeSlice.p(0.75) }

// P95 returns event duration nth percentiles ..
func (s *SampleSnapshot) P95() time.Duration { return s.timeSlice.p(0.95) }

// P99 returns event duration nth percentiles ..
func (s *SampleSnapshot) P99() time.Duration { return s.timeSlice.p(0.99) }

// P999 returns event duration nth percentiles ..
func (s *SampleSnapshot) P999() time.Duration { return s.timeSlice.p(0.999) }

// StdDev returns standard deviation.
func (s *SampleSnapshot) StdDev() time.Duration { return s.timeSlice.stdDev() }

// Long5p returns average of the longest 5% event durations.
func (s *SampleSnapshot) Long5p() time.Duration { return s.timeSlice.long5p() }

// Short5p returns average of the shortest 5% event durations.
func (s *SampleSnapshot) Short5p() time.Duration { return s.timeSlice.short5p() }

// Min returns lowest event duration.
func (s *SampleSnapshot) Min() time.Duration { return s.timeSlice.min() }

// Max returns highest event duration.
func (s *SampleSnapshot) Max() time.Duration { return s.timeSlice.max() }

//  Range returns event duration range (Max-Min).
func (s *SampleSnapshot) Range() time.Duration { return s.timeSlice.srange() }

// AddTime panics.
func (*SampleSnapshot) AddTime(time.Duration) {
	panic("AddTime called on a SampleSnapshot")
}

// SetWallTime panics.
func (*SampleSnapshot) SetWallTime(time.Duration) {
	panic("SetWallTime called on a SampleSnapshot")
}

// Snapshot returns the snapshot.
func (s *SampleSnapshot) Snapshot() Sample { return s }

// These should be self-explanatory. They expect a sorted slice and return 0
// for an empty one.

func (ts timeSlice) cumulative() time.Duration {
	var total time.Duration
	for _, t := range ts {
		total += t
	}

	return total
}

func (ts timeSlice) hMean() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	var total float64

	for _, t := range ts {
		total += (1 / float64(t))
	}

	return time.Duration(float64(ts.Len()) / total)
}

func (ts timeSlice) avg() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	var total time.Duration
	for _, t := range ts {
		total += t
	}
	return time.Duration(int(total) / ts.Len())
}

func (ts timeSlice) p(p float64) time.Duration {
	if len(ts) == 0 {
		return 0
	}
	i := int(float64(ts.Len())*p+0.5) - 1
	if i < 0 {
		i = 0
	}
	return ts[i]
}

func (ts timeSlice) stdDev() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	m := ts.avg()
	s := 0.00

	for _, t := range ts {
		s += math.Pow(float64(m-t), 2)
	}

	msq := s / float64(ts.Len())

	return time.Duration(math.Sqrt(msq))
}

func (ts timeSlice) long5p() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	set := ts[int(float64(ts.Len())*0.95+0.5):]

	if len(set) <= 1 {
		return ts[ts.Len()-1]
	}

	var t time.Duration
	var i int
	for _, n := range set {
		t += n
		i++
	}

	return time.Duration(int(t) / i)
}

func (ts timeSlice) short5p() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	set := ts[:int(float64(ts.Len())*0.05+0.5)]

	if len(set) <= 1 {
		return ts[0]
	}

	var t time.Duration
	var i int
	for _, n := range set {
		t += n
		i++
	}

	return time.Duration(int(t) / i)
}

func (ts timeSlice) min() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	return ts[0]
}

func (ts timeSlice) max() time.Duration {
	if len(ts) == 0 {
		return 0
	}
	return ts[ts.Len()-1]
}

func (ts timeSlice) srange() time.Duration {
	return ts.max() - ts.min()
}
