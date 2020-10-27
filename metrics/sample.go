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
	"sync/atomic"
	"time"
)

// Sample a stream.
type Sample interface {
	Reset()
	// Count() uint64
	Cumulative() time.Duration // Cumulative time of all sampled events.
	HMean() time.Duration      // Event duration harmonic mean.
	Avg() time.Duration        // Event duration average.
	P50() time.Duration        // Event duration nth percentiles.
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

// _Timeslice holds time.Duration values.
type _TimeSlice []time.Duration

// Satisfy sort for timeSlice.
func (ts _TimeSlice) Len() int           { return len(ts) }
func (ts _TimeSlice) Less(i, j int) bool { return int64(ts[i]) < int64(ts[j]) }
func (ts _TimeSlice) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }

// Config sample config
type Config struct {
	Size int
}

type _Sample struct {
	sync.Mutex
	Size     uint64
	Times    _TimeSlice
	Count    uint64
	Samples  int
	WallTime time.Duration
}

// NewSample initializes a new metric sample.
func NewSample(c *Config) *_Sample {
	return &_Sample{
		Size:  uint64(c.Size),
		Times: make([]time.Duration, c.Size),
	}
}

// Reset resets sample
func (s *_Sample) Reset() {
	s.Lock()
	defer s.Unlock()
	s.Count = 0
}

// Cumulative returns cumulative time of all sampled events.
func (s *_Sample) Cumulative() time.Duration { return s.Times.cumulative() }

// HMean returns event duration harmonic mean.
func (s *_Sample) HMean() time.Duration { return s.Times.hMean() }

// Avg returns average of number of events recorded.
func (s *_Sample) Avg() time.Duration { return s.Times.avg() }

// P50 returns event duration nth percentiles.
func (s *_Sample) P50() time.Duration { return s.Times[s.Times.Len()/2] }

// P75 returns event duration nth percentiles.
func (s *_Sample) P75() time.Duration { return s.Times.p(0.75) }

// P95 returns event duration nth percentiles.
func (s *_Sample) P95() time.Duration { return s.Times.p(0.95) }

// P99 returns event duration nth percentiles.
func (s *_Sample) P99() time.Duration { return s.Times.p(0.99) }

// P999 returns event duration nth percentiles.
func (s *_Sample) P999() time.Duration { return s.Times.p(0.999) }

// StdDev returns standard deviation.
func (s *_Sample) StdDev() time.Duration { return s.Times.stdDev() }

// Long5p returns average of the longest 5% event durations.
func (s *_Sample) Long5p() time.Duration { return s.Times.long5p() }

// Short5p returns average of the shortest 5% event durations.
func (s *_Sample) Short5p() time.Duration { return s.Times.short5p() }

// Min returns lowest event duration.
func (s *_Sample) Min() time.Duration { return s.Times.min() }

// Max returns highest event duration.
func (s *_Sample) Max() time.Duration { return s.Times.max() }

// Range returns event duration range (Max-Min).
func (s *_Sample) Range() time.Duration { return s.Times.srange() }

// AddTime adds a time.Duration to metrics.
func (s *_Sample) AddTime(t time.Duration) {
	s.Times[(atomic.AddUint64(&s.Count, 1)-1)%s.Size] = t
}

// SetWallTime optionally sets an elapsed wall time duration.
// This affects rate output by using total events counted over time.
// This is useful for concurrent/parallelized events that overlap
// in wall time and are writing to a shared metrics instance.
func (s *_Sample) SetWallTime(t time.Duration) {
	s.WallTime = t
}

// Snapshot returns a read-only copy of the sample.
func (s *_Sample) Snapshot() Sample {
	sample := &_Sample{}

	s.Lock()
	defer s.Unlock()
	sample.Samples = int(math.Min(float64(atomic.LoadUint64(&s.Count)), float64(s.Size)))
	sample.Count = atomic.LoadUint64(&s.Count)
	times := make(_TimeSlice, sample.Samples)
	copy(times, s.Times[:sample.Samples])
	sort.Sort(times)

	return &SampleSnapshot{
		count:     sample.Count,
		timeSlice: times,
	}
}

// SampleSnapshot is a read-only copy of another Sample.
type SampleSnapshot struct {
	count     uint64
	timeSlice _TimeSlice
}

// NewSampleSnapshot returns a read-only copy of the sample.
func NewSampleSnapshot(count uint64, samples int) *SampleSnapshot {
	return &SampleSnapshot{
		count:     count,
		timeSlice: make(_TimeSlice, samples),
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

// P50 returns event duration nth percentiles.
func (s *SampleSnapshot) P50() time.Duration { return s.timeSlice[s.timeSlice.Len()/2] }

// P75 returns event duration nth percentiles.
func (s *SampleSnapshot) P75() time.Duration { return s.timeSlice.p(0.75) }

// P95 returns event duration nth percentiles.
func (s *SampleSnapshot) P95() time.Duration { return s.timeSlice.p(0.95) }

// P99 returns event duration nth percentiles.
func (s *SampleSnapshot) P99() time.Duration { return s.timeSlice.p(0.99) }

// P999 returns event duration nth percentiles.
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

// Range returns event duration range (Max-Min).
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

// These should be self-explanatory:

func (ts _TimeSlice) cumulative() time.Duration {
	var total time.Duration
	for _, t := range ts {
		total += t
	}

	return total
}

func (ts _TimeSlice) hMean() time.Duration {
	var total float64

	for _, t := range ts {
		total += (1 / float64(t))
	}

	return time.Duration(float64(ts.Len()) / total)
}

func (ts _TimeSlice) avg() time.Duration {
	var total time.Duration
	for _, t := range ts {
		total += t
	}
	return time.Duration(int(total) / ts.Len())
}

func (ts _TimeSlice) p(p float64) time.Duration {
	return ts[int(float64(ts.Len())*p+0.5)-1]
}

func (ts _TimeSlice) stdDev() time.Duration {
	m := ts.avg()
	s := 0.00

	for _, t := range ts {
		s += math.Pow(float64(m-t), 2)
	}

	msq := s / float64(ts.Len())

	return time.Duration(math.Sqrt(msq))
}

func (ts _TimeSlice) long5p() time.Duration {
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

func (ts _TimeSlice) short5p() time.Duration {
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

func (ts _TimeSlice) min() time.Duration {
	return ts[0]
}

func (ts _TimeSlice) max() time.Duration {
	return ts[ts.Len()-1]
}

func (ts _TimeSlice) srange() time.Duration {
	return ts.max() - ts.min()
}
