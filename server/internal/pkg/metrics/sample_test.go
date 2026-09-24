package metrics

import (
	"sync"
	"testing"
	"time"
)

// TestTimeSeriesConcurrentUse records and reads from many goroutines, the way
// every connection updates the connection time series; run it with -race.
func TestTimeSeriesConcurrentUse(t *testing.T) {
	ts := NewTimeSeries()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 1; i <= 1000; i++ {
				ts.AddTime(time.Duration(i))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ts.Snapshot().P99()
				ts.P50()
				ts.Avg()
			}
		}()
	}
	wg.Wait()
	if ts.Max() == 0 {
		t.Fatal("no samples recorded")
	}
}

func TestSampleStatistics(t *testing.T) {
	s := NewSample(&Config{Size: 10})
	// Record 1..20: the ring keeps the last 10, 11..20, in arrival order.
	for i := 1; i <= 20; i++ {
		s.AddTime(time.Duration(i))
	}
	snap := s.Snapshot()
	if snap.Min() != 11 || snap.Max() != 20 {
		t.Fatalf("min %d max %d, want 11 and 20", snap.Min(), snap.Max())
	}
	if snap.P50() != 16 {
		t.Fatalf("p50 %d, want 16", snap.P50())
	}
	if snap.Avg() != 15 {
		t.Fatalf("avg %d, want 15", snap.Avg())
	}
	// The live sample agrees with its snapshot.
	if s.P50() != snap.P50() || s.Max() != snap.Max() || s.Range() != 9 {
		t.Fatalf("live p50 %d max %d range %d", s.P50(), s.Max(), s.Range())
	}
}

func TestSampleStatisticsBeforeFull(t *testing.T) {
	s := NewSample(&Config{Size: 50})
	for _, d := range []time.Duration{30, 10, 20} {
		s.AddTime(d)
	}
	// Unused slots of the ring must not count as zero durations.
	if s.Min() != 10 || s.Max() != 30 || s.P50() != 20 || s.Avg() != 20 {
		t.Fatalf("min %d max %d p50 %d avg %d, want 10 30 20 20", s.Min(), s.Max(), s.P50(), s.Avg())
	}
}

func TestEmptyTimeSeries(t *testing.T) {
	ts := NewTimeSeries()
	for name, f := range map[string]func() time.Duration{
		"Cumulative": ts.Cumulative, "HMean": ts.HMean, "Avg": ts.Avg, "P50": ts.P50,
		"P75": ts.P75, "P95": ts.P95, "P99": ts.P99, "P999": ts.P999, "StdDev": ts.StdDev,
		"Long5p": ts.Long5p, "Short5p": ts.Short5p, "Min": ts.Min, "Max": ts.Max, "Range": ts.Range,
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s panicked on an empty time series: %v", name, r)
				}
			}()
			if got := f(); got != 0 {
				t.Errorf("%s = %d on an empty time series, want 0", name, got)
			}
		}()
	}
}
