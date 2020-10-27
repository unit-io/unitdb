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

import "sync/atomic"

// Gauge hold an int64 value that can be set arbitrarily.
type Gauge interface {
	Snapshot() Gauge
	Update(int64)
	Value() int64
}

// GetOrRegisterGauge returns an existing Gauge or constructs and registers a
// new StandardGauge.
func GetOrRegisterGauge(name string, r Metrics) Gauge {
	return r.GetOrRegister(name, NewGauge).(Gauge)
}

// NewGauge constructs a new StandardGauge.
func NewGauge() Gauge {
	return &_Gauge{0}
}

// GaugeSnapshot is a read-only copy of another Gauge.
type GaugeSnapshot int64

// Snapshot returns the snapshot.
func (g GaugeSnapshot) Snapshot() Gauge { return g }

// Update panics.
func (GaugeSnapshot) Update(int64) {
	panic("Update called on a GaugeSnapshot")
}

// Value returns the value at the time the snapshot was taken.
func (g GaugeSnapshot) Value() int64 { return int64(g) }

// StandardGauge is the standard implementation of a Gauge and uses the
// sync/atomic package to manage a single int64 value.
type _Gauge struct {
	value int64
}

// Snapshot returns a read-only copy of the gauge.
func (g *_Gauge) Snapshot() Gauge {
	return GaugeSnapshot(g.Value())
}

// Update updates the gauge's value.
func (g *_Gauge) Update(v int64) {
	atomic.StoreInt64(&g.value, v)
}

// Value returns the gauge's current value.
func (g *_Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}
