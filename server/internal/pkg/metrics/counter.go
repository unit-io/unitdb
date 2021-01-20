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

// Counters hold an int64 value that can be incremented and decremented.
type Counter interface {
	Reset()
	Count() int64
	Dec(int64)
	Inc(int64)
	Snapshot() Counter
}

// GetOrRegisterCounter returns an existing Counter or constructs and registers
// a new StandardCounter.
func GetOrRegisterCounter(name string, r Metrics) Counter {
	return r.GetOrRegister(name, NewCounter).(Counter)
}

// NewCounter constructs a new StandardCounter.
func NewCounter() Counter {
	return &counter{0}
}

// CounterSnapshot is a read-only copy of another Counter.
type CounterSnapshot int64

// Clear panics.
func (CounterSnapshot) Reset() {
	panic("Clear called on a CounterSnapshot")
}

// Count returns the count at the time the snapshot was taken.
func (c CounterSnapshot) Count() int64 { return int64(c) }

// Dec panics.
func (CounterSnapshot) Dec(int64) {
	panic("Dec called on a CounterSnapshot")
}

// Inc panics.
func (CounterSnapshot) Inc(int64) {
	panic("Inc called on a CounterSnapshot")
}

// Snapshot returns the snapshot.
func (c CounterSnapshot) Snapshot() Counter { return c }

// StandardCounter is the standard implementation of a Counter and uses the
// sync/atomic package to manage a single int64 value.
type counter struct {
	count int64
}

// Clear sets the counter to zero.
func (c *counter) Reset() {
	atomic.StoreInt64(&c.count, 0)
}

// Count returns the current count.
func (c *counter) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

// Dec decrements the counter by the given amount.
func (c *counter) Dec(i int64) {
	atomic.AddInt64(&c.count, -i)
}

// Inc increments the counter by the given amount.
func (c *counter) Inc(i int64) {
	atomic.AddInt64(&c.count, i)
}

// Snapshot returns a read-only copy of the counter.
func (c *counter) Snapshot() Counter {
	return CounterSnapshot(c.Count())
}
