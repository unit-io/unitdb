package metrics

import (
	"fmt"
	"reflect"
	"sync"
)

// Metrics implementation is copied from github.com/rcrowley/go-metrics and it is simplified for tracking Trace metrics

// DuplicateMetric is the error returned by Registry.Register when a metric
// already exists.  If you mean to Register that metric you must first
// Unregister the existing metric.
type DuplicateMetric string

func (err DuplicateMetric) Error() string {
	return fmt.Sprintf("duplicate metric: %s", string(err))
}

// A Metrics holds registry references to a set of metrics by name and can iterate
// over them, calling callback functions provided by the user.
//
// This is an interface so as to encourage other structs to implement
// the Metrics API as appropriate.
type Metrics interface {

	// Gets an existing metric or registers the given one.
	// The interface can be the metric to register if not found in registry,
	// or a function returning the metric for lazy instantiation.
	GetOrRegister(string, interface{}) interface{}

	// Unregister the metric with the given name.
	Unregister(string)

	// Unregister all metrics.  (Mostly for testing.)
	UnregisterAll()
}

// The standard implementation of a Registry is a mutex-protected map
// of names to metrics.
type StandardMetrics struct {
	metrics map[string]interface{}
	mutex   sync.RWMutex
}

// Create a new registry.
func NewMetrics() Metrics {
	return &StandardMetrics{metrics: make(map[string]interface{})}
}

// Gets an existing metric or creates and registers a new one. Threadsafe
// alternative to calling Get and Register on failure.
// The interface can be the metric to register if not found in registry,
// or a function returning the metric for lazy instantiation.
func (m *StandardMetrics) GetOrRegister(name string, i interface{}) interface{} {
	// access the read lock first which should be re-entrant
	m.mutex.RLock()
	metric, ok := m.metrics[name]
	m.mutex.RUnlock()
	if ok {
		return metric
	}

	// only take the write lock if we'll be modifying the metrics map
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if metric, ok := m.metrics[name]; ok {
		return metric
	}
	if v := reflect.ValueOf(i); v.Kind() == reflect.Func {
		i = v.Call(nil)[0].Interface()
	}
	m.register(name, i)
	return i
}

// Unregister the metric with the given name.
func (m *StandardMetrics) Unregister(name string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.stop(name)
	delete(m.metrics, name)
}

// Unregister all metrics.  (Mostly for testing.)
func (m *StandardMetrics) UnregisterAll() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for name, _ := range m.metrics {
		m.stop(name)
		delete(m.metrics, name)
	}
}

func (m *StandardMetrics) register(name string, i interface{}) error {
	if _, ok := m.metrics[name]; ok {
		return DuplicateMetric(name)
	}
	switch i.(type) {
	case Counter, Gauge:
		m.metrics[name] = i
	}
	return nil
}

func (m *StandardMetrics) stop(name string) {
	if i, ok := m.metrics[name]; ok {
		if s, ok := i.(Stoppable); ok {
			s.Stop()
		}
	}
}

// Stoppable defines the metrics which has to be stopped.
type Stoppable interface {
	Stop()
}
