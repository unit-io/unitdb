package tracedb

import "expvar"

// Metrics holds the DB metrics.
type Metrics struct {
	Puts           *expvar.Int
	Dels           *expvar.Int
	Gets           *expvar.Int
	HashCollisions *expvar.Int
	BlockProbes    *expvar.Int
}

func newMetrics() Metrics {
	return Metrics{
		Puts:           &expvar.Int{},
		Dels:           &expvar.Int{},
		Gets:           &expvar.Int{},
		HashCollisions: &expvar.Int{},
		BlockProbes:    &expvar.Int{},
	}
}
