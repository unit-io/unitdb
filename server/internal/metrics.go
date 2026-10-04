package internal

import (
	"fmt"
	"io"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/unit-io/unitdb/server/internal/store"
)

// Metrics, in the Prometheus text format, at /_metrics on the monitor port:
// what /varz computes (connections, subscriptions, messages and bytes, and
// the event durations), the health checks, and in a cluster this node's
// place in it. Labels stay bounded: never a client or a topic.

// buildInfo is the version and commit the binary was built from, when go
// recorded them.
var buildInfo = func() (version, commit string) {
	version, commit = "unknown", "unknown"
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	if bi.Main.Version != "" {
		version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			commit = s.Value
			if len(commit) > 12 {
				commit = commit[:12]
			}
		}
	}
	return
}

type metricsWriter struct {
	w    io.Writer
	seen map[string]bool
}

// head writes a metric's HELP and TYPE once.
func (m *metricsWriter) head(name, typ, help string) {
	if m.seen[name] {
		return
	}
	m.seen[name] = true
	fmt.Fprintf(m.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (m *metricsWriter) value(name, labels string, v float64) {
	if labels != "" {
		labels = "{" + labels + "}"
	}
	fmt.Fprintf(m.w, "%s%s %s\n", name, labels, formatFloat(v))
}

func (m *metricsWriter) one(name, typ, help string, v float64) {
	m.head(name, typ, help)
	m.value(name, "", v)
}

func formatFloat(v float64) string {
	s := fmt.Sprintf("%g", v)
	if strings.Contains(s, "e+") {
		s = fmt.Sprintf("%.0f", v)
	}
	return s
}

func quote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + r.Replace(s) + `"`
}

// writeMetrics writes the server's metrics.
func (s *_Service) writeMetrics(w io.Writer) {
	m := &metricsWriter{w: w, seen: map[string]bool{}}

	version, commit := buildInfo()
	m.head("unitdb_build_info", "gauge", "The build: its version and commit.")
	m.value("unitdb_build_info", "version="+quote(version)+",commit="+quote(commit), 1)
	m.one("unitdb_up_seconds", "gauge", "Seconds since the server started.", time.Since(s.start).Seconds())

	if s.meter != nil {
		m.one("unitdb_connections", "gauge", "Client connections open.", float64(s.meter.Connections.Count()))
		m.one("unitdb_subscriptions", "gauge", "Subscriptions held.", float64(s.meter.Subscriptions.Count()))
		m.one("unitdb_messages_in_total", "counter", "Messages published to this server.", float64(s.meter.InMsgs.Count()))
		m.one("unitdb_messages_out_total", "counter", "Messages delivered to subscribers.", float64(s.meter.OutMsgs.Count()))
		m.one("unitdb_bytes_in_total", "counter", "Payload bytes published to this server.", float64(s.meter.InBytes.Count()))
		m.one("unitdb_bytes_out_total", "counter", "Payload bytes delivered to subscribers.", float64(s.meter.OutBytes.Count()))

		// How long handling each client packet took (hdl_conn.go), in nanoseconds.
		ts := s.meter.ConnTimeSeries.Snapshot()
		const name = "unitdb_event_duration_seconds"
		m.head(name, "summary", "Time to handle a client's packet, from the meter's sample.")
		for _, q := range []struct {
			q string
			v int64
		}{{"0.5", int64(ts.P50())}, {"0.75", int64(ts.P75())}, {"0.95", int64(ts.P95())}, {"0.99", int64(ts.P99())}, {"0.999", int64(ts.P999())}} {
			m.value(name, "quantile="+quote(q.q), time.Duration(q.v).Seconds())
		}
	}

	if s.health != nil {
		m.head("unitdb_dependency_up", "gauge", "Whether a health check passes (1) or fails (0).")
		m.head("unitdb_dependency_check_seconds", "gauge", "How long a health check took, last time.")
		s.health.mu.RLock()
		names := make([]string, 0, len(s.health.results))
		for n := range s.health.results {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			r := s.health.results[n]
			up := 0.0
			if r.OK {
				up = 1
			}
			m.value("unitdb_dependency_up", "dep="+quote(n), up)
			m.value("unitdb_dependency_check_seconds", "dep="+quote(n), float64(r.LatencyMs)/1000)
		}
		s.health.mu.RUnlock()
		draining := 0.0
		if s.health.draining.Load() {
			draining = 1
		}
		m.one("unitdb_draining", "gauge", "Whether the server is shutting down.", draining)
	}

	// The store: cheap to read per scrape.
	if st := s.storeStats(); st != nil {
		m.one("unitdb_store_messages", "gauge", "Messages in the store.", float64(st.Messages))
		m.one("unitdb_store_disk_bytes", "gauge", "Bytes of the store's files.", float64(st.DiskBytes))
		m.one("unitdb_store_mem_entries", "gauge", "Records in the memory store (memdb): entries, not bytes.", float64(st.MemEntries))
		m.one("unitdb_store_mem_size", "gauge", "The memory store's configured mem_size; 0 if unset.", float64(st.MemSize))
	}
	if s.health != nil && s.health.checkpoints != nil {
		s.health.checkpoints.writeMetrics(m)
	}
	writeOffsiteMetrics(m)

	if c := Globals.Cluster; c != nil {
		c.writeMetrics(m)
	}
}

// storeStats is the store's size, or nil when it isn't open.
func (s *_Service) storeStats() *store.Stats {
	if !store.IsOpen() {
		return nil
	}
	st := store.StoreStats()
	return &st
}
