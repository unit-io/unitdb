package internal

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	s := &_Service{start: time.Now().Add(-time.Minute), meter: NewMeter()}
	defer s.meter.UnregisterAll()
	s.meter.Connections.Inc(3)
	s.meter.InMsgs.Inc(7)
	s.meter.ConnTimeSeries.AddTime(2 * time.Millisecond)
	s.health = newHealthMonitor(s.start)
	s.health.add("store", func() (string, error) { return "open", nil })
	s.health.runAll()

	var b bytes.Buffer
	s.writeMetrics(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE unitdb_build_info gauge",
		"unitdb_connections 3",
		"unitdb_messages_in_total 7",
		`unitdb_event_duration_seconds{quantile="0.5"}`,
		`unitdb_dependency_up{dep="store"} 1`,
		"unitdb_draining 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
	// Each metric's HELP and TYPE once, and every sample line well formed.
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if f := strings.Fields(line); len(f) != 2 || !strings.HasPrefix(f[0], "unitdb_") {
			t.Errorf("bad sample line %q", line)
		}
	}
	if n := strings.Count(out, "# TYPE unitdb_dependency_up "); n != 1 {
		t.Errorf("TYPE of unitdb_dependency_up %d times", n)
	}
}

func TestClusterMetrics(t *testing.T) {
	c := &Cluster{thisNodeName: "a", nodes: map[string]*ClusterNode{
		"b": {name: "b", repl: make(chan replicaItem, 4)},
		"c": {name: "c", repl: make(chan replicaItem, 4)},
	}}
	c.ringNodes = []string{"a", "b"}
	c.health.leaderSeen("b")
	var b bytes.Buffer
	c.writeMetrics(&metricsWriter{w: &b, seen: map[string]bool{}})
	out := b.String()
	for _, want := range []string{
		"unitdb_cluster_nodes 3",
		"unitdb_cluster_members 2",
		`unitdb_replication_queue{peer="b"} 0`,
		`unitdb_replication_queue{peer="c"} 0`,
		"unitdb_cluster_peers_without_tls 2",
		"unitdb_cluster_leader_age_seconds",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
}
