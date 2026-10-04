package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w.Code, strings.TrimSpace(w.Body.String())
}

func grpcStatus(t *testing.T, h *healthMonitor) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	r, err := h.grpc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "unitdb.Unitdb"})
	if err != nil {
		t.Fatalf("grpc health: %v", err)
	}
	return r.Status
}

func TestHealthReadiness(t *testing.T) {
	h := newHealthMonitor(time.Now())
	storeOK := true
	h.add("store", func() (string, error) {
		if !storeOK {
			return "", errors.New("a read failed")
		}
		return "open", nil
	})
	web := h.handler()

	// Liveness never depends on the checks.
	if code, body := get(t, web, "/_healthz"); code != 200 || body != "ok" {
		t.Errorf("/_healthz: %d %q", code, body)
	}
	if code, body := get(t, web, "/_readyz"); code != 503 || body != "store: not checked yet" {
		t.Errorf("/_readyz before a check: %d %q", code, body)
	}

	h.runAll()
	if code, body := get(t, web, "/_readyz"); code != 200 || body != "ready" {
		t.Errorf("/_readyz: %d %q", code, body)
	}
	if s := grpcStatus(t, h); s != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("grpc: %v", s)
	}

	storeOK = false
	h.runAll()
	if code, body := get(t, web, "/_readyz"); code != 503 || body != "store: a read failed" {
		t.Errorf("/_readyz with the store failing: %d %q", code, body)
	}
	if s := grpcStatus(t, h); s != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("grpc with the store failing: %v", s)
	}
	if code, _ := get(t, web, "/_healthz"); code != 200 {
		t.Errorf("/_healthz with the store failing: %d", code)
	}

	// The detail, as JSON.
	_, body := get(t, web, "/_status")
	var status struct {
		Status string                  `json:"status"`
		Reason string                  `json:"reason"`
		Checks map[string]healthResult `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatalf("/_status: %v: %s", err, body)
	}
	if status.Status != "not_ready" || status.Reason != "store: a read failed" ||
		status.Checks["store"].OK || status.Checks["store"].CheckedAt.IsZero() {
		t.Errorf("/_status: %s", body)
	}

	storeOK = true
	h.runAll()
	h.drain()
	if code, body := get(t, web, "/_readyz"); code != 503 || body != "draining" {
		t.Errorf("/_readyz draining: %d %q", code, body)
	}
	if s := grpcStatus(t, h); s != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("grpc draining: %v", s)
	}
}

func TestHealthCheckTimeout(t *testing.T) {
	h := newHealthMonitor(time.Now())
	block := make(chan struct{})
	defer close(block)
	h.add("slow", func() (string, error) {
		<-block
		return "late", nil
	})
	start := time.Now()
	h.runAll()
	if d := time.Since(start); d > healthTimeout+time.Second {
		t.Errorf("runAll took %s", d)
	}
	if why := h.notReady(); !strings.HasPrefix(why, "slow: no answer in") {
		t.Errorf("notReady: %q", why)
	}
}

func TestClusterReadiness(t *testing.T) {
	var none *Cluster
	if ok, detail := none.readiness(); !ok || detail != "standalone" {
		t.Errorf("no cluster: %v %q", ok, detail)
	}

	c := &Cluster{thisNodeName: "a", nodes: map[string]*ClusterNode{"b": {}, "c": {}}}
	c.fo = &clusterFailover{heartBeat: 100 * time.Millisecond, voteTimeout: 3}
	if ok, detail := c.readiness(); ok || !strings.Contains(detail, "not in the ring") {
		t.Errorf("before joining: %v %q", ok, detail)
	}
	c.ringNodes = []string{"a", "b", "c"}
	if ok, detail := c.readiness(); ok || detail != "no leader heard from yet" {
		t.Errorf("no leader: %v %q", ok, detail)
	}
	c.health.leaderSeen("b")
	if ok, detail := c.readiness(); !ok || !strings.Contains(detail, "leader b") {
		t.Errorf("ready: %v %q", ok, detail)
	}
	c.health.lastLeader.Store(time.Now().Add(-time.Second).UnixNano())
	if ok, detail := c.readiness(); ok || !strings.HasPrefix(detail, "no leader for") {
		t.Errorf("a stale leader: %v %q", ok, detail)
	}
	c.health.leaderSeen("b")
	c.rebuilding.Store(true)
	if ok, detail := c.readiness(); ok || !strings.Contains(detail, "catching up") {
		t.Errorf("rebuilding: %v %q", ok, detail)
	}
	c.rebuilding.Store(false)
	c.leaving.Store(true)
	if ok, detail := c.readiness(); ok || detail != "leaving the cluster" {
		t.Errorf("leaving: %v %q", ok, detail)
	}
}
