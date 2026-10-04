package e2e

// The health checks: /_healthz, /_readyz and /_status on the monitor port,
// and grpc.health.v1 on the gRPC port, for a standalone server and a
// cluster's nodes.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type healthStatus struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Checks map[string]struct {
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	} `json:"checks"`
}

func monitorGet(t *testing.T, s *server, path string) (int, string) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + s.monitorAddr + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// untilReady waits for s's /_readyz to answer 200, and returns its status.
func untilReady(t *testing.T, s *server, within time.Duration) healthStatus {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		code, body := monitorGet(t, s, "/_readyz")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			_, status := monitorGet(t, s, "/_status")
			t.Fatalf("not ready after %s: %d %q\n%s", within, code, body, status)
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, body := monitorGet(t, s, "/_status")
	var st healthStatus
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("/_status: %v: %s", err, body)
	}
	return st
}

func grpcHealth(t *testing.T, s *server) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// NewClient connects on the first call: Check waits for it, within ctx.
	conn, err := grpc.NewClient(s.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	defer conn.Close()
	r, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("grpc health: %v", err)
	}
	return r.Status
}

func TestHealthStandalone(t *testing.T) {
	s := startServer(t)

	if code, body := monitorGet(t, s, "/_healthz"); code != 200 || body != "ok" {
		t.Errorf("/_healthz: %d %q", code, body)
	}
	st := untilReady(t, s, 15*time.Second)
	if st.Status != "ready" || !st.Checks["store"].OK {
		t.Errorf("/_status: %+v", st)
	}
	if _, ok := st.Checks["cluster"]; ok {
		t.Errorf("a standalone server has a cluster check: %+v", st)
	}
	if got := grpcHealth(t, s); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("grpc health: %v", got)
	}
}

func TestHealthCluster(t *testing.T) {
	c := startCluster(t, "one", "two", "three")
	for _, n := range c.nodes {
		st := untilReady(t, n.server, 30*time.Second)
		if !strings.Contains(st.Checks["cluster"].Detail, "in the ring: 3 of 3 nodes") {
			t.Errorf("%s: cluster check: %+v", n.name, st.Checks["cluster"])
		}
	}

	// A node shut down answers nothing; the others stay ready.
	gone := c.nodes[2]
	gone.shutdown()
	if code, _ := monitorGet(t, gone.server, "/_readyz"); code == http.StatusOK {
		t.Errorf("%s is ready after its shutdown", gone.name)
	}
	for _, n := range c.nodes[:2] {
		// The leader may have been the one that left: an election first.
		st := untilReady(t, n.server, 30*time.Second)
		if !st.Checks["cluster"].OK {
			t.Errorf("%s: %+v", n.name, st.Checks["cluster"])
		}
	}
}
