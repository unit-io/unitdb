package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// Health checks, on a port of their own (monitor_listen), apart from the
// clients':
//
//   - /_healthz: the process serves requests. It checks nothing else.
//   - /_readyz: 200 "ready", or 503 and why: draining, or a check failed. It
//     reads the results the checks left, and never waits on them.
//   - /_status: the same, as JSON, with each check's detail.
//
// The gRPC server also answers grpc.health.v1, SERVING while ready, and
// /_metrics has the server's metrics (metrics.go).
//
// Each check runs every healthInterval in the background, with a
// healthTimeout, and keeps its last result.

const (
	healthInterval = 5 * time.Second
	healthTimeout  = 3 * time.Second
)

type healthResult struct {
	OK        bool      `json:"ok"`
	Detail    string    `json:"detail"`
	CheckedAt time.Time `json:"checked_at"`
	LatencyMs int64     `json:"latency_ms"`
}

type healthCheck struct {
	name string
	run  func() (string, error)
}

type healthMonitor struct {
	started  time.Time
	checks   []healthCheck
	mu       sync.RWMutex
	results  map[string]healthResult
	draining atomic.Bool
	grpc     *health.Server
	srv      *http.Server
	// metrics writes /_metrics, when set.
	metrics func(io.Writer)
	// checkpoints takes POST /_checkpoint, when set.
	checkpoints *checkpointer
}

func newHealthMonitor(started time.Time) *healthMonitor {
	return &healthMonitor{
		started: started,
		results: map[string]healthResult{},
		grpc:    health.NewServer(),
	}
}

// add adds a check, before run.
func (h *healthMonitor) add(name string, run func() (string, error)) {
	h.checks = append(h.checks, healthCheck{name, run})
}

// runAll runs every check once, each within healthTimeout.
func (h *healthMonitor) runAll() {
	var wg sync.WaitGroup
	for _, c := range h.checks {
		wg.Add(1)
		go func(c healthCheck) {
			defer wg.Done()
			start := time.Now()
			type answer struct {
				detail string
				err    error
			}
			done := make(chan answer, 1)
			go func() {
				d, err := c.run()
				done <- answer{d, err}
			}()
			r := healthResult{CheckedAt: time.Now().UTC()}
			select {
			case a := <-done:
				r.OK, r.Detail = a.err == nil, a.detail
				if a.err != nil {
					r.Detail = a.err.Error()
				}
			case <-time.After(healthTimeout):
				r.Detail = fmt.Sprintf("no answer in %s", healthTimeout)
			}
			r.LatencyMs = time.Since(start).Milliseconds()
			h.mu.Lock()
			h.results[c.name] = r
			h.mu.Unlock()
		}(c)
	}
	wg.Wait()
	h.updateGRPC()
}

// run checks now, then every healthInterval, until ctx is done.
func (h *healthMonitor) run(ctx context.Context) {
	h.runAll()
	t := time.NewTicker(healthInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.runAll()
		}
	}
}

// notReady returns why the server isn't ready, or "".
func (h *healthMonitor) notReady() string {
	if h.draining.Load() {
		return "draining"
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.checks {
		r, ok := h.results[c.name]
		if !ok {
			return c.name + ": not checked yet"
		}
		if !r.OK {
			return c.name + ": " + r.Detail
		}
	}
	return ""
}

func (h *healthMonitor) updateGRPC() {
	status := healthpb.HealthCheckResponse_SERVING
	if h.notReady() != "" {
		status = healthpb.HealthCheckResponse_NOT_SERVING
	}
	h.grpc.SetServingStatus("", status)
	h.grpc.SetServingStatus("unitdb.Unitdb", status)
}

// drain stops the server being ready: SIGTERM, or Close.
func (h *healthMonitor) drain() {
	h.draining.Store(true)
	h.updateGRPC()
}

func (h *healthMonitor) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/_readyz", func(w http.ResponseWriter, r *http.Request) {
		if why := h.notReady(); why != "" {
			http.Error(w, why, http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready"))
	})
	if h.checkpoints != nil {
		mux.HandleFunc("/_checkpoint", h.checkpoints.handle)
		mux.HandleFunc("/_checkpoint/manifest", h.checkpoints.handleManifest)
		mux.HandleFunc("/_checkpoint/upload", h.checkpoints.handleUpload)
		mux.HandleFunc("/_checkpoint/canary", h.checkpoints.handleCanary)
	}
	mux.HandleFunc("/_metrics", func(w http.ResponseWriter, r *http.Request) {
		if h.metrics == nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "text/plain; version=0.0.4")
		h.metrics(w)
	})
	mux.HandleFunc("/_status", func(w http.ResponseWriter, r *http.Request) {
		why := h.notReady()
		status := "ready"
		switch {
		case h.draining.Load():
			status = "draining"
		case why != "":
			status = "not_ready"
		}
		h.mu.RLock()
		checks := make(map[string]healthResult, len(h.results))
		for k, v := range h.results {
			checks[k] = v
		}
		h.mu.RUnlock()
		names := make([]string, 0, len(checks))
		for k := range checks {
			names = append(names, k)
		}
		sort.Strings(names)
		body := map[string]interface{}{
			"status":  status,
			"service": "unitdb",
			"started": h.started.UTC().Format(time.RFC3339),
			"checks":  checks,
		}
		if why != "" {
			body["reason"] = why
		}
		w.Header().Set("content-type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(body)
	})
	return mux
}

// serve serves the checks on addr, until close.
func (h *healthMonitor) serve(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	h.srv = &http.Server{Handler: h.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := h.srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health", "serve: "+err.Error())
		}
	}()
	log.Info("health", "checks served at "+addr)
	return nil
}

func (h *healthMonitor) close() {
	if h.srv != nil {
		h.srv.Close()
	}
}

// addServiceChecks adds the server's checks: the store, and in a cluster,
// this node's place in it.
func (h *healthMonitor) addServiceChecks() {
	// Checkpoints, when the environment asks for them (checkpoint.go).
	h.checkpoints = newCheckpointerFromEnv()

	h.add("store", func() (string, error) {
		if !store.IsOpen() {
			return "", errors.New("the store is not open")
		}
		// A write and a read of the store's own key, which nothing
		// replicates.
		if err := store.Probe(); err != nil {
			return "", err
		}
		return store.GetAdapterName() + " open, takes a write", nil
	})
	if Globals.Cluster != nil {
		h.add("cluster", func() (string, error) {
			ok, detail := Globals.Cluster.readiness()
			if !ok {
				return "", errors.New(detail)
			}
			return detail, nil
		})
	}
}
