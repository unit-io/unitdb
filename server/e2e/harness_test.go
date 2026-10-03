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

// Package e2e drives a real unitdb server process over the wire (uTP) to test
// concurrency, races, performance, integrity and security end to end.
//
// The server binary is built once (with -race when the test binary is built
// with -race, so server-side races surface too) and each test starts its own
// instance on free ports with its own temp DB path, so tests are isolated and
// can run in parallel.
package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// testKey is the encryption key the test servers run with. The client-id and
// topic-key helpers use it to mint credentials the server will accept.
const testKey = "test-only-key-do-not-use-0000000"

var build struct {
	once sync.Once
	bin  string
	err  error
}

// serverBinary builds the server once and returns its path.
func serverBinary(t *testing.T) string {
	t.Helper()
	build.once.Do(func() {
		dir, err := os.MkdirTemp("", "unitdb-e2e-bin")
		if err != nil {
			build.err = err
			return
		}
		bin := filepath.Join(dir, "unitdb-server")
		args := []string{"build"}
		if raceEnabled {
			args = append(args, "-race")
		}
		args = append(args, "-o", bin, ".")
		cmd := exec.Command("go", args...)
		cmd.Dir = serverSourceDir(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			build.err = fmt.Errorf("build server: %v\n%s", err, out)
			return
		}
		build.bin = bin
	})
	if build.err != nil {
		t.Fatal(build.err)
	}
	return build.bin
}

// serverSourceDir returns the directory holding the server's main.go to build.
// UNITDB_SERVER_DIR overrides it; this lets the build use a checkout pinned at a
// committed revision, so results are reproducible and isolated from any
// uncommitted edits in the working tree. It defaults to the parent of this
// package.
func serverSourceDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("UNITDB_SERVER_DIR"); dir != "" {
		return dir
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	return filepath.Dir(filepath.Dir(file))
}

// freePort asks the OS for an unused TCP port and returns it, released.
// freePort returns a port no one listens on, for a server to bind later.
//
// It is below the ephemeral range (from 32768 on Linux, 49152 on macOS): a
// port from there, free when picked, can be taken by any outgoing
// connection before the server binds it, such as the nodes' own cluster
// connections while a node starts, and the server then fails to start.
func freePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 1000; i++ {
		port := 20000 + rand.Intn(12000)
		if !takePort(port) {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		l.Close()
		return port
	}
	t.Fatal("no free port")
	return 0
}

// takenPorts are the ports freePort returned, which it does not return again.
var takenPorts sync.Map

func takePort(port int) bool {
	_, taken := takenPorts.LoadOrStore(port, true)
	return !taken
}

// server is a running unitdb server instance.
type server struct {
	t        *testing.T
	cmd      *exec.Cmd
	dbPath   string
	tcpAddr  string
	grpcAddr string
	logs     *syncBuffer
	// env is added to the server's environment.
	env []string
	// noWait starts the server without waiting for it to be ready.
	noWait bool
	// exited is closed when the current process has exited.
	exited chan struct{}
	// confPath is the server's config file, and confWith renders it with
	// another cluster_config, for setCluster.
	confPath string
	confWith func(cluster string) string
}

// setCluster rewrites the server's config with cluster as its
// cluster_config, for the next start.
func (s *server) setCluster(cluster string) {
	s.t.Helper()
	if err := os.WriteFile(s.confPath, []byte(s.confWith(cluster)), 0644); err != nil {
		s.t.Fatal(err)
	}
}

// watch reaps the current process in the background and closes exited when
// it exits, so liveness does not depend on signalling a zombie.
func (s *server) watch() {
	cmd, exited := s.cmd, make(chan struct{})
	s.exited = exited
	go func() {
		cmd.Wait()
		close(exited)
	}()
}

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.WriteString(string(p))
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startServer builds (once) and starts a server instance. It is stopped and
// its DB removed on test cleanup.
func startServer(t *testing.T) *server {
	t.Helper()
	return startServerWith(t, serverOpts{})
}

// serverOpts customizes a server instance.
type serverOpts struct {
	// cluster is the cluster_config JSON object; empty runs standalone.
	cluster string
	// args are extra command line arguments, e.g. -cluster_self.
	args []string
	// logLevel is the server logging level; default "Error".
	logLevel string
	// env is added to the server's environment, e.g. UNITDB_CLUSTER_CAPS.
	env []string
	// key is encryption_config's key; default testKey.
	key string
	// allowInsecure sets allow_insecure (default false): clients that send
	// the insecure flag are refused without it.
	allowInsecure bool
	// expectExit starts the server without waiting for it to be ready, for
	// a server expected to refuse to start.
	expectExit bool
	// extra is added to the config's top level object, such as
	// `"client_id_ttl": "10s",`, with its trailing comma.
	extra string
	// bin is the server binary to run, such as an older version's
	// (oldServerBinary); the one built from this tree if empty.
	bin string
}

func startServerWith(t *testing.T, opts serverOpts) *server {
	t.Helper()
	if opts.cluster == "" {
		opts.cluster = `{"self": ""}`
	}
	if opts.logLevel == "" {
		opts.logLevel = "Error"
	}
	if opts.key == "" {
		opts.key = testKey
	}
	bin := opts.bin
	if bin == "" {
		bin = serverBinary(t)
	}
	binDir := filepath.Dir(bin)

	// The server always resolves -config against the executable's directory,
	// so the config lives next to the binary. Instances share the binary dir
	// but each writes its own db_path, so a per-instance conf name avoids
	// clobbering. main.go joins Dir(exe)+config, so use a plain filename.
	confName := fmt.Sprintf("e2e-%d.conf", freePort(t))
	tcpPort := freePort(t)
	grpcPort := freePort(t)
	dbPath, err := os.MkdirTemp("", "unitdb-e2e-db")
	if err != nil {
		t.Fatal(err)
	}
	confWith := func(cluster string) string {
		return fmt.Sprintf(`{
  "listen": "127.0.0.1:%d",
  "grpc_listen": "127.0.0.1:%d",
  "logging_level": %q,
  "allow_insecure": %t,
  %s
  "encryption_config": {"key": %q, "identifier": "local", "sealed": false, "timestamp": 1522325758},
  "cluster_config": %s,
  "store_config": {"reset": false, "adapters": {"unitdb": {"database": "unitdb", "mem_size": 500000000}}}
}`, tcpPort, grpcPort, opts.logLevel, opts.allowInsecure, opts.extra, opts.key, cluster)
	}
	confPath := filepath.Join(binDir, confName)
	if err := os.WriteFile(confPath, []byte(confWith(opts.cluster)), 0644); err != nil {
		t.Fatal(err)
	}

	logs := &syncBuffer{}
	cmd := exec.Command(bin, append([]string{"-config", confName, "-db_path", filepath.Join(dbPath, "db")}, opts.args...)...)
	cmd.Env = append(os.Environ(), opts.env...)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	s := &server{
		t:        t,
		cmd:      cmd,
		dbPath:   dbPath,
		tcpAddr:  fmt.Sprintf("127.0.0.1:%d", tcpPort),
		grpcAddr: fmt.Sprintf("127.0.0.1:%d", grpcPort),
		logs:     logs,
		env:      opts.env,
		noWait:   opts.expectExit,
		confPath: confPath,
		confWith: confWith,
	}
	s.watch()
	t.Cleanup(func() {
		s.stop()
		os.Remove(s.confPath)
		os.RemoveAll(dbPath)
		// A race-built server prints race reports to its own output; fail the
		// test that exercised it.
		if logs := s.logs.String(); strings.Contains(logs, "WARNING: DATA RACE") {
			t.Errorf("server reported a data race:\n%s", logs)
		} else if t.Failed() && logs != "" {
			// Where a failure shows only as a refused connection, the server's
			// own output says why.
			t.Logf("server %s logs:\n%s", s.tcpAddr, logs)
		}
	})

	if opts.expectExit {
		return s
	}
	if err := s.waitReady(); err != nil {
		t.Fatalf("%v\nserver logs:\n%s", err, logs.String())
	}
	return s
}

// waitReady blocks until the TCP listener accepts connections.
func (s *server) waitReady() error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !s.alive() {
			return fmt.Errorf("server exited early")
		}
		c, err := net.DialTimeout("tcp", s.tcpAddr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("server not ready at %s", s.tcpAddr)
}

// stop kills the server. SIGKILL models a crash; the server also ignores
// graceful shutdown cleanly enough for a clean-restart test via restart().
func (s *server) stop() {
	if s.cmd.Process != nil {
		s.cmd.Process.Kill()
		<-s.exited
	}
}

// shutdown stops the server with SIGTERM, as a deploy would, and waits for it
// to exit, killing it if it does not within 15 seconds.
func (s *server) shutdown() {
	if s.cmd == nil || !s.alive() {
		return
	}
	syscall.Kill(s.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(15 * time.Second):
		s.t.Logf("server did not exit on SIGTERM; killing it")
		s.stop()
	}
}

// restart kills the current process and starts a new one on the same ports and
// DB path, modelling a crash-and-recover. It reuses the config already on disk.
func (s *server) restart() error {
	s.stop()
	return s.start()
}

// start starts the server process again with the same arguments, appending to
// the same logs, after stop.
func (s *server) start() error {
	cmd := exec.Command(s.cmd.Args[0], s.cmd.Args[1:]...)
	// From env, so that a test can change the environment between starts.
	cmd.Env = append(os.Environ(), s.env...)
	cmd.Stdout = s.logs
	cmd.Stderr = s.logs
	if err := cmd.Start(); err != nil {
		return err
	}
	s.cmd = cmd
	s.watch()
	if s.noWait {
		return nil
	}
	return s.waitReady()
}

// alive reports whether the server process is still running.
func (s *server) alive() bool {
	select {
	case <-s.exited:
		return false
	default:
		return true
	}
}

func dialContext(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// switchBinary has the stopped server s run bin from its next start, with
// the same config and store: an upgrade, or a rollback.
func (s *server) switchBinary(bin string) {
	s.t.Helper()
	b, err := os.ReadFile(s.confPath)
	if err != nil {
		s.t.Fatal(err)
	}
	// The server reads its config from beside its binary.
	confPath := filepath.Join(filepath.Dir(bin), filepath.Base(s.confPath))
	if confPath != s.confPath {
		if err := os.WriteFile(confPath, b, 0644); err != nil {
			s.t.Fatal(err)
		}
		os.Remove(s.confPath)
		s.confPath = confPath
	}
	s.cmd.Args[0] = bin
}

var oldBuilds sync.Map // tag -> *oldBuild

type oldBuild struct {
	once sync.Once
	bin  string
	err  error
}

// oldServerBinary builds the server of an older version, tag, from this
// repository's history, once, and returns its path. UNITDB_OLD_SERVER_BIN
// names one built already. The test is skipped if it can't be built: no git,
// or no such tag in a shallow clone.
func oldServerBinary(t *testing.T, tag string) string {
	t.Helper()
	if bin := os.Getenv("UNITDB_OLD_SERVER_BIN"); bin != "" {
		return bin
	}
	v, _ := oldBuilds.LoadOrStore(tag, &oldBuild{})
	b := v.(*oldBuild)
	b.once.Do(func() {
		repo := filepath.Dir(serverSourceDir(t))
		dir, err := os.MkdirTemp("", "unitdb-e2e-"+tag)
		if err != nil {
			b.err = err
			return
		}
		archive := exec.Command("git", "-C", repo, "archive", "--format=tar", "-o", filepath.Join(dir, "src.tar"), tag)
		if out, err := archive.CombinedOutput(); err != nil {
			b.err = fmt.Errorf("git archive %s: %v\n%s", tag, err, out)
			return
		}
		src := filepath.Join(dir, "src")
		if err := os.MkdirAll(src, 0755); err != nil {
			b.err = err
			return
		}
		if out, err := exec.Command("tar", "-xf", filepath.Join(dir, "src.tar"), "-C", src).CombinedOutput(); err != nil {
			b.err = fmt.Errorf("untar %s: %v\n%s", tag, err, out)
			return
		}
		bin := filepath.Join(dir, "bin", "unitdb-server")
		build := exec.Command("go", "build", "-o", bin, ".")
		build.Dir = filepath.Join(src, "server")
		build.Env = append(os.Environ(), "GOWORK=off")
		if out, err := build.CombinedOutput(); err != nil {
			b.err = fmt.Errorf("build %s: %v\n%s", tag, err, out)
			return
		}
		b.bin = bin
	})
	if b.err != nil {
		t.Skipf("no %s server to run: %v", tag, b.err)
	}
	return b.bin
}
