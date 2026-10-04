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

package internal

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/keys"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/net/listener"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/stats"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"

	// Database store
	_ "github.com/unit-io/unitdb/server/internal/db/unitdb"
	"github.com/unit-io/unitdb/server/internal/store"
)

// _Service is a main struct
type _Service struct {
	pid  uint32    // The processid is unique Id for the application
	keys *keys.Set // Issues and reads client ids and topic keys, with the keyring.
	// revocations is the cluster's security state: what was revoked in each
	// contract (unitdb/revoke).
	revocations *revocations
	// Lifetimes of v2 client ids that are not primary, of primary ones, and
	// of topic keys whose keygen request gives none; 0 never expires.
	clientIDTTL, primaryIDTTL, topicKeyTTL time.Duration
	// allowInsecure accepts clients that connect with the insecure flag
	// (allow_insecure); only ever set on a standalone server.
	allowInsecure atomic.Bool
	context       context.Context    // context for the service
	config        *config.Config     // The configuration for the service.
	cancel        context.CancelFunc // cancellation function
	start         time.Time          // The service start time
	http          *lp.HttpServer     // The underlying HTTP server.
	tcp           *lp.TcpServer      // The underlying TCP server.
	grpc          *lp.GrpcServer     // The underlying GRPC server.
	meter         *Meter             // The metircs to measure timeseries on message events
	health        *healthMonitor     // The health checks, on their own port
	stats         *stats.Stats

	// Shutdown.
	mu        sync.Mutex
	closing   bool               // set by Close: new connections are refused
	listener  *listener.Listener // the main listener, closed by Close
	conns     sync.WaitGroup     // accepted connections not yet closed
	inflight  sync.WaitGroup     // publish fan-outs in progress
	closeOnce sync.Once
}

func NewService(cfg *config.Config) (s *_Service, err error) {
	// Settings that are gone: one the server can't honour any more stops it,
	// before anything is set up.
	if warning, err := cfg.CheckRemoved(); err != nil {
		return nil, err
	} else if warning != "" {
		log.ErrLogger.Warn().Str("context", "NewService").Msg(warning)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s = &_Service{
		pid:     uid.NewUnique(),
		context: ctx,
		config:  cfg,
		cancel:  cancel,
		start:   time.Now(),
		// subscriptions: message.NewSubscriptions(),
		http:  lp.NewHttpServer(),
		tcp:   lp.NewTcpServer(),
		grpc:  lp.NewGrpcServer(lp.WithDefaultOptions()),
		meter: NewMeter(),
		stats: stats.New(&stats.Config{Addr: "localhost:8094", Size: 50}, stats.MaxPacketSize(1400), stats.MetricPrefix("trace")),
	}

	Globals.connCache = NewConnCache()

	// // Varz
	// if cfg.VarzPath != "" {
	// 	s.http.HandleFunc(cfg.VarzPath, s.HandleVarz)
	// 	log.Info("service", "Stats variables exposed at "+cfg.VarzPath)
	// }

	//attach handlers
	s.grpc.Handler = s.onAcceptConn
	s.http.Handler = s.onAcceptConn
	s.tcp.Handler = s.onAcceptConn

	// The keyring: UNITDB_KEYRING, keyring_file, or the single key.
	keyring, err := s.config.Keyring()
	if err != nil {
		return nil, err
	}
	if s.keys, err = keys.New(keyring); err != nil {
		return nil, err
	}
	if s.clientIDTTL, s.primaryIDTTL, s.topicKeyTTL, err = cfg.TTLs(); err != nil {
		return nil, err
	}
	if cfg.AllowInsecure {
		// A cluster would honour an insecure client's flag on every node
		// its requests are forwarded to, and an older node forwards its
		// clients' own flag: insecure clients are for a standalone server.
		if Globals.Cluster != nil {
			return nil, errors.New("allow_insecure is set, but this node is part of a cluster: a cluster does not accept insecure clients, which skip every topic key check; give trusted services a service client id instead (server/cmd/mintid -service)")
		}
		s.allowInsecure.Store(true)
		log.ErrLogger.Warn().Str("context", "NewService").Msg("allow_insecure is set: clients that connect with the insecure flag skip every topic key check; use it for development only")
	}

	// Sealed records are opened whatever encrypt_at_rest says, so that
	// turning it off leaves the ones sealed while it was on readable. Set
	// before Open, which reads the topic index.
	if err := store.SetSealing(s.keys.IssueKeyID(), s.keys.StoreKeys(), cfg.SealsAtRest()); err != nil {
		return nil, err
	}
	if cfg.SealsAtRest() {
		log.ErrLogger.Info().Str("context", "NewService").Uint8("key", s.keys.IssueKeyID()).Msg("encrypt_at_rest: sealing stored records")
	} else {
		log.ErrLogger.Warn().Str("context", "NewService").Msg("encrypt_at_rest is false: stored records are written as they are")
	}

	// Open database connection
	err = store.Open(string(s.config.DBPath), string(s.config.StoreConfig), s.config.Store(s.config.StoreConfig).Reset)
	if err != nil {
		log.Fatal("service", "Failed to connect to DB:", err)
	}
	if s.revocations, err = loadRevocations(); err != nil {
		return nil, err
	}
	// Hints a v0.6.0 node kept, before the cluster hands any off.
	if err := moveLegacyHints(); err != nil {
		return nil, err
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-s.context.Done():
				return
			case <-ticker.C:
				log.ErrLogger.Debug().Str("context", "NewService").Int64("goroutines", int64(runtime.NumGoroutine())).Int64("connections", s.meter.Connections.Count()).Msg("")
			}
		}
	}()

	return s, nil
}

// netListener creates net.Listener for tcp and unix domains:
// if addr is is in the form "unix:/run/tinode.sock" it's a unix socket, otherwise TCP host:port.
func netListener(addr string) (net.Listener, error) {
	addrParts := strings.SplitN(addr, ":", 2)
	if len(addrParts) == 2 && addrParts[0] == "unix" {
		return net.Listen("unix", addrParts[1])
	}
	return net.Listen("tcp", addr)
}

// Listen starts the service
func (s *_Service) Listen() (err error) {
	defer s.Close()
	s.hookSignals()

	s.listen(s.config.Listen)

	log.Info("service", "service started")
	select {}
}

// listen configures main listerner on specefied address
func (s *_Service) listen(addr string) {
	//Create a new listener
	log.Info("service.listen", "starting the listner at "+addr)

	l, err := listener.New(addr)
	if err != nil {
		panic(err)
	}

	l.SetReadTimeout(120 * time.Second)

	// The health checks, and grpc.health.v1 on the gRPC server.
	s.health = newHealthMonitor(s.start)
	s.health.addServiceChecks()
	s.grpc.Register = func(g *grpc.Server) {
		healthpb.RegisterHealthServer(g, s.health.grpc)
	}
	if s.config.MonitorListen != "" {
		if err := s.health.serve(s.config.MonitorListen); err != nil {
			panic(err)
		}
	}
	go s.health.run(s.context)

	// Configure the protos
	if s.config.GrpcListen != "" {
		grpcList, err := netListener(s.config.GrpcListen)
		if err != nil {
			return
		}
		s.grpc.Serve(grpcList)
	}
	l.ServeCallback(listener.MatchWS("GET"), s.http.Serve)
	l.ServeCallback(listener.MatchAny(), s.tcp.Serve)

	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	go l.Serve()
}

// Handle a new connection request
func (s *_Service) onAcceptConn(t net.Conn) {
	// Register the connection under the lock so that Close either refuses it
	// or waits for it.
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		t.Close()
		return
	}
	conn := s.newConn(t)
	conn.tracked = true
	s.conns.Add(1)
	s.mu.Unlock()

	conn.closeW.Add(2)
	go conn.readLoop(s.context)
	go conn.writeLoop(s.context)
}

func (s *_Service) onSignal(sig os.Signal) {
	switch sig {
	case syscall.SIGTERM:
		fallthrough
	case syscall.SIGINT:
		log.Info("service.onSignal", "received signal, exiting..."+sig.String())
		s.Close()
		os.Exit(0)
	}
}

func (s *_Service) hookSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for sig := range c {
			s.onSignal(sig)
		}
	}()
}

// Close shuts the service down: it stops accepting connections, closes the
// open ones, waits for publishes in progress and only then closes the store,
// so that nothing uses the store after it is closed. Close may be called more
// than once.
func (s *_Service) Close() {
	s.closeOnce.Do(s.close)
}

func (s *_Service) close() {
	// Not ready from now on: the load balancer moves away.
	if s.health != nil {
		s.health.drain()
		defer s.health.close()
	}

	// Leave the cluster first, while this node's clients are still served:
	// the others take over what it holds, and its clients' subscriptions.
	Globals.Cluster.drain()

	s.mu.Lock()
	s.closing = true
	l := s.listener
	s.mu.Unlock()

	// Stop accepting connections.
	if l != nil {
		l.Close()
	}
	s.grpc.Stop()

	// Close the open connections and wait for all of them, including those
	// another goroutine is already closing.
	for _, c := range Globals.connCache.all() {
		if c.tracked {
			c.close()
		}
	}
	s.conns.Wait()
	s.inflight.Wait()

	if s.cancel != nil {
		s.cancel()
	}

	s.meter.UnregisterAll()
	s.stats.Unregister()

	store.Close()

	// Shutdown local cluster node, if it's a part of a cluster.
	Globals.Cluster.shutdown()
}
