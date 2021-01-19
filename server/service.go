package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/unit-io/unitdb/server/config"
	lp "github.com/unit-io/unitdb/server/net"
	"github.com/unit-io/unitdb/server/net/listener"
	"github.com/unit-io/unitdb/server/pkg/crypto"
	"github.com/unit-io/unitdb/server/pkg/log"
	"github.com/unit-io/unitdb/server/pkg/stats"
	"github.com/unit-io/unitdb/server/pkg/uid"

	// Database store
	_ "github.com/unit-io/unitdb/server/db/unitdb"
	"github.com/unit-io/unitdb/server/store"
)

//Service is a main struct
type Service struct {
	PID     uint32             // The processid is unique Id for the application
	MAC     *crypto.MAC        // The MAC to use for decoding and encoding keys.
	cache   *sync.Map          // The cache for the contracts.
	context context.Context    // context for the service
	config  *config.Config     // The configuration for the service.
	cancel  context.CancelFunc // cancellation function
	start   time.Time          // The service start time
	http    *lp.HttpServer     // The underlying HTTP server.
	tcp     *lp.TcpServer      // The underlying TCP server.
	grpc    *lp.GrpcServer     // The underlying GRPC server.
	meter   *Meter             // The metircs to measure timeseries on message events
	stats   *stats.Stats
}

func NewService(ctx context.Context, cfg *config.Config) (s *Service, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	s = &Service{
		PID:     uid.NewUnique(),
		cache:   new(sync.Map),
		context: ctx,
		config:  cfg,
		cancel:  cancel,
		start:   time.Now(),
		// subscriptions: message.NewSubscriptions(),
		http:  lp.NewHttpServer(),
		tcp:   lp.NewTcpServer(),
		grpc:  lp.NewGrpcServer(),
		meter: NewMeter(),
		stats: stats.New(&stats.Config{Addr: "localhost:8094", Size: 50}, stats.MaxPacketSize(1400), stats.MetricPrefix("trace")),
	}

	// // Varz
	// if cfg.VarzPath != "" {
	// 	s.http.HandleFunc(cfg.VarzPath, s.HandleVarz)
	// 	log.Info("service", "Stats variables exposed at "+cfg.VarzPath)
	// }

	//attach handlers
	s.grpc.Handler = s.onAcceptConn
	s.http.Handler = s.onAcceptConn
	s.tcp.Handler = s.onAcceptConn

	// Create a new MAC from the key.
	if s.MAC, err = crypto.New([]byte(s.config.Encryption(s.config.EncryptionConfig).Key)); err != nil {
		return nil, err
	}

	// Open database connection
	err = store.Open(string(s.config.StoreConfig), s.config.Store(s.config.StoreConfig).CleanSession)
	if err != nil {
		log.Fatal("service", "Failed to connect to DB:", err)
	}

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

//Listen starts the service
func (s *Service) Listen() (err error) {
	defer s.Close()
	s.hookSignals()

	s.listen(s.config.Listen)

	log.Info("service", "service started")
	select {}
}

//listen configures main listerner on specefied address
func (s *Service) listen(addr string) {
	//Create a new listener
	log.Info("service.listen", "starting the listner at "+addr)

	l, err := listener.New(addr)
	if err != nil {
		panic(err)
	}

	l.SetReadTimeout(120 * time.Second)

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

	go l.Serve()
}

// Handle a new connection request
func (s *Service) onAcceptConn(t net.Conn, proto lp.Proto) {
	conn := s.newConn(t, proto)
	go conn.readLoop()
	go conn.writeLoop(s.context)
}

func (s *Service) onSignal(sig os.Signal) {
	switch sig {
	case syscall.SIGTERM:
		fallthrough
	case syscall.SIGINT:
		log.Info("service.onSignal", "received signal, exiting..."+sig.String())
		s.Close()
		os.Exit(0)
	}
}

func (s *Service) hookSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for sig := range c {
			s.onSignal(sig)
		}
	}()
}

func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
	}

	s.meter.UnregisterAll()
	s.stats.Unregister()

	store.Close()

	// Shutdown local cluster node, if it's a part of a cluster.
	Globals.Cluster.shutdown()
}
