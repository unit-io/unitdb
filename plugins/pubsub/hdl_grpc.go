package pubsub

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	plugins "github.com/unit-io/unite/plugins/grpc"
	pbx "github.com/unit-io/unite/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	MaxMessageSize = 1 << 19
)

type GrpcServer server

func NewGrpcServer(opts ...Options) *GrpcServer {
	srv := &GrpcServer{
		opts: new(options),
	}
	WithDefaultOptions().set(srv.opts)
	for _, opt := range opts {
		opt.set(srv.opts)
	}
	return srv
}

// Start implements unite.Connect
func (s *GrpcServer) Start(ctx context.Context, info *pbx.ConnInfo) (*pbx.ConnInfo, error) {
	if info != nil {
		// Will panic if msg is not of *pbx.Conn type. This is an intentional panic.
		return &pbx.ConnInfo{}, nil
	}
	return nil, nil
}

func StreamConn(
	stream grpc.Stream,
) *plugins.Conn {
	packetFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pbx.Packet).Data
	}
	return &plugins.Conn{
		Stream: stream,
		InMsg:  &pbx.Packet{},
		OutMsg: &pbx.Packet{},
		Encode: plugins.Encode(packetFunc),
		Decode: plugins.Decode(packetFunc),
	}
}

// Stream implements duplex unite.Stream
func (s *GrpcServer) Stream(stream pbx.Unite_StreamServer) error {
	conn := StreamConn(stream)
	defer conn.Close()

	go s.Handler(conn, GRPC)
	<-stream.Context().Done()
	return nil
}

// Disconnect implements unite.Disconnect
func (s *GrpcServer) Stop(context.Context, *pbx.Empty) (*pbx.Empty, error) {
	return nil, nil
}

func (s *GrpcServer) Serve(list net.Listener) error {
	secure := ""
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(int(MaxMessageSize)))
	if s.opts.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.opts.TLSConfig)))
		secure = " secure"
	}

	if s.opts.KeepAlive {
		kepConfig := keepalive.EnforcementPolicy{
			MinTime:             1 * time.Second, // If a client pings more than once every second, terminate the connection
			PermitWithoutStream: true,            // Allow pings even when there are no active streams
		}
		opts = append(opts, grpc.KeepaliveEnforcementPolicy(kepConfig))

		kpConfig := keepalive.ServerParameters{
			Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
			Timeout: 20 * time.Second, // Wait 20 second for the ping ack before assuming the connection is dead
		}
		opts = append(opts, grpc.KeepaliveParams(kpConfig))
	}

	srv := grpc.NewServer(opts...)
	pbx.RegisterUniteServer(srv, s)
	log.Printf("gRPC/%s%s server is registered", grpc.Version, secure)
	go func() {
		if err := srv.Serve(list); err != nil {
			log.Println("gRPC server failed:", err)
		}
	}()
	return nil
}

var _ pbx.UniteServer = (*GrpcServer)(nil)
