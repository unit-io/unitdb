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

package net

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/unit-io/unitdb/server/common"
	pbx "github.com/unit-io/unitdb/server/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
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

// Start implements Connect
func (s *GrpcServer) Start(ctx context.Context, info *pbx.ConnInfo) (*pbx.ConnInfo, error) {
	if info != nil {
		// Will panic if msg is not of *Conn type. This is an intentional panic.
		return &pbx.ConnInfo{}, nil
	}
	return nil, nil
}

func StreamConn(
	stream grpc.Stream,
) *common.Conn {
	packetFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pbx.Packet).Data
	}
	return &common.Conn{
		Stream: stream,
		InMsg:  &pbx.Packet{},
		OutMsg: &pbx.Packet{},
		Encode: common.Encode(packetFunc),
		Decode: common.Decode(packetFunc),
	}
}

// Stream implements duplex unitdb.Stream
func (s *GrpcServer) Stream(stream pbx.Unitdb_StreamServer) error {
	conn := StreamConn(stream)
	defer conn.Close()

	go s.Handler(conn, GRPC)
	<-stream.Context().Done()
	return nil
}

// Disconnect implements unitdb.Disconnect
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
	pbx.RegisterUnitdbServer(srv, s)
	log.Printf("gRPC/%s%s server is registered", grpc.Version, secure)
	go func() {
		if err := srv.Serve(list); err != nil {
			log.Println("gRPC server failed:", err)
		}
	}()
	return nil
}

var _ pbx.UnitdbServer = (*GrpcServer)(nil)
