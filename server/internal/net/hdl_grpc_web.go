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
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/unit-io/unitdb/server/common"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type HttpServer server

func NewHttpServer(opts ...Options) *HttpServer {
	srv := &HttpServer{
		opts: new(options),
	}
	WithDefaultOptions().set(srv.opts)
	for _, opt := range opts {
		opt.set(srv.opts)
	}
	return srv
}

// Conn implements net.Conn across a Websocket.
//
// Methods such as
// LocalAddr, RemoteAddr, deadlines, etc. do not work.
type Conn struct {
	// Stream is the stream to wrap into a Conn. This is duplex stream.
	Stream *websocket.Conn

	// InMsg is the type to use for reading request data from the streaming
	// endpoint. This must be a non-nil allocated value and must NOT point to
	// the same value as OutMsg since they may be used concurrently.
	//
	// The Reset method will be called on InMsg during Reads so data you
	// set initially will be lost.
	InMsg proto.Message

	// OutMsg is the type to use for sending data to streaming endpoint. This must be
	// a non-nil allocated value and must NOT point to the same value as InMsg
	// since they may be used concurrently.
	//
	// The Reset method is never called on OutMsg so they will be sent for every request
	// unless the Encode field changes it.
	OutMsg proto.Message

	// WriteLock, if non-nil, will be locked while calling SendMsg
	// on the Stream. This can be used to prevent concurrent access to
	// SendMsg which is unsafe.
	WriteLock *sync.Mutex

	// Encode encodes messages into the Request. See Encoder for more information.
	Encode common.Encoder

	// Decode decodes messages from the Response into a byte slice. See
	// Decoder for more information.
	Decode common.Decoder

	// readOffset tracks where we've read up to if we're reading a result
	// that didn't fully fit into the target slice. See Read.
	readOffset int

	// locks to ensure that only one reader/writer are operating at a time.
	// Go documents the `net.Conn` interface as being safe for simultaneous
	// readers/writers so we need to implement locking.
	readLock, writeLock sync.Mutex
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	Subprotocols:    []string{"grpc_web"},
}

func WebSocketConn(
	stream *websocket.Conn,
) *Conn {
	packetFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pbx.Packet).Data
	}
	return &Conn{
		Stream: stream,
		InMsg:  &pbx.Packet{},
		OutMsg: &pbx.Packet{},
		Encode: common.Encode(packetFunc),
		Decode: common.Decode(packetFunc),
	}
}

func (s *HttpServer) HandleFunc(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn := WebSocketConn(ws)
	go s.Handler(conn)
}

func (s *HttpServer) Serve(list net.Listener) error {
	srv := new(http.Server)
	// Create a new HTTP request multiplexer
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.HandleFunc)

	srv.Handler = mux
	go func() {
		if err := srv.Serve(list); err != nil {
			log.Println("gRPC server failed:", err)
		}
	}()
	return nil
}

// Read implements io.Reader.
func (c *Conn) Read(p []byte) (n int, err error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()

	// Attempt to read a value only if we're not still decoding a
	// partial read value from the last result.
	if c.readOffset == 0 {
		_, rawData, err := c.Stream.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.InMsg = &pbx.Packet{Data: rawData}
	}

	// Decode into our slice
	data, err := c.Decode(c.InMsg, c.readOffset, p)

	// If we have an error or we've decoded the full amount then we're done.
	// The error case is obvious. The case where we've read the full amount
	// is also a terminating condition and err == nil (we know this since its
	// checking that case) so we return the full amount and no error.
	if err != nil || len(data) <= (len(p)+c.readOffset) {
		// Reset the read offset since we're done.
		n := len(data) - c.readOffset
		c.readOffset = 0

		// Reset our response value for the next read and so that we
		// don't potentially store a large response structure in memory.
		c.InMsg.Reset()

		return n, err
	}

	// We didn't read the full amount so we need to store this for future reads
	c.readOffset += len(p)

	return len(p), err
}

// Write implements io.Writer.
func (c *Conn) Write(p []byte) (int, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	total := len(p)
	for {

		// Encode our data into the request. Any error means we abort.
		n, err := c.Encode(c.OutMsg, p)
		if err != nil {
			return 0, err
		}

		// We lock for SendMsg if we have a lock set.
		if c.WriteLock != nil {
			c.WriteLock.Lock()
		}

		// Send our message. Any error we also just abort out.
		err = c.Stream.WriteMessage(websocket.BinaryMessage, c.OutMsg.(*pbx.Packet).Data)

		if c.WriteLock != nil {
			c.WriteLock.Unlock()
		}
		if err != nil {
			return 0, err
		}

		// If we sent the full amount of data, we're done. We respond with
		// "total" in case we sent across multiple frames.
		if n == len(p) {
			return total, nil
		}

		// We sent partial data so we continue writing the remainder
		p = p[n:]
	}
}

// Close will close the client if this is a client. If this is a server
// stream this does nothing since gRPC expects you to close the stream by
// returning from the RPC call.
//
// This calls CloseSend underneath for clients, so read the documentation
// for that to understand the semantics of this call.
func (c *Conn) Close() error {
	if err := c.Stream.Close(); err != nil {
		return err
	}

	// We have to acquire the write lock since the gRPC docs state:
	// "It is also not safe to call CloseSend concurrently with SendMsg."
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	return nil
}

// LocalAddr returns nil.
func (c *Conn) LocalAddr() net.Addr { return nil }

// RemoteAddr returns nil.
func (c *Conn) RemoteAddr() net.Addr { return nil }

// SetDeadline is non-functional due to limitations on how gRPC works.
// You can mimic deadlines often using call options.
func (c *Conn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline is non-functional, see SetDeadline.
func (c *Conn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline is non-functional, see SetDeadline.
func (c *Conn) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = (*Conn)(nil)
