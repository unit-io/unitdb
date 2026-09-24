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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	// MaxFrameSize is the largest message body accepted on a connection. It
	// covers the largest publish batch a client sends (3.5 MiB).
	MaxFrameSize = 4 << 20

	// MaxMessageSize is the largest gRPC message accepted: a frame with its
	// length prefix, fixed header and the gRPC packet around it, so that the
	// same messages are accepted over gRPC and TCP.
	MaxMessageSize = MaxFrameSize + 1<<10
)

// ErrServerClosed occurs when a tcp server is closed.
var ErrServerClosed = errors.New("Server closed")

// Proto represents the type of connection
type Proto int

const (
	UTP    Proto = iota // Unit Transport Protocol
	UNITQL              // Unit Query Language
)

//Handler is a callback which get called when a tcp, websocket connection is established or a grpc stream is established
type Handler func(c net.Conn)

type options struct {
	TLSConfig *tls.Config
	KeepAlive bool
}

// Options it contains configurable options for client
type Options interface {
	set(*options)
}

// fOption wraps a function that modifies options into an
// implementation of the Option interface.
type fOption struct {
	f func(*options)
}

func (fo *fOption) set(o *options) {
	fo.f(o)
}

func newFuncOption(f func(*options)) *fOption {
	return &fOption{
		f: f,
	}
}

// WithDefaultOptions will create client connection with some default values.
//   KeepAlive: true
//   TlsConfig: nil
func WithDefaultOptions() Options {
	return newFuncOption(func(o *options) {
		o.KeepAlive = true
		o.TLSConfig = nil
	})
}

// WithTLSConfig will set an SSL/TLS configuration to be used when connecting
// to server.
func WithTLSConfig(t *tls.Config) Options {
	return newFuncOption(func(o *options) {
		o.TLSConfig = t
	})
}

type Server interface {
	// Serve serve the requests if type tcp, websocket or grpc stream
	Serve(net.Listener) error
}

type server struct {
	sync.Mutex
	opts    *options
	stop    func() // stops serving, if the server supports it
	Handler Handler //The handler to invoke when a connection is accepted
}

func signalHandler() <-chan bool {
	stop := make(chan bool)

	signchan := make(chan os.Signal, 1)
	signal.Notify(signchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		// Wait for a signal. Don't care which signal it is
		sig := <-signchan
		fmt.Printf("Signal received: '%s', shutting down\n", sig)
		stop <- true
	}()

	return stop
}
