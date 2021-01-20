package net

import (
	"net"
	"time"
)

// //onAccept is a callback which get called when a connection is accepted
// type TcpHandler func(c net.Conn, proto Proto)

type TcpServer server

func NewTcpServer(opts ...Options) *TcpServer {
	srv := &TcpServer{
		opts: new(options),
	}
	WithDefaultOptions().set(srv.opts)
	for _, opt := range opts {
		opt.set(srv.opts)
	}
	return srv
}

func (s *TcpServer) Serve(list net.Listener) error {
	defer list.Close()

	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		conn, err := list.Accept()
		if err != nil {
			select {
			case <-signalHandler():
				return ErrServerClosed
			default:
			}

			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		tempDelay = 0
		go s.Handler(conn, GRPC)
	}
}
