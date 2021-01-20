package net

import (
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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

type websocketConn interface {
	NextReader() (messageType int, r io.Reader, err error)
	NextWriter(messageType int) (io.WriteCloser, error)
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// session represents a websocket connection.
type session struct {
	sync.Mutex
	ws      websocketConn
	reader  io.Reader
	closing chan bool
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = time.Second * 55

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Time to wait before force close on connection.
	closeGracePeriod = 10 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	Subprotocols:    []string{"grpc_web"},
}

func (s *HttpServer) HandleFunc(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	ws.SetReadLimit(MaxMessageSize)
	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	go s.Handler(newConn(ws), GRPC_WEB)
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

// newConn creates a new transport from websocket.
func newConn(ws websocketConn) net.Conn {
	conn := &session{
		ws:      ws,
		closing: make(chan bool),
	}

	return conn
}

func (sess *session) Read(b []byte) (n int, err error) {
	if sess.reader == nil {
		for {
			// Read a ClientComMessage
			c, r, err := sess.ws.NextReader()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure,
					websocket.CloseNormalClosure) {
				}
				return 0, err
			}

			if c != websocket.BinaryMessage && c != websocket.TextMessage {
				continue
			}

			sess.reader = r
			break
		}
	}
	// Read from the reader
	n, err = sess.reader.Read(b)
	if err != nil {
		if err == io.EOF {
			sess.reader = nil
			err = nil
		}
	}
	return
}

func (sess *session) Write(b []byte) (n int, err error) {
	// Serialize write to avoid concurrent write
	sess.Lock()
	defer func() {
		sess.Unlock()
	}()

	var w io.WriteCloser
	if w, err = sess.ws.NextWriter(websocket.BinaryMessage); err == nil {
		if n, err = w.Write(b); err == nil {
			err = w.Close()
		}
	}
	return
}

// Close terminates the connection.
func (sess *session) Close() error {
	return sess.ws.Close()
}

// LocalAddr returns the local network address.
func (sess *session) LocalAddr() net.Addr {
	return sess.ws.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (sess *session) RemoteAddr() net.Addr {
	return sess.ws.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (sess *session) SetDeadline(t time.Time) (err error) {
	if err = sess.ws.SetReadDeadline(t); err == nil {
		err = sess.ws.SetWriteDeadline(t)
	}
	return
}

// SetReadDeadline sets the deadline for Read calls.
func (sess *session) SetReadDeadline(t time.Time) error {
	return sess.ws.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for Write calls.
func (sess *session) SetWriteDeadline(t time.Time) error {
	return sess.ws.SetWriteDeadline(t)
}
