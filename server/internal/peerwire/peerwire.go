/*
 * Copyright 2026 Saffat Technologies, Ltd.
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

// Package peerwire is the protocol unitdb cluster nodes speak to each other:
// length-prefixed frames over one TCP or TLS connection, a hello exchange
// that names both ends, and many concurrent calls multiplexed on the
// connection by call id. Bodies are encoding/gob values.
//
// It is an independent implementation written for
// docs/design/cluster-spec.md (section 1.5); it knows nothing of what the
// calls mean.
//
// A frame is:
//
//	uint32  length of what follows (big endian)
//	byte    kind
//	uint64  call id (big endian; 0 for hello, ping and pong)
//	[]byte  payload
//
// A call's payload is a uint16 method name length, the name, and the gob
// encoded request. A reply's payload is the gob encoded response, or empty;
// a failure's is a gob encoded RemoteError.
//
// A frame is processed only once it has been read whole, so a write that
// fails part way through (the connection is then closed) delivers nothing:
// a call whose frame could not be written was not sent.
package peerwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Magic opens every hello, so that a node never mistakes another protocol
// for this one.
const Magic = "unitdb-peer"

// Frame kinds.
const (
	kindHello byte = 1
	kindCall  byte = 2
	kindReply byte = 3
	kindFail  byte = 4
	kindPing  byte = 5
	kindPong  byte = 6
)

const (
	headerSize = 4 + 1 + 8
	// MaxFrame is the largest frame either end accepts.
	MaxFrame = 256 << 20
	// KeepaliveEvery is how often an idle dialer pings the other end.
	KeepaliveEvery = 15 * time.Second
	// IdleTimeout is how long either end waits for a frame before it takes
	// the connection for dead: a few missed keepalives.
	IdleTimeout = 3 * KeepaliveEvery
	// WriteTimeout bounds one frame's write, so that a peer that stopped
	// reading can't hold a writer forever.
	WriteTimeout = 10 * time.Second
	// HelloTimeout bounds the hello exchange on the accepting side.
	HelloTimeout = 5 * time.Second
)

// Hello is what each end says first. The dialer names itself and the node
// it means to reach; the acceptor answers with its own hello, or a refusal.
type Hello struct {
	Magic    string
	Protocol int
	// From is the sender's node name; To the node it means to reach.
	From string
	To   string
	// Incarnation tells one run of a node's process from the next.
	Incarnation int64
	// Info is opaque to this package: what the node can do.
	Info []byte
	// Refused is set, in an answer, when the acceptor turns the dialer away.
	Refused string
}

// Code classifies an error answered by the other end.
type Code int

const (
	// CodeFailed is a handler's error.
	CodeFailed Code = 1
	// CodeNoMethod is a call the other end does not serve: a method it
	// lacks, or one it has turned off.
	CodeNoMethod Code = 2
	// CodeSender is a call naming a sender other than the connection's.
	CodeSender Code = 3
	// CodeRefused is a hello turned away.
	CodeRefused Code = 4
)

// RemoteError is an error the other end answered: the call was processed,
// or deliberately not, and the connection is fine.
type RemoteError struct {
	Code    Code
	Message string
}

func (e *RemoteError) Error() string { return e.Message }

// CallError is a transport failure of a call. Sent tells whether the call
// may have reached the other end: if not, it certainly was not processed.
type CallError struct {
	Sent bool
	Err  error
}

func (e *CallError) Error() string {
	if e.Sent {
		return "peer call failed after it was sent: " + e.Err.Error()
	}
	return "peer call not sent: " + e.Err.Error()
}

func (e *CallError) Unwrap() error { return e.Err }

var (
	// ErrTimeout is a call not answered in time. It may still complete.
	ErrTimeout = errors.New("no answer in time")
	// ErrClosed is a call on a connection already closed.
	ErrClosed = errors.New("connection closed")
	// ErrLost is a call in flight when the connection failed.
	ErrLost = errors.New("connection lost")
)

// NotSent reports whether err is a call that certainly did not reach the
// other end.
func NotSent(err error) bool {
	var ce *CallError
	return errors.As(err, &ce) && !ce.Sent
}

// Answered reports whether err is the other end's answer.
func Answered(err error) bool {
	var re *RemoteError
	return errors.As(err, &re)
}

// Encode gob-encodes v.
func Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Decode gob-decodes body into v.
func Decode(body []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(body)).Decode(v)
}

func frame(kind byte, id uint64, parts ...[]byte) []byte {
	size := 1 + 8
	for _, p := range parts {
		size += len(p)
	}
	b := make([]byte, 4+size)
	binary.BigEndian.PutUint32(b[0:4], uint32(size))
	b[4] = kind
	binary.BigEndian.PutUint64(b[5:13], id)
	off := headerSize
	for _, p := range parts {
		off += copy(b[off:], p)
	}
	return b
}

func readFrame(r *bufio.Reader) (kind byte, id uint64, payload []byte, err error) {
	var h [headerSize]byte
	if _, err = io.ReadFull(r, h[:]); err != nil {
		return 0, 0, nil, err
	}
	size := binary.BigEndian.Uint32(h[0:4])
	if size < 9 || size > MaxFrame {
		return 0, 0, nil, fmt.Errorf("peerwire: bad frame length %d", size)
	}
	kind, id = h[4], binary.BigEndian.Uint64(h[5:13])
	payload = make([]byte, size-9)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, 0, nil, err
	}
	return kind, id, payload, nil
}

// writer serializes the frames written on a connection.
type writer struct {
	mu   sync.Mutex
	conn net.Conn
}

func (w *writer) write(b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
	_, err := w.conn.Write(b)
	return err
}

func callPayload(method string, body []byte) ([]byte, error) {
	if len(method) > 0xffff {
		return nil, errors.New("peerwire: method name too long")
	}
	b := make([]byte, 2+len(method)+len(body))
	binary.BigEndian.PutUint16(b, uint16(len(method)))
	copy(b[2:], method)
	copy(b[2+len(method):], body)
	return b, nil
}

func splitCall(p []byte) (string, []byte, error) {
	if len(p) < 2 {
		return "", nil, errors.New("peerwire: short call")
	}
	n := int(binary.BigEndian.Uint16(p))
	if len(p) < 2+n {
		return "", nil, errors.New("peerwire: short call")
	}
	return string(p[2 : 2+n]), p[2+n:], nil
}

// pending is a call waiting for its answer.
type pending struct {
	resp   interface{}
	finish func(error)
	timer  *time.Timer
}

// Link is the dialing end of a connection: it makes calls.
type Link struct {
	// Peer is the other end's hello.
	Peer Hello

	conn net.Conn
	w    writer

	mu      sync.Mutex
	next    uint64
	calls   map[uint64]*pending
	closed  bool
	failure error
	done    chan struct{}
}

// Dial says hello on conn, which the caller connected, and waits up to
// timeout for the answer. A refusal is a *RemoteError with CodeRefused.
func Dial(conn net.Conn, hello Hello, timeout time.Duration) (*Link, error) {
	hello.Magic = Magic
	body, err := Encode(&hello)
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(frame(kindHello, 0, body)); err != nil {
		conn.Close()
		return nil, err
	}
	r := bufio.NewReader(conn)
	kind, _, payload, err := readFrame(r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	var peer Hello
	if kind != kindHello || Decode(payload, &peer) != nil || peer.Magic != Magic {
		conn.Close()
		return nil, errors.New("peerwire: the other end does not speak this protocol")
	}
	if peer.Refused != "" {
		conn.Close()
		return nil, &RemoteError{Code: CodeRefused, Message: peer.Refused}
	}
	conn.SetDeadline(time.Time{})
	l := &Link{Peer: peer, conn: conn, w: writer{conn: conn}, calls: make(map[uint64]*pending), done: make(chan struct{})}
	go l.readLoop(r)
	go l.keepalive()
	return l, nil
}

// Done is closed once the link failed or was closed.
func (l *Link) Done() <-chan struct{} { return l.done }

// Alive reports whether the link can still carry calls.
func (l *Link) Alive() bool {
	select {
	case <-l.done:
		return false
	default:
		return true
	}
}

// Err returns why the link ended, once it has.
func (l *Link) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failure
}

// Close closes the link: calls in flight fail as sent.
func (l *Link) Close() { l.fail(ErrClosed) }

// Call calls method with req and decodes the answer into resp, which may be
// nil. A timeout of 0 waits until the answer or the link's end.
func (l *Link) Call(method string, req, resp interface{}, timeout time.Duration) error {
	ch := make(chan error, 1)
	l.Go(method, req, resp, timeout, func(err error) { ch <- err })
	return <-ch
}

// Go calls method without waiting: done is called once with the outcome.
// done may be called on the caller's goroutine.
func (l *Link) Go(method string, req, resp interface{}, timeout time.Duration, done func(error)) {
	body, err := Encode(req)
	if err == nil {
		body, err = callPayload(method, body)
	}
	if err != nil {
		done(&CallError{Sent: false, Err: err})
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		done(&CallError{Sent: false, Err: ErrClosed})
		return
	}
	l.next++
	id := l.next
	p := &pending{resp: resp, finish: done}
	l.calls[id] = p
	if timeout > 0 {
		p.timer = time.AfterFunc(timeout, func() {
			if l.take(id) != nil {
				done(&CallError{Sent: true, Err: ErrTimeout})
			}
		})
	}
	l.mu.Unlock()
	if err := l.w.write(frame(kindCall, id, body)); err != nil {
		// Nothing of a frame not written whole is processed, and the
		// connection closes so no other frame continues it.
		if l.take(id) != nil {
			done(&CallError{Sent: false, Err: err})
		}
		l.fail(err)
	}
}

// take removes and returns call id, if it is still waiting.
func (l *Link) take(id uint64) *pending {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.calls[id]
	if p != nil {
		delete(l.calls, id)
		if p.timer != nil {
			p.timer.Stop()
		}
	}
	return p
}

// fail ends the link: every call in flight fails as sent.
func (l *Link) fail(err error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.failure = err
	calls := l.calls
	l.calls = nil
	close(l.done)
	l.mu.Unlock()
	l.conn.Close()
	for _, p := range calls {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.finish(&CallError{Sent: true, Err: fmt.Errorf("%w: %v", ErrLost, err)})
	}
}

func (l *Link) readLoop(r *bufio.Reader) {
	for {
		l.conn.SetReadDeadline(time.Now().Add(IdleTimeout))
		kind, id, payload, err := readFrame(r)
		if err != nil {
			l.fail(err)
			return
		}
		switch kind {
		case kindReply:
			if p := l.take(id); p != nil {
				var err error
				if p.resp != nil && len(payload) > 0 {
					err = Decode(payload, p.resp)
				}
				p.finish(err)
			}
		case kindFail:
			if p := l.take(id); p != nil {
				re := &RemoteError{}
				if err := Decode(payload, re); err != nil {
					re = &RemoteError{Code: CodeFailed, Message: "peerwire: unreadable error answer"}
				}
				p.finish(re)
			}
		case kindPong:
		default:
			l.fail(fmt.Errorf("peerwire: unexpected frame kind %d", kind))
			return
		}
	}
}

func (l *Link) keepalive() {
	t := time.NewTicker(KeepaliveEvery)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
			if err := l.w.write(frame(kindPing, 0)); err != nil {
				l.fail(err)
				return
			}
		}
	}
}

// Dispatcher serves one call: it decodes body with Decode, and calls reply
// once, from any goroutine. It runs on the connection's read loop, so it
// must hand slow work to another goroutine; calls it takes in order are
// in the order they arrived.
type Dispatcher func(method string, body []byte, reply func(resp interface{}, err error))

// Accept decides on a dialer's hello: the hello to answer, and the
// dispatcher for the connection's calls, or why it is refused.
type Accept func(hello *Hello) (Hello, Dispatcher, error)

// Serve serves the accepting end of conn until it fails, and closes it.
func Serve(conn net.Conn, accept Accept) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	conn.SetDeadline(time.Now().Add(HelloTimeout))
	kind, _, payload, err := readFrame(r)
	if err != nil || kind != kindHello {
		return
	}
	var hello Hello
	if Decode(payload, &hello) != nil || hello.Magic != Magic {
		return
	}
	answer, dispatch, err := accept(&hello)
	answer.Magic = Magic
	if err != nil {
		answer = Hello{Magic: Magic, Protocol: answer.Protocol, Refused: err.Error()}
	}
	body, encErr := Encode(&answer)
	if encErr != nil {
		return
	}
	w := &writer{conn: conn}
	if w.write(frame(kindHello, 0, body)) != nil || err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	for {
		conn.SetReadDeadline(time.Now().Add(IdleTimeout))
		kind, id, payload, err := readFrame(r)
		if err != nil {
			return
		}
		switch kind {
		case kindPing:
			if w.write(frame(kindPong, 0)) != nil {
				return
			}
		case kindCall:
			method, body, err := splitCall(payload)
			if err != nil {
				return
			}
			dispatch(method, body, replier(w, id))
		default:
			return
		}
	}
}

// replier returns the reply function of call id.
func replier(w *writer, id uint64) func(interface{}, error) {
	var once sync.Once
	return func(resp interface{}, err error) {
		once.Do(func() {
			var b []byte
			if err == nil {
				body, encErr := Encode(resp)
				if encErr == nil {
					b = frame(kindReply, id, body)
				} else {
					err = encErr
				}
			}
			if err != nil {
				re := &RemoteError{}
				if !errors.As(err, &re) {
					re = &RemoteError{Code: CodeFailed, Message: err.Error()}
				}
				body, _ := Encode(re)
				b = frame(kindFail, id, body)
			}
			if w.write(b) != nil {
				w.conn.Close()
			}
		})
	}
}
