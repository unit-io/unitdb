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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// Nodes talk over mutual TLS when cluster_config.tls is set: each node has a
// certificate for its node name (a DNS name of the certificate), signed by
// the cluster's CA, and a node takes a connection only from a certificate
// naming one configured node. Each call on the connection is tied to that
// name: a call that names its sender must name the certificate's node.
//
// A node listens on its tls_addr beside its plain addr, and dials a peer's
// tls_addr when it has one, so a cluster can move to TLS node by node (see
// docs/rolling-deploys.md). With require set, a node neither listens on nor
// dials the plain address.

// clusterTLSConfig is cluster_config.tls.
type clusterTLSConfig struct {
	// CAFile holds the cluster CA's certificate, in PEM: a peer's
	// certificate must be signed by it.
	CAFile string `json:"ca_file"`
	// CertFile and KeyFile are this node's certificate, for its node name,
	// and its key, in PEM.
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	// Require stops the plain listener and plain dials. Off by default, for
	// a cluster moving to TLS node by node.
	Require bool `json:"require"`
}

// clusterTLS is a node's TLS setup.
type clusterTLS struct {
	cert    tls.Certificate
	roots   *x509.CertPool
	require bool
	// listenOn is this node's tls_addr.
	listenOn string
}

// loadClusterTLS reads the CA and this node's certificate, and checks that
// the certificate is the CA's and names this node, for both ends of a
// connection.
func loadClusterTLS(conf *clusterTLSConfig, self, listenOn string) (*clusterTLS, error) {
	if conf.CAFile == "" || conf.CertFile == "" || conf.KeyFile == "" {
		return nil, errors.New("cluster_config.tls needs ca_file, cert_file and key_file")
	}
	if listenOn == "" {
		return nil, fmt.Errorf("cluster_config.tls is set, but node '%s' has no tls_addr", self)
	}
	cert, err := tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(conf.CAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("no certificate in %s", conf.CAFile)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	inter := x509.NewCertPool()
	for _, der := range cert.Certificate[1:] {
		if c, err := x509.ParseCertificate(der); err == nil {
			inter.AddCert(c)
		}
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, fmt.Errorf("%s: %w", conf.CertFile, err)
		}
	}
	if !certNames(leaf, self) {
		return nil, fmt.Errorf("%s does not name node '%s'", conf.CertFile, self)
	}
	return &clusterTLS{cert: cert, roots: roots, require: conf.Require, listenOn: listenOn}, nil
}

// certNames reports whether the certificate names node: one of its DNS
// names is the node's name. Wildcards are not matched.
func certNames(cert *x509.Certificate, node string) bool {
	for _, name := range cert.DNSNames {
		if strings.EqualFold(name, node) {
			return true
		}
	}
	return false
}

// serverConfig takes only client certificates signed by the cluster's CA.
// Which node the certificate names is checked once the handshake is done.
func (t *clusterTLS) serverConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		ClientCAs:    t.roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

// clientConfig dials the node name, which the node's certificate, signed by
// the cluster's CA, must name.
func (t *clusterTLS) clientConfig(name string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		RootCAs:      t.roots,
		ServerName:   name,
		MinVersion:   tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 || !certNames(cs.PeerCertificates[0], name) {
				return fmt.Errorf("cluster: the certificate of %s does not name it", name)
			}
			return nil
		},
	}
}

// dial connects to the node: over TLS to its tls_addr when this node has
// cluster_config.tls and the node a tls_addr, else to its plain address
// unless TLS is required.
func (n *ClusterNode) dial() (*rpc.Client, *watchedConn, error) {
	c := Globals.Cluster
	var conn net.Conn
	var err error
	switch {
	case c != nil && c.tls != nil && n.tlsAddress != "":
		d := &net.Dialer{Timeout: time.Second}
		conn, err = tls.DialWithDialer(d, "tcp", n.tlsAddress, c.tls.clientConfig(n.name))
	case c != nil && c.tls != nil && c.tls.require:
		return nil, nil, fmt.Errorf("node %s has no tls_addr, and TLS is required", n.name)
	default:
		conn, err = net.DialTimeout("tcp", n.address, time.Second)
	}
	if err != nil {
		return nil, nil, err
	}
	wc := &watchedConn{Conn: conn}
	return rpc.NewClient(wc), wc, nil
}

// serveTLS accepts cluster connections over TLS.
func (c *Cluster) serveTLS(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if c.stopped.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			log.ErrLogger.Error().Err(err).Str("context", "cluster.serveTLS").Msg("accept")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go c.serveTLSConn(conn.(*tls.Conn))
	}
}

// serveTLSConn serves one connection, once its certificate names a node.
func (c *Cluster) serveTLSConn(conn *tls.Conn) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := conn.Handshake(); err != nil {
		log.ErrLogger.Warn().Err(err).Str("remote", conn.RemoteAddr().String()).Msg("cluster TLS handshake failed")
		conn.Close()
		return
	}
	conn.SetDeadline(time.Time{})
	peer := c.certNode(conn.ConnectionState().PeerCertificates[0])
	if peer == "" {
		log.ErrLogger.Warn().Str("remote", conn.RemoteAddr().String()).Msg("refused a cluster connection whose certificate names no other node, or more than one")
		conn.Close()
		return
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("Cluster", &peerRPC{c: c, peer: peer}); err != nil {
		log.ErrLogger.Error().Err(err).Msg("cluster TLS: register")
		conn.Close()
		return
	}
	srv.ServeConn(conn)
}

// certNode returns the configured node, other than this one, that a verified
// certificate names; or "" if it names none, or more than one.
func (c *Cluster) certNode(cert *x509.Certificate) string {
	found := ""
	for _, name := range c.allNodes {
		if name == c.thisNodeName || !certNames(cert, name) {
			continue
		}
		if found != "" {
			return ""
		}
		found = name
	}
	return found
}

// peerRPC serves the Cluster calls of one TLS connection, whose certificate
// names peer. Every call of Cluster has a method here, and every call that
// names its sender is checked to name peer: TestPeerRPCCoversCluster fails
// for a call of Cluster added without one.
type peerRPC struct {
	c    *Cluster
	peer string
}

// is returns an error unless sender, the node a call says sent it, is the
// connection's node.
func (p *peerRPC) is(sender string) error {
	if sender != p.peer {
		return fmt.Errorf("cluster: a call from %s names %q as its sender", p.peer, sender)
	}
	return nil
}

func (p *peerRPC) Ping(ping *ClusterPing, pong *ClusterPong) error {
	if err := p.is(ping.Leader); err != nil {
		return err
	}
	return p.c.Ping(ping, pong)
}

func (p *peerRPC) Vote(vreq *ClusterVoteRequest, response *ClusterVoteResponse) error {
	if err := p.is(vreq.Node); err != nil {
		return err
	}
	return p.c.Vote(vreq, response)
}

func (p *peerRPC) Master(req *ClusterReq, rejected *bool) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.Master(req, rejected)
}

// Proxy names no sender: it is a topic owner's answer to a connection of
// this node, which any node may send.
func (p *peerRPC) Proxy(resp *ClusterResp, unused *bool) error {
	return p.c.Proxy(resp, unused)
}

func (p *peerRPC) Deliver(req *DeliverReq, unused *bool) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.Deliver(req, unused)
}

func (p *peerRPC) RebuildTopics(req *RebuildReq, resp *RebuildTopicsResp) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.RebuildTopics(req, resp)
}

func (p *peerRPC) RebuildHistory(req *RebuildHistoryReq, resp *RebuildHistoryResp) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.RebuildHistory(req, resp)
}

func (p *peerRPC) FetchSession(req *FetchSessionReq, resp *FetchSessionResp) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.FetchSession(req, resp)
}

func (p *peerRPC) ForgetSession(req *ForgetSessionReq, unused *bool) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.ForgetSession(req, unused)
}

func (p *peerRPC) Replicate(req *ReplicateReq, unused *bool) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.Replicate(req, unused)
}

func (p *peerRPC) Resync(req *ResyncReq, unused *bool) error {
	if err := p.is(req.Node); err != nil {
		return err
	}
	return p.c.Resync(req, unused)
}
