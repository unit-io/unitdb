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
	"os"
	"strings"
	"time"

	"github.com/unit-io/unitdb/server/internal/peerwire"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// Nodes talk over mutual TLS when cluster_config.tls is set: each node has a
// certificate for its node name, signed by the cluster's CA, and a node
// takes a connection only from a certificate naming a configured node. The
// connection is bound to that name: its hello, and every call on it, must
// name the certificate's node as the sender (checked once for all calls,
// in cluster_transport.go's dispatcher). See docs/security-review.md,
// finding 3.
//
// A node listens on its tls_addr beside its plain addr, and dials a peer's
// tls_addr when it has one, so a cluster can move to TLS node by node. With
// require set, a node neither listens on nor dials the plain address.

// clusterTLSConfig is cluster_config.tls.
type clusterTLSConfig struct {
	CAFile   string `json:"ca_file"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	// Require stops the plain listener and plain dials.
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
func (t *clusterTLS) serverConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		ClientCAs:    t.roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

// clientConfig dials the node name, which its certificate must name.
func (t *clusterTLS) clientConfig(name string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		RootCAs:      t.roots,
		ServerName:   name,
		MinVersion:   tls.VersionTLS12,
	}
}

// dial connects to the node: over TLS to its tls_addr when this node and it
// have one, else to its plain address unless TLS is required.
func (n *ClusterNode) dial() (net.Conn, error) {
	var t *clusterTLS
	if n.owner != nil {
		t = n.owner.tls
	}
	switch {
	case t != nil && n.tlsAddress != "":
		d := &net.Dialer{Timeout: dialTimeout}
		return tls.DialWithDialer(d, "tcp", n.tlsAddress, t.clientConfig(n.name))
	case t != nil && t.require:
		return nil, fmt.Errorf("node %s has no tls_addr, and TLS is required", n.name)
	case n.address == "":
		return nil, fmt.Errorf("node %s has no addr", n.name)
	default:
		return net.DialTimeout("tcp", n.address, dialTimeout)
	}
}

// tlsListener wraps l to take mutual TLS connections.
func tlsListener(l net.Listener, t *clusterTLS) net.Listener {
	return tls.NewListener(l, t.serverConfig())
}

// serveTLS accepts cluster connections over TLS.
func (c *Cluster) serveTLS(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if c.stopped.Load() {
				return
			}
			log.ErrLogger.Error().Err(err).Str("context", "cluster.serveTLS").Msg("accept")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go c.serveTLSConn(conn.(*tls.Conn))
	}
}

// serveTLSConn serves a TLS connection as the node its certificate names:
// the hello, and every call on the connection, must name that node.
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
		log.ErrLogger.Warn().Str("remote", conn.RemoteAddr().String()).Msg("refused a cluster connection whose certificate names no node")
		conn.Close()
		return
	}
	peerwire.Serve(conn, c.acceptPeer(peer))
}

// certNode returns the configured node a verified certificate names, or "".
func (c *Cluster) certNode(cert *x509.Certificate) string {
	for _, name := range c.allNodes {
		if name == c.thisNodeName {
			continue
		}
		if cert.VerifyHostname(name) == nil {
			return name
		}
	}
	return ""
}
