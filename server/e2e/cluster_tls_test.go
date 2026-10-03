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

package e2e

// Mutual TLS between nodes (cluster_config.tls): a cluster over TLS works as
// a plain one, a caller without a node's certificate is refused, a node's
// call can't name another node as its sender, and a cluster moves to TLS
// node by node, then requires it.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPKI is a cluster CA and the certificates it signs.
type testPKI struct {
	dir  string
	ca   *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "e2e cluster CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	p := &testPKI{dir: t.TempDir(), ca: ca, key: key, pool: x509.NewCertPool()}
	p.pool.AddCert(ca)
	writePEM(t, filepath.Join(p.dir, "ca.crt"), "CERTIFICATE", der)
	return p
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
}

// cert issues a certificate for node name, and returns its files.
func (p *testPKI) cert(t *testing.T, name string) (certFile, keyFile string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	// Files of their own: a running node's are not replaced.
	base := filepath.Join(p.dir, fmt.Sprintf("%s-%x", name, serial))
	certFile, keyFile = base+".crt", base+".key"
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

// tlsConf is a cluster_config.tls for node name.
func (p *testPKI) tlsConf(t *testing.T, name string, require bool) map[string]interface{} {
	certFile, keyFile := p.cert(t, name)
	return map[string]interface{}{"ca_file": filepath.Join(p.dir, "ca.crt"), "cert_file": certFile, "key_file": keyFile, "require": require}
}

// dialPeer connects to a node's TLS address with a certificate of p naming
// name, expecting the node's certificate to name serverName.
func (p *testPKI) dialPeer(t *testing.T, addr, name, serverName string) (*rpc.Client, error) {
	t.Helper()
	certFile, keyFile := p.cert(t, name)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: p.pool, ServerName: serverName})
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	return rpc.NewClient(conn), nil
}

// Requests with the fields of the cluster's that gob matches: the sender's
// name, and nothing else. Each names its sender as Node, but a ping, which
// names it as Leader.
type (
	senderReq struct{ Node string }
	pingReq   struct{ Leader string }
)

// spoofable are the calls that name their sender, with a request naming
// sender.
func spoofable(sender string) map[string]interface{} {
	req := &senderReq{Node: sender}
	return map[string]interface{}{
		"Cluster.Ping":           &pingReq{Leader: sender},
		"Cluster.Vote":           req,
		"Cluster.Master":         req,
		"Cluster.Deliver":        req,
		"Cluster.RebuildTopics":  req,
		"Cluster.RebuildHistory": req,
		"Cluster.FetchSession":   req,
		"Cluster.ForgetSession":  req,
		"Cluster.Replicate":      req,
		"Cluster.Resync":         req,
		"Cluster.Revocations":    req,
	}
}

// deliversEach checks delivery on a topic owned by each node, with the
// subscriber and the publisher on the other two.
func (c *cluster) deliversEach(t *testing.T, contract uint32, prefix, when string) {
	t.Helper()
	for i, own := range names {
		topic := topicOwnedBy(own, contract, fmt.Sprintf("%s.%d", prefix, i), names...)
		sub, pub := c.nodes[(i+1)%3], c.nodes[(i+2)%3]
		if !deliversRoute(t, route{sub: sub, pub: pub, username: "s@e2e.test", contract: contract, topic: topic}) {
			t.Errorf("%s: not delivered: owner %s, subscriber on %s, publisher on %s", when, own, sub.name, pub.name)
		}
	}
}

// TestClusterTLS runs a cluster over mutual TLS: it delivers, fails over and
// replicates as a plain one; and each node refuses a caller without a
// certificate of the cluster's CA naming another node, and a call naming
// another node than its certificate's as its sender.
func TestClusterTLS(t *testing.T) {
	p := newTestPKI(t)
	tlsConf := map[string]map[string]interface{}{}
	for _, n := range names {
		tlsConf[n] = p.tlsConf(t, n, false)
	}
	c := startClusterWith(t, clusterOpts{tls: tlsConf}, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.deliversEach(t, 0x0c7a0001, "groups.tls", "over TLS")

	t.Run("refused", func(t *testing.T) {
		one := c.node("one")
		call := func(cl *rpc.Client, method string, req interface{}) error {
			defer cl.Close()
			var unused bool
			return cl.Call(method, req, &unused)
		}
		// No client certificate.
		if conn, err := tls.Dial("tcp", one.tlsAddr, &tls.Config{RootCAs: p.pool, ServerName: "one"}); err == nil {
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			if err := call(rpc.NewClient(conn), "Cluster.Replicate", &senderReq{Node: "two"}); err == nil {
				t.Error("a caller without a certificate was served")
			}
		}
		// A certificate of another CA, naming a node.
		other := newTestPKI(t)
		if cl, err := other.dialPeer(t, one.tlsAddr, "two", "one"); err == nil {
			if err := call(cl, "Cluster.Replicate", &senderReq{Node: "two"}); err == nil {
				t.Error("a caller with a certificate of another CA was served")
			}
		}
		// A certificate of the CA naming no node.
		if cl, err := p.dialPeer(t, one.tlsAddr, "intruder", "one"); err == nil {
			if err := call(cl, "Cluster.Replicate", &senderReq{Node: "intruder"}); err == nil {
				t.Error("a caller whose certificate names no node was served")
			}
		}
		// Plain TCP to the TLS address.
		if conn, err := net.DialTimeout("tcp", one.tlsAddr, time.Second); err == nil {
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			if err := call(rpc.NewClient(conn), "Cluster.Replicate", &senderReq{Node: "two"}); err == nil {
				t.Error("a plain caller was served on the TLS address")
			}
		}
		// Node two's certificate, for each call that names a sender, naming
		// node three.
		for method, req := range spoofable("three") {
			cl, err := p.dialPeer(t, one.tlsAddr, "two", "one")
			if err != nil {
				t.Fatalf("node two's certificate: %v", err)
			}
			if err := call(cl, method, req); err == nil || !strings.Contains(err.Error(), `names "three"`) {
				t.Errorf("%s from two naming three: %v, want it refused", method, err)
			}
		}
		cl, err := p.dialPeer(t, one.tlsAddr, "two", "one")
		if err != nil {
			t.Fatalf("node two's certificate: %v", err)
		}
		if err := call(cl, "Cluster.Replicate", &senderReq{Node: "two"}); err != nil {
			t.Errorf("a call from two naming itself: %v", err)
		}
		c.assertAlive(t, c.nodes, "after the refused calls")
	})

	t.Run("failover", func(t *testing.T) {
		// The leader goes: the survivors elect another over TLS, its topics
		// move, and its messages are relayed from their replicas.
		dead := c.node(leader)
		var live []*clusterNode
		var liveNames []string
		for _, n := range c.nodes {
			if n != dead {
				live = append(live, n)
				liveNames = append(liveNames, n.name)
			}
		}
		contract := uint32(0x0c7a0002)
		cid := newClientID(contract)
		stored := topicOwnedBy(dead.name, contract, "groups.tls.replicated", names...)
		storeOn(t, live[0], cid, []string{stored}, "replicated over TLS")
		time.Sleep(500 * time.Millisecond) // replication is asynchronous

		dead.stop()
		time.Sleep(4 * time.Second)
		if _, err := c.waitLeader(live, 10*time.Second); err != nil {
			t.Fatalf("survivors: %v", err)
		}
		for _, n := range live {
			if !relayFinds(t, n, cid, stored, "replicated over TLS") {
				t.Errorf("relay on %s after %s (the owner) died: message not found", n.name, dead.name)
			}
		}
		topic := topicOwnedBy(dead.name, contract, "groups.tls.failover", names...)
		if got := topicOwner(contract, topic, liveNames...); got == dead.name {
			t.Fatal("test bug: topic still maps to the dead node")
		}
		if !delivers(t, live[0], live[1], "user@e2e.test", contract, topic) {
			t.Errorf("after %s died: not delivered on its former topic", dead.name)
		}

		if err := dead.start(); err != nil {
			t.Fatalf("restart %s: %v", dead.name, err)
		}
		time.Sleep(3 * time.Second)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatalf("after rejoin: %v", err)
		}
		c.deliversEach(t, 0x0c7a0003, "groups.tls.rejoined", "after "+dead.name+" rejoined")
		c.assertAlive(t, c.nodes, "after failing over over TLS")
	})
}

// TestClusterTLSBadCertificate checks that a node refuses to start with a
// certificate that does not name it, or is not the cluster CA's.
func TestClusterTLSBadCertificate(t *testing.T) {
	p, other := newTestPKI(t), newTestPKI(t)
	for _, tc := range []struct {
		what string
		tls  map[string]interface{}
		want string
	}{
		{"naming another node", p.tlsConf(t, "two", false), "does not name node 'one'"},
		{"of another CA", func() map[string]interface{} {
			conf := other.tlsConf(t, "one", false)
			conf["ca_file"] = filepath.Join(p.dir, "ca.crt")
			return conf
		}(), "invalid cluster_config.tls"},
	} {
		t.Run(tc.what, func(t *testing.T) {
			conf, _ := json.Marshal(map[string]interface{}{
				"self": "",
				"nodes": []nodeConf{
					{Name: "one", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), TLSAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))},
					{Name: "two", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), TLSAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))},
				},
				"tls": tc.tls,
			})
			s := startServerWith(t, serverOpts{cluster: string(conf), args: []string{"-cluster_self", "one"}, expectExit: true})
			select {
			case <-s.exited:
			case <-time.After(10 * time.Second):
				t.Fatal("the node started")
			}
			if logs := s.logs.String(); !strings.Contains(logs, tc.want) {
				t.Errorf("the node exited without saying %q:\n%s", tc.want, logs)
			}
		})
	}
}

// TestClusterMoveToTLS moves a running plain cluster to TLS node by node, as
// docs/rolling-deploys.md describes, and checks it delivers at every step:
// each node starts listening on TLS, listing the tls_addr of the nodes
// already moved; then the nodes moved before the last list them all; then
// each requires TLS, and stops taking plain connections.
func TestClusterMoveToTLS(t *testing.T) {
	p := newTestPKI(t)
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	tlsConf := map[string]map[string]interface{}{}
	moved := map[string]bool{}
	restart := func(n *clusterNode, when string) {
		t.Helper()
		n.stop()
		n.setCluster(c.conf(tlsConf[n.name], moved))
		if err := n.start(); err != nil {
			t.Fatalf("%s: restart %s: %v", when, n.name, err)
		}
		time.Sleep(3 * time.Second)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		c.deliversEach(t, 0x0c7b0000+uint32(len(when)), "groups.move."+strings.ReplaceAll(when, " ", "."), when)
		c.assertAlive(t, c.nodes, when)
	}

	// 1. Each node listens on TLS beside its plain address.
	for _, n := range c.nodes {
		tlsConf[n.name] = p.tlsConf(t, n.name, false)
		moved[n.name] = true
		restart(n, n.name+" on TLS")
	}
	// 2. Each node dials every other over TLS: the last node moved already
	// does.
	for _, n := range c.nodes[:len(c.nodes)-1] {
		restart(n, n.name+" dials TLS")
	}
	// 3. Each node requires TLS.
	for _, n := range c.nodes {
		tlsConf[n.name]["require"] = true
		restart(n, n.name+" requires TLS")
	}
	for _, n := range c.nodes {
		if conn, err := net.DialTimeout("tcp", n.addr, time.Second); err == nil {
			conn.Close()
			t.Errorf("node %s takes plain cluster connections with TLS required", n.name)
		}
	}
}
