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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// testCA is a cluster CA that issues node certificates into a directory.
type testCA struct {
	dir  string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test cluster CA"},
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
	cert, _ := x509.ParseCertificate(der)
	ca := &testCA{dir: t.TempDir(), cert: cert, key: key}
	writeTestPEM(t, ca.caFile(), "CERTIFICATE", der)
	return ca
}

func (ca *testCA) caFile() string { return filepath.Join(ca.dir, "ca.crt") }

func writeTestPEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
}

// issue writes a certificate with the given DNS names, and its key, and
// returns their files.
func (ca *testCA) issue(t *testing.T, file string, dnsNames ...string) (certFile, keyFile string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: file},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile = filepath.Join(ca.dir, file+".crt"), filepath.Join(ca.dir, file+".key")
	writeTestPEM(t, certFile, "CERTIFICATE", der)
	writeTestPEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

// nodeTLS loads the TLS setup of node name, with a certificate of ca naming
// it, trusting trust's CA.
func nodeTLS(t *testing.T, ca, trust *testCA, name string) *clusterTLS {
	t.Helper()
	certFile, keyFile := ca.issue(t, name, name)
	nt, err := loadClusterTLS(&clusterTLSConfig{CAFile: trust.caFile(), CertFile: certFile, KeyFile: keyFile}, name, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return nt
}

func TestLoadClusterTLS(t *testing.T) {
	ca, other := newTestCA(t), newTestCA(t)
	certFile, keyFile := ca.issue(t, "one", "one")
	conf := &clusterTLSConfig{CAFile: ca.caFile(), CertFile: certFile, KeyFile: keyFile}
	if _, err := loadClusterTLS(conf, "one", "127.0.0.1:0"); err != nil {
		t.Fatalf("a certificate naming the node: %v", err)
	}
	if _, err := loadClusterTLS(conf, "one", ""); err == nil {
		t.Error("taken without a tls_addr")
	}
	if _, err := loadClusterTLS(conf, "two", "127.0.0.1:0"); err == nil {
		t.Error("node two took a certificate naming one")
	}
	if _, err := loadClusterTLS(&clusterTLSConfig{CAFile: other.caFile(), CertFile: certFile, KeyFile: keyFile}, "one", "127.0.0.1:0"); err == nil {
		t.Error("took a certificate of another CA")
	}
	if _, err := loadClusterTLS(&clusterTLSConfig{CAFile: ca.caFile(), CertFile: certFile}, "one", "127.0.0.1:0"); err == nil {
		t.Error("took a config without key_file")
	}
}

func TestCertNode(t *testing.T) {
	ca := newTestCA(t)
	c := &Cluster{thisNodeName: "one", allNodes: []string{"one", "two", "three"}}
	parse := func(names ...string) *x509.Certificate {
		certFile, _ := ca.issue(t, "c"+strings.Join(names, "_"), names...)
		b, _ := os.ReadFile(certFile)
		blk, _ := pem.Decode(b)
		cert, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return cert
	}
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{[]string{"two"}, "two"},
		{[]string{"THREE"}, "three"},
		{[]string{"intruder"}, ""},
		{[]string{"one"}, ""},                // this node's own name
		{[]string{"two", "three"}, ""},       // more than one node
		{[]string{"*.two", "two.evil"}, ""},  // no wildcards, no prefixes
		{[]string{"intruder", "two"}, "two"}, // other names are ignored
	} {
		if got := c.certNode(parse(tc.names...)); got != tc.want {
			t.Errorf("certificate for %v names %q, want %q", tc.names, got, tc.want)
		}
	}
}

// startTLSNode serves the cluster calls of node one over TLS, as Start does.
func startTLSNode(t *testing.T, nt *clusterTLS) (*Cluster, string) {
	t.Helper()
	c := &Cluster{thisNodeName: "one", allNodes: []string{"one", "two", "three"}, tls: nt}
	l, err := tls.Listen("tcp", "127.0.0.1:0", nt.serverConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.stopped.Store(true); l.Close() })
	go c.serveTLS(l)
	return c, l.Addr().String()
}

// callAs dials addr with cfg and makes an empty Replicate call, which
// changes nothing, naming sender.
func callAs(t *testing.T, addr string, cfg *tls.Config, sender string) error {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, cfg)
	if err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	cl := rpc.NewClient(conn)
	defer cl.Close()
	var unused bool
	return cl.Call("Cluster.Replicate", &ReplicateReq{Node: sender}, &unused)
}

func TestClusterTLSConnections(t *testing.T) {
	ca, other := newTestCA(t), newTestCA(t)
	_, addr := startTLSNode(t, nodeTLS(t, ca, ca, "one"))

	two := nodeTLS(t, ca, ca, "two")
	if err := callAs(t, addr, two.clientConfig("one"), "two"); err != nil {
		t.Fatalf("node two naming itself: %v", err)
	}
	if err := callAs(t, addr, two.clientConfig("one"), "three"); err == nil || !strings.Contains(err.Error(), "names") {
		t.Errorf("node two naming three: %v, want it refused", err)
	}

	// A certificate of another CA, for a configured node's name.
	certFile, keyFile := other.issue(t, "foreign", "two")
	foreign, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := callAs(t, addr, &tls.Config{Certificates: []tls.Certificate{foreign}, RootCAs: two.roots, ServerName: "one"}, "two"); err == nil {
		t.Error("a certificate of another CA was served")
	}
	// A certificate of the CA naming no configured node.
	intruder := nodeTLS(t, ca, ca, "intruder")
	if err := callAs(t, addr, intruder.clientConfig("one"), "intruder"); err == nil {
		t.Error("a certificate naming no node was served")
	}
	// A certificate naming this node itself.
	self := nodeTLS(t, ca, ca, "one")
	if err := callAs(t, addr, self.clientConfig("one"), "one"); err == nil {
		t.Error("a certificate naming the node itself was served")
	}
	// No certificate.
	if err := callAs(t, addr, &tls.Config{RootCAs: two.roots, ServerName: "one"}, "two"); err == nil {
		t.Error("a caller without a certificate was served")
	}
	// The dialer checks the node it reaches: node one's certificate does not
	// name three.
	if err := callAs(t, addr, two.clientConfig("three"), "two"); err == nil {
		t.Error("node two took node one's certificate for three")
	}
}

// TestPeerRPCSenders checks each call that names its sender is refused over
// a connection of another node, before it is handled.
func TestPeerRPCSenders(t *testing.T) {
	p := &peerRPC{c: nil, peer: "two"}
	var b bool
	calls := map[string]func() error{
		"Ping":           func() error { return p.Ping(&ClusterPing{Leader: "three"}, &ClusterPong{}) },
		"Vote":           func() error { return p.Vote(&ClusterVoteRequest{Node: "three"}, &ClusterVoteResponse{}) },
		"Master":         func() error { return p.Master(&ClusterReq{Node: "three", Conn: &ClusterSess{}}, &b) },
		"Deliver":        func() error { return p.Deliver(&DeliverReq{Node: "three"}, &b) },
		"RebuildTopics":  func() error { return p.RebuildTopics(&RebuildReq{Node: "three"}, &RebuildTopicsResp{}) },
		"RebuildHistory": func() error { return p.RebuildHistory(&RebuildHistoryReq{Node: "three"}, &RebuildHistoryResp{}) },
		"FetchSession":   func() error { return p.FetchSession(&FetchSessionReq{Node: "three"}, &FetchSessionResp{}) },
		"ForgetSession":  func() error { return p.ForgetSession(&ForgetSessionReq{Node: "three"}, &b) },
		"Replicate":      func() error { return p.Replicate(&ReplicateReq{Node: "three"}, &b) },
		"Resync":         func() error { return p.Resync(&ResyncReq{Node: "three"}, &b) },
		"Revocations":    func() error { return p.Revocations(&RevocationsReq{Node: "three"}, &RevocationsResp{}) },
	}
	for name, call := range calls {
		if err := call(); err == nil || !strings.Contains(err.Error(), `names "three"`) {
			t.Errorf("%s from two naming three: %v, want it refused", name, err)
		}
	}
	// Every call of Cluster but Proxy, which names no sender, is checked here.
	for _, m := range rpcMethods(reflect.TypeOf(&Cluster{})) {
		if _, ok := calls[m]; !ok && m != "Proxy" {
			t.Errorf("Cluster.%s is not checked for its sender", m)
		}
	}
}

// TestPeerRPCCoversCluster checks a TLS connection serves every call of
// Cluster, and no other: a call added to Cluster without a peerRPC method
// would not be served over TLS.
func TestPeerRPCCoversCluster(t *testing.T) {
	want := rpcMethods(reflect.TypeOf(&Cluster{}))
	got := rpcMethods(reflect.TypeOf(&peerRPC{}))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("peerRPC serves %v, Cluster %v", got, want)
	}
}

// rpcMethods returns the methods of typ that net/rpc serves: exported, with
// two arguments, the second a pointer, and an error result.
func rpcMethods(typ reflect.Type) []string {
	errType := reflect.TypeOf((*error)(nil)).Elem()
	var names []string
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		mt := m.Type
		if !m.IsExported() || mt.NumIn() != 3 || mt.NumOut() != 1 || mt.In(2).Kind() != reflect.Ptr || mt.Out(0) != errType {
			continue
		}
		names = append(names, m.Name)
	}
	return names
}
