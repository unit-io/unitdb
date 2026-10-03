package e2e

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
)

const (
	atRestOn  = `"encrypt_at_rest": true,`
	atRestOff = `"encrypt_at_rest": false,`
)

// dirContains reports whether a file under dir holds needle.
func dirContains(t *testing.T, dir string, needle []byte) bool {
	t.Helper()
	found := false
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || found {
			return nil
		}
		if b, err := os.ReadFile(path); err == nil && bytes.Contains(b, needle) {
			found = true
		}
		return nil
	})
	return found
}

// setAtRest rewrites the config of the stopped server s with encrypt_at_rest
// set to on, for its next start.
func setAtRest(t *testing.T, s *server, on bool) {
	t.Helper()
	path := filepath.Join(filepath.Dir(s.cmd.Args[0]), s.cmd.Args[2])
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	from, to := atRestOn, atRestOff
	if on {
		from, to = atRestOff, atRestOn
	}
	if !bytes.Contains(b, []byte(from)) {
		t.Fatalf("the config of %s does not set %s", s.tcpAddr, from)
	}
	if err := os.WriteFile(path, bytes.Replace(b, []byte(from), []byte(to), 1), 0644); err != nil {
		t.Fatal(err)
	}
}

// standalone names a standalone server, for the cluster helpers.
func standalone(s *server) *clusterNode { return &clusterNode{name: "standalone", server: s} }

// atRestMarker returns a payload no other test stores.
func atRestMarker(what string) string {
	return fmt.Sprintf("at-rest-%s-%d-payload", what, time.Now().UnixNano())
}

// TestEncryptAtRest checks that with encrypt_at_rest a stored payload is not
// in the data directory, and is still relayed after a restart; and, as a
// control, that with it off, as by default, the payload is there.
func TestEncryptAtRest(t *testing.T) {
	stored := func(t *testing.T, extra string) (found, relayed bool) {
		s := startServerWith(t, serverOpts{extra: extra})
		cid := newClientID(0x2e1ea5e6)
		topic := "groups.at.rest"
		marker := atRestMarker("standalone")
		storeOn(t, standalone(s), cid, []string{topic}, marker)
		s.shutdown()
		found = dirContains(t, s.dbPath, []byte(marker))
		if err := s.start(); err != nil {
			t.Fatal(err)
		}
		return found, relayFinds(t, standalone(s), cid, topic, marker)
	}
	for name, extra := range map[string]string{"off": atRestOff, "by default": ""} {
		t.Run(name, func(t *testing.T) {
			if found, relayed := stored(t, extra); !found || !relayed {
				t.Fatalf("control: payload in the data directory %t, relayed %t; want both", found, relayed)
			}
		})
	}
	t.Run("on", func(t *testing.T) {
		found, relayed := stored(t, atRestOn)
		if found {
			t.Error("the payload is in the data directory in the clear")
		}
		if !relayed {
			t.Error("the sealed payload was not relayed after a restart")
		}
	})
}

// TestEncryptAtRestTurnedOn turns encrypt_at_rest on for a server that
// stored messages without it, and off again: every message is relayed
// throughout, and only those stored while it was on are sealed.
func TestEncryptAtRestTurnedOn(t *testing.T) {
	s := startServerWith(t, serverOpts{extra: atRestOff})
	n := standalone(s)
	cid := newClientID(0x2e1ea5ea)
	plain, sealed, after := atRestMarker("plain"), atRestMarker("sealed"), atRestMarker("after")
	topics := map[string]string{plain: "groups.at.rest.plain", sealed: "groups.at.rest.sealed", after: "groups.at.rest.after"}
	storeOn(t, n, cid, []string{topics[plain]}, plain)

	restart := func(on bool) {
		t.Helper()
		s.shutdown()
		setAtRest(t, s, on)
		if err := s.start(); err != nil {
			t.Fatal(err)
		}
	}
	relaysAll := func(when string, markers ...string) {
		t.Helper()
		for _, m := range markers {
			if !relayFinds(t, n, cid, topics[m], m) {
				t.Errorf("%s: %s was not relayed", when, topics[m])
			}
		}
	}
	restart(true)
	relaysAll("turned on", plain)
	storeOn(t, n, cid, []string{topics[sealed]}, sealed)
	relaysAll("turned on", plain, sealed)
	s.shutdown()
	if !dirContains(t, s.dbPath, []byte(plain)) {
		t.Error("the message stored with encrypt_at_rest off is not in the data directory")
	}
	if dirContains(t, s.dbPath, []byte(sealed)) {
		t.Error("the message stored with encrypt_at_rest on is in the data directory in the clear")
	}
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	relaysAll("on, after a restart", plain, sealed)

	restart(false)
	storeOn(t, n, cid, []string{topics[after]}, after)
	relaysAll("turned off", plain, sealed, after)
}

// TestEncryptAtRestRotation rotates the keyring of a server that seals its
// records: messages sealed with the old key are relayed while it is kept to
// read with, and once it is removed are refused, and logged, rather than
// relayed as garbage.
func TestEncryptAtRestRotation(t *testing.T) {
	keyA := []byte("at-rest-rotation-key-a-012345678")
	keyB := []byte("at-rest-rotation-key-b-012345678")
	entry := func(id int, key []byte, use string) string {
		return fmt.Sprintf(`{"id": %d, "key": %q, "use": %q}`, id, base64.StdEncoding.EncodeToString(key), use)
	}
	ring := func(entries ...string) []string {
		return []string{config.KeyringEnv + "=[" + strings.Join(entries, ",") + "]"}
	}
	// The test servers' key, which the test's client id and topic keys
	// are sealed and signed with, stays in the keyring to read them with.
	test := entry(0, []byte(testKey), "read")
	s := startServerWith(t, serverOpts{extra: atRestOn, env: ring(entry(1, keyA, "issue"), test)})
	n := standalone(s)
	cid := newClientID(0x2e1ea5eb)
	old, current := atRestMarker("key-a"), atRestMarker("key-b")
	storeOn(t, n, cid, []string{"groups.at.rest.a"}, old)

	s.env = ring(entry(2, keyB, "issue"), entry(1, keyA, "read"), test)
	s.shutdown()
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	storeOn(t, n, cid, []string{"groups.at.rest.b"}, current)
	if !relayFinds(t, n, cid, "groups.at.rest.a", old) {
		t.Fatal("during the rotation, a message sealed with the old key was not relayed")
	}
	if !relayFinds(t, n, cid, "groups.at.rest.b", current) {
		t.Fatal("during the rotation, a message sealed with the new key was not relayed")
	}

	s.env = ring(entry(2, keyB, "issue"), test)
	s.shutdown()
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	if relayFinds(t, n, cid, "groups.at.rest.a", old) {
		t.Error("a message sealed with a removed key was relayed")
	}
	if !relayFinds(t, n, cid, "groups.at.rest.b", current) {
		t.Error("a message sealed with the issue key was not relayed")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(s.logs.String(), "not in the keyring") {
		if time.Now().After(deadline) {
			t.Fatalf("no log of a record sealed with a removed key:\n%s", s.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestClusterEncryptAtRest stores messages on topics owned by each node of a
// replicated cluster, all of whose nodes seal their records, or only some
// ("mixed": node one does not, as a node of an earlier version doesn't):
// sealing nodes hold no payload in the clear, replicas relay the messages
// of a dead owner, and hints and history reach it once it restarts. Nodes
// send each other records opened, so a mixed cluster works without a
// capability.
func TestClusterEncryptAtRest(t *testing.T) {
	for name, extra := range map[string]map[string]string{
		"on":    {"one": atRestOn, "two": atRestOn, "three": atRestOn},
		"mixed": {"one": atRestOff, "two": atRestOn, "three": atRestOn},
	} {
		t.Run(name, func(t *testing.T) {
			c := startClusterWith(t, clusterOpts{extra: extra}, names...)
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			waitCapabilities()
			contract := uint32(0x2e1ea5ec)
			cid := newClientID(contract)
			markers := make(map[string]string) // topic: marker
			for i, own := range names {
				topic := topicOwnedBy(own, contract, "groups.at.rest."+name, names...)
				markers[topic] = atRestMarker(name + "-" + own)
				storeOn(t, c.nodes[(i+1)%3], cid, []string{topic}, markers[topic])
			}
			time.Sleep(500 * time.Millisecond) // replication is asynchronous
			relaysAll := func(nodes []*clusterNode, when string) {
				t.Helper()
				for _, n := range nodes {
					for topic, m := range markers {
						if !relayFinds(t, n, cid, topic, m) {
							t.Errorf("%s: relay of %s on %s: not found", when, topic, n.name)
						}
					}
				}
			}
			relaysAll(c.nodes, "stored")

			// Two dies: a replica relays its topic, and stores what is
			// published meanwhile, handing it to two once it restarts.
			dead := c.node("two")
			live := []*clusterNode{c.node("one"), c.node("three")}
			dead.stop()
			time.Sleep(4 * time.Second)
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			relaysAll(live, "two dead")
			topic := topicOwnedBy("two", contract, "groups.at.rest.outage."+name, names...)
			markers[topic] = atRestMarker(name + "-outage")
			storeOn(t, live[0], cid, []string{topic}, markers[topic])
			if err := dead.start(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			relaysAll(c.nodes, "two restarted")

			for _, n := range c.nodes {
				n.shutdown()
			}
			for _, n := range c.nodes {
				for _, m := range markers {
					if found := dirContains(t, n.dbPath, []byte(m)); found && extra[n.name] == atRestOn {
						t.Errorf("%s seals its records, but holds %s in the clear", n.name, m)
					}
				}
			}
		})
	}
}

// TestClusterEncryptAtRestSessionFailover runs TestClusterSessionFailover
// with encrypt_at_rest on every node, and on some: a session's log is sent
// opened to its replicas, which seal it, or not, as they are set.
func TestClusterEncryptAtRestSessionFailover(t *testing.T) {
	for name, extra := range map[string]map[string]string{
		"on":    {"one": atRestOn, "two": atRestOn, "three": atRestOn},
		"mixed": {"one": atRestOn, "two": atRestOff, "three": atRestOn},
	} {
		t.Run(name, func(t *testing.T) {
			sessionFailover(t, clusterOpts{extra: extra}, "follower")
		})
	}
}
