package e2e

// Revocation (unitdb/revoke): a contract's primary client revokes a client
// id or a topic key by its uuid, or everything the contract issued before
// now. Every node holds what was revoked, a node that was down gets it when
// it is back, and it survives restarts. An older node, without the
// revocations capability, gets none of it.

import (
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
)

// revokeStatus sends a unitdb/revoke request on c and returns the answer's
// status.
func revokeStatus(t *testing.T, c *client, req types.RevokeRequest) int {
	t.Helper()
	return answerStatus(t, request(t, c, "revoke", req))
}

// revoke sends a unitdb/revoke request on c and fails the test unless it is
// taken.
func revoke(t *testing.T, c *client, req types.RevokeRequest) {
	t.Helper()
	if status := revokeStatus(t, c, req); status != 200 {
		t.Fatalf("revoke %+v: status %d", req, status)
	}
}

// keygenUuid requests a read/write key for topic on c, and returns it and
// the uuid keygen answers with.
func keygenUuid(t *testing.T, c *client, topic string) (string, string) {
	t.Helper()
	answer := request(t, c, "keygen", []types.KeyGenRequest{{Topic: topic, Type: "rw"}})
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(answer, &resp); err != nil || len(resp) != 1 || resp[0].Status != 200 {
		t.Fatalf("keygen: %s (%v)", answer, err)
	}
	return resp[0].Key, resp[0].Uuid
}

// secondaryUuid asks the primary client c for a secondary client id, and
// returns it and the uuid the answer gives.
func secondaryUuid(t *testing.T, c *client) (string, string) {
	t.Helper()
	answer := request(t, c, "clientid", nil)
	var resp types.ClientIdResponse
	if err := json.Unmarshal(answer, &resp); err != nil || resp.Status != 200 {
		t.Fatalf("client id request: %s (%v)", answer, err)
	}
	return resp.ClientId, resp.Uuid
}

// uuidOf returns the uuid of a client id minted with the test key.
func uuidOf(t *testing.T, clientID string) string {
	t.Helper()
	id, _, err := openClientID(clientID)
	if err != nil {
		t.Fatal(err)
	}
	return strconv.FormatUint(id.Uuid(), 10)
}

// connects reports whether clientID connects to s.
func connects(t *testing.T, s *server, clientID string) bool {
	t.Helper()
	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: clientID})
	c.close()
	return err == nil
}

func eventually(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("not within %s: %s", d, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestRevocation checks that a primary client revokes a topic key and a
// client id by the uuids keygen and unitdb/clientid answer with, and every id
// and key of its contract at once, on every node, including one that was
// down when they were revoked; and that a secondary client may not.
func TestRevocation(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	one, two, three := c.node("one"), c.node("two"), c.node("three")
	contract := uint32(0x7e5c0e01)
	admin := primaryClientID(contract)
	a, err := connectTo(t, one.tcpAddr, connectOpts{clientID: admin})
	if err != nil {
		t.Fatal(err)
	}
	user := newClientID(contract)
	topics := map[string]string{}
	for _, n := range c.nodes {
		topics[n.name] = topicOwnedBy(n.name, contract, "groups.revoked", names...)
	}
	key, keyUuid := keygenUuid(t, a, topics["two"])
	if len(key) != security.KeyLenV2 || keyUuid == "" {
		t.Fatalf("keygen gave key %q, uuid %q, want a v2 key and its uuid", key, keyUuid)
	}
	if !keyedDelivers(t, two.server, user, key, topics["two"]) {
		t.Fatal("control: the key does not open its topic")
	}
	victim, victimUuid := secondaryUuid(t, a)
	if len(victim) != uid.EncodedLenV2 || victimUuid != uuidOf(t, victim) {
		t.Fatalf("unitdb/clientid gave %q, uuid %q", victim, victimUuid)
	}

	// Not by a secondary client.
	s, err := connectTo(t, two.tcpAddr, connectOpts{clientID: user})
	if err != nil {
		t.Fatal(err)
	}
	if status := revokeStatus(t, s, types.RevokeRequest{Uuid: keyUuid}); status != types.ErrForbidden.Status {
		t.Fatalf("a secondary client's revoke: status %d", status)
	}
	if status := revokeStatus(t, s, types.RevokeRequest{All: true}); status != types.ErrForbidden.Status {
		t.Fatalf("a secondary client's revoke all: status %d", status)
	}
	if !keyedDelivers(t, two.server, user, key, topics["two"]) {
		t.Fatal("a refused revoke revoked the key")
	}

	// Taken on one node, refused on every node: each key is checked by its
	// topic's owner, here the client's node too.
	revoke(t, a, types.RevokeRequest{Uuid: keyUuid})
	keys := map[string]string{"two": key}
	for _, name := range []string{"one", "three"} {
		k, u := keygenUuid(t, a, topics[name])
		keys[name] = k
		revoke(t, a, types.RevokeRequest{Uuid: u})
	}
	for _, n := range c.nodes {
		eventually(t, 5*time.Second, "the key is refused on "+n.name, func() bool {
			return !keyedDelivers(t, n.server, user, keys[n.name], topics[n.name])
		})
	}

	// A node that was down gets what was revoked while it was.
	three.stop()
	revoke(t, a, types.RevokeRequest{Uuid: victimUuid})
	eventually(t, 5*time.Second, "the client id is refused on another node", func() bool {
		return !connects(t, two.server, victim)
	})
	if !connects(t, two.server, user) {
		t.Fatal("another client id of the contract was refused")
	}
	if err := three.start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the client id is refused on the node that was down", func() bool {
		return !connects(t, three.server, victim)
	})

	// Revoking everything issued before now refuses the contract's ids and
	// keys issued before, v1 ones included; issue times are whole seconds.
	issuedBefore := newClientID(contract)
	keyBefore := topicKeyV2(contract, topics["three"], security.AllowReadWrite)
	v1Key := signedTopicKey(contract, topics["three"], security.AllowReadWrite)
	time.Sleep(1100 * time.Millisecond)
	revoke(t, a, types.RevokeRequest{All: true})
	for _, n := range c.nodes {
		eventually(t, 5*time.Second, "ids issued before are refused on "+n.name, func() bool {
			return !connects(t, n.server, issuedBefore)
		})
		if connects(t, n.server, admin) {
			t.Errorf("the admin's id still connects to %s after revoking all", n.name)
		}
		if connects(t, n.server, newClientIDV1(contract)) {
			t.Errorf("a v1 id still connects to %s after revoking all", n.name)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	after := newClientID(contract)
	for _, k := range []string{keyBefore, v1Key} {
		if keyedDelivers(t, three.server, after, k, topics["three"]) {
			t.Errorf("a key issued before, %q, opens its topic after revoking all", k)
		}
	}
	for _, n := range c.nodes {
		if !connects(t, n.server, after) {
			t.Errorf("an id issued after revoking all was refused on %s", n.name)
		}
	}
	if !keyedDelivers(t, three.server, after, topicKeyV2(contract, topics["three"], security.AllowReadWrite), topics["three"]) {
		t.Error("a key issued after revoking all does not open its topic")
	}
	// Other contracts are not affected.
	if !connects(t, two.server, newClientID(contract+1)) {
		t.Error("a client of another contract was refused")
	}
}

// TestRevocationSurvivesRestart checks that what was revoked is still
// refused after the server is killed right after answering the revoke.
func TestRevocationSurvivesRestart(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x7e5c0e02)
	a, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	victim, victimUuid := secondaryUuid(t, a)
	const topic = "groups.revoked.restart"
	key, keyUuid := keygenUuid(t, a, topic)
	issuedBefore := newClientID(0x7e5c0e03)
	b, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(0x7e5c0e03)})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	revoke(t, a, types.RevokeRequest{Uuid: victimUuid})
	revoke(t, a, types.RevokeRequest{Uuid: keyUuid})
	revoke(t, b, types.RevokeRequest{All: true})
	if err := s.restart(); err != nil { // SIGKILL, then start
		t.Fatalf("restart: %v\nlogs:\n%s", err, s.logs.String())
	}
	user := newClientID(contract)
	if connects(t, s, victim) {
		t.Error("the revoked id connects after a restart")
	}
	if keyedDelivers(t, s, user, key, topic) {
		t.Error("the revoked key opens its topic after a restart")
	}
	if connects(t, s, issuedBefore) {
		t.Error("an id issued before revoking all connects after a restart")
	}
	if !keyedDelivers(t, s, user, topicKeyV2(contract, topic, security.AllowReadWrite), topic) {
		t.Error("another key is refused after a restart")
	}
	// And after a clean one.
	s.shutdown()
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	if connects(t, s, victim) || connects(t, s, issuedBefore) {
		t.Error("a revoked id connects after a clean restart")
	}
}

// TestRevocationConverges checks that revocations taken on every node at
// once, of the same contract, reach every node, whatever order they arrive
// in; and that a cluster restarted as a whole keeps them.
func TestRevocationConverges(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	contract := uint32(0x7e5c0e04)
	victims := make([]string, 0, 3*len(c.nodes))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, n := range c.nodes {
		a, err := connectTo(t, n.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		mine := []string{newClientID(contract), newClientID(contract), newClientID(contract)}
		mu.Lock()
		victims = append(victims, mine...)
		mu.Unlock()
		wg.Add(1)
		go func(a *client, mine []string) {
			defer wg.Done()
			for i, v := range mine {
				// One for ever, the others until later: the later end is
				// kept wherever the same uuid is revoked twice.
				var until int64
				if i > 0 {
					until = time.Now().Unix() + 3600
				}
				revoke(t, a, types.RevokeRequest{Uuid: uuidOf(t, v), Until: until})
			}
		}(a, mine)
	}
	wg.Wait()
	for _, n := range c.nodes {
		for _, v := range victims {
			eventually(t, 5*time.Second, "every revoked id is refused on "+n.name, func() bool {
				return !connects(t, n.server, v)
			})
		}
	}
	for _, n := range c.nodes {
		n.stop()
	}
	for _, n := range c.nodes {
		if err := n.start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range c.nodes {
		for _, v := range victims {
			if connects(t, n.server, v) {
				t.Errorf("a revoked id connects to %s after the cluster restarted", n.name)
			}
		}
		if !connects(t, n.server, newClientID(contract)) {
			t.Errorf("another id is refused on %s after the cluster restarted", n.name)
		}
	}
}

// TestRevocationMixedCluster checks a cluster with a node of an earlier
// version, without the revocations capability: the others hold and enforce
// what is revoked; the older node is sent none of it, and refuses nothing.
// A key is refused where the client's node or the topic's owner has the
// state. Revoking everything is refused while the cluster issues v1 ids and
// keys.
func TestRevocationMixedCluster(t *testing.T) {
	t.Run("without revocations", func(t *testing.T) {
		older := []string{"UNITDB_CLUSTER_CAPS=replicate,deliver,sessions,resync,service,v2keys"}
		c := startClusterWith(t, clusterOpts{env: map[string][]string{"one": older}}, names...)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		waitCapabilities()
		one, two, three := c.node("one"), c.node("two"), c.node("three")
		contract := uint32(0x7e5c0e05)
		a, err := connectTo(t, two.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		victim, victimUuid := secondaryUuid(t, a)
		ownedByOne := topicOwnedBy("one", contract, "groups.mixed.revoked", names...)
		key, keyUuid := keygenUuid(t, a, ownedByOne)
		revoke(t, a, types.RevokeRequest{Uuid: victimUuid})
		revoke(t, a, types.RevokeRequest{Uuid: keyUuid})
		eventually(t, 5*time.Second, "the id is refused on three", func() bool {
			return !connects(t, three.server, victim)
		})
		if connects(t, two.server, victim) {
			t.Error("the revoked id connects to two")
		}
		// The older node knows nothing of it.
		if !connects(t, one.server, victim) {
			t.Error("the older node refused the revoked id: it has no state")
		}
		older1, err := connectTo(t, one.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		if status := revokeStatus(t, older1, types.RevokeRequest{Uuid: keyUuid}); status != types.ErrNotFound.Status {
			t.Errorf("revoke on the older node: status %d, want %d", status, types.ErrNotFound.Status)
		}
		user := newClientID(contract)
		// Client and owner the older node: taken.
		if !keyedDelivers(t, one.server, user, key, ownedByOne) {
			t.Error("the revoked key was refused with client and owner on the older node")
		}
		// The client's node has the state: refused before it is forwarded.
		if deliversRoute(t, route{sub: two, pub: two, username: "s@e2e.test", contract: contract, topic: ownedByOne, secure: true, key: key}) {
			t.Error("the revoked key opened its topic from a node with the state")
		}
		// The topic's owner has the state, the client's node not.
		ownedByThree := topicOwnedBy("three", contract, "groups.mixed.revoked.three", names...)
		key3, key3Uuid := keygenUuid(t, a, ownedByThree)
		revoke(t, a, types.RevokeRequest{Uuid: key3Uuid})
		eventually(t, 5*time.Second, "the key is refused by its owner", func() bool {
			return !deliversRoute(t, route{sub: one, pub: one, username: "s@e2e.test", contract: contract, topic: ownedByThree, secure: true, key: key3})
		})
	})

	t.Run("v1 issued", func(t *testing.T) {
		older := []string{"UNITDB_CLUSTER_CAPS=replicate,deliver,sessions,resync,service"}
		c := startClusterWith(t, clusterOpts{env: map[string][]string{"one": older}}, names...)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		waitCapabilities()
		contract := uint32(0x7e5c0e06)
		a, err := connectTo(t, c.node("two").tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		if status := revokeStatus(t, a, types.RevokeRequest{All: true}); status != types.ErrRevokeAllUnavailable.Status {
			t.Errorf("revoke all while v1 is issued: status %d, want %d", status, types.ErrRevokeAllUnavailable.Status)
		}
		// A v1 key and id have no uuid to revoke them by.
		key, keyUuid := keygenUuid(t, a, "groups.mixed.v1")
		id, idUuid := secondaryUuid(t, a)
		if len(key) != security.SignedKeyLen || keyUuid != "" || len(id) != uid.EncodedLenV1 || idUuid != "" {
			t.Errorf("v1 key %q uuid %q, id %q uuid %q", key, keyUuid, id, idUuid)
		}
		// A v2 id still has one.
		v2 := newClientID(contract)
		revoke(t, a, types.RevokeRequest{Uuid: uuidOf(t, v2)})
		eventually(t, 5*time.Second, "the v2 id is refused on three", func() bool {
			return !connects(t, c.node("three").server, v2)
		})
	})
}
