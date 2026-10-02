package internal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

// contractOf returns the contract of a client id the server issued.
func contractOf(t *testing.T, clientID string) uint32 {
	t.Helper()
	id, err := uid.Decode([]byte(clientID), Globals.Service.mac)
	if err != nil {
		t.Fatal(err)
	}
	return id.Contract()
}

// serviceClientID returns a trusted service's client id for contract, as
// `mintid -service` issues.
func serviceClientID(t *testing.T, contract uint32) string {
	t.Helper()
	id, err := uid.MintClientID(contract, true)
	if err != nil {
		t.Fatal(err)
	}
	return id.Encode(Globals.Service.mac)
}

// withoutInsecure runs the test with allow_insecure off.
func withoutInsecure(t *testing.T) {
	t.Helper()
	Globals.Service.allowInsecure.Store(false)
	t.Cleanup(func() { Globals.Service.allowInsecure.Store(true) })
}

// special sends the special request unitdb/name and returns the answer.
func (c *testClient) special(name string, payload interface{}) []byte {
	c.t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		c.t.Fatal(err)
	}
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/" + name, Payload: b}}})
	return payloadOf(c.waitFor(name+" answer", isPublishOn("unitdb/"+name)))
}

// status returns the status of a special request's answer: an object, or
// the first of an array of them.
func status(t *testing.T, answer []byte) int {
	t.Helper()
	var one struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(answer, &one); err == nil {
		return one.Status
	}
	var many []struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(answer, &many); err != nil || len(many) == 0 {
		t.Fatalf("answer %s: %v", answer, err)
	}
	return many[0].Status
}

// vouch sends unitdb/service with serviceID and returns the answer's status.
func (c *testClient) vouch(serviceID string) int {
	c.t.Helper()
	return status(c.t, c.special("service", ServiceRequest{ClientID: serviceID}))
}

// keygenStatus requests a key for topic and returns the answer's status.
func (c *testClient) keygenStatus(topic string) int {
	c.t.Helper()
	return status(c.t, c.special("keygen", []types.KeyGenRequest{{Topic: topic, Type: "rw"}}))
}

// unkeyedDelivery subscribes on sub and publishes on pub on topic without a
// key, and reports whether the subscriber received the message. A refused
// subscription or publish is answered with an error, which is not delivery.
func unkeyedDelivery(t *testing.T, sub, pub *testClient, topic string, id uint16) bool {
	t.Helper()
	sub.send(&utp.Subscribe{MessageID: id, Subscriptions: []*utp.Subscription{{Topic: topic}}})
	sub.waitFor("subscribe acknowledge", isAck(utp.SUBSCRIBE, id))
	pub.send(&utp.Publish{MessageID: id + 1, Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte("unkeyed")}}})
	_, ok := sub.next(isPublishOn(topic), time.Second)
	return ok
}

// TestInsecureFlagRefused checks that a client's insecure flag is refused
// unless the server allows insecure clients, and that a refused connection
// is served nothing.
func TestInsecureFlagRefused(t *testing.T) {
	withoutInsecure(t)
	id := newClientID(t)
	c := dialTCP(t)
	if ack := c.connect(&utp.Connect{ClientID: id, InsecureFlag: true}); ack.ReturnCode != types.ErrUnauthorized.ReturnCode {
		t.Fatalf("insecure connect without allow_insecure: return code %d, want %d", ack.ReturnCode, types.ErrUnauthorized.ReturnCode)
	}
	// A refused connection takes no request but another CONNECT.
	c.send(&utp.Subscribe{MessageID: 1, Subscriptions: []*utp.Subscription{{Topic: "insecure.refused"}}})
	c.waitClosed()

	// The same client id connects without the flag, and needs keys.
	secure := connectedClient(t, id, false)
	secure.send(&utp.Subscribe{MessageID: 2, Subscriptions: []*utp.Subscription{{Topic: "insecure.refused"}}})
	if e := secure.serverError(2); e.Status != types.ErrBadRequest.Status {
		t.Fatalf("subscribe without a key: status %d, want %d", e.Status, types.ErrBadRequest.Status)
	}

	// With allow_insecure, the flag is taken.
	Globals.Service.allowInsecure.Store(true)
	insecure := connectedClient(t, id, true)
	if !unkeyedDelivery(t, insecure, insecure, "insecure.allowed", 3) {
		t.Fatal("an insecure client of a server that allows them needed keys")
	}
}

func TestServiceIDSkipsKeys(t *testing.T) {
	withoutInsecure(t)
	primaryID := newClientID(t)
	service := serviceClientID(t, contractOf(t, primaryID))

	for _, flag := range []bool{false, true} {
		sub := connectedClient(t, service, flag)
		pub := connectedClient(t, service, false)
		if !unkeyedDelivery(t, sub, pub, "service.unkeyed", 1) {
			t.Fatalf("a service's connections (insecure flag %t) needed topic keys", flag)
		}
	}

	// The service shares its contract's topics with keyed clients.
	primary := connectedClient(t, primaryID, false)
	key := primary.keygen("service.shared", "rw")
	primary.subscribe(1, key+"/service.shared", 0)
	pub := connectedClient(t, service, false)
	pub.publish(2, "service.shared", "from the service", 0)
	primary.waitFor("message from the service", isPublishOn("service.shared"))

	// A service is a primary client: it generates keys.
	if s := connectedClient(t, service, false).keygenStatus("service.key"); s != 200 {
		t.Fatalf("keygen by a service: status %d", s)
	}
}

func TestServiceVouches(t *testing.T) {
	withoutInsecure(t)
	primaryID := newClientID(t)
	primary := connectedClient(t, primaryID, false)
	service := serviceClientID(t, contractOf(t, primaryID))
	pub := connectedClient(t, service, false)

	user := connectedClient(t, primary.secondaryClientID(), false)
	if unkeyedDelivery(t, user, pub, "vouch.before", 1) {
		t.Fatal("a connection nothing vouched for subscribed without a key")
	}
	if s := user.keygenStatus("vouch.key"); s != types.ErrKeyGenForbidden.Status {
		t.Fatalf("keygen before the vouch: status %d, want %d", s, types.ErrKeyGenForbidden.Status)
	}
	if s := user.vouch(service); s != 200 {
		t.Fatalf("vouching with the service's id: status %d", s)
	}
	if !unkeyedDelivery(t, user, pub, "vouch.after", 3) {
		t.Fatal("a connection a service vouched for still needed topic keys")
	}
	// A vouched connection generates keys, for the contract.
	if s := user.keygenStatus("vouch.key"); s != 200 {
		t.Fatalf("keygen after the vouch: status %d", s)
	}

	for name, id := range map[string]string{
		"a primary id":                  primaryID,
		"a service of another contract": serviceClientID(t, contractOf(t, primaryID)+1),
		"garbage":                       "not-a-client-id",
		"nothing":                       "",
	} {
		c := connectedClient(t, primary.secondaryClientID(), false)
		if s := c.vouch(id); s != types.ErrForbidden.Status {
			t.Errorf("%s vouched: status %d, want %d", name, s, types.ErrForbidden.Status)
		}
		if s := c.keygenStatus("vouch.refused"); s != types.ErrKeyGenForbidden.Status {
			t.Errorf("keygen after %s refused to vouch: status %d", name, s)
		}
	}

	// The insecure flag alone gives no keys, even where it is allowed:
	// they would outlast insecure mode.
	Globals.Service.allowInsecure.Store(true)
	flagged := connectedClient(t, primary.secondaryClientID(), true)
	if s := flagged.keygenStatus("vouch.flag"); s != types.ErrKeyGenForbidden.Status {
		t.Fatalf("keygen with the insecure flag: status %d, want %d", s, types.ErrKeyGenForbidden.Status)
	}
}

func TestReservedTopics(t *testing.T) {
	primaryID := newClientID(t)
	service := connectedClient(t, serviceClientID(t, contractOf(t, primaryID)), false)
	insecure := connectedClient(t, primaryID, true)
	for _, c := range []*testClient{service, insecure} {
		c.send(&utp.Subscribe{MessageID: 1, Subscriptions: []*utp.Subscription{{Topic: "$sys.users"}}})
		if e := c.serverError(1); e.Status != types.ErrForbidden.Status {
			t.Fatalf("subscribe to a reserved topic: status %d, want %d", e.Status, types.ErrForbidden.Status)
		}
		c.send(&utp.Relay{MessageID: 2, RelayRequests: []*utp.RelayRequest{{Topic: "$sys.users", Last: "1h"}}})
		if e := c.serverError(2); e.Status != types.ErrForbidden.Status {
			t.Fatalf("relay of a reserved topic: status %d", e.Status)
		}
		c.send(&utp.Unsubscribe{MessageID: 3, Subscriptions: []*utp.Subscription{{Topic: "$sys.users"}}})
		if e := c.serverError(3); e.Status != types.ErrForbidden.Status {
			t.Fatalf("unsubscribe of a reserved topic: status %d", e.Status)
		}
		c.send(&utp.Publish{MessageID: 4, Messages: []*utp.PublishMessage{{Topic: "$sys.users?ttl=1m", Payload: []byte("x")}}})
		if e := c.serverError(4); e.Status != types.ErrForbidden.Status {
			t.Fatalf("publish to a reserved topic: status %d", e.Status)
		}
		c.expectNone("publish acknowledge", isAck(utp.PUBLISH, 4), 300*time.Millisecond)
	}
	if s := service.keygenStatus("$sys.users"); s != types.ErrForbidden.Status {
		t.Fatalf("keygen for a reserved topic: status %d, want %d", s, types.ErrForbidden.Status)
	}
}

// TestKeygenErrorStatus checks that a keygen request that fails is answered
// with the failure's status, not 200.
func TestKeygenErrorStatus(t *testing.T) {
	primary := connectedClient(t, newClientID(t), false)
	long := "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y"
	if s := primary.keygenStatus(long); s != types.ErrTargetTooLong.Status {
		t.Errorf("keygen for a topic of too many parts: status %d, want %d", s, types.ErrTargetTooLong.Status)
	}
	primary.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: []byte("{not json")}}})
	if s := status(t, payloadOf(primary.waitFor("keygen answer", isPublishOn("unitdb/keygen")))); s != types.ErrBadRequest.Status {
		t.Errorf("keygen request that does not decode: status %d, want %d", s, types.ErrBadRequest.Status)
	}
	secondary := connectedClient(t, primary.secondaryClientID(), false)
	if s := secondary.keygenStatus("teams.a"); s != types.ErrKeyGenForbidden.Status {
		t.Errorf("keygen by a secondary client: status %d, want %d", s, types.ErrKeyGenForbidden.Status)
	}
	if s := primary.keygenStatus("teams.a"); s != 200 {
		t.Errorf("keygen by the primary client: status %d", s)
	}
}

// TestSessionBoundToOwner checks that a client of the same contract that
// sends another client's session key does not resume its session.
func TestSessionBoundToOwner(t *testing.T) {
	primaryID := newClientID(t)
	primary := connectedClient(t, primaryID, true)
	victimID := primary.secondaryClientID()
	connect := func(id string) *testClient {
		c := dialTCP(t)
		if ack := c.rawConnect(&utp.Connect{ClientID: id, InsecureFlag: true, SessKey: 535353}); ack.ReturnCode != utp.Accepted {
			t.Fatalf("connect: return code %d", ack.ReturnCode)
		}
		return c
	}
	victim := connect(victimID)
	victim.subscribe(1, "owner.inbox", 1)
	primary.publish(2, "owner.inbox", "for the owner", 1)
	id := victim.waitFor("notify", isNotify).(*utp.ControlMessage).MessageID
	// Leave without receiving the message, which the session keeps.
	victim.send(&utp.Disconnect{})
	victim.conn.Close()

	// The primary client's id is of the same contract, but not the owner.
	intruder := connect(primaryID)
	intruder.expectNone("notify of the session's message", isNotify, 500*time.Millisecond)

	// The owner resumes it.
	connect(victimID).waitFor("resumed notify", isControl(utp.PUBLISH, utp.NOTIFY, id))
}

func TestOwnedSession(t *testing.T) {
	const key, owner, other = uint64(1<<63 | 0x51), uint64(0xaaaa), uint64(0xbbbb)
	put := func(row []byte) {
		t.Helper()
		if err := store.Session.Put(key, row); err != nil {
			t.Fatal(err)
		}
	}
	check := func(desc string, unowned bool, wantID uint32, wantOwned, wantForeign bool) {
		t.Helper()
		id, owned, foreign := ownedSession(key, owner, unowned)
		if id != wantID || owned != wantOwned || foreign != wantForeign {
			t.Errorf("%s: session %d, owned %t, foreign %t; want %d, %t, %t", desc, id, owned, foreign, wantID, wantOwned, wantForeign)
		}
	}
	check("no row", true, 0, false, false)
	put(sessionRow(7, owner))
	check("the owner's row", false, 7, true, false)
	put(sessionRow(8, other))
	check("another owner's row", true, 0, false, true)
	put([]byte{9, 0, 0, 0})
	check("an old row, taken", true, 9, true, false)
	check("an old row, not taken", false, 0, false, true)
}
