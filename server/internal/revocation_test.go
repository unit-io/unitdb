package internal

import (
	"encoding/json"
	"math/rand"
	"net/rpc"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

// TestContractStatesConverge checks that merging the same changes in any
// order, and more than once, gives the same state.
func TestContractStatesConverge(t *testing.T) {
	now := time.Now().Unix()
	changes := []map[uint32]*ContractState{
		{1: {NotBefore: now - 100}},
		{1: {NotBefore: now - 50, Revoked: map[uint64]int64{7: now + 100}}},
		{1: {Revoked: map[uint64]int64{7: 0, 8: now + 10}}},
		{1: {Revoked: map[uint64]int64{8: now + 20, 9: now - 1}}},
		{2: {Revoked: map[uint64]int64{7: now + 5}}},
		{2: {NotBefore: now - 10}, 3: {}},
	}
	want := map[uint32]*ContractState{
		1: {NotBefore: now - 50, Revoked: map[uint64]int64{7: 0, 8: now + 20}},
		2: {NotBefore: now - 10, Revoked: map[uint64]int64{7: now + 5}},
	}
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		r := &revocations{contracts: make(map[uint32]*ContractState)}
		for _, j := range rnd.Perm(len(changes)) {
			r.mergeLocked(changes[j], now)
			if rnd.Intn(2) == 0 {
				r.mergeLocked(changes[j], now) // again
			}
		}
		if got := r.all(); !reflect.DeepEqual(got, want) {
			t.Fatalf("order %d: state %v, want %v", i, dump(got), dump(want))
		}
		// Merging what is already there changes nothing.
		for _, c := range changes {
			if changed := r.mergeLocked(c, now); len(changed) != 0 {
				t.Fatalf("merging a change again changed %v", dump(changed))
			}
		}
	}
}

func dump(m map[uint32]*ContractState) string {
	b, _ := json.Marshal(m)
	return string(b)
}

// TestRevocationsRefuse checks what a contract's state refuses.
func TestRevocationsRefuse(t *testing.T) {
	now := time.Now().Unix()
	r := &revocations{contracts: map[uint32]*ContractState{
		1: {NotBefore: now - 100, Revoked: map[uint64]int64{7: 0, 8: now + 100, 9: now - 1}},
	}}
	for name, tc := range map[string]struct {
		contract uint32
		uuid     uint64
		issuedAt uint32
		refused  bool
	}{
		"revoked for ever":             {1, 7, uint32(now), true},
		"revoked until later":          {1, 8, uint32(now), true},
		"revoked until before now":     {1, 9, uint32(now), false},
		"another uuid":                 {1, 10, uint32(now), false},
		"issued before not-before":     {1, 10, uint32(now - 101), true},
		"issued at not-before":         {1, 10, uint32(now - 100), false},
		"v1, with no issue time":       {1, 0, 0, true},
		"another contract":             {2, 7, 0, false},
		"a uuid of another contract's": {2, 8, uint32(now), false},
		"no uuid, issued after":        {1, 0, uint32(now), false},
	} {
		if got := r.refuses(tc.contract, tc.uuid, tc.issuedAt) != ""; got != tc.refused {
			t.Errorf("%s: refused %t, want %t", name, got, tc.refused)
		}
	}
	var none *revocations
	if none.refuses(1, 7, 0) != "" {
		t.Error("no state refuses")
	}
}

// revoke sends unitdb/revoke and returns the answer's status.
func (c *testClient) revoke(req types.RevokeRequest) int {
	c.t.Helper()
	return status(c.t, c.special("revoke", req))
}

// keygenUuid requests a read/write key for topic, and returns it and its
// uuid.
func (c *testClient) keygenUuid(topic string) (string, string) {
	c.t.Helper()
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(c.special("keygen", []types.KeyGenRequest{{Topic: topic, Type: "rw"}}), &resp); err != nil || len(resp) != 1 || resp[0].Status != 200 {
		c.t.Fatalf("keygen: %+v %v", resp, err)
	}
	return resp[0].Key, resp[0].Uuid
}

// secondaryUuid requests a secondary client id, and returns it and its uuid.
func (c *testClient) secondaryUuid() (string, string) {
	c.t.Helper()
	var resp types.ClientIdResponse
	if err := json.Unmarshal(c.special("clientid", nil), &resp); err != nil || resp.Status != 200 {
		c.t.Fatalf("client id request: %+v %v", resp, err)
	}
	return resp.ClientId, resp.Uuid
}

// keyRefused publishes on topic with key and reports whether it is refused.
func (c *testClient) keyRefused(id uint16, key, topic string) bool {
	c.t.Helper()
	c.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{{Topic: key + "/" + topic, Payload: []byte("x")}}})
	m, ok := c.next(func(m lp.MessagePack) bool {
		return isPublishOn("unitdb/error/")(m) || isAck(utp.PUBLISH, id)(m)
	}, waitTimeout)
	if !ok {
		c.t.Fatalf("no answer to publish %d", id)
	}
	return isPublishOn("unitdb/error/")(m)
}

// connectCode connects with clientID and returns the CONNACK's return code.
func connectCode(t *testing.T, clientID string) uint8 {
	t.Helper()
	return dialTCP(t).connect(&utp.Connect{ClientID: clientID}).ReturnCode
}

// TestRevoke checks that a contract's primary client revokes a client id and
// a topic key by the uuid keygen and unitdb/clientid answer with, and
// everything issued before now, v1 ids and keys included; and that nothing
// else may.
func TestRevoke(t *testing.T) {
	primaryID := newClientID(t)
	primary := connectedClient(t, primaryID, false)
	contract := contractOf(t, primaryID)
	const topic = "revoke.t"

	key, keyUuid := primary.keygenUuid(topic)
	if k, err := Globals.Service.keys.DecodeTopicKeyV2(contract, key, topic); err != nil || strconv.FormatUint(k.Uuid, 10) != keyUuid {
		t.Fatalf("keygen answered uuid %q for key %+v (%v)", keyUuid, k, err)
	}
	other, _ := primary.keygenUuid(topic)
	victim, victimUuid := primary.secondaryUuid()
	if id := openID(t, victim); strconv.FormatUint(id.Uuid(), 10) != victimUuid {
		t.Fatalf("unitdb/clientid answered uuid %q for id %x", victimUuid, []byte(id))
	}
	bystander, _ := primary.secondaryUuid()
	secondary := connectedClient(t, bystander, false)

	// Only a primary client revokes.
	if s := secondary.revoke(types.RevokeRequest{Uuid: keyUuid}); s != types.ErrForbidden.Status {
		t.Fatalf("a secondary client's revoke: status %d", s)
	}
	if secondary.keyRefused(1, key, topic) {
		t.Fatal("a refused revoke revoked the key")
	}
	// Nor does a connection a service vouched for.
	vouched := connectedClient(t, bystander, false)
	if s := vouched.vouch(serviceClientID(t, contract)); s != 200 {
		t.Fatalf("vouch: status %d", s)
	}
	if s := vouched.revoke(types.RevokeRequest{Uuid: keyUuid}); s != types.ErrForbidden.Status {
		t.Fatalf("a vouched for connection's revoke: status %d", s)
	}
	for name, req := range map[string]types.RevokeRequest{
		"nothing":            {},
		"a uuid of 0":        {Uuid: "0"},
		"a uuid not decimal": {Uuid: "x1"},
		"an end gone by":     {Uuid: keyUuid, Until: 1},
		"an end and no uuid": {Until: time.Now().Unix() + 60},
	} {
		if s := primary.revoke(req); s != types.ErrBadRequest.Status {
			t.Errorf("revoke %s: status %d", name, s)
		}
	}

	// By uuid.
	if s := primary.revoke(types.RevokeRequest{Uuid: keyUuid}); s != 200 {
		t.Fatalf("revoke the key: status %d", s)
	}
	if !secondary.keyRefused(2, key, topic) {
		t.Error("the revoked key is taken")
	}
	if secondary.keyRefused(3, other, topic) {
		t.Error("another key is refused")
	}
	if s := primary.revoke(types.RevokeRequest{Uuid: victimUuid}); s != 200 {
		t.Fatalf("revoke the id: status %d", s)
	}
	refused := dialTCP(t)
	if ack := refused.connect(&utp.Connect{ClientID: victim}); ack.ReturnCode != types.ErrInvalidClientID.ReturnCode {
		t.Errorf("the revoked id: return code %d", ack.ReturnCode)
	}
	refused.expectNone("a new id for a revoked one", isPublishOn("unitdb/clientid/"), 300*time.Millisecond)
	if code := connectCode(t, bystander); code != utp.Accepted {
		t.Errorf("another id of the contract: return code %d", code)
	}
	// Revoked until a time: taken again after.
	short, shortUuid := primary.keygenUuid(topic)
	if s := primary.revoke(types.RevokeRequest{Uuid: shortUuid, Until: time.Now().Unix() + 2}); s != 200 {
		t.Fatalf("revoke for a while: status %d", s)
	}
	if !secondary.keyRefused(4, short, topic) {
		t.Error("the key revoked for a while is taken")
	}
	time.Sleep(2100 * time.Millisecond)
	if secondary.keyRefused(5, short, topic) {
		t.Error("the key is refused once its revocation is over")
	}

	// Everything issued before now, v1 included. Issue times are whole
	// seconds.
	v1Key, _ := Globals.Service.keys.TopicKeyV1(contract, topic, security.AllowReadWrite)
	v1ID := Globals.Service.keys.EncodeClientIDV1(openID(t, bystander))
	if secondary.keyRefused(6, v1Key, topic) || connectCode(t, v1ID) != utp.Accepted {
		t.Fatal("control: a v1 key or id is refused")
	}
	time.Sleep(1100 * time.Millisecond)
	if s := secondary.revoke(types.RevokeRequest{All: true}); s != types.ErrForbidden.Status {
		t.Fatalf("a secondary client's revoke all: status %d", s)
	}
	if s := primary.revoke(types.RevokeRequest{All: true}); s != 200 {
		t.Fatalf("revoke all: status %d", s)
	}
	if !secondary.keyRefused(7, other, topic) {
		t.Error("a key issued before is taken")
	}
	if !secondary.keyRefused(8, v1Key, topic) {
		t.Error("a v1 key is taken")
	}
	for name, id := range map[string]string{"an id issued before": bystander, "the primary id": primaryID, "a v1 id": v1ID} {
		if code := connectCode(t, id); code != types.ErrInvalidClientID.ReturnCode {
			t.Errorf("%s: return code %d", name, code)
		}
	}
	// The connections already open stay; what is issued from now is taken.
	time.Sleep(1100 * time.Millisecond)
	fresh, _ := primary.keygenUuid(topic)
	if secondary.keyRefused(9, fresh, topic) {
		t.Error("a key issued after is refused")
	}
	if id, _ := primary.secondaryUuid(); connectCode(t, id) != utp.Accepted {
		t.Error("an id issued after is refused")
	}
	// A trusted service's id, which is primary, revokes too.
	target, targetUuid := primary.secondaryUuid()
	if s := connectedClient(t, serviceClientID(t, contract), false).revoke(types.RevokeRequest{Uuid: targetUuid}); s != 200 {
		t.Errorf("a service's revoke: status %d", s)
	}
	if code := connectCode(t, target); code != types.ErrInvalidClientID.ReturnCode {
		t.Errorf("the id a service revoked: return code %d", code)
	}
	// Other contracts are not affected.
	if code := connectCode(t, newClientID(t)); code != utp.Accepted {
		t.Errorf("another contract's id: return code %d", code)
	}
}

// TestRevokedServiceRefused checks that a revoked service id neither
// connects nor vouches.
func TestRevokedServiceRefused(t *testing.T) {
	primaryID := newClientID(t)
	primary := connectedClient(t, primaryID, false)
	contract := contractOf(t, primaryID)
	service := serviceClientID(t, contract)
	user := connectedClient(t, primary.secondaryClientID(), false)
	if s := primary.revoke(types.RevokeRequest{Uuid: strconv.FormatUint(openID(t, service).Uuid(), 10)}); s != 200 {
		t.Fatalf("revoke: status %d", s)
	}
	if code := connectCode(t, service); code != types.ErrInvalidClientID.ReturnCode {
		t.Errorf("the revoked service id: return code %d", code)
	}
	if s := user.vouch(service); s != types.ErrForbidden.Status {
		t.Errorf("vouch with the revoked service id: status %d", s)
	}
}

// TestRevocationsPersist checks that the security state is read back from
// the store, and that several records, as a crash leaves, are merged into
// one.
func TestRevocationsPersist(t *testing.T) {
	live := securityState.Load()
	t.Cleanup(func() { securityState.Store(live) })
	now := time.Now().Unix()
	const contract = 0x7e5c0001
	if _, err := live.apply(map[uint32]*ContractState{contract: {Revoked: map[uint64]int64{42: 0}}}, ""); err != nil {
		t.Fatal(err)
	}
	// A record of another state, as if the crash came before the first
	// was deleted.
	id, _ := store.Security.NewID()
	b, _ := json.Marshal(securityRecord{ID: id, Contracts: map[uint32]*ContractState{contract: {NotBefore: now}, contract + 1: {Revoked: map[uint64]int64{43: now + 3600}}}})
	if err := store.Security.Put(id, b); err != nil {
		t.Fatal(err)
	}
	if raw, _ := store.Security.All(); len(raw) != 2 {
		t.Fatalf("%d records stored, want 2", len(raw))
	}
	r, err := loadRevocations()
	if err != nil {
		t.Fatal(err)
	}
	if r.refuses(contract, 42, uint32(now)) == "" || r.refuses(contract, 1, uint32(now-1)) == "" || r.refuses(contract+1, 43, uint32(now)) == "" {
		t.Fatalf("read back %v", dump(r.all()))
	}
	if raw, _ := store.Security.All(); len(raw) != 1 {
		t.Fatalf("%d records stored after reading them, want them merged into 1", len(raw))
	}
	if again, err := loadRevocations(); err != nil || !reflect.DeepEqual(again.all(), r.all()) {
		t.Fatalf("read back %v, want %v", dump(again.all()), dump(r.all()))
	}
}

// TestRevocationsCall checks the cluster's call: a node not in the cluster
// is refused, and a node that reconnects is answered with the whole state.
func TestRevocationsCall(t *testing.T) {
	live := securityState.Load()
	t.Cleanup(func() { securityState.Store(live) })
	r := &revocations{contracts: map[uint32]*ContractState{0x7e5c0002: {NotBefore: 10}}}
	securityState.Store(r)
	a := &ClusterNode{name: "a"}
	a.lacks(rpc.ServerError(errCapabilityOff(capRevocations).Error()), capRevocations)
	c := &Cluster{thisNodeName: "self", nodes: map[string]*ClusterNode{"a": a}}

	var resp RevocationsResp
	if err := c.Revocations(&RevocationsReq{Node: "b", Contracts: map[uint32]*ContractState{0x7e5c0003: {NotBefore: 20}}}, &resp); err == nil {
		t.Fatal("a call from an unknown node was taken")
	}
	if r.refuses(0x7e5c0003, 0, 19) != "" {
		t.Fatal("an unknown node's state was merged")
	}
	if err := c.Revocations(&RevocationsReq{Node: "a", Contracts: map[uint32]*ContractState{0x7e5c0003: {NotBefore: 20}}}, &resp); err != nil {
		t.Fatal(err)
	}
	if r.refuses(0x7e5c0003, 0, 19) == "" || resp.Contracts != nil {
		t.Fatalf("merged %v, answered %v", dump(r.all()), dump(resp.Contracts))
	}
	if !a.supports(capRevocations) {
		t.Error("a node that sent the state is still taken to lack it")
	}
	if err := c.Revocations(&RevocationsReq{Node: "a", Full: true}, &resp); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resp.Contracts, r.all()) {
		t.Fatalf("answered %v, want the whole state %v", dump(resp.Contracts), dump(r.all()))
	}
}
