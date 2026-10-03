package internal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/internal/v1test"
	"github.com/unit-io/unitdb/server/utp"
)

func TestRenewsAt(t *testing.T) {
	const issue = 2
	for name, tc := range map[string]struct {
		claims uid.Claims
		now    int64
		want   bool
	}{
		"one that never expires":   {uid.Claims{KeyID: issue, IssuedAt: 100}, 1 << 40, false},
		"early in its lifetime":    {uid.Claims{KeyID: issue, IssuedAt: 100, ExpiresAt: 200}, 179, false},
		"past 80% of its lifetime": {uid.Claims{KeyID: issue, IssuedAt: 100, ExpiresAt: 200}, 180, true},
		"of a key being retired":   {uid.Claims{KeyID: 1, IssuedAt: 100}, 101, true},
		"with times out of order":  {uid.Claims{KeyID: issue, IssuedAt: 200, ExpiresAt: 100}, 150, false},
	} {
		if got := renewsAt(tc.claims, issue, tc.now); got != tc.want {
			t.Errorf("%s: renewed %t, want %t", name, got, tc.want)
		}
	}
}

// sealedID returns id sealed as a v2 client id with the given times.
func sealedID(t *testing.T, id uid.ID, issuedAt, expiresAt uint32) string {
	t.Helper()
	text, err := Globals.Service.keys.SealClientIDAt(id, issuedAt, expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

// TestV2ClientIDs checks that the server issues v2 ids, refuses v1 ones
// without sending a new id, renews a v2 id near the end of its lifetime, and
// refuses an expired one.
func TestV2ClientIDs(t *testing.T) {
	primaryID := newClientID(t)
	if len(primaryID) != uid.EncodedLenV2 {
		t.Fatalf("assigned id %q is not v2", primaryID)
	}
	primary := connectedClient(t, primaryID, false)
	primary.expectNone("a new id for a fresh v2 id", isPublishOn("unitdb/clientid/"), 300*time.Millisecond)
	secondaryID := primary.secondaryClientID()
	secondary, claims, err := Globals.Service.keys.OpenClientID([]byte(secondaryID))
	if err != nil || claims.IssuedAt == 0 || secondary.IsPrimary() || secondary.Uuid() == 0 || secondary.Contract() != contractOf(t, primaryID) {
		t.Fatalf("secondary id %x %+v %v", []byte(secondary), claims, err)
	}

	// A v1 id, of the server's key: refused, without a new id, which
	// would be of another contract.
	v1 := dialTCP(t)
	if ack := v1.connect(&utp.Connect{ClientID: v1test.ClientID(secondary, []byte(testServiceKey))}); ack.ReturnCode != types.ErrV1ClientID.ReturnCode {
		t.Fatalf("a v1 id: return code %d, want %d", ack.ReturnCode, types.ErrV1ClientID.ReturnCode)
	}
	v1.expectNone("a new id for a v1 one", isPublishOn("unitdb/clientid/"), 300*time.Millisecond)
	v1.waitClosed()

	// Near the end of its lifetime.
	now := uint32(time.Now().Unix())
	old := connectedClient(t, sealedID(t, secondary, now-90, now+10), false)
	m := old.waitFor("the old id renewed", isPublishOn("unitdb/clientid/"))
	renewed, renewedClaims, err := Globals.Service.keys.OpenClientID(payloadOf(m))
	if err != nil || string(renewed) != string(secondary) || renewedClaims.ExpiresAt != 0 {
		t.Fatalf("renewed %x %+v %v, want %x, never expiring as client_id_ttl says", []byte(renewed), renewedClaims, err, []byte(secondary))
	}
	if young := connectedClient(t, sealedID(t, secondary, now, now+3600), false); young != nil {
		young.expectNone("a new id for an id early in its lifetime", isPublishOn("unitdb/clientid/"), 300*time.Millisecond)
	}

	// Expired: refused, without a new id.
	expired := dialTCP(t)
	if ack := expired.connect(&utp.Connect{ClientID: sealedID(t, secondary, now-20, now-10)}); ack.ReturnCode != types.ErrInvalidClientID.ReturnCode {
		t.Fatalf("an expired id: return code %d, want %d", ack.ReturnCode, types.ErrInvalidClientID.ReturnCode)
	}
	expired.expectNone("a new id for an expired one", isPublishOn("unitdb/clientid/"), 300*time.Millisecond)
	expired.waitClosed()
}

// TestV2SecondaryIDsOwnSessions checks that secondary ids issued in the same
// second own different sessions: v1 ones were the same id.
func TestV2SecondaryIDsOwnSessions(t *testing.T) {
	primary := connectedClient(t, newClientID(t), false)
	a, b := primary.secondaryClientID(), primary.secondaryClientID()
	idA, idB := openID(t, a), openID(t, b)
	if sessionOwner(idA) == sessionOwner(idB) {
		t.Fatal("two secondary ids own the same session")
	}
	// A renewed id keeps its session.
	again := openID(t, sealedID(t, idA, 1, 0))
	if sessionOwner(again) != sessionOwner(idA) {
		t.Fatal("a renewed id owns another session")
	}
}

// keygenTTL requests a read/write key for topic lasting ttl.
func (c *testClient) keygenTTL(topic, ttl string) (string, int) {
	c.t.Helper()
	req, _ := json.Marshal([]types.KeyGenRequest{{Topic: topic, Type: "rw", Ttl: ttl}})
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: req}}})
	m := c.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(payloadOf(m), &resp); err == nil && len(resp) == 1 {
		return resp[0].Key, resp[0].Status
	}
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		c.t.Fatalf("keygen response %s", payloadOf(m))
	}
	return "", e.Status
}

// TestV2TopicKeys checks that keygen issues v2 keys, which open their topic
// only and expire with their ttl, and that v1 keys are refused.
func TestV2TopicKeys(t *testing.T) {
	ownerID := newClientID(t)
	owner := connectedClient(t, ownerID, false)
	const topic = "v2keys.t"
	key := owner.keygen(topic, "rw")
	if len(key) != security.KeyLenV2 {
		t.Fatalf("keygen returned %q, want a v2 key", key)
	}
	owner.subscribe(1, key+"/"+topic, 0)
	owner.publish(2, key+"/"+topic, "v2", 0)
	owner.waitFor("published with a v2 key", isPublishOn(topic))

	// A key opens its topic only: a topic of the same 32-bit hash would
	// do with a v1 key.
	owner.send(&utp.Subscribe{MessageID: 3, Subscriptions: []*utp.Subscription{{Topic: key + "/" + topic + ".x"}}})
	if e := owner.serverError(3); e.Status != types.ErrUnauthorized.Status {
		t.Fatalf("a v2 key on another topic: status %d", e.Status)
	}

	// A v1 signed key is refused, and the error says why.
	v1 := v1test.SignedTopicKey([]byte(testServiceKey), contractOf(t, ownerID), topic, security.AllowReadWrite)
	owner.send(&utp.Publish{MessageID: 4, Messages: []*utp.PublishMessage{{Topic: v1 + "/" + topic, Payload: []byte("v1")}}})
	if e := owner.serverError(4); e.Status != types.ErrV1Key.Status || !strings.Contains(e.Message, "no longer accepted") {
		t.Fatalf("a v1 key: %+v", e)
	}

	// A key with a ttl expires.
	short, status := owner.keygenTTL(topic, "1s")
	if status != 200 || len(short) != security.KeyLenV2 {
		t.Fatalf("keygen with a ttl: %q, status %d", short, status)
	}
	if k, err := Globals.Service.keys.DecodeTopicKeyV2(contractOf(t, ownerID), short, topic); err != nil || k.ExpiresAt == 0 {
		t.Fatalf("the key with a ttl: %+v %v", k, err)
	}
	time.Sleep(2100 * time.Millisecond)
	owner.send(&utp.Publish{MessageID: 5, Messages: []*utp.PublishMessage{{Topic: short + "/" + topic, Payload: []byte("late")}}})
	if e := owner.serverError(5); e.Status != types.ErrUnauthorized.Status {
		t.Fatalf("an expired key: status %d", e.Status)
	}
	if _, status := owner.keygenTTL(topic, "a while"); status != types.ErrBadRequest.Status {
		t.Fatalf("keygen with a bad ttl: status %d", status)
	}

	// A key for "..." reads every topic, as v1 ones do, and publishes none.
	all := owner.keygen("...", "rw")
	owner.subscribe(6, all+"/v2keys.any.thing", 0)
	owner.publish(7, owner.keygen("v2keys.any.thing", "w")+"/v2keys.any.thing", "seen by ...", 0)
	owner.waitFor("delivered to a subscription with the key for ...", isPublishOn("v2keys.any.thing"))
	owner.send(&utp.Publish{MessageID: 8, Messages: []*utp.PublishMessage{{Topic: all + "/v2keys.any.thing", Payload: []byte("x")}}})
	if e := owner.serverError(8); e.Status != types.ErrForbidden.Status {
		t.Fatalf("publish with the key for ...: status %d", e.Status)
	}
}
