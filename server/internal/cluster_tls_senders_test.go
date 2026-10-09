package internal

import (
	"reflect"
	"strings"
	"testing"

	"github.com/unit-io/unitdb/server/internal/peerwire"
)

// TestPeerCallsCheckSender checks that every call this node serves is
// refused, before it is handled, when it names a sender other than the node
// of its connection (over TLS, the node its certificate names): every
// request type has a Node field, and the dispatcher checks it for all.
func TestPeerCallsCheckSender(t *testing.T) {
	c := &Cluster{thisNodeName: "one"}
	methods := c.peerMethods()
	d := c.dispatcher("two", methods)
	for name, m := range methods {
		req := m.newReq()
		f := reflect.ValueOf(req).Elem().FieldByName("Node")
		if !f.IsValid() || f.Kind() != reflect.String {
			t.Errorf("%s: request %T names no sender", name, req)
			continue
		}
		f.SetString("three")
		body, err := peerwire.Encode(req)
		if err != nil {
			t.Fatal(err)
		}
		var got error
		replied := false
		d(name, body, func(_ interface{}, err error) { got, replied = err, true })
		if !replied || got == nil || !strings.Contains(got.Error(), `names "three"`) {
			t.Errorf("%s from two naming three: %v, want it refused", name, got)
		}
	}
	// A request without a sender is refused too.
	if err := checkSender("two", &struct{ Other string }{}); err == nil {
		t.Error("a request naming no sender was taken")
	}
	if err := checkSender("two", &LeaveReq{Node: "two"}); err != nil {
		t.Errorf("a request naming its connection's node was refused: %v", err)
	}
}
