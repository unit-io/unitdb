package internal

import (
	"errors"
	"testing"

	"github.com/unit-io/unitdb/server/internal/peerwire"
)

// TestNodeCapabilities checks how this node learns what another can do: what
// it told, or everything until it refuses a call as a method it lacks.
func TestNodeCapabilities(t *testing.T) {
	n := &ClusterNode{name: "peer"}
	if !n.supports(capDeliver) {
		t.Fatal("a node not heard from yet should be taken to do everything")
	}

	// A method the node lacks, as an older node answers.
	missing := &peerwire.RemoteError{Code: peerwire.CodeNoMethod, Message: "cluster: no method Cluster.Deliver"}
	if !n.lacks(missing, capDeliver) || n.supports(capDeliver) {
		t.Fatal("a missing method should mark the capability missing")
	}
	if !n.supports(capSessions) {
		t.Fatal("a missing method should mark only its capability missing")
	}

	// A capability the node has turned off names it.
	off := errCapabilityOff(capSessions)
	if n.lacks(off, capReplicate) {
		t.Fatal("refusing sessions should not mark replicate missing")
	}
	if !n.lacks(off, capSessions) || n.supports(capSessions) {
		t.Fatal("refusing sessions should mark sessions missing")
	}
	if n.lacks(errors.New("connection reset"), capResync) {
		t.Fatal("a failed call is not a missing method")
	}
	if n.lacks(&peerwire.RemoteError{Code: peerwire.CodeFailed, Message: "cluster: no method in the handler's words"}, capResync) {
		t.Fatal("a handler's error is not a missing method")
	}

	// What the node tells replaces what was learned from refusals.
	n.setCapabilities(NodeCapabilities{Version: clusterProtocolVersion, Capabilities: []string{capDeliver}})
	if !n.supports(capDeliver) || n.supports(capSessions) || n.supports(capReplicate) {
		t.Fatal("a node's told capabilities should be what it supports")
	}
}
