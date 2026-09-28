package internal

import "testing"

// TestChooseRingVersion checks the ring version the leader picks: the highest
// every live node supports, up to the target, and none until every live node
// has told what it supports.
func TestChooseRingVersion(t *testing.T) {
	peer := func(versions ...int) *ClusterNode {
		n := &ClusterNode{}
		n.setCapabilities(NodeCapabilities{Version: clusterProtocolVersion, RingVersions: versions})
		return n
	}
	cases := []struct {
		name    string
		target  int
		current int
		live    []*ClusterNode
		want    int
	}{
		{"all support 2", 0, 1, []*ClusterNode{peer(1, 2), peer(1, 2)}, 2},
		{"one supports 1 only", 0, 2, []*ClusterNode{peer(1, 2), peer(1)}, 1},
		{"built before versions", 0, 1, []*ClusterNode{peer(1, 2), peer()}, preVersionRing},
		{"target holds it at 1", 1, 1, []*ClusterNode{peer(1, 2), peer(1, 2)}, 1},
		{"a node not heard from", 0, 1, []*ClusterNode{peer(1, 2), {}}, 1},
		{"no version in common", 0, 2, []*ClusterNode{peer(1), peer(3)}, 2},
	}
	for _, tc := range cases {
		c := &Cluster{ringTarget: tc.target, ringVersion: tc.current}
		if got := c.chooseRingVersion(tc.live); got != tc.want {
			t.Errorf("%s: chose ring version %d, want %d", tc.name, got, tc.want)
		}
	}
}
