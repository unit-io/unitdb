package message

import "testing"

// TestTopicUnmarshalShort decodes topics cut short: a name read from a
// corrupt file panicked.
func TestTopicUnmarshalShort(t *testing.T) {
	full := (&Topic{Depth: 2, Parts: []Part{{Hash: 1}, {Hash: 2, Wildchars: 1}}}).Marshal()
	for n := 0; n < len(full); n++ {
		var top Topic
		if err := top.Unmarshal(full[:n]); err == nil && n != 1 && n != 6 {
			t.Errorf("%d of %d bytes: no error", n, len(full))
		}
	}
	var top Topic
	if err := top.Unmarshal(full); err != nil || len(top.Parts) != 2 || top.Parts[1].Hash != 2 {
		t.Errorf("whole topic: %v, %+v", err, top)
	}
}
