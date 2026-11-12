package panewire

// hk 3460: a lane.event pane_id holding any control byte (< 0x20 or DEL) is no
// pane, at the hub decoder and at the node sender, as docs/r21-lane-event.md
// promises. A valid pane id is kept whole.

import (
	"encoding/json"
	"testing"
)

func TestLaneEventPaneIDWithAControlByteIsNoPane(t *testing.T) {
	for _, tc := range []struct{ name, pane, want string }{
		{"plain pane", "w1:p7", "w1:p7"},
		{"NUL", "w1:\x00p7", ""},
		{"SOH", "w1:\x01p7", ""},
		{"CR", "w1:p\r7", ""},
		{"LF", "w1:p\n7", ""},
		{"TAB", "w1:\tp7", ""},
		{"ESC", "w1:\x1bp7", ""},
		{"US", "w1:p\x1f7", ""},
		{"DEL", "w1:p7\x7f", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(map[string]string{"owner_lane": "lane-a", "event_id": "e-1", "text": "hi", "pane_id": tc.pane})
			if err != nil {
				t.Fatal(err)
			}
			event, ok := decodeHubLaneEventPayload(payload)
			if !ok || event.PaneID != tc.want {
				t.Fatalf("hub decoded ok=%t pane=%q, want pane %q", ok, event.PaneID, tc.want)
			}
			if got := laneEventSenderPane(hubScannedRelayEvent{PaneID: tc.pane}); got != tc.want {
				t.Fatalf("node sender pane=%q, want %q", got, tc.want)
			}
		})
	}
}
