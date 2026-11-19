package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
)

// hk 4269: `panewire emit --priority` reaches the hub's decoded event through
// the event file, the node scan and the wire payload.
func TestEmitOrderPriorityReachesHubEvent_t4269(t *testing.T) {
	absent := CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}
	for _, priority := range []string{"", "stop", "hold", "urgent", "normal", "low"} {
		for _, newHub := range []bool{true, false} {
			t.Run(fmt.Sprintf("priority=%s/newHub=%v", priority, newHub), func(t *testing.T) {
				root := t.TempDir()
				args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "p1", "--type", "order", "--text", "do it", "--inbox-root", root}
				if priority != "" {
					args = append(args, "--priority", priority)
				}
				if code := runEmitCLI(args, io.Discard, io.Discard, absent); code != ExitOK {
					t.Fatal(code)
				}
				records := scanHubRelayEvents(root)
				if len(records) != 1 {
					t.Fatal(records)
				}
				node := &HubClient{completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}}
				event, ok := node.relayEventForSend(records[0])
				if !ok {
					t.Fatal("no wire event")
				}
				// A hub that never advertised lane_event_priority refuses a payload
				// with the key and the order would never arrive: it gets the order
				// unmarked, which interrupts as before.
				peer := &hubClientConnection{}
				peer.acceptsLaneEventPriority.Store(newHub)
				raw, err := json.Marshal(peer.relayWire(event))
				if err != nil {
					t.Fatal(err)
				}
				var wire struct {
					Payload json.RawMessage `json:"payload"`
				}
				if err := json.Unmarshal(raw, &wire); err != nil {
					t.Fatal(err)
				}
				want := priority
				if !newHub {
					want = ""
				}
				e, ok := decodeHubLaneEventPayload(wire.Payload)
				if !ok || e.NotifyType != "order" || e.Priority != want {
					t.Fatalf("%s decoded=%+v ok=%v, want priority %q", wire.Payload, e, ok, want)
				}
			})
		}
	}
}

// The hub advertises the feature the node gates the key on.
func TestHubAdvertisesLaneEventPriority_t4269(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, hubFeatureLaneEventPriority) {
		t.Fatal("hub.features does not list " + hubFeatureLaneEventPriority)
	}
}

func TestEmitOrderPriorityRefusals_t4269(t *testing.T) {
	absent := CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}
	base := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "p2", "--text", "x"}
	for name, extra := range map[string][]string{
		"unknown value": {"--type", "order", "--priority", "someday"},
		"not an order":  {"--type", "info", "--priority", "low"},
		"untyped":       {"--priority", "low"},
	} {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			args := append(append([]string{}, base...), "--inbox-root", t.TempDir())
			if code := runEmitCLI(append(args, extra...), io.Discard, &stderr, absent); code != ExitUsage || stderr.Len() == 0 {
				t.Fatalf("code=%d stderr=%q, want ExitUsage with a message", code, stderr.String())
			}
		})
	}
}

// The hub never accepts a priority on anything but an order.
func TestDecodeLaneEventPriorityOnlyOnOrders_t4269(t *testing.T) {
	for payload, want := range map[string]bool{
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","notify_type":"order","priority":"low"}`:   true,
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","notify_type":"order","priority":"later"}`: false,
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","notify_type":"order","priority":""}`:      false,
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","notify_type":"info","priority":"low"}`:    false,
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","priority":"low"}`:                         false,
		`{"owner_lane":"lane-a","event_id":"e1","text":"x","notify_type":"order"}`:                    true,
	} {
		if _, ok := decodeHubLaneEventPayload([]byte(payload)); ok != want {
			t.Errorf("%s: accepted=%v, want %v", payload, ok, want)
		}
	}
}
