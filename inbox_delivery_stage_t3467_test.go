package panewire

// hk 3467: a sender can see whether its lane.event reached the pane. The
// ack-state lookup reports the delivery stage recorded on the lane inbox row,
// with the same sender scoping as the ack answer, and the Linux `deliveries`
// default resolves to the daemon's actual database path.

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func deliveryStageHub(t *testing.T) (*HubServer, *httptest.Server, *time.Time) {
	t.Helper()
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	now := time.Date(2026, 11, 7, 13, 0, 0, 0, time.UTC)
	h, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": ackStateOperatorToken, "node-a": ackStateNodeAToken, "node-b": ackStateNodeBToken,
		},
		ReportRelayPath:    r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"},"lane-b":{"machine":"node-b","pane":"w2:p1"}}}`),
		DeliveryPolicyPath: "default", InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		handoffkeep: hk,
		Now:         func() time.Time { return now },
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	return h, server, &now
}

func relayStageEvent(t *testing.T, h *HubServer, node, lane, eventID string) (relayLaneEventResult, hubJobEventPayload) {
	t.Helper()
	agent := &hubAgent{machineID: node, relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: "approval requested"}
	result := h.relayLaneEvent(event, agent)
	if result.ID == 0 || result.PersistFailed {
		t.Fatalf("relay %s from %s: %+v", eventID, node, result)
	}
	return result, event
}

func deliveryState(t *testing.T, server *httptest.Server, caller pw2Caller, eventID string) map[string]any {
	t.Helper()
	status, body := ackStateGet(t, server, caller, eventID)
	if status != http.StatusOK {
		t.Fatalf("%s %s: %d %s", caller.name, eventID, status, body)
	}
	answer := decodeAckState(t, body)
	if len(answer.Events) != 1 {
		t.Fatalf("%s %s: %s", caller.name, eventID, body)
	}
	return answer.Events[0]
}

// A node that relayed an event sees its stage reach delivered; another node's
// id stays indistinguishable from an unknown one.
func TestInboxStatusDeliveryStageDelivered(t *testing.T) {
	h, server, now := deliveryStageHub(t)
	result, event := relayStageEvent(t, h, "node-a", "lane-a", "ev-a")
	if !h.markRelayEventDelivered(relayPending{machine: "node-a", pane: "w1:p1", eventID: result.ID, kind: "lane.event", event: event}) {
		t.Fatal("delivery was not recorded")
	}
	nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
	state := deliveryState(t, server, nodeA, "ev-a")
	if state["lane"] != "lane-a" || state["stage"] != laneInboxStageDelivered || state["machine"] != "node-a" || state["pane"] != "w1:p1" || state["acked"] != false {
		t.Fatalf("delivered state: %v", state)
	}
	if state["at"] != now.Format(time.RFC3339) {
		t.Fatalf("stage time: %v, want %s", state["at"], now.Format(time.RFC3339))
	}
	nodeB := pw2Caller{"node-b", "node-b", ackStateNodeBToken}
	if status, body := ackStateGet(t, server, nodeB, "ev-a"); status != http.StatusUnauthorized || strings.Contains(body, "ev-a") || strings.Contains(body, "delivered") {
		t.Fatalf("foreign node: %d %s", status, body)
	}
}

// An unconfirmed report keeps the node's reason, so the sender can tell "not
// proven" from "never injected".
func TestInboxStatusDeliveryStageUnconfirmed(t *testing.T) {
	h, server, _ := deliveryStageHub(t)
	result, event := relayStageEvent(t, h, "node-a", "lane-a", "ev-u")
	h.registerRelayAckEvent("lane.event", event, "node-a", "w1:p1", result.ID)
	payload, _ := json.Marshal(relayAckPayload{JobID: event.JobID, Pane: "w1:p1", Reason: "maybe_in_pane presend:composer_pending", OriginalEventID: result.ID})
	h.handleRelayAck("node-a", hubInbound{Kind: "relay.unconfirmed", Payload: payload})
	nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
	state := deliveryState(t, server, nodeA, "ev-u")
	if state["stage"] != laneInboxStageUnconfirmed || state["reason"] != "maybe_in_pane presend:composer_pending" || state["machine"] != "node-a" || state["pane"] != "w1:p1" {
		t.Fatalf("unconfirmed state: %v", state)
	}
}

// The stage is on the row, so a hub restart (a fresh store over the same file)
// still answers delivered.
func TestLaneInboxDeliveryStagePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.UnixMilli(1_700_000_000_000).UTC()
	event := quietEvent("ev-persist", "approval requested")
	key := relayEventDedupeKey("lane.event", event)
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.putFrom("node-a", key, "lane-a", "lane.event", "interrupt", event, now, 0, 0); err != nil {
		t.Fatal(err)
	}
	state := func(store *laneInboxStore) laneInboxAckState {
		t.Helper()
		states, err := store.ackStates([]string{"ev-persist"}, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		return states["ev-persist"]
	}
	// The row delivered nothing yet: this is the queued state the mutant
	// (a missing delivery write) would leave behind.
	if got := state(s); got.Stage != laneInboxStageQueued || got.Machine != "" || got.Pane != "" {
		t.Fatalf("queued state: %+v", got)
	}
	if err := s.recordDelivery(key, "node-a", "w1:p1", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if got := state(reopened); got.Stage != laneInboxStageDelivered || got.Machine != "node-a" || got.Pane != "w1:p1" || got.At != now.Add(time.Second).Format(time.RFC3339) {
		t.Fatalf("persisted state: %+v", got)
	}
}

// A node report that names only the durable relay record updates the row
// through relay_id; a later terminal stage wins over an earlier one.
func TestLaneInboxDeliveryStageByRelayID(t *testing.T) {
	event := quietEvent("ev-relay", "approval requested")
	key := relayEventDedupeKey("lane.event", event)
	now := time.UnixMilli(1_700_000_000_000).UTC()
	s, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := s.putFrom("node-a", key, "lane-a", "lane.event", "interrupt", event, now, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.linkRelayID(key, 77); err != nil {
		t.Fatal(err)
	}
	stage := func() laneInboxAckState {
		t.Helper()
		states, err := s.ackStates([]string{"ev-relay"}, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		return states["ev-relay"]
	}
	if err := s.recordStageByRelayID(77, laneInboxStageHeld, "working", "node-a", "w1:p1", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := stage(); got.Stage != laneInboxStageHeld || got.Reason != "working" {
		t.Fatalf("held state: %+v", got)
	}
	if err := s.recordStageByRelayID(77, laneInboxStageDropped, "working_wait_expired", "node-a", "w1:p1", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := stage(); got.Stage != laneInboxStageDropped || got.Reason != "working_wait_expired" {
		t.Fatalf("dropped state: %+v", got)
	}
	// Delivered is final: a held/typed report that arrives later cannot undo it.
	if err := s.recordDelivery(key, "node-a", "w1:p1", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.recordStageByRelayID(77, laneInboxStageTyped, "", "node-a", "w1:p1", now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := stage(); got.Stage != laneInboxStageDelivered {
		t.Fatalf("delivered regressed: %+v", got)
	}
}

// The Linux default for `panewire deliveries` is the daemon's data directory,
// never the macOS one.
func TestDefaultDBPathPerOS(t *testing.T) {
	for _, tc := range []struct {
		name, goos, home, data, want string
	}{
		{"darwin", "darwin", "/Users/u", "", filepath.Join("/Users/u", "Library", "Application Support", "panewire", "panewire.sqlite3")},
		{"linux default", "linux", "/home/u", "", filepath.Join("/home/u", ".local", "share", "panewire", "panewire.sqlite3")},
		{"linux xdg", "linux", "/home/u", "/data", filepath.Join("/data", "panewire", "panewire.sqlite3")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultDBPathFor(tc.goos, tc.home, tc.data)
			if got != tc.want {
				t.Fatalf("defaultDBPathFor = %q, want %q", got, tc.want)
			}
			if tc.goos != "darwin" && strings.Contains(got, "Library/Application Support") {
				t.Fatalf("non-darwin default uses the macOS path: %q", got)
			}
		})
	}
}

// A missing node database is named plainly, and the sender is pointed at the
// command that answers the same question without one.
func TestDeliveriesMissingDatabaseSuggestsInboxStatus(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runDeliveriesCLI([]string{"list", "--db", filepath.Join(t.TempDir(), "missing.sqlite3")}, &out, &errOut)
	if code != ExitDaemonUnavailable || !strings.Contains(errOut.String(), "inbox status --event-id") {
		t.Fatalf("missing db: exit=%d stderr=%q", code, errOut.String())
	}
}

// emit prints the event id it sent and the one command that checks the stage.
func TestEmitLaneEventPrintsIDAndStatusCommand(t *testing.T) {
	for _, given := range []string{"", "given-event"} {
		t.Run("given="+given, func(t *testing.T) {
			root := t.TempDir()
			args := []string{"--kind", "lane.event", "--lane", "lane-a", "--text", "hello", "--inbox-root", root}
			if given != "" {
				args = append(args, "--event-id", given)
			}
			var out, errOut bytes.Buffer
			if code := runEmitCLI(args, &out, &errOut, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
				t.Fatalf("emit exit=%d stderr=%q", code, errOut.String())
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != 2 || !strings.HasPrefix(lines[0], "event_id=") {
				t.Fatalf("emit output %q", out.String())
			}
			id := strings.TrimPrefix(lines[0], "event_id=")
			if id == "" || (given != "" && id != given) || lines[1] != "panewire inbox status --event-id "+id {
				t.Fatalf("emit output %q, want id %q", out.String(), given)
			}
			if _, err := os.Stat(filepath.Join(root, "events-lane")); err != nil {
				t.Fatalf("no durable lane event file: %v", err)
			}
		})
	}
}
