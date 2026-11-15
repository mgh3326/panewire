package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

// hk 4835: a lane.event longer than the typed-line limit is kept whole in the
// lane inbox and typed as a head plus a pointer. These tests pin the typed
// line, the wire gate for an older hub, the inbox row, and the ceiling.

const longLaneTestPrefix = "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: "

func longLaneText(n int) string {
	return strings.Repeat("x", n)
}

func TestLaneEventRelayTextTypesHeadAndPointerWithinLimit(t *testing.T) {
	event := hubJobEventPayload{OwnerLane: "lane-a", EventID: "long-1", Text: longLaneText(10 << 10), inboxID: 42}
	typed := laneEventRelayText(event)
	if len(typed) > laneEventTextLimit || !utf8.ValidString(typed) {
		t.Fatalf("typed line bytes=%d valid=%t, want <= %d", len(typed), utf8.ValidString(typed), laneEventTextLimit)
	}
	if !strings.HasPrefix(typed, longLaneTestPrefix) || !strings.HasSuffix(typed, "[full text: panewire inbox read 42]") {
		t.Fatalf("typed line lacks prefix or pointer: %q", typed)
	}
}

func TestLaneEventRelayTextWithoutInboxRowSaysItWasCut(t *testing.T) {
	typed := laneEventRelayText(hubJobEventPayload{OwnerLane: "lane-a", Text: longLaneText(10 << 10)})
	if len(typed) > laneEventTextLimit || !strings.HasSuffix(typed, "[truncated]") || strings.Contains(typed, "panewire inbox read") {
		t.Fatalf("no-inbox typed line=%q bytes=%d", typed, len(typed))
	}
}

func TestLaneEventRelayTextWithinLimitIsUnchanged(t *testing.T) {
	text := longLaneText(laneEventTextLimit)
	if got := laneEventRelayText(hubJobEventPayload{OwnerLane: "lane-a", Text: text, inboxID: 7}); got != longLaneTestPrefix+text {
		t.Fatalf("a text within the typed limit was altered: bytes=%d", len(got))
	}
}

// longLaneWireText decodes the lane.event payload a connection writes.
func longLaneWireText(t *testing.T, wire any) (string, bool) {
	t.Helper()
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Payload struct {
			Text      string `json:"text"`
			Truncated bool   `json:"truncated"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Payload.Text, envelope.Payload.Truncated
}

func longLaneWireEvent(t *testing.T, text string, sink bool) hubClientEvent {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"owner_lane": "lane-a", "event_id": "long-1", "text": text, "epoch": 1})
	if err != nil {
		t.Fatal(err)
	}
	return hubClientEvent{Kind: "lane.event", Payload: payload, sinkRecord: sink}
}

// The gate: a hub that did not advertise lane_event_long gets the cut form, so
// it never refuses the frame. Skipping the gate in relayWire turns this red.
func TestLaneEventWireCutsOnlyForAHubWithoutTheFeature(t *testing.T) {
	long := longLaneText(10 << 10)
	older := &hubClientConnection{}
	if text, truncated := longLaneWireText(t, older.relayWire(longLaneWireEvent(t, long, false))); len(text) > laneEventTextLimit || !truncated || !strings.HasSuffix(text, "[truncated]") {
		t.Fatalf("older hub got text bytes=%d truncated=%t, want the typed-line cut", len(text), truncated)
	}
	newer := &hubClientConnection{}
	newer.acceptsLaneEventLong.Store(true)
	if text, truncated := longLaneWireText(t, newer.relayWire(longLaneWireEvent(t, long, false))); text != long || truncated {
		t.Fatalf("hub with the feature got bytes=%d truncated=%t, want the whole text", len(text), truncated)
	}
}

func TestLaneEventWireKeepsTheSinkBoundForAnOlderHub(t *testing.T) {
	sink := longLaneText(laneEventTextLimitSink - 100)
	older := &hubClientConnection{}
	if text, truncated := longLaneWireText(t, older.relayWire(longLaneWireEvent(t, sink, true))); text != sink || truncated {
		t.Fatalf("sink text within the sink bound was altered: bytes=%d truncated=%t", len(text), truncated)
	}
}

func TestLaneEventEmitKeepsLongTextWholeAndRefusesAboveCeiling(t *testing.T) {
	inbox := t.TempDir()
	var stderr bytes.Buffer
	long := longLaneText(10 << 10)
	if code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "long-emit", "--text", long, "--inbox-root", inbox}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("long emit code=%d stderr=%q", code, stderr.String())
	}
	entries, err := os.ReadDir(filepath.Join(inbox, "events-lane"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("event files=%+v err=%v", entries, err)
	}
	contents, err := os.ReadFile(filepath.Join(inbox, "events-lane", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var record emitRecord
	if json.Unmarshal(contents, &record) != nil || len(record.Text) != len(long) || record.Truncated || record.Sink {
		t.Fatalf("event file bytes=%d truncated=%t sink=%t, want the whole text", len(record.Text), record.Truncated, record.Sink)
	}

	stderr.Reset()
	tooLarge := longLaneText(laneEventTextCeiling + 1)
	if code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "too-large", "--text", tooLarge, "--inbox-root", t.TempDir()}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitUsage || !strings.Contains(stderr.String(), "ceiling") {
		t.Fatalf("above-ceiling emit code=%d stderr=%q, want a ceiling refusal", code, stderr.String())
	}
}

func TestLaneEventLongTextKeptWholeInInboxWithTypedPointer(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	inbox, err := newHubInbox("default", filepath.Join(t.TempDir(), "inbox.sqlite3"), "")
	if err != nil {
		t.Fatal(err)
	}
	hub.inbox = inbox
	t.Cleanup(func() { inbox.store.close() })
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 2)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}

	long := longLaneText(10 << 10)
	// An interrupt class is typed into the pane at once, so the typed head is exercised.
	result := hub.relayLaneEvent(hubJobEventPayload{OwnerLane: "lane-a", EventID: "long-hub", Text: long, Epoch: 1, DeliveryClass: "interrupt"}, source)
	if result.RejectedTooLong || fake.rowCount() != 1 {
		t.Fatalf("10 KB event refused or unpersisted: rejected=%t rows=%d", result.RejectedTooLong, fake.rowCount())
	}
	items, err := inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	var stored *laneInboxItem
	for i := range items {
		if items[i].Event.EventID == "long-hub" {
			stored = &items[i]
		}
	}
	if stored == nil || len(stored.Event.Text) != len(long) {
		t.Fatalf("inbox row missing or cut: found=%t", stored != nil)
	}
	// The pane gets one typed line: the head, and a pointer to this inbox row.
	injects := drainRelaysAll(destination)
	if len(injects) != 1 {
		t.Fatalf("typed injections=%d, want 1", len(injects))
	}
	pointer := fmt.Sprintf("[full text: panewire inbox read %d]", stored.ID)
	if len(injects[0].Text) > laneEventTextLimit || !strings.HasSuffix(injects[0].Text, pointer) {
		t.Fatalf("typed line bytes=%d lacks head or pointer %q", len(injects[0].Text), pointer)
	}
}

func drainRelaysAll(agent *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case event := <-agent.relays:
			out = append(out, event)
		default:
			return out
		}
	}
}

func TestLaneEventAboveCeilingIsRefusedFromNodeSender(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 2)}
	seen := r20t5Subscribe(t, hub)
	result := hub.relayLaneEvent(hubJobEventPayload{OwnerLane: "lane-a", EventID: "over-ceiling", Text: longLaneText(laneEventTextCeiling + 1), Epoch: 1}, source)
	if !result.RejectedTooLong || fake.rowCount() != 0 || len(seen("relay.rejected")) != 1 {
		t.Fatalf("above-ceiling text: rejected=%t rows=%d rejections=%d", result.RejectedTooLong, fake.rowCount(), len(seen("relay.rejected")))
	}
}
