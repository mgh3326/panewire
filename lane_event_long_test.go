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

	"github.com/mgh3326/panewire/internal/fleet"
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

// A replayed event is rebuilt from its handoffkeep record, which has no inbox
// row id. The typed head must still name the row the live path stored.
func TestLaneEventReplayedTypedLineNamesInboxRow(t *testing.T) {
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
	hub.nodes["host-a"] = &hubNodeRecord{agent: &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}}
	long := longLaneText(10 << 10)
	hub.relayLaneEvent(hubJobEventPayload{OwnerLane: "lane-a", EventID: "long-replay", Text: long, Epoch: 1}, source)
	if fake.rowCount() != 1 {
		t.Fatalf("rows=%d, want 1", fake.rowCount())
	}
	items, err := inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	var rowID int64
	for _, item := range items {
		if item.Event.EventID == "long-replay" {
			rowID = item.ID
		}
	}
	if rowID == 0 {
		t.Fatal("inbox row missing")
	}
	// The replay rebuilds the event from its record: no inboxID.
	replayed := hubJobEventPayload{OwnerLane: "lane-a", EventID: "long-replay", Text: long, Epoch: 1}
	directive := hub.inboxRelayDirective("lane.event", replayed, loadReportRelayRoutes(hub.reportRelayPath)["lane-a"], 1)
	if want := fmt.Sprintf("[full text: panewire inbox read %d]", rowID); !strings.HasSuffix(directive.Text, want) {
		t.Fatalf("replayed typed line does not name row %d: %q", rowID, directive.Text[len(directive.Text)-min(len(directive.Text), 80):])
	}
}

// A file written before hk 4835 has no ceiling marker. Its text was cut at emit
// to the sink bound or the typed line, so the node keeps the sink bound on the wire.
func TestLaneEventFileWithoutCeilingMarkerKeepsSinkBound(t *testing.T) {
	inbox := t.TempDir()
	var stderr bytes.Buffer
	text := longLaneText(5000)
	if code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "legacy-file", "--text", text, "--inbox-root", inbox}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("emit code=%d stderr=%q", code, stderr.String())
	}
	entries, err := os.ReadDir(filepath.Join(inbox, "events-lane"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("event files=%+v err=%v", entries, err)
	}
	path := filepath.Join(inbox, "events-lane", entries[0].Name())
	current, ok, _, _ := readLaneEventFile(path)
	if !ok || current.Sink {
		t.Fatalf("new non-sink file: ok=%t sink=%t, want sink=false", ok, current.Sink)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if json.Unmarshal(contents, &fields) != nil {
		t.Fatal("event file is not JSON")
	}
	delete(fields, "ceiling")
	legacy, _ := json.Marshal(fields)
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	scanned, ok, _, _ := readLaneEventFile(path)
	if !ok || !scanned.Sink {
		t.Fatalf("legacy file: ok=%t sink=%t, want sink=true", ok, scanned.Sink)
	}
	if text, truncated := longLaneWireText(t, (&hubClientConnection{}).relayWire(longLaneWireEvent(t, scanned.Text, scanned.Sink))); text != scanned.Text || truncated {
		t.Fatalf("legacy 5000-byte text was cut to %d for an older hub", len(text))
	}
}

// A sink route records text and types none. Its inbox row must hold the same
// bound as its relay record, not the whole ceiling-sized text.
func TestLaneEventSinkRouteInboxRowMatchesRecord(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-web":{"machine":"host-a","pane":"pane-a","sink":true}}}`, client, nil)
	inbox, err := newHubInbox("default", filepath.Join(t.TempDir(), "inbox.sqlite3"), "")
	if err != nil {
		t.Fatal(err)
	}
	hub.inbox = inbox
	t.Cleanup(func() { inbox.store.close() })
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 2)}
	result := hub.relayLaneEvent(hubJobEventPayload{OwnerLane: "lane-web", EventID: "sink-long", Text: longLaneText(10 << 10), Epoch: 1}, source)
	if result.RejectedTooLong || fake.rowCount() != 1 {
		t.Fatalf("sink node text refused or unpersisted: rejected=%t rows=%d", result.RejectedTooLong, fake.rowCount())
	}
	items, err := inbox.store.list("lane-web", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].Event.Text) != laneEventTextLimitSink {
		t.Fatalf("sink inbox row bytes=%d items=%d, want %d", len(items[0].Event.Text), len(items), laneEventTextLimitSink)
	}
}

// A connection holds a lane.event until its hub answers or announces its
// features. Before that a cut could reach an upgraded hub, which would then
// store the cut text (hk 4835, PR 640 finding).
func TestLaneEventHeldUntilTheHubIsKnown(t *testing.T) {
	events := []hubClientEvent{{Kind: "lane.event", Payload: []byte(`{"text":"a"}`)}, {Kind: "job.completed"}}
	write, held := splitRelayBurst(events, false)
	if len(write) != 1 || write[0].Kind != "job.completed" || len(held) != 1 || held[0].Kind != "lane.event" {
		t.Fatalf("before the hub is known: write=%+v held=%+v", write, held)
	}
	write, held = splitRelayBurst(events, true)
	if len(write) != 2 || len(held) != 0 {
		t.Fatalf("once the hub is known: write=%d held=%d", len(write), len(held))
	}
}

func TestLaneEventsKnownByAnswerOrFeatures(t *testing.T) {
	peer := &hubClientConnection{}
	if peer.laneEventsKnown(false) {
		t.Fatal("a fresh connection knows its hub's features")
	}
	if !peer.laneEventsKnown(true) {
		t.Fatal("an answered connection does not know its hub")
	}
	features := &hubClientConnection{}
	features.featuresKnown.Store(true)
	if !features.laneEventsKnown(false) {
		t.Fatal("a connection with hub.features does not know its hub")
	}
}

// `inbox read` prints the whole stored text: its pointer names that command, so
// printing the typed head again would point at nothing (PR 640 finding).
func TestInboxReadPrintsTheWholeLaneEvent(t *testing.T) {
	long := longLaneText(10 << 10)
	printed := inboxItemPrintText("lane.event", hubJobEventPayload{OwnerLane: "lane-a", Text: long, inboxID: 42})
	if !strings.HasSuffix(printed, long) || len(printed) <= laneEventTextLimit {
		t.Fatalf("inbox read printed %d bytes, want the whole text", len(printed))
	}
	if short := inboxItemPrintText("lane.event", hubJobEventPayload{OwnerLane: "lane-a", Text: "hi"}); short != longLaneTestPrefix+"hi" {
		t.Fatalf("short lane.event print changed: %q", short)
	}
}

// internal/fleet cannot import the root package, so its record cap is pinned here.
func TestFleetLaneEventCapMatchesTheFrameBound(t *testing.T) {
	if fleet.LaneEventMaxBytes != laneEventFrameMax {
		t.Fatalf("fleet.LaneEventMaxBytes=%d, want laneEventFrameMax=%d", fleet.LaneEventMaxBytes, laneEventFrameMax)
	}
}
