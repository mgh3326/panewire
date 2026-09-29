package panewire

// #1002: a relay row whose pane submission could not be proven
// (relay.unconfirmed) is replayed at most once after a hub restart, visibly
// labelled with its original send time, and retired whatever that replay's
// outcome. The durable mark is a lane.event marker row on the unroutable
// hub/unconfirmed lane; the marked row itself stays an ordinary undelivered
// row until its one replay, so late relay.delivered proof, node resends, and
// the stall observation keep their existing meaning.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const t1002Lane = "lane-t1002"
const t1002Sink = "sink-t1002"

// t1002Log is a mutexed log sink: the arm and replay paths may write from a
// timer goroutine.
type t1002Log struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *t1002Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *t1002Log) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *t1002Log) count(sub string) int {
	return strings.Count(l.String(), sub)
}

type t1002Rig struct {
	t        *testing.T
	fake     *fakeHandoffkeep
	client   *handoffkeepRelayClient
	lanePath string
	logs     *t1002Log
	hub      *HubServer
	dest     *hubAgent
	src      *hubAgent
}

func t1002Lanes() string {
	encoded, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{
		t1002Lane: {Machine: "host-a", Pane: "w1:pA"},
		t1002Sink: {Machine: "host-a", Pane: "w1:p2", Sink: true},
	}})
	return string(encoded)
}

func t1002Start(t *testing.T) *t1002Rig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	rig := &t1002Rig{t: t, fake: fake, client: client, lanePath: r20LanesFile(t, t1002Lanes()), logs: &t1002Log{}}
	rig.startHub()
	return rig
}

func (rig *t1002Rig) startHub() {
	rig.t.Helper()
	logger := slog.New(slog.NewTextHandler(rig.logs, nil))
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node", "host-b": "node-b"},
		ReportRelayPath: rig.lanePath,
		Logger:          logger,
		handoffkeep:     rig.client,
	})
	if err != nil {
		rig.t.Fatal(err)
	}
	hub.r19a.relayAckTimeout = time.Hour
	rig.hub = hub
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.src = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	hub.nodes["host-b"] = &hubNodeRecord{agent: rig.src}
	hub.mu.Unlock()
}

// restart is what a deploy does: a fresh hub over the same lanes file and the
// same handoffkeep, then the startup replay and the hello replay.
func (rig *t1002Rig) restart() {
	rig.t.Helper()
	rig.startHub()
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	rig.hub.replayUndeliveredLaneEvents(context.Background())
}

func (rig *t1002Rig) sendLaneEvent(eventID, text string) hubRelayInjectEvent {
	rig.t.Helper()
	event := hubJobEventPayload{JobID: laneEventTransportID(t1002Lane, eventID), Epoch: 1, OwnerLane: t1002Lane, EventID: eventID, Text: text}
	rig.hub.relayLaneEvent(event, rig.src)
	return rig.directive()
}

func (rig *t1002Rig) directive() hubRelayInjectEvent {
	rig.t.Helper()
	select {
	case directive := <-rig.dest.relays:
		if directive.EventID < 1 {
			rig.t.Fatalf("directive carried no durable id: %+v", directive)
		}
		return directive
	case <-time.After(2 * time.Second):
		rig.t.Fatal("no relay.inject was queued for host-a")
	}
	return hubRelayInjectEvent{}
}

func (rig *t1002Rig) injects() []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case directive := <-rig.dest.relays:
			out = append(out, directive)
		default:
			return out
		}
	}
}

func (rig *t1002Rig) unconfirmed(directive hubRelayInjectEvent, reason string) {
	rig.t.Helper()
	t650Send(rig.t, rig.hub, "host-a", rig.dest, "relay.unconfirmed", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, Reason: reason, OriginalEventID: directive.EventID})
}

func (rig *t1002Rig) delivered(directive hubRelayInjectEvent) {
	rig.t.Helper()
	t650Send(rig.t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
}

func (rig *t1002Rig) rows() []handoffkeepRelayEvent {
	rig.t.Helper()
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	out := make([]handoffkeepRelayEvent, 0, len(rig.fake.rows))
	for _, row := range rig.fake.rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (rig *t1002Rig) row(id int64) handoffkeepRelayEvent {
	rig.t.Helper()
	for _, row := range rig.rows() {
		if row.ID == id {
			return row
		}
	}
	rig.t.Fatalf("no durable row %d", id)
	return handoffkeepRelayEvent{}
}

func (rig *t1002Rig) markers() []handoffkeepRelayEvent {
	rig.t.Helper()
	var out []handoffkeepRelayEvent
	for _, row := range rig.rows() {
		if isUnconfirmedMarkRecord(row) {
			out = append(out, row)
		}
	}
	return out
}

// AC1 + AC5: a row acked relay.unconfirmed gets exactly one labelled replay
// on the next restart and is then retired; a second restart injects nothing.
// The durable mark must already be on record while the live window still
// exists — the observe hook checks hub state at the moment the marker POST
// arrives.
func TestT1002MarkedRowReplaysOnceWithLabel(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-a", "quarterly report body")
	eventID := directive.EventID

	pendingKey := relayPendingKey(directive.EventID, directive.JobID)
	markObserved := false
	rig.fake.mu.Lock()
	rig.fake.observe = func(method, path string) {
		if method != http.MethodPost || path != "/v1/relay/events" {
			return
		}
		rig.hub.mu.Lock()
		_, live := rig.hub.r19a.relayPending[pendingKey]
		rig.hub.mu.Unlock()
		if live {
			markObserved = true
		}
	}
	rig.fake.mu.Unlock()
	rig.unconfirmed(directive, "maybe_in_pane")
	rig.fake.mu.Lock()
	rig.fake.observe = nil
	rig.fake.mu.Unlock()

	if !markObserved {
		t.Fatal("the unconfirmed mark was written after the live window retired (AC5)")
	}
	rig.hub.mu.Lock()
	_, live := rig.hub.r19a.relayPending[pendingKey]
	rig.hub.mu.Unlock()
	if live {
		t.Fatal("an accepted unconfirmed left the live window registered")
	}
	if rig.logs.count("relay delivery unconfirmed") != 1 {
		t.Fatalf("unconfirmed WARNs=%d, want 1: %s", rig.logs.count("relay delivery unconfirmed"), rig.logs.String())
	}
	row := rig.row(eventID)
	if row.DeliveredAt != "" {
		t.Fatalf("unconfirmed marked the row delivered: %+v", row)
	}
	markers := rig.markers()
	if len(markers) != 1 || markers[0].EventID != unconfirmedMarkEventID(eventID) || markers[0].DeliveredAt != "" {
		t.Fatalf("markers after unconfirmed: %+v", markers)
	}

	// Age the original send so the label cannot pass by using the replay
	// time (M3): it must name the row's own event_time.
	key := fakeHandoffkeepStoredKey(rig.row(eventID))
	rig.fake.mu.Lock()
	rig.fake.rows[key].EventTime = "2026-09-29T20:15:30Z"
	rig.fake.mu.Unlock()

	// First restart: exactly one inject, labelled with the row's original
	// event_time rendered in KST.
	rig.restart()
	injects := rig.injects()
	if len(injects) != 1 {
		t.Fatalf("first restart injected %d rows, want 1", len(injects))
	}
	row = rig.row(eventID)
	firstSent, err := time.Parse(time.RFC3339, row.EventTime)
	if err != nil {
		t.Fatalf("row has no event_time to label: %q", row.EventTime)
	}
	wantStamp := firstSent.In(relayUnconfirmedLabelZone).Format("2006-01-02 15:04:05 MST")
	wantText := "[replayed after hub restart - first sent " + wantStamp + " - may be a duplicate, do not re-run] " + relayTextForKind("lane.event", hubJobEventPayload{OwnerLane: t1002Lane, Text: "quarterly report body"})
	if injects[0].Text != wantText {
		t.Fatalf("replayed text=%q, want %q", injects[0].Text, wantText)
	}
	if injects[0].EventID != eventID || injects[0].JobID != directive.JobID || injects[0].Pane != directive.Pane {
		t.Fatalf("replayed directive identity=%+v", injects[0])
	}
	if !strings.Contains(rig.logs.String(), "relay unconfirmed replay injected") {
		t.Fatalf("the one-shot replay left no log line: %s", rig.logs.String())
	}
	row = rig.row(eventID)
	if row.DeliveredAt == "" || row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("marked row after its replay: delivered_at=%q delivered_to=%q", row.DeliveredAt, row.DeliveredTo)
	}
	markers = rig.markers()
	if len(markers) != 1 || markers[0].DeliveredAt == "" || markers[0].DeliveredTo != "hub/replay-retired:"+relayUnconfirmedMarkRetireReason {
		t.Fatalf("marker after the replay: %+v", markers)
	}

	// Second restart: nothing left to inject.
	rig.restart()
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("second restart injected %d rows, want 0", len(injects))
	}
	if got := rig.logs.count("relay unconfirmed replay injected"); got != 1 {
		t.Fatalf("one-shot replay log lines=%d, want 1", got)
	}
}

// AC2a: the replay's own report does not matter — an unconfirmed report on
// the replayed window still leaves the row retired and mints no new mark.
func TestT1002ReplayUnconfirmedStillRetired(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-b", "body b")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")
	rig.restart()
	injects := rig.injects()
	if len(injects) != 1 {
		t.Fatalf("restart injected %d rows, want 1", len(injects))
	}
	rig.unconfirmed(injects[0], "maybe_in_pane again")
	if got := rig.logs.count("relay delivery unconfirmed"); got != 2 {
		t.Fatalf("unconfirmed WARNs=%d, want 2", got)
	}
	if markers := rig.markers(); len(markers) != 1 {
		t.Fatalf("a second unconfirmed minted %d markers, want the original 1", len(markers))
	}
	row := rig.row(eventID)
	if row.DeliveredAt == "" || row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after second unconfirmed: delivered_at=%q delivered_to=%q", row.DeliveredAt, row.DeliveredTo)
	}
	rig.restart()
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("restart after second unconfirmed injected %d rows, want 0", len(injects))
	}
}

// AC2b: the replay ending held — then dropped by its node — is still
// retired; no restart brings it back.
func TestT1002ReplayHeldStillRetired(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-c", "body c")
	rig.unconfirmed(directive, "maybe_in_pane")
	rig.restart()
	injects := rig.injects()
	if len(injects) != 1 {
		t.Fatalf("restart injected %d rows, want 1", len(injects))
	}
	labelled := injects[0]
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.held", relayHeldPayload{EventID: labelled.EventID, JobID: labelled.JobID, Pane: labelled.Pane, Lane: t1002Lane, Reason: "working", HeldSince: time.Now().UTC().Format(time.RFC3339Nano)})
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", relayDroppedPayload{JobID: labelled.JobID, Pane: labelled.Pane, Lane: t1002Lane, OriginalEventID: labelled.EventID, Reason: "ttl"})
	rig.restart()
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("restart after held+dropped injected %d rows, want 0", len(injects))
	}
}

// AC3: an unconfirmed row a late relay.delivered later proves is closed
// under the real machine/pane, is never replayed, and carries no label.
func TestT1002LateDeliveredMarkedRowNeverReplays(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-d", "body d")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")
	if markers := rig.markers(); len(markers) != 1 {
		t.Fatalf("markers after unconfirmed: %d", len(markers))
	}
	// The window is gone, so this delivered resolves through the late path —
	// exactly what recordLateRelayDelivery exists for.
	rig.delivered(directive)
	row := rig.row(eventID)
	if row.DeliveredAt == "" || row.DeliveredTo != "host-a/w1:pA" {
		t.Fatalf("late proof did not close the row normally: delivered_at=%q delivered_to=%q", row.DeliveredAt, row.DeliveredTo)
	}
	rig.restart()
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("restart injected %d rows after late proof, want 0", len(injects))
	}
	if got := rig.logs.count("replayed after hub restart"); got != 0 {
		t.Fatalf("a delivered row was replayed: %s", rig.logs.String())
	}
	// The spent marker retires on sight instead of staying undelivered.
	markers := rig.markers()
	if len(markers) != 1 || markers[0].DeliveredAt == "" || markers[0].DeliveredTo != "hub/replay-retired:"+relayUnconfirmedMarkRetireReason {
		t.Fatalf("marker after late proof: %+v", markers)
	}
}

// AC4: every other undelivered row class is untouched — never-attempted,
// exhausted, chat, and sink rows replay exactly as before across two
// restarts, with no label and no retire, even alongside a marked row.
func TestT1002UnmarkedRowsReplayUnchanged(t *testing.T) {
	rig := t1002Start(t)
	ctx := context.Background()

	// never-attempted: a durable row no injection ever reached.
	neverEvent := hubJobEventPayload{JobID: laneEventTransportID(t1002Lane, "ev-never"), Epoch: 1, OwnerLane: t1002Lane, EventID: "ev-never", Text: "never text"}
	if _, _, err := rig.client.appendEvent(ctx, rig.hub.relayEventRequest("lane.event", neverEvent, reportRelayRoute{})); err != nil {
		t.Fatal(err)
	}
	// exhausted: attempts already spent.
	spent := hubJobEventPayload{JobID: laneEventTransportID(t1002Lane, "ev-spent"), Epoch: 1, OwnerLane: t1002Lane, EventID: "ev-spent", Text: "spent text"}
	for i := 0; i < relayReplayMaxAttempts; i++ {
		if _, _, err := rig.client.appendEvent(ctx, rig.hub.relayEventRequest("lane.event", spent, reportRelayRoute{})); err != nil {
			t.Fatal(err)
		}
	}
	// chat: no chat store is configured, so its own gate answers "inject" —
	// the disposition replayRelayEvent sees is unchanged.
	chat := hubJobEventPayload{JobID: laneEventTransportID(t1002Lane, "chat-42"), Epoch: 1, OwnerLane: t1002Lane, EventID: "chat-42", Text: "chat text"}
	if _, _, err := rig.client.appendEvent(ctx, rig.hub.relayEventRequest("lane.event", chat, reportRelayRoute{})); err != nil {
		t.Fatal(err)
	}
	// sink: a durable row for a sink lane is never a pane target.
	sink := hubJobEventPayload{JobID: laneEventTransportID(t1002Sink, "ev-sink"), Epoch: 1, OwnerLane: t1002Sink, EventID: "ev-sink", Text: "sink text"}
	if _, _, err := rig.client.appendEvent(ctx, rig.hub.relayEventRequest("lane.event", sink, reportRelayRoute{})); err != nil {
		t.Fatal(err)
	}
	// A marked row shares the same listing to prove it does not perturb the
	// others.
	markedDirective := rig.sendLaneEvent("ev-1002-marked", "marked body")
	rig.unconfirmed(markedDirective, "maybe_in_pane")

	restartTexts := func() map[string]string {
		rig.restart()
		out := map[string]string{}
		for _, directive := range rig.injects() {
			out[directive.JobID] = directive.Text
		}
		return out
	}
	first := restartTexts()
	second := restartTexts()

	if len(first) != 3 {
		t.Fatalf("first restart injected %d rows, want 3 (never+chat+marked): %v", len(first), first)
	}
	if len(second) != 2 {
		t.Fatalf("second restart injected %d rows, want 2 (never+chat): %v", len(second), second)
	}
	neverText := relayTextForKind("lane.event", neverEvent)
	chatText := relayTextForKind("lane.event", chat)
	if first[neverEvent.JobID] != neverText || second[neverEvent.JobID] != neverText {
		t.Fatalf("never-attempted row text changed across restarts: %q / %q, want %q", first[neverEvent.JobID], second[neverEvent.JobID], neverText)
	}
	if first[chat.JobID] != chatText || second[chat.JobID] != chatText {
		t.Fatalf("chat row text changed across restarts: %q / %q, want %q", first[chat.JobID], second[chat.JobID], chatText)
	}
	if !strings.HasPrefix(first[markedDirective.JobID], "[replayed after hub restart - first sent ") {
		t.Fatalf("marked row replayed without the label: %q", first[markedDirective.JobID])
	}
	for jobID, text := range second {
		if strings.Contains(text, "replayed after hub restart") {
			t.Fatalf("an unmarked row was labelled: %s=%q", jobID, text)
		}
	}
	// The exhausted and sink rows never reached a pane; the marked row's own
	// text only ever appeared once.
	if _, injected := first[spent.JobID]; injected {
		t.Fatal("an exhausted row was injected")
	}
	if _, injected := first[sink.JobID]; injected {
		t.Fatal("a sink row was injected")
	}
	if got := rig.logs.count("relay unconfirmed replay injected"); got != 1 {
		t.Fatalf("marked replay log lines=%d, want 1", got)
	}
}

// A job.* row carries the same mark: the labelled replay prefixes the
// kind-formatted text once, then the row retires.
func TestT1002MarkedJobRowReplaysOnceWithLabel(t *testing.T) {
	rig := t1002Start(t)
	event := hubJobEventPayload{JobID: "job-t1002-1", Epoch: 1, OwnerLane: t1002Lane, ReportPath: "report.md", ReportLastLine: "VERDICT: DONE"}
	rig.hub.relayJobCompletion(event)
	directive := rig.directive()
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	rig.restart()
	injects := rig.injects()
	if len(injects) != 1 {
		t.Fatalf("restart injected %d rows, want 1", len(injects))
	}
	plain := relayTextForKind("job.completed", event)
	want := relayUnconfirmedLabel(rig.row(eventID)) + plain
	if injects[0].Text != want {
		t.Fatalf("job replay text=%q, want %q", injects[0].Text, want)
	}
	rig.restart()
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("second restart injected %d rows, want 0", len(injects))
	}
}

// The hello trigger — a node registration replay — applies the same one-shot
// rule: on a fresh hub (the dedupe map is what makes a hello on the same hub
// skip a live-sent row, unchanged from main) the row is labelled once and
// then gone for every later hello.
func TestT1002HelloReplayLabelsOnce(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-hello", "hello body")
	rig.unconfirmed(directive, "maybe_in_pane")

	rig.startHub()
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	injects := rig.injects()
	if len(injects) != 1 || !strings.HasPrefix(injects[0].Text, "[replayed after hub restart - first sent ") {
		t.Fatalf("hello replay injects=%+v", injects)
	}
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	if injects := rig.injects(); len(injects) != 0 {
		t.Fatalf("second hello injected %d rows, want 0", len(injects))
	}
}

// An unconfirmed report with no live window keeps the old path: one WARN,
// unknown-message accounting, no marker.
func TestT1002UnconfirmedWithoutWindowMarksNothing(t *testing.T) {
	rig := t1002Start(t)
	warns := func() int { return rig.logs.count("relay unconfirmed report matched no live window") }
	before := warns()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.unconfirmed", relayAckPayload{JobID: "job-t1002-ghost", Pane: "w1:pA", Reason: "maybe_in_pane", OriginalEventID: 987654})
	if got := warns() - before; got != 1 {
		t.Fatalf("windowless unconfirmed WARNs=%d, want 1", got)
	}
	if markers := rig.markers(); len(markers) != 0 {
		t.Fatalf("a windowless unconfirmed minted a marker: %+v", markers)
	}
}

// An unconfirmed on a window that is already resolved — the row delivered —
// writes no marker either.
func TestT1002UnconfirmedOnDeliveredRowMarksNothing(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-closed", "closed body")
	rig.delivered(directive)
	// A stale node retries the report; the window is gone, the row is closed.
	rig.unconfirmed(directive, "maybe_in_pane")
	if markers := rig.markers(); len(markers) != 0 {
		t.Fatalf("an unconfirmed on a closed row minted a marker: %+v", markers)
	}
}

// The lane-stall observer keeps counting a marked row as ordinary backlog
// until its one replay retires it — bounded by the next restart, never
// forever. The marker row itself is invisible to it (an unroutable lane).
func TestT1002StallCountsMarkedRowUntilRetired(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1002-stall", "stall body")
	eventID := directive.EventID
	// Age the row so the stall observation would count it.
	rig.fake.mu.Lock()
	for _, row := range rig.fake.rows {
		if row.ID == eventID {
			row.ReceivedAt = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
		}
	}
	rig.fake.mu.Unlock()
	rig.unconfirmed(directive, "maybe_in_pane")

	obs := rig.hub.observeLaneUndelivered(context.Background(), t1002Lane, time.Now())
	if !obs.observed || !obs.stalled || obs.oldestEventID != eventID || obs.undelivered != 1 {
		t.Fatalf("marked row was not counted as backlog: %+v", obs)
	}
	rig.restart()
	obs = rig.hub.observeLaneUndelivered(context.Background(), t1002Lane, time.Now())
	if obs.stalled || obs.undelivered != 0 {
		t.Fatalf("retired row still counted as backlog: %+v", obs)
	}
}
