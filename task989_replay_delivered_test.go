package panewire

// #989: hub pw-322d3b3 restarted and its hello replay re-injected twelve
// durable rows whose panes had already shown them. Two mechanisms feed the
// incident: a relay.delivered that cannot be matched dies silently (the
// pending window demanded pane equality, and the late path demanded the
// row's stored pane or the current route's pane — a lane re-pointed between
// forward and ack stranded the row), and a delivered item that cannot name
// its durable row skipped the node's relay_delivered record without a word.
// These tests pin the fixed behavior end to end: lane.event in, durable row
// persisted, inject to the node, ack back, restart, no re-injection.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const t989Lane = "lane-t989"

// t989Lanes writes the lanes document with lane-t989 living on host-a at the
// given pane; repointing the lane is a file rewrite.
func t989Lanes(pane string) string {
	encoded, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{t989Lane: {Machine: "host-a", Pane: pane}}})
	return string(encoded)
}

// t989Rig is a hub over a fake handoffkeep plus the destination node (host-a)
// as a real HubClient with fake herdr, so offers, holds, releases and acks
// all run the production code paths. host-b is the lane.event producer.
type t989Rig struct {
	t        *testing.T
	fake     *fakeHandoffkeep
	client   *handoffkeepRelayClient
	lanePath string
	logs     *bytes.Buffer
	hub      *HubServer
	dest     *hubAgent
	src      *hubAgent
	node     *HubClient
	events   chan hubClientEvent
	prompts  *[]string
	herdr    *r27FakeHerdr
	store    *Store
}

func t989Start(t *testing.T, pane string) *t989Rig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	store := NewMemoryStore(t)
	t.Cleanup(func() { store.Close() })
	rig := &t989Rig{t: t, fake: fake, client: client, logs: &bytes.Buffer{}, lanePath: r20LanesFile(t, t989Lanes(pane)), store: store}
	rig.startHub()
	rig.herdr = &r27FakeHerdr{t: t, waitStatus: "idle", started: make(chan struct{}, 8), getStatusByPane: map[string]string{}}
	rig.node, rig.prompts, rig.events = r27Node(t, store, rig.herdr)
	return rig
}

func (rig *t989Rig) startHub() {
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
	rig.hub = hub
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.src = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	hub.nodes["host-b"] = &hubNodeRecord{agent: rig.src}
	hub.mu.Unlock()
}

// repoint moves the lane to a new pane by rewriting the lanes file — what an
// operator's lanes.json edit does mid-flight.
func (rig *t989Rig) repoint(pane string) {
	rig.t.Helper()
	if err := os.WriteFile(rig.lanePath, []byte(t989Lanes(pane)), 0600); err != nil {
		rig.t.Fatal(err)
	}
}

// restart is what a deploy does to the hub: a new server with no memory over
// the same lanes file and the same handoffkeep, then the startup replay and
// the per-hello lane replay. It returns the restarted hub's event reader.
func (rig *t989Rig) restart() func(kind string) []json.RawMessage {
	rig.t.Helper()
	rig.startHub()
	events := r20t5Subscribe(rig.t, rig.hub)
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	return events
}

func (rig *t989Rig) sendLaneEvent(eventID, text string) {
	rig.t.Helper()
	t650Send(rig.t, rig.hub, "host-b", rig.src, "lane.event", map[string]any{"owner_lane": t989Lane, "event_id": eventID, "text": text, "epoch": 1})
}

// directive pops the next queued relay.inject for host-a's node.
func (rig *t989Rig) directive() hubRelayInjectEvent {
	rig.t.Helper()
	select {
	case directive := <-rig.dest.relays:
		return directive
	case <-time.After(2 * time.Second):
		rig.t.Fatal("no relay.inject was queued for host-a")
	}
	return hubRelayInjectEvent{}
}

// offer feeds a directive through the wire parser into the node's busy-relay
// offer, the same handoff handleRelayInject performs.
func (rig *t989Rig) offer(directive hubRelayInjectEvent) {
	rig.t.Helper()
	wire, err := json.Marshal(directive)
	if err != nil {
		rig.t.Fatal(err)
	}
	message, ok := parseHubOutbound(wire)
	if !ok {
		rig.t.Fatalf("directive did not parse: %s", wire)
	}
	rig.node.relayBusyManager().offer(context.Background(), message)
}

// flush delivers every event the node emitted back to the hub over the real
// inbound path — the same envelope the node's socket writer produces.
func (rig *t989Rig) flush() {
	rig.t.Helper()
	for {
		select {
		case event := <-rig.events:
			t650Send(rig.t, rig.hub, "host-a", rig.dest, event.Kind, json.RawMessage(event.Payload))
		default:
			return
		}
	}
}

func (rig *t989Rig) row(id int64) handoffkeepRelayEvent {
	rig.t.Helper()
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	for _, row := range rig.fake.rows {
		if row.ID == id {
			return *row
		}
	}
	rig.t.Fatalf("no durable row %d", id)
	return handoffkeepRelayEvent{}
}

func (rig *t989Rig) assertDelivered(id int64, pane string) {
	rig.t.Helper()
	row := rig.row(id)
	if row.DeliveredAt == "" || row.DeliveredTo != "host-a/"+pane {
		rig.t.Fatalf("row %d after ack: delivered_at=%q delivered_to=%q, want delivered to host-a/%s", id, row.DeliveredAt, row.DeliveredTo, pane)
	}
}

// assertNoReplay fails when a restarted hub re-injected anything.
func (rig *t989Rig) assertNoReplay(events func(kind string) []json.RawMessage, name string) {
	rig.t.Helper()
	if injected := drainRelays(rig.dest); injected != 0 {
		rig.t.Fatalf("%s: restart re-injected %d rows, want 0", name, injected)
	}
	if injected := events("relay.replay_injected"); len(injected) != 0 {
		rig.t.Fatalf("%s: restart broadcast %d relay.replay_injected, want 0", name, len(injected))
	}
}

// awaitDelivered flushes node emits until every listed row is delivered on
// both sides, or fails. The release happens on the wait goroutine, so the
// emits arrive asynchronously.
func (rig *t989Rig) awaitDelivered(ids map[int64]bool, pane string) {
	rig.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		done := true
		for id := range ids {
			if rig.fake.deliveredToFor(id) == "" {
				done = false
				break
			}
		}
		if done {
			break
		}
		rig.flush()
		if time.Now().After(deadline) {
			rig.t.Fatalf("not all rows closed: %+v", ids)
		}
		time.Sleep(5 * time.Millisecond)
	}
	rig.flush()
	for id := range ids {
		rig.assertDelivered(id, pane)
		if _, found, err := rig.store.RelayDeliveredByKey(context.Background(), t989Lane, id); err != nil || !found {
			rig.t.Fatalf("relay_delivered missing for %d: found=%v err=%v", id, found, err)
		}
	}
}

// AC1 idle path: one lane.event through the whole stack — producer's wire
// event, durable row, directive, node inject on an idle pane, node's ack back
// — then a hub restart must not re-inject, and both records must exist.
func TestT989IdleDeliveryClosesRowAcrossRestart(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "idle"

	rig.sendLaneEvent("t989-idle-1", "hello the pane")
	directive := rig.directive()
	if directive.Pane != "w1:pA" || directive.EventID < 1 || directive.Lane != t989Lane {
		t.Fatalf("directive=%+v", directive)
	}
	rig.offer(directive)
	if delivered := len(*rig.prompts); delivered != 1 {
		t.Fatalf("node injected %d prompts, want 1", delivered)
	}
	rig.flush()
	rig.assertDelivered(directive.EventID, "w1:pA")
	if _, found, err := rig.store.RelayDeliveredByKey(context.Background(), t989Lane, directive.EventID); err != nil || !found {
		t.Fatalf("node relay_delivered row missing: found=%v err=%v", found, err)
	}

	rig.assertNoReplay(rig.restart(), "idle delivery")
}

// AC1 held/batched path: three lane.events arrive while the pane is working,
// hold together, and deliver as one batch when it idles — every member gets
// its local delivered record and every durable row closes before a restart.
func TestT989HeldBatchClosesEveryRowAcrossRestart(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	// Gate the pane's wait so all three offers land in relay_held before any
	// release can run.
	rig.herdr.waitGate = make(chan struct{})

	ids := map[int64]bool{}
	for index := 1; index <= 3; index++ {
		rig.sendLaneEvent(fmt.Sprintf("t989-batch-%d", index), fmt.Sprintf("held note %d", index))
		directive := rig.directive()
		rig.offer(directive)
		ids[directive.EventID] = true
	}
	rig.flush() // the three relay.held reach the hub while the wait is gated
	close(rig.herdr.waitGate)
	rig.awaitDelivered(ids, "w1:pA")
	if len(*rig.prompts) != 1 {
		t.Fatalf("a 3-member hold injected %d prompts, want the single batch", len(*rig.prompts))
	}
	rig.assertNoReplay(rig.restart(), "held batch")
}

// The #989 mechanism, hub side: the lane moved panes between the inject and
// the ack — the stored pane is the first target, the live route has already
// moved on, and the node's delivery happened in between. A pane difference
// alone must not drop the ack; the machine is the entitlement.
func TestT989RepointedLaneAckIsRecorded(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	// The first inject goes out on the original pane; its ack window dies
	// with the row still open — the lost-ack shape every stranded row comes
	// from. Expiry releases the in-flight claim so a replay may spend the
	// next attempt.
	rig.sendLaneEvent("t989-moved-1", "forwarded before the move")
	first := rig.directive()
	if first.Pane != "w1:pA" {
		t.Fatalf("first directive pane=%q, want w1:pA", first.Pane)
	}
	rig.hub.expireRelayAck(relayPendingKey(first.EventID, first.JobID))

	// The lane re-points and the replay re-injects onto the new pane.
	rig.repoint("w1:pB")
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	second := rig.directive()
	if second.Pane != "w1:pB" || second.EventID != first.EventID {
		t.Fatalf("replayed directive=%+v, want row %d on w1:pB", second, first.EventID)
	}
	// The second window is lost too (the restart/expiry shape that strands
	// rows) — only the durable row, the machine and the route remain.
	rig.hub.forgetRelayAckEvent(second.EventID, second.JobID)

	// The lane moves again before the node's ack lands: neither the stored
	// pane (w1:pA) nor the live route (w1:pC) is where the node delivered.
	rig.repoint("w1:pC")
	rig.herdr.getStatusByPane["w1:pB"] = "idle"
	rig.offer(second)
	rig.flush()

	rig.assertDelivered(first.EventID, "w1:pB")
	rig.assertNoReplay(rig.restart(), "repointed ack")
}

// The late variant of the same move: the window expired before the ack, so
// recordLateRelayDelivery has to bind the row by event id and machine while
// the pane differs from the live route.
func TestT989LateAckAfterRepointIsRecorded(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-moved-2", "window lost before the ack")
	directive := rig.directive()
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))

	rig.herdr.getStatusByPane["w1:pA"] = "idle"
	rig.offer(directive)
	// The deliver happened on the row's own pane, but the lane re-pointed
	// away before the ack landed.
	rig.repoint("w1:pB")
	rig.flush()

	rig.assertDelivered(directive.EventID, "w1:pA")
	rig.assertNoReplay(rig.restart(), "late ack after repoint")
}

// The batched variant the b953 rows showed: a re-pointed lane's replay lands
// three rows on the working pane, they hold and release as one batch, and
// every member's ack must close its durable row even though the stored pane
// and the live route both differ from the delivered one.
func TestT989HeldBatchAfterRepointRecordsEveryMember(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	pending := []hubRelayInjectEvent{}
	for index := 1; index <= 3; index++ {
		rig.sendLaneEvent(fmt.Sprintf("t989-move-batch-%d", index), fmt.Sprintf("held note %d", index))
		pending = append(pending, rig.directive()) // first injects on w1:pA
	}
	// Their ack windows die unrecorded; expiry releases each in-flight claim.
	for _, directive := range pending {
		rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
	}

	// Re-point and replay — the b953 shape: three re-injections on the new
	// pane of a working session.
	rig.repoint("w1:pB")
	rig.hub.replayUndeliveredLaneEvents(context.Background())

	rig.herdr.getStatusByPane["w1:pB"] = "working"
	rig.herdr.waitGate = make(chan struct{})
	ids := map[int64]bool{}
	replayed := []hubRelayInjectEvent{}
	for index := 1; index <= 3; index++ {
		directive := rig.directive()
		if directive.Pane != "w1:pB" {
			t.Fatalf("replayed directive pane=%q, want w1:pB", directive.Pane)
		}
		rig.offer(directive)
		ids[directive.EventID] = true
		replayed = append(replayed, directive)
	}
	rig.flush() // relay.held for the three
	// The lane moves again while the items sit held on w1:pB, and the replay
	// windows are gone — the acks will land on the late path.
	rig.repoint("w1:pC")
	for _, directive := range replayed {
		rig.hub.forgetRelayAckEvent(directive.EventID, directive.JobID)
	}
	close(rig.herdr.waitGate)

	rig.awaitDelivered(ids, "w1:pB")
	rig.assertNoReplay(rig.restart(), "held batch after repoint")
}

// AC2 shape: the ack names no durable row (original_event_id omitted, as an
// inject that left before the id was assigned produces) — the row is found
// through the lane.event transport id the job_id echoes.
func TestT989ZeroEventIDAckResolvesTheRow(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-idless-1", "delivered before the id arrived")
	directive := rig.directive()
	transportID := directive.JobID
	// The window is gone (restart/expiry) — only the transport id remains.
	rig.hub.forgetRelayAckEvent(directive.EventID, transportID)

	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: transportID, Pane: "w1:pA"})
	rig.assertDelivered(directive.EventID, "w1:pA")
	rig.assertNoReplay(rig.restart(), "id-less ack")
}

// The same resolution through a live window: a pending that was registered
// before the durable id was known still closes its row once the maps can
// name it.
func TestT989ZeroIDPendingResolvesRowOnDelivered(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-idless-2", "id assigned after inject")
	directive := rig.directive()
	transportID := directive.JobID
	event := relayEventFromRecord(rig.row(directive.EventID))
	// Replace the id-keyed window with the 0-id shape a pre-id inject leaves.
	rig.hub.forgetRelayAckEvent(directive.EventID, transportID)
	rig.hub.registerRelayAckEvent("lane.event", event, "host-a", "w1:pA", 0)
	rig.hub.armRelayAckEvent(0, transportID)

	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: transportID, Pane: "w1:pA"})
	rig.assertDelivered(directive.EventID, "w1:pA")
}

// An ack arriving on a superseded connection is still the machine's report on
// its own pane — connection freshness is not what binds it to the row.
func TestT989StaleConnectionAckStillClosesRow(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-stale-1", "acked on a superseded connection")
	directive := rig.directive()

	// The node reconnects: its old agent is stale, the new one is live.
	stale := rig.dest
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.hub.mu.Lock()
	rig.hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	rig.hub.mu.Unlock()

	t650Send(t, rig.hub, "host-a", stale, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
	rig.assertDelivered(directive.EventID, "w1:pA")
}

// B: every ack the hub cannot record leaves a WARN naming the reason — no
// silent drop path remains.
func TestT989UnrecordedAcksLeaveWarnTrail(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-warn-1", "the row the acks will circle")
	directive := rig.directive()
	transportID := directive.JobID

	// Unknown durable id (the expired window pushes every ack onto the late
	// path for the rest of this test).
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, transportID))
	warns := func() int { return strings.Count(rig.logs.String(), "WARN") }
	before := warns()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: transportID, Pane: "w1:pA", OriginalEventID: directive.EventID + 5000})
	if got := strings.Count(rig.logs.String(), "relay delivery ack named an unknown durable row"); got != 1 {
		t.Fatalf("unknown-id ack left %d WARN lines, want 1: %s", got, rig.logs.String())
	}

	// Job mismatch: the id names this row, the job names a different event.
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: "lane-event-00000000000000000000000000000000", Pane: "w1:pA", OriginalEventID: directive.EventID})
	if got := strings.Count(rig.logs.String(), "relay delivery ack job mismatch"); got != 1 {
		t.Fatalf("job-mismatch ack left %d WARN lines, want 1: %s", got, rig.logs.String())
	}

	// Machine mismatch: a node may only close rows routed to it.
	t650Send(t, rig.hub, "host-b", rig.src, "relay.delivered", relayAckPayload{JobID: transportID, Pane: "w1:pA", OriginalEventID: directive.EventID})
	if got := strings.Count(rig.logs.String(), "relay delivery ack from a machine the row was not routed to"); got != 1 {
		t.Fatalf("machine-mismatch ack left %d WARN lines, want 1: %s", got, rig.logs.String())
	}

	// Write failure on the late path: the matching ack arrives but
	// handoffkeep refuses.
	rig.fake.mu.Lock()
	rig.fake.deliveredStatus = http.StatusInternalServerError
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: transportID, Pane: "w1:pA", OriginalEventID: directive.EventID})
	if got := strings.Count(rig.logs.String(), "relay delivery was not recorded"); got != 1 {
		t.Fatalf("write failure left %d WARN lines, want 1: %s", got, rig.logs.String())
	}
	if got := rig.row(directive.EventID).DeliveredAt; got != "" {
		t.Fatalf("the rejected write still closed the row: delivered_at=%q", got)
	}

	// The same write failure through a live window.
	rig.sendLaneEvent("t989-warn-2", "a second row for the pending path")
	second := rig.directive()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: second.JobID, Pane: second.Pane, OriginalEventID: second.EventID})
	if got := strings.Count(rig.logs.String(), "relay delivery was not recorded"); got != 2 {
		t.Fatalf("in-window write failure left %d WARN lines, want 2: %s", got, rig.logs.String())
	}
	rig.fake.mu.Lock()
	rig.fake.deliveredStatus = 0
	rig.fake.mu.Unlock()

	// A delivered ack that decodes into nothing the hub can use still names
	// itself in the journal.
	rig.hub.handleAgentMessage("host-a", "fixture", rig.dest, []byte(`{"type":"event","kind":"relay.delivered","payload":{"job_id":42}}`))
	if got := strings.Count(rig.logs.String(), "relay ack could not be decoded"); got != 1 {
		t.Fatalf("undecodable ack left %d WARN lines, want 1: %s", got, rig.logs.String())
	}

	// An unconfirmed report consumed by a live window is visible too.
	rig.sendLaneEvent("t989-warn-3", "consumed unconfirmed")
	third := rig.directive()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.unconfirmed", relayAckPayload{JobID: third.JobID, Pane: third.Pane, Reason: "maybe_in_pane", OriginalEventID: third.EventID})
	if got := strings.Count(rig.logs.String(), "relay delivery unconfirmed"); got != 1 {
		t.Fatalf("consumed unconfirmed left %d WARN lines, want 1: %s", got, rig.logs.String())
	}
	if delta := warns() - before; delta != 7 {
		t.Fatalf("expected exactly 7 WARN lines for the seven unrecorded shapes, got %d", delta)
	}
}

// The node side of B: a delivered item that cannot name its durable row must
// not pass silently — the held path never admits an id-less item, so this is
// the legacy fire-and-forget shape landing direct.
func TestT989IdlessDeliveryStillWarns(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "idle"

	var warns []string
	rig.node.warn = func(message string) { warns = append(warns, message) }
	// A legacy-shape inject carries no lane and no event id.
	rig.node.relayBusyManager().offer(context.Background(), hubOutboundMessage{Type: "relay.inject", Pane: "w1:pA", JobID: "job-legacy", Text: "a note with no durable identity"})
	if len(*rig.prompts) != 1 {
		t.Fatalf("legacy inject produced %d prompts, want 1", len(*rig.prompts))
	}
	count := 0
	for _, warn := range warns {
		if strings.Contains(warn, "no durable event id") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("delivered id-less item left %d skip WARNs, want 1: %v", count, warns)
	}
}

// AC3 stays: a row nobody ever acknowledged is still replayed after restart.
func TestT989UndeliveredRowStillReplays(t *testing.T) {
	rig := t989Start(t, "w1:pA")

	rig.sendLaneEvent("t989-owed-1", "never delivered")
	directive := rig.directive()

	events := rig.restart()
	replayed := rig.directive()
	if replayed.EventID != directive.EventID || replayed.Pane != "w1:pA" {
		t.Fatalf("undelivered row replayed as %+v, want row %d on w1:pA", replayed, directive.EventID)
	}
	if injected := events("relay.replay_injected"); len(injected) != 1 {
		t.Fatalf("the replay was not announced: relay.replay_injected=%d, want 1", len(injected))
	}
}
