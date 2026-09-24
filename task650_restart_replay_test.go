package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// #650: a hub restart replayed every idle-wake director-1 had received since
// 09-21. The rows were delivered, but handoffkeep never heard so: the node's
// relay.delivered only closed a row while an in-memory ack window still
// matched it.

const t650Director = "director-1"

// t650Lanes is director-1 on host-a plus one worker lane per pane on host-b.
func t650Lanes(panes map[string]string) string {
	lanes := map[string]reportRelayRoute{t650Director: {Machine: "host-a", Pane: "wB:pD8"}}
	for lane, pane := range panes {
		lanes[lane] = reportRelayRoute{Machine: "host-b", Pane: pane, Parent: t650Director}
	}
	encoded, _ := json.Marshal(map[string]any{"lanes": lanes})
	return string(encoded)
}

func t650Workers(count int) map[string]string {
	panes := map[string]string{}
	for index := 1; index <= count; index++ {
		panes[fmt.Sprintf("b%d", index)] = fmt.Sprintf("w16:p%d", index)
	}
	return panes
}

// t650Wake is the lane.event the node materializes for an idle-wake the hub
// routed to director-1, with the text idleWakeRouteText writes.
func t650Wake(pane, label string, sequence uint64, changedAt time.Time) hubJobEventPayload {
	eventID := idleWakeEventID(strings.Repeat("a", 32), pane, sequence)
	request := idleWakeRouteRequest{EventID: eventID, Pane: pane, Label: label, State: "done", ChangedAt: changedAt.UTC().Format(time.RFC3339Nano), StateChangeSeq: sequence}
	text := idleWakeRouteText(request, idleWakeOwnerResolution{Lane: t650Director})
	return hubJobEventPayload{JobID: laneEventTransportID(t650Director, eventID), Epoch: 1, OwnerLane: t650Director, EventID: eventID, Text: text}
}

// t650Attach connects director-1's node (host-a) and the workers' node
// (host-b) to hub, the way a node hello registers them.
func t650Attach(hub *HubServer) (director, workers *hubAgent) {
	director = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	workers = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: director}
	hub.nodes["host-b"] = &hubNodeRecord{agent: workers}
	hub.mu.Unlock()
	return director, workers
}

// t650Send delivers one node event over the same parser and handler a node's
// socket uses.
func t650Send(t *testing.T, hub *HubServer, machine string, agent *hubAgent, kind string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: kind, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := parseHubInbound(wire); !valid {
		t.Fatalf("parseHubInbound rejected %s", wire)
	}
	hub.handleAgentMessage(machine, "fixture", agent, wire)
}

// t650Restart is what a deploy does to the hub: a new process with no memory,
// the same lanes file contents and the same handoffkeep. It runs the startup
// replay, then the replay each node hello triggers once the nodes are back.
func t650Restart(t *testing.T, lanes string, client *handoffkeepRelayClient) (*HubServer, *hubAgent) {
	t.Helper()
	hub := r20Hub(t, lanes, client, nil)
	hub.replayUndeliveredRelayEvents(context.Background())
	director, _ := t650Attach(hub)
	hub.replayUndeliveredLaneEvents(context.Background())
	return hub, director
}

func t650Inject(t *testing.T, hub *HubServer, workers, director *hubAgent, event hubJobEventPayload) hubRelayInjectEvent {
	t.Helper()
	hub.relayLaneEvent(event, workers)
	select {
	case directive := <-director.relays:
		if directive.EventID < 1 || directive.JobID != event.JobID || directive.Pane != "wB:pD8" {
			t.Fatalf("directive=%+v", directive)
		}
		return directive
	case <-time.After(time.Second):
		t.Fatalf("idle-wake %s was not injected", event.EventID)
	}
	return hubRelayInjectEvent{}
}

func (f *fakeHandoffkeep) deliveredToFor(id int64) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deliveredTo[id]
}

// AC0/AC1: every idle-wake the director's node acknowledged stays delivered
// across a hub restart, whichever way the acknowledgement reached the hub.
// On the pre-#650 hub the unconfirmed-first and late shapes left delivered_at
// NULL, and the restarted hub re-sent them: that is the incident, reproduced.
func TestT650HubRestartResendsNoAcknowledgedIdleWake(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(6)
	lanes := t650Lanes(workers)
	hub := r20Hub(t, lanes, client, nil)
	director, workerNode := t650Attach(hub)
	changed := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Millisecond)

	var directives []hubRelayInjectEvent
	for index := 1; index <= 6; index++ {
		lane := fmt.Sprintf("b%d", index)
		directives = append(directives, t650Inject(t, hub, workerNode, director, t650Wake(workers[lane], lane, uint64(index), changed)))
	}
	ack := func(directive hubRelayInjectEvent) relayAckPayload {
		return relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID}
	}
	// In-window delivery: the only shape the old hub recorded.
	for _, directive := range directives[0:2] {
		t650Send(t, hub, "host-a", director, "relay.delivered", ack(directive))
	}
	// What wB:pD8 reports live: the claude inject verifies as unproven, the
	// node says relay.unconfirmed (which retires the window), and its retry
	// then lands and says relay.delivered.
	for _, directive := range directives[2:4] {
		t650Send(t, hub, "host-a", director, "relay.unconfirmed", ack(directive))
		t650Send(t, hub, "host-a", director, "relay.delivered", ack(directive))
	}
	// The window expired before the node's acknowledgement arrived.
	for _, directive := range directives[4:6] {
		hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
		t650Send(t, hub, "host-a", director, "relay.delivered", ack(directive))
	}
	for index, directive := range directives {
		if got := fake.deliveredToFor(directive.EventID); got != "host-a/wB:pD8" {
			// Not fatal: the restart below is what shows the damage.
			t.Errorf("idle-wake %d (row %d) delivered_to=%q before the restart, want host-a/wB:pD8", index+1, directive.EventID, got)
		}
	}

	_, restartedDirector := t650Restart(t, lanes, client)
	if resent := drainRelays(restartedDirector); resent != 0 {
		t.Fatalf("hub restart re-sent %d of 6 acknowledged idle-wakes, want 0", resent)
	}
}

// AC1: an acknowledgement that reaches a restarted hub, before any replay
// registered a window there, still closes the row.
func TestT650AckAfterHubRestartIsRecorded(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(2)
	lanes := t650Lanes(workers)
	hub := r20Hub(t, lanes, client, nil)
	director, workerNode := t650Attach(hub)
	changed := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	var directives []hubRelayInjectEvent
	for index := 1; index <= 2; index++ {
		lane := fmt.Sprintf("b%d", index)
		directive := t650Inject(t, hub, workerNode, director, t650Wake(workers[lane], lane, uint64(index), changed))
		// Director-1 was working: the node holds the relay across the restart.
		t650Send(t, hub, "host-a", director, "relay.held", relayHeldPayload{EventID: directive.EventID, JobID: directive.JobID, Pane: directive.Pane, Lane: t650Director, Reason: "working", Preview: "wake", HeldSince: changed.Format(time.RFC3339Nano), DeliverPolicy: "idle"})
		directives = append(directives, directive)
	}

	restarted := r20Hub(t, lanes, client, nil)
	restartedDirector, _ := t650Attach(restarted)
	for _, directive := range directives {
		t650Send(t, restarted, "host-a", restartedDirector, "relay.released", relayReleasedPayload{JobID: directive.JobID, Pane: directive.Pane, Lane: t650Director, FinalText: directive.Text, OriginalEventID: directive.EventID})
		t650Send(t, restarted, "host-a", restartedDirector, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
		if got := fake.deliveredToFor(directive.EventID); got != "host-a/wB:pD8" {
			t.Fatalf("row %d delivered_to=%q after an ack to the restarted hub", directive.EventID, got)
		}
	}
	_, again := t650Restart(t, lanes, client)
	if resent := drainRelays(again); resent != 0 {
		t.Fatalf("second restart re-sent %d delivered idle-wakes, want 0", resent)
	}
}

// A late acknowledgement is checked against the row: it must name the row's
// transport id and come from where the row was sent.
func TestT650LateAckMustMatchTheRow(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(1)
	lanes := t650Lanes(workers)
	hub := r20Hub(t, lanes, client, nil)
	director, workerNode := t650Attach(hub)
	directive := t650Inject(t, hub, workerNode, director, t650Wake("w16:p1", "b1", 1, time.Now().UTC().Truncate(time.Millisecond)))
	hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))

	for _, forged := range []struct {
		name    string
		machine string
		ack     relayAckPayload
	}{
		{"other machine", "host-b", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID}},
		{"other pane", "host-a", relayAckPayload{JobID: directive.JobID, Pane: "wB:p99", OriginalEventID: directive.EventID}},
		{"other job", "host-a", relayAckPayload{JobID: laneEventTransportID(t650Director, "someone-else"), Pane: directive.Pane, OriginalEventID: directive.EventID}},
		{"unknown row", "host-a", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID + 50}},
	} {
		before := hub.unknownMessages
		t650Send(t, hub, forged.machine, director, "relay.delivered", forged.ack)
		if got := fake.deliveredToFor(directive.EventID); got != "" {
			t.Fatalf("%s: a mismatched late ack closed the row (delivered_to=%q)", forged.name, got)
		}
		if hub.unknownMessages != before+1 {
			t.Fatalf("%s: unknown_messages=%d, want %d", forged.name, hub.unknownMessages, before+1)
		}
	}
	t650Send(t, hub, "host-a", director, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
	if got := fake.deliveredToFor(directive.EventID); got != "host-a/wB:pD8" {
		t.Fatalf("the matching late ack did not close the row: delivered_to=%q", got)
	}
}

// AC2: after a restart, an undelivered idle-wake whose pane is no lane's pane
// any more — the lane was removed, or re-homed to another pane — is retired
// and recorded, never delivered. A lane still on its pane replays.
func TestT650ReplayRetiresIdleWakeForGoneLanes(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(3)
	hub := r20Hub(t, t650Lanes(workers), client, nil)
	director, workerNode := t650Attach(hub)
	changed := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	rows := map[string]int64{}
	for index := 1; index <= 3; index++ {
		lane := fmt.Sprintf("b%d", index)
		// Never acknowledged: all three are still owed after the restart.
		rows[lane] = t650Inject(t, hub, workerNode, director, t650Wake(workers[lane], lane, uint64(index), changed)).EventID
	}

	// b1 was removed (lanes rm); b2's pane is gone and the lane now lives on
	// another pane; b3 is untouched.
	after := t650Lanes(map[string]string{"b2": "w16:p99", "b3": workers["b3"]})
	restarted := r20Hub(t, after, client, nil)
	events := r20t5Subscribe(t, restarted)
	restarted.replayUndeliveredRelayEvents(context.Background())
	restartedDirector, _ := t650Attach(restarted)
	restarted.replayUndeliveredLaneEvents(context.Background())

	var injected []string
	for len(restartedDirector.relays) > 0 {
		directive := <-restartedDirector.relays
		injected = append(injected, t650LaneOf(rows, directive.EventID))
	}
	if len(injected) != 1 || injected[0] != "b3" {
		t.Fatalf("restart delivered %v, want only the live lane b3", injected)
	}
	for _, lane := range []string{"b1", "b2"} {
		if got := fake.deliveredToFor(rows[lane]); got != "hub/replay-retired:lane_gone" {
			t.Fatalf("%s idle-wake delivered_to=%q, want hub/replay-retired:lane_gone", lane, got)
		}
	}
	if got := fake.deliveredToFor(rows["b3"]); got != "" {
		t.Fatalf("live lane's idle-wake was closed without delivery: %q", got)
	}
	retired := events("relay.replay_retired")
	if len(retired) != 2 {
		t.Fatalf("relay.replay_retired broadcasts=%d, want 2", len(retired))
	}
	for _, raw := range retired {
		var payload struct {
			EventID int64  `json:"event_id"`
			Lane    string `json:"lane"`
			Reason  string `json:"reason"`
		}
		if json.Unmarshal(raw, &payload) != nil || payload.Lane != t650Director || payload.Reason != "lane_gone" || (payload.EventID != rows["b1"] && payload.EventID != rows["b2"]) {
			t.Fatalf("relay.replay_retired payload=%s", raw)
		}
	}

	// Retired rows are closed for good: the next restart does not list them.
	_, again := t650Restart(t, after, client)
	if got := drainRelays(again); got != 1 {
		t.Fatalf("second restart injected %d, want only b3's still-unacknowledged wake", got)
	}
}

// t650LaneOf names the lane a row was created for.
func t650LaneOf(rows map[string]int64, row int64) string {
	for lane, id := range rows {
		if id == row {
			return lane
		}
	}
	return fmt.Sprintf("row-%d", row)
}

// A lanes file the hub cannot read proves nothing about which lanes exist:
// nothing is retired on lane grounds while it is unreadable.
func TestT650UnreadableLanesRetireNothing(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(2)
	hub := r20Hub(t, t650Lanes(workers), client, nil)
	director, workerNode := t650Attach(hub)
	changed := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	var ids []int64
	for index := 1; index <= 2; index++ {
		lane := fmt.Sprintf("b%d", index)
		ids = append(ids, t650Inject(t, hub, workerNode, director, t650Wake(workers[lane], lane, uint64(index), changed)).EventID)
	}
	t650Restart(t, `{"lanes":`, client)
	for _, id := range ids {
		if got := fake.deliveredToFor(id); got != "" {
			t.Fatalf("row %d closed while lanes were unreadable: delivered_to=%q", id, got)
		}
	}
}

// Replay has an age bound: an idle-wake reports one pane state at one
// instant, and no other row is re-injected after the day the node outbox
// itself stops offering it. Rows without a provable age still replay.
func TestT650ReplayRetiresStaleRows(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now().UTC()
	workers := t650Workers(2)
	lanes := t650Lanes(workers)
	stamp := func(age time.Duration) string { return now.Add(-age).Format(time.RFC3339Nano) }
	wake := func(id int64, lane string, age time.Duration) handoffkeepRelayEvent {
		event := t650Wake(workers[lane], lane, uint64(id), now.Add(-age).Truncate(time.Millisecond))
		return handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: event.JobID, Epoch: 1, OwnerLane: t650Director, EventID: event.EventID, Text: event.Text, ReceivedAt: stamp(age)}
	}
	note := func(id int64, receivedAt string) handoffkeepRelayEvent {
		eventID := fmt.Sprintf("consult-done-%d", id)
		return handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: laneEventTransportID(t650Director, eventID), Epoch: 1, OwnerLane: t650Director, EventID: eventID, Text: "[consult-done] advice ready", ReceivedAt: receivedAt}
	}
	fake.seedUndelivered(
		wake(11, "b1", idleWakeReplayMaxAge+time.Minute),
		wake(12, "b2", 5*time.Minute),
		note(13, stamp(relayReplayMaxAge+time.Hour)),
		note(14, stamp(time.Hour)),
		note(15, ""),
		handoffkeepRelayEvent{ID: 16, Kind: "job.completed", JobID: "t650-old-job", Epoch: 1, OwnerLane: t650Director, ReportPath: "report.md", ReportLastLine: "done", EventID: "00001-job.completed.json", ReceivedAt: stamp(72 * time.Hour)},
	)
	_, director := t650Restart(t, lanes, client)
	replayed := map[int64]bool{}
	for len(director.relays) > 0 {
		replayed[(<-director.relays).EventID] = true
	}
	for _, id := range []int64{12, 14, 15} {
		if !replayed[id] {
			t.Fatalf("row %d was not replayed (replayed=%v)", id, replayed)
		}
	}
	for _, id := range []int64{11, 13, 16} {
		if replayed[id] {
			t.Fatalf("stale row %d was replayed", id)
		}
		if got := fake.deliveredToFor(id); got != "hub/replay-retired:stale" {
			t.Fatalf("stale row %d delivered_to=%q, want hub/replay-retired:stale", id, got)
		}
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed=%v, want exactly rows 12, 14 and 15", replayed)
	}
}
