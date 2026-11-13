package panewire

// hk 3435, hk 2777: the node's hello repeats what its panes already received,
// so a relay.delivered ack lost with a socket does not make a hub restart
// replay a delivered batch; a lane owed more than relayReplayGapThreshold rows
// gets one summary line instead of a replay.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hello registers host-a as its node does: the hello advertising the
// capabilities, then the applied report right behind it. Both go over the
// hub's inbound path and must parse.
func (rig *t989Rig) hello() { rig.helloFrom("host-a", rig.dest, hubSpawnCapabilities) }

func (rig *t989Rig) helloFrom(machine string, agent *hubAgent, capabilities []string) {
	rig.t.Helper()
	capabilityJSON, _ := json.Marshal(capabilities)
	rig.send(machine, agent, fmt.Sprintf(`{"type":"hello","machine_id":%q,"version":"panewire-t2777","capabilities":%s}`, machine, capabilityJSON))
	if message, ok := rig.node.appliedMessage(context.Background(), capabilities); ok {
		wire, err := json.Marshal(message)
		if err != nil {
			rig.t.Fatal(err)
		}
		rig.send(machine, agent, string(wire))
	}
}

func (rig *t989Rig) send(machine string, agent *hubAgent, wire string) {
	rig.t.Helper()
	if _, valid := parseHubInbound([]byte(wire)); !valid {
		rig.t.Fatalf("hub refused %s", wire)
	}
	rig.hub.handleAgentMessage(machine, "fixture", agent, []byte(wire))
}

// deliverAndLoseAcks sends one lane.event per text, offers the first n
// directives to the node on an idle pane, then drops every event the node
// emitted, as a dead socket would. It returns every row id and the delivered
// subset.
func (rig *t989Rig) deliverAndLoseAcks(texts []string, n int) (all, delivered []int64) {
	rig.t.Helper()
	rig.herdr.getStatusByPane["w1:pA"] = "idle"
	for i, text := range texts {
		rig.sendLaneEvent(fmt.Sprintf("t2777-%d", i), text)
		directive := rig.directive()
		all = append(all, directive.EventID)
		if i < n {
			rig.offer(directive)
			delivered = append(delivered, directive.EventID)
		}
	}
	for _, id := range delivered {
		id := id
		eventually(rig.t, 5*time.Second, "node recorded the delivery", func() bool {
			_, found, err := rig.store.RelayDeliveredByKey(context.Background(), t989Lane, id)
			return err == nil && found
		})
	}
	for len(rig.events) > 0 {
		<-rig.events
	}
	return all, delivered
}

func TestHelloAppliedReportClosesRowsWhoseAckWasLost(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	rig.herdr.waitStatus = "timeout"
	rig.herdr.waitGate = make(chan struct{})
	var ids []int64
	for i := 0; i < 3; i++ {
		rig.sendLaneEvent(fmt.Sprintf("lost-ack-%d", i), fmt.Sprintf("note %d", i))
		directive := rig.directive()
		directive.Deliver = "max_wait=1"
		rig.offer(directive)
		ids = append(ids, directive.EventID)
	}
	rig.flush()
	close(rig.herdr.waitGate)
	eventually(t, 5*time.Second, "expired batch recorded delivered on the node", func() bool {
		for _, id := range ids {
			if _, found, err := rig.store.RelayDeliveredByKey(context.Background(), t989Lane, id); err != nil || !found {
				return false
			}
		}
		return true
	})
	if len(*rig.prompts) != 1 || !strings.Contains((*rig.prompts)[0], "[대기 만료") {
		t.Fatalf("want one expired batch prompt, got %q", *rig.prompts)
	}
	// The release and delivered events die with the socket (hk 3435).
	for len(rig.events) > 0 {
		<-rig.events
	}
	for _, id := range ids {
		if rig.row(id).DeliveredAt != "" {
			t.Fatalf("row %d acknowledged although its ack was lost", id)
		}
	}

	rig.startHub()
	rig.hello()
	eventually(t, 5*time.Second, "hello closes the delivered batch rows", func() bool {
		for _, id := range ids {
			if rig.row(id).DeliveredAt == "" {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		if got := rig.row(id).DeliveredTo; got != "host-a/"+relayAppliedReportPane {
			t.Fatalf("row %d delivered_to=%q, want host-a/%s", id, got, relayAppliedReportPane)
		}
	}
	if injected := drainRelays(rig.dest); injected != 0 {
		t.Fatalf("registration replayed %d rows the pane already read", injected)
	}
}

func TestHelloAppliedReportLeavesUnreportedRowsToReplay(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	ids, delivered := rig.deliverAndLoseAcks([]string{"one", "two", "three"}, 2)
	rig.startHub()
	rig.hello()
	eventually(t, 5*time.Second, "the unreported row is replayed", func() bool { return len(rig.dest.relays) == 1 })
	directive := <-rig.dest.relays
	if directive.EventID != ids[2] {
		t.Fatalf("replayed row %d, want the unreported %d", directive.EventID, ids[2])
	}
	for _, id := range delivered {
		if rig.row(id).DeliveredTo != "host-a/"+relayAppliedReportPane {
			t.Fatalf("row %d was not closed by the report: %q", id, rig.row(id).DeliveredTo)
		}
	}
	if rig.row(ids[2]).DeliveredAt != "" {
		t.Fatal("the unreported row was closed without delivery")
	}
}

func TestAppliedReportOnlyWithItsCapability(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.deliverAndLoseAcks([]string{"one"}, 1)
	if _, ok := rig.node.appliedMessage(context.Background(), nil); ok {
		t.Fatal("the capability-free fallback hello is followed by an applied report; a hub that predates it counts it as unknown")
	}
	got, ok := rig.node.appliedMessage(context.Background(), hubSpawnCapabilities)
	if !ok || len(got.Applied[t989Lane]) != 1 {
		t.Fatalf("report=%+v ok=%v, want the delivered row", got, ok)
	}
}

func TestHelloAppliedReportBounds(t *testing.T) {
	hello := func(applied string) []byte {
		return []byte(`{"type":"applied","applied":` + applied + `}`)
	}
	ids := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprint(i + 1)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	lanes := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"lane-%d":[1]`, i)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	accepted := map[string]string{
		"empty":          `{}`,
		"one lane":       `{"lane-a":[5,9]}`,
		"per-lane max":   `{"lane-a":` + ids(relayAppliedMaxPerLane) + `}`,
		"lane count max": lanes(relayAppliedMaxLanes),
	}
	for name, applied := range accepted {
		if _, valid := parseHubInbound(hello(applied)); !valid {
			t.Errorf("%s: hello refused", name)
		}
	}
	refused := map[string]string{
		"zero id":           `{"lane-a":[0]}`,
		"negative id":       `{"lane-a":[-3]}`,
		"bad lane name":     `{"Lane A!":[1]}`,
		"too many per lane": `{"lane-a":` + ids(relayAppliedMaxPerLane+1) + `}`,
		"too many lanes":    lanes(relayAppliedMaxLanes + 1),
		"not a map":         `[1,2]`,
	}
	for name, applied := range refused {
		if _, valid := parseHubInbound(hello(applied)); valid {
			t.Errorf("%s: hello accepted", name)
		}
	}
	message, _ := parseHubInbound(hello(`{"lane-a":[5,9]}`))
	if got := message.Applied["lane-a"]; len(got) != 2 || got[0] != 5 || got[1] != 9 {
		t.Fatalf("parsed applied=%v", message.Applied)
	}
}

func TestRelayAppliedIDsNewestFirstAndBounded(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	base := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	for i := 1; i <= relayAppliedMaxPerLane+10; i++ {
		if err := store.RecordRelayDelivered(context.Background(), "lane-a", int64(i), "w1:p1", "sha", base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordRelayDelivered(context.Background(), "lane-b", 7, "w1:p2", "sha", base); err != nil {
		t.Fatal(err)
	}
	applied, err := store.RelayAppliedIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := applied["lane-a"]; len(got) != relayAppliedMaxPerLane || got[0] != relayAppliedMaxPerLane+10 {
		t.Fatalf("lane-a reported %d ids starting at %v, want %d newest first", len(got), got[:1], relayAppliedMaxPerLane)
	}
	if got := applied["lane-b"]; len(got) != 1 || got[0] != 7 {
		t.Fatalf("lane-b=%v, want [7]", got)
	}
}

// gapRows files n lane.events through the hub and drops the directives the
// original hub queued, as a restart does.
func (rig *t989Rig) gapRows(n int) []int64 {
	rig.t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		rig.sendLaneEvent(fmt.Sprintf("gap-%d", i), fmt.Sprintf("old news %d", i))
		ids = append(ids, rig.directive().EventID)
	}
	return ids
}

// t989StartWithInbox is t989Start with the hub's lane inbox open, which the
// gap summary points the pane at.
func t989StartWithInbox(t *testing.T) *t989Rig {
	t.Helper()
	rig := t989Start(t, "w1:pA")
	rig.inboxPath = filepath.Join(t.TempDir(), "inbox.sqlite3")
	rig.startHub()
	return rig
}

func TestRegistrationOverThresholdSendsOneSummaryLine(t *testing.T) {
	rig := t989StartWithInbox(t)
	ids := rig.gapRows(relayReplayGapThreshold + 1)
	rig.startHub()
	rig.hello()
	eventually(t, 5*time.Second, "summary line queued", func() bool { return len(rig.dest.relays) >= 1 })
	eventually(t, 5*time.Second, "every row retired", func() bool {
		for _, id := range ids {
			if !strings.HasPrefix(rig.row(id).DeliveredTo, relayReplayRetiredMarker+relayGapRetireReason) {
				return false
			}
		}
		return true
	})
	directive := <-rig.dest.relays
	want := fmt.Sprintf("[panewire] gap: %d events for %s were not replayed", len(ids), t989Lane)
	if !strings.Contains(directive.Text, want) || !strings.Contains(directive.Text, "panewire inbox list --lane "+t989Lane) {
		t.Fatalf("summary text %q does not carry %q and the inbox command", directive.Text, want)
	}
	if extra := drainRelays(rig.dest); extra != 0 {
		t.Fatalf("%d further directives followed the summary line", extra)
	}
}

func TestRegistrationAtThresholdReplaysEveryRow(t *testing.T) {
	rig := t989StartWithInbox(t)
	ids := rig.gapRows(relayReplayGapThreshold)
	rig.startHub()
	rig.hello()
	eventually(t, 5*time.Second, "every row replayed", func() bool { return len(rig.dest.relays) >= len(ids) })
	seen := map[int64]bool{}
	for len(rig.dest.relays) > 0 {
		directive := <-rig.dest.relays
		if strings.Contains(directive.Text, "gap:") {
			t.Fatalf("summary line sent for a backlog within the threshold: %q", directive.Text)
		}
		seen[directive.EventID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("row %d was not replayed", id)
		}
	}
}

func TestSummaryLineBacklogCountsOnlyRowsTheNodeHasNotApplied(t *testing.T) {
	rig := t989StartWithInbox(t)
	texts := make([]string, relayReplayGapThreshold+5)
	for i := range texts {
		texts[i] = fmt.Sprintf("note %d", i)
	}
	ids, _ := rig.deliverAndLoseAcks(texts, 10)
	rig.startHub()
	rig.hello()
	// 25 undelivered, 10 reported applied: 15 left, within the threshold.
	eventually(t, 5*time.Second, "the unreported rows replay", func() bool { return len(rig.dest.relays) >= len(ids)-10 })
	for len(rig.dest.relays) > 0 {
		directive := <-rig.dest.relays
		if strings.Contains(directive.Text, "gap:") {
			t.Fatalf("summary line sent after the applied report left %d rows: %q", len(ids)-10, directive.Text)
		}
	}
}

// A held journal lock must not stall the registration: the report is sent
// empty after relayAppliedReadBudget instead of waiting for the store.
func TestAppliedReportDoesNotWaitOnAHeldStoreLock(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.deliverAndLoseAcks([]string{"one"}, 1)
	rig.store.mu.Lock()
	released := false
	defer func() {
		if !released {
			rig.store.mu.Unlock()
		}
	}()
	started := time.Now()
	got, ok := rig.node.appliedMessage(context.Background(), hubSpawnCapabilities)
	if waited := time.Since(started); waited > 3*relayAppliedReadBudget {
		t.Fatalf("report waited %v on the held store lock", waited)
	}
	if !ok || len(got.Applied) != 0 {
		t.Fatalf("report=%+v ok=%v, want an empty report", got, ok)
	}
	rig.store.mu.Unlock()
	released = true
}

// Without a lane inbox the summary's "panewire inbox list" points nowhere, so
// the rows replay one by one and none retires.
func TestRegistrationWithoutInboxReplaysAnOverThresholdBacklog(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	ids := rig.gapRows(relayReplayGapThreshold + 1)
	rig.startHub()
	rig.hello()
	eventually(t, 5*time.Second, "every row replayed", func() bool { return len(rig.dest.relays) >= len(ids) })
	for len(rig.dest.relays) > 0 {
		if directive := <-rig.dest.relays; strings.Contains(directive.Text, "gap:") {
			t.Fatalf("summary line sent without a lane inbox: %q", directive.Text)
		}
	}
	for _, id := range ids {
		if strings.HasPrefix(rig.row(id).DeliveredTo, relayReplayRetiredMarker) {
			t.Fatalf("row %d retired without an inbox copy", id)
		}
	}
}

// A node is entitled to close only rows routed to it: another machine naming
// the ids closes nothing, and the rows replay to their own node.
func TestAppliedReportFromAnotherMachineClosesNothing(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	ids, _ := rig.deliverAndLoseAcks([]string{"one", "two"}, 2)
	rig.startHub()
	report, _ := json.Marshal(hubAppliedMessage{Type: "applied", Applied: map[string][]int64{t989Lane: ids}})
	rig.send("host-b", rig.src, string(report))
	eventually(t, 5*time.Second, "the rows replay to their own node", func() bool { return len(rig.dest.relays) == len(ids) })
	for _, id := range ids {
		if rig.row(id).DeliveredAt != "" {
			t.Fatalf("row %d closed by a machine it was not routed to: %q", id, rig.row(id).DeliveredTo)
		}
	}
}

// A node whose hello does not advertise the report (an older node) replays at
// registration, as before: nothing waits for a report that will not come.
func TestRegistrationWithoutReportCapabilityReplaysAtOnce(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	ids := rig.gapRows(2)
	rig.startHub()
	rig.helloFrom("host-a", rig.dest, []string{"spawn_v2", "spawn_ops_v1"})
	eventually(t, 5*time.Second, "rows replayed", func() bool { return len(rig.dest.relays) == len(ids) })
}
