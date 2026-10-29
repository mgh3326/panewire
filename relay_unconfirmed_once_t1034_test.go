package panewire

// #1034: a marked row's one labelled replay is bounded by its durable
// retire, not by the inject. The retire is written BEFORE the directive is
// queued — a failed retire spends nothing and defers to the next trigger,
// while a queue failure after a successful retire spends the replay without
// a pane inject. The window a relay.unconfirmed bound survives a failed
// mark so a repeated report can still mark the row, and a stale listing
// page is re-read before the row's one replay is spent.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// t1034Labelled drains the destination relays and returns the directives
// that carry the unconfirmed replay label for the given durable row.
func (rig *t1002Rig) t1034Labelled(eventID int64) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for _, directive := range rig.injects() {
		if directive.EventID == eventID && strings.HasPrefix(directive.Text, "[replayed after hub restart - first sent ") {
			out = append(out, directive)
		}
	}
	return out
}

// t1034OfflineRestart is a restart whose lane never came back: the hub is
// rebuilt without the destination node registered, so route resolution
// fails and every marked row keeps its pending replay.
func (rig *t1002Rig) t1034OfflineRestart() {
	rig.t.Helper()
	rig.startHub()
	rig.hub.mu.Lock()
	delete(rig.hub.nodes, "host-a")
	rig.hub.mu.Unlock()
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	rig.hub.replayUndeliveredLaneEvents(context.Background())
}

// S1: the retire write fails on the first restart and succeeds on the
// second. The rule — no labelled copy is ever queued unless the durable
// retire for that row already succeeded — gives injects [0 1 0 0 0], not
// the old code's [1 1 1 1 …].
func TestT1034RetireFailsThenSucceedsInjectsOnce(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-s1", "s1 body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = map[int64]bool{eventID: true}
	rig.fake.mu.Unlock()
	rig.restart()
	if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
		t.Fatalf("restart 1 injected %d labelled copies while its retire write failed, want 0", len(injects))
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt != "" {
		t.Fatalf("marker after a failed retire: %+v, want one live marker", markers)
	}

	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = nil
	rig.fake.mu.Unlock()
	rig.restart()
	injects := rig.t1034Labelled(eventID)
	if len(injects) != 1 {
		t.Fatalf("restart 2 injected %d labelled copies, want exactly 1", len(injects))
	}
	row := rig.row(eventID)
	if row.DeliveredAt == "" || row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after its replay: delivered_at=%q delivered_to=%q", row.DeliveredAt, row.DeliveredTo)
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt == "" {
		t.Fatalf("marker after the replay: %+v, want retired", markers)
	}

	for restart := 3; restart <= 5; restart++ {
		rig.restart()
		if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
			t.Fatalf("restart %d injected %d labelled copies, want 0", restart, len(injects))
		}
	}
}

// S1: a store that always refuses the retire write must starve the replay,
// not feed it one inject per restart. Five restarts with the lane online:
// zero injects, the marker still live, one warning per failed attempt.
func TestT1034RetireAlwaysFailsNeverInjects(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-s1f", "s1f body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = map[int64]bool{eventID: true}
	rig.fake.mu.Unlock()

	warns := func() int { return rig.logs.count("unconfirmed replay could not retire its row") }
	for restart := 1; restart <= 5; restart++ {
		before := warns()
		rig.restart()
		if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
			t.Fatalf("restart %d injected %d labelled copies while the retire write always failed, want 0", restart, len(injects))
		}
		// The rig's restart runs the startup walk and the hello walk; each
		// attempts the retire once and logs exactly one warning.
		if got := warns() - before; got != 2 {
			t.Fatalf("restart %d logged %d retire-failure warnings, want 2 (one per walk)", restart, got)
		}
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt != "" {
		t.Fatalf("marker after five failed restarts: %+v, want one live marker", markers)
	}
	if row := rig.row(eventID); row.DeliveredAt != "" {
		t.Fatalf("row closed by a failed retire: %+v", row)
	}
}

// The retire lands but the injection queue refuses the directive: the one
// replay is spent without reaching a pane — one relay.unrouted broadcast,
// one spent-not-queued warning naming the row, the row stays retired, and
// no later restart injects or re-marks it.
func TestT1034QueueFailAfterRetireSpendsReplay(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-qf", "qf body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	// A restart whose destination queue cannot take another directive.
	rig.startHub()
	events := r20t5Subscribe(t, rig.hub)
	rig.dest.relays = make(chan hubRelayInjectEvent, 1)
	rig.dest.relays <- hubRelayInjectEvent{Type: "relay.inject", JobID: "filler", Text: "filler"}
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	rig.hub.replayUndeliveredLaneEvents(context.Background())

	if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
		t.Fatalf("queue-full restart injected %d labelled copies, want 0", len(injects))
	}
	if got := rig.logs.count("unconfirmed replay was retired but never queued"); got != 1 {
		t.Fatalf("spent-not-queued warnings=%d, want 1: %s", got, rig.logs.String())
	}
	if got := len(events("relay.unrouted")); got != 1 {
		t.Fatalf("relay.unrouted broadcasts=%d, want 1", got)
	}
	row := rig.row(eventID)
	if row.DeliveredAt == "" || row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after the spent replay: delivered_at=%q delivered_to=%q", row.DeliveredAt, row.DeliveredTo)
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt == "" {
		t.Fatalf("marker after the spent replay: %+v, want retired", markers)
	}
	for restart := 1; restart <= 2; restart++ {
		rig.restart()
		if injects := rig.injects(); len(injects) != 0 {
			t.Fatalf("restart %d after the spent replay injected %d rows, want 0", restart, len(injects))
		}
	}
}

// S2 offline path: while the destination is offline the mark keeps the one
// replay pending; the first trigger that can route the row injects it once.
// [0 0 | 1 | 0 0 0].
func TestT1034OfflineRestartsThenHelloInjectsOnce(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-off", "offline body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	for restart := 1; restart <= 2; restart++ {
		rig.t1034OfflineRestart()
		if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
			t.Fatalf("offline restart %d injected %d labelled copies, want 0", restart, len(injects))
		}
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt != "" {
		t.Fatalf("marker after offline restarts: %+v, want one live marker", markers)
	}

	// The lane comes back: the hello replay — not a restart — is the first
	// trigger that can route the row, and it injects the one labelled copy.
	rig.startHub()
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	injects := rig.t1034Labelled(eventID)
	if len(injects) != 1 {
		t.Fatalf("hello with the lane online injected %d labelled copies, want 1", len(injects))
	}
	if row := rig.row(eventID); row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after the deferred replay: delivered_to=%q", row.DeliveredTo)
	}
	for restart := 1; restart <= 3; restart++ {
		rig.restart()
		if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
			t.Fatalf("restart %d after the deferred replay injected %d labelled copies, want 0", restart, len(injects))
		}
	}
}

// The job.* shape of the same offline bound: a job marked row is invisible
// to the hello walk (lane.event only), so the first restart after the lane
// returns is what injects it — once.
func TestT1034OfflineJobRowInjectsOnce(t *testing.T) {
	rig := t1002Start(t)
	event := hubJobEventPayload{JobID: "job-t1034-off", Epoch: 1, OwnerLane: t1002Lane, ReportPath: "report.md", ReportLastLine: "VERDICT: DONE"}
	rig.hub.relayJobCompletion(event)
	directive := rig.directive()
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	for restart := 1; restart <= 2; restart++ {
		rig.t1034OfflineRestart()
		if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
			t.Fatalf("offline restart %d injected %d labelled copies, want 0", restart, len(injects))
		}
	}
	// A node hello cannot see the job row at all — the hello listing is
	// lane.event only.
	rig.startHub()
	rig.hub.replayUndeliveredLaneEvents(context.Background())
	if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
		t.Fatalf("hello injected %d labelled job copies, want 0", len(injects))
	}
	for restart, want := range []int{1, 0, 0} {
		rig.restart()
		if injects := rig.t1034Labelled(eventID); len(injects) != want {
			t.Fatalf("restart %d injected %d labelled job copies, want %d", restart+3, len(injects), want)
		}
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt == "" {
		t.Fatalf("job marker after the replay: %+v, want retired", markers)
	}
}

// S2 racing walks: the startup replay and the hello replay run at the same
// time on the same hub, started on one barrier. Every iteration marks a
// fresh row; the two walks' one-row re-reads are barriered so a walk whose
// listing predates the other walk's retire still meets it at the re-read.
// Each iteration must inject the marked row exactly once.
func TestT1034RacingWalksInjectOnce(t *testing.T) {
	rig := t1002Start(t)
	ctx := context.Background()
	for iteration := 1; iteration <= 50; iteration++ {
		directive := rig.sendLaneEvent(fmt.Sprintf("ev-1034-race-%d", iteration), fmt.Sprintf("race body %d", iteration))
		eventID := directive.EventID
		rig.unconfirmed(directive, "maybe_in_pane")
		// A restart: the replay walks run on a hub whose dedupe claims are
		// all cold, like the real post-restart race.
		rig.startHub()

		// Barrier the per-row re-read GETs (after_id=<id-1>, no
		// undelivered flag): released either when two of them are queued
		// inside the fake — both walks racing at the re-read — or when one
		// walk already finished, which means the other never got past the
		// dedupe claim.
		afterID := "after_id=" + strconv.FormatInt(eventID-1, 10)
		arrived := make(chan struct{}, 4)
		release := make(chan struct{})
		var releaseOnce sync.Once
		rig.fake.mu.Lock()
		rig.fake.observeQuery = func(method, path, rawQuery string) {
			if method != http.MethodGet || path != "/v1/relay/events" {
				return
			}
			if strings.Contains(rawQuery, "undelivered") || !strings.Contains(rawQuery, afterID) {
				return
			}
			arrived <- struct{}{}
			<-release
		}
		rig.fake.mu.Unlock()

		start := make(chan struct{})
		done := make(chan struct{}, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			rig.hub.replayUndeliveredRelayEvents(ctx)
			done <- struct{}{}
		}()
		go func() {
			defer wg.Done()
			<-start
			rig.hub.replayUndeliveredLaneEvents(ctx)
			done <- struct{}{}
		}()
		go func() {
			for i := 0; i < 2; i++ {
				select {
				case <-arrived:
				case <-release:
					return
				}
			}
			releaseOnce.Do(func() { close(release) })
		}()
		go func() {
			<-done
			releaseOnce.Do(func() { close(release) })
		}()
		close(start)
		waited := make(chan struct{})
		go func() { wg.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(30 * time.Second):
			rig.fake.mu.Lock()
			rig.fake.observeQuery = nil
			rig.fake.mu.Unlock()
			releaseOnce.Do(func() { close(release) })
			t.Fatalf("iteration %d: racing walks did not finish", iteration)
		}
		rig.fake.mu.Lock()
		rig.fake.observeQuery = nil
		rig.fake.mu.Unlock()

		if injects := rig.t1034Labelled(eventID); len(injects) != 1 {
			t.Fatalf("iteration %d: the marked row was injected %d times by racing walks, want exactly 1", iteration, len(injects))
		}
	}
}

// N4 stale page: a walk listed the marked row while it was undelivered;
// another walk then spent the replay — retire landed — and the ack timeout
// released the dedupe claim the second walk held. The first walk must
// re-read the row before spending the replay and refuse the inject.
func TestT1034StalePageDoesNotInject(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-n4", "n4 body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	// A restart: the replay walks run on a hub whose dedupe claims are all
	// cold, like the real post-restart state.
	rig.startHub()

	// Walk A's view, captured while the row was still undelivered.
	marks, err := rig.hub.unconfirmedMarkOrigins(context.Background())
	if err != nil || !marks[eventID] {
		t.Fatalf("marks=%v err=%v, want the mark collected", marks, err)
	}
	page, err := rig.client.listUndelivered(context.Background(), "", "", 0, handoffkeepReplayLimit)
	if err != nil {
		t.Fatal(err)
	}
	var stale handoffkeepRelayEvent
	for _, record := range page {
		if record.ID == eventID {
			stale = record
		}
	}
	if stale.ID == 0 {
		t.Fatal("the marked row was not in the listed page")
	}

	// Walk B runs to completion: it spends the replay (retire + inject).
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	if injects := rig.t1034Labelled(eventID); len(injects) != 1 {
		t.Fatalf("the completing walk injected %d labelled copies, want 1", len(injects))
	}
	if row := rig.row(eventID); row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after walk B: delivered_to=%q", row.DeliveredTo)
	}
	// The ack timeout path releases the dedupe claim walk B's inject held,
	// so walk A's claim below is not what saves it — the re-read is.
	rig.hub.expireRelayAck(relayPendingKey(eventID, directive.JobID))

	// Walk A resumes on its stale page.
	rig.hub.replayMarkedUnconfirmed(context.Background(), stale, relayReplaySourceStartup)
	if injects := rig.t1034Labelled(eventID); len(injects) != 0 {
		t.Fatalf("a stale listing page injected %d more labelled copies of a retired row, want 0", len(injects))
	}
}

// N2: the marker POST fails, so the live window stays pending for a retry
// rather than being consumed into the unlabelled ordinary replay. The
// repeated report writes the mark once, and the restart replay carries the
// label — not the unlabelled fallback.
func TestT1034FailedMarkKeepsWindowAndRemarksOnce(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-n2", "n2 body")
	eventID := directive.EventID
	pendingKey := relayPendingKey(eventID, directive.JobID)

	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = relayUnconfirmedMarkLane
	rig.fake.mu.Unlock()
	rig.unconfirmed(directive, "maybe_in_pane")

	rig.hub.mu.Lock()
	_, live := rig.hub.r19a.relayPending[pendingKey]
	rig.hub.mu.Unlock()
	if !live {
		t.Fatal("a failed unconfirmed mark consumed the live window")
	}
	if markers := rig.markers(); len(markers) != 0 {
		t.Fatalf("a failed mark still wrote a marker: %+v", markers)
	}
	if got := rig.logs.count("relay unconfirmed mark was not persisted"); got != 1 {
		t.Fatalf("mark-failure warnings=%d, want 1: %s", got, rig.logs.String())
	}
	if got := rig.logs.count("relay delivery unconfirmed"); got != 0 {
		t.Fatalf("the kept-window report logged %d consumed-window warnings, want 0", got)
	}

	// The repeated report marks the row exactly once and the window is
	// retired as usual.
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()
	rig.unconfirmed(directive, "maybe_in_pane")
	rig.hub.mu.Lock()
	_, live = rig.hub.r19a.relayPending[pendingKey]
	rig.hub.mu.Unlock()
	if live {
		t.Fatal("a successful re-mark left the live window registered")
	}
	markers := rig.markers()
	if len(markers) != 1 || markers[0].EventID != unconfirmedMarkEventID(eventID) || markers[0].DeliveredAt != "" {
		t.Fatalf("markers after the re-mark: %+v, want one live marker", markers)
	}

	// The row now replays through the labelled path — the unlabelled
	// ordinary fallback it used to fall into does not inject it.
	rig.restart()
	injects := rig.t1034Labelled(eventID)
	if len(injects) != 1 {
		t.Fatalf("restart injected %d labelled copies after the re-mark, want 1", len(injects))
	}
}

// N2, transport-id path: an id-less lane.event window still names its row
// through the transport id, so the marker lookup runs listRelayEvents. A
// failed lookup is not "no row" — the window is kept for a retry, exactly
// like a failed marker POST. Restoring the old err==nil&&found shape lets
// the error fall through to the nothing-to-mark outcome and consumes the
// window: the mutant this test exists to catch.
func TestT1034TransportLookupFailureKeepsWindow(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-1034-tx", "tx body")
	transportID := directive.JobID

	// Reshape the live window into the pre-id form T989 exercises: the
	// durable id is gone from the window and from every dedupe map, so the
	// report must resolve the row by its transport id.
	event := relayEventFromRecord(rig.row(directive.EventID))
	rig.hub.forgetRelayAckEvent(directive.EventID, transportID)
	key := relayEventDedupeKey("lane.event", event)
	rig.hub.forgetRelayEvent(key)
	rig.hub.mu.Lock()
	rig.hub.forgetLanePersistedLocked(key)
	rig.hub.mu.Unlock()
	rig.hub.registerRelayAckEvent("lane.event", event, "host-a", directive.Pane, 0)
	rig.hub.armRelayAckEvent(0, transportID)
	pendingKey := relayPendingKey(0, transportID)

	rig.fake.mu.Lock()
	rig.fake.failGetKind = "lane.event"
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.unconfirmed", relayAckPayload{JobID: transportID, Pane: directive.Pane, Reason: "maybe_in_pane"})

	rig.hub.mu.Lock()
	_, live := rig.hub.r19a.relayPending[pendingKey]
	rig.hub.mu.Unlock()
	if !live {
		t.Fatal("a failed transport-id lookup consumed the live window")
	}
	if markers := rig.markers(); len(markers) != 0 {
		t.Fatalf("a failed lookup still wrote a marker: %+v", markers)
	}
	if got := rig.logs.count("relay unconfirmed transport lookup failed"); got != 1 {
		t.Fatalf("lookup-failure warnings=%d, want 1: %s", got, rig.logs.String())
	}
	if got := rig.logs.count("relay delivery unconfirmed"); got != 0 {
		t.Fatalf("the kept-window report logged %d consumed-window warnings, want 0", got)
	}
	if got := rig.logs.count("relay unconfirmed named no durable row"); got != 0 {
		t.Fatalf("the failed lookup fell through to the no-row outcome %d times, want 0", got)
	}

	// The repeated report resolves the row by transport id, marks it once,
	// and retires the window as usual.
	rig.fake.mu.Lock()
	rig.fake.failGetKind = ""
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.unconfirmed", relayAckPayload{JobID: transportID, Pane: directive.Pane, Reason: "maybe_in_pane"})
	rig.hub.mu.Lock()
	_, live = rig.hub.r19a.relayPending[pendingKey]
	rig.hub.mu.Unlock()
	if live {
		t.Fatal("a successful re-mark left the live window registered")
	}
	markers := rig.markers()
	if len(markers) != 1 || markers[0].EventID != unconfirmedMarkEventID(directive.EventID) || markers[0].DeliveredAt != "" {
		t.Fatalf("markers after the re-mark: %+v, want one live marker", markers)
	}
}

// Fake parity: /delivered is first-write-wins like the real store — a
// second /delivered keeps the first delivered_to and still answers 200.
func TestT1034DeliveredFirstWriteWins(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	ctx := context.Background()
	stored, status, err := client.appendEvent(ctx, handoffkeepRelayEventRequest{
		Kind: "job.completed", JobID: "job-t1034-fw", Epoch: 1, OwnerLane: "lane-a",
		ReportPath: "r.md", EventTime: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil || status != http.StatusCreated {
		t.Fatalf("appendEvent status=%d err=%v", status, err)
	}
	if err := client.markDelivered(ctx, stored.ID, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	if err := client.markDelivered(ctx, stored.ID, "host-b", "w9:p9"); err != nil {
		t.Fatal(err)
	}
	row, found, err := client.relayEvent(ctx, stored.ID)
	if err != nil || !found {
		t.Fatalf("relayEvent found=%v err=%v", found, err)
	}
	if row.DeliveredTo != "host-a/w1:p1" {
		t.Fatalf("second /delivered overwrote delivered_to: %q, want host-a/w1:p1", row.DeliveredTo)
	}
	if row.DeliveredAt == "" {
		t.Fatal("the first /delivered did not set delivered_at")
	}
	if got := fake.deliveredToFor(stored.ID); got != "host-a/w1:p1" {
		t.Fatalf("fake deliveredTo[%d]=%q, want host-a/w1:p1", stored.ID, got)
	}
}
