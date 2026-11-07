package panewire

// hk 3411: a row held for a working cmd pane has no working-wait expiry while
// its job is live. It stays held past the old max_wait and is typed when the
// pane turns idle; it is dropped only when the pane is gone, the job ended, or
// relay.cmd_hold_max passed. A row an older node already dropped as
// working_wait_expired is re-delivered once at the pane's next idle wake.
// Everything runs on fakes: a real hub, a real node relay and a shell herdr.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// t3411Clock is the node's clock, moved forward by a test to stand for the
// time a cmd pane spends working.
type t3411Clock struct{ offset atomic.Int64 }

func (clock *t3411Clock) now() time.Time {
	return time.Now().Add(time.Duration(clock.offset.Load()))
}

func (clock *t3411Clock) advance(by time.Duration) { clock.offset.Add(int64(by)) }

// t3411Start is t3276Start with the node on a clock the test moves. The clock
// is installed before any relay goroutine starts.
func t3411Start(t *testing.T) (*t3276Rig, *t3411Clock) {
	t.Helper()
	rig := t3276Start(t)
	clock := &t3411Clock{}
	rig.node.now = clock.now
	return rig, clock
}

func t3411WaitCount(rig *t3276Rig) int {
	rig.waitMu.Lock()
	defer rig.waitMu.Unlock()
	return len(rig.waits)
}

// t3411LastWaitTimeout is the --timeout of the newest wait the node armed.
func t3411LastWaitTimeout(t *testing.T, rig *t3276Rig) time.Duration {
	t.Helper()
	rig.waitMu.Lock()
	wait := rig.waits[len(rig.waits)-1]
	rig.waitMu.Unlock()
	_, value, found := strings.Cut(wait, "--timeout ")
	if !found {
		t.Fatalf("wait %q has no --timeout", wait)
	}
	ms, err := strconv.ParseInt(strings.Fields(value)[0], 10, 64)
	if err != nil {
		t.Fatalf("wait %q: %v", wait, err)
	}
	return time.Duration(ms) * time.Millisecond
}

func t3411AssertNothingTyped(t *testing.T, pane *t3384Pane) {
	t.Helper()
	for _, pair := range []string{"pane send-text", "pane send-keys", "agent prompt", "agent send-keys"} {
		if got := pane.calls(t, pair); got != 0 {
			t.Fatalf("a working cmd pane got %d %q calls, typed=%q", got, pair, pane.typed(t))
		}
	}
}

// t3411ExpireWorking is the wake the old max_wait produced: the wait ran out
// with the pane still working. The node clock is first moved past it.
func t3411ExpireWorking(t *testing.T, rig *t3276Rig, clock *t3411Clock, past time.Duration) {
	t.Helper()
	clock.advance(past)
	before := t3411WaitCount(rig)
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)
	eventually(t, 2*time.Second, "the wait re-armed after the expiry", func() bool { return t3411WaitCount(rig) > before })
}

// Acceptance (a): a cmd pane working past the old max_wait keeps its row held
// and gets no keystroke; the wait is armed again, not spun at zero. When the
// pane turns idle the row is typed once and proven.
func TestCmdHoldOutlivesOldMaxWait_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)

	t3411ExpireWorking(t, rig, clock, defaultRelayMaxWait+time.Minute)

	t3411AssertNothingTyped(t, pane)
	if kinds, _ := rig.drainEvents(); len(kinds) != 0 {
		t.Fatalf("events=%v after the old max_wait, want none: the row stays held", kinds)
	}
	rows := t3384HeldRows(t, rig)
	if len(rows) != 1 || rows[0].EventID != held[0].EventID {
		t.Fatalf("held rows=%+v, want row %d still held", rows, held[0].EventID)
	}
	if deadline := rows[0].HeldSince.Add(rows[0].MaxWait); !deadline.After(clock.now()) {
		t.Fatalf("held deadline %s is not after now %s: the next wait would expire at once", deadline, clock.now())
	}
	if timeout := t3411LastWaitTimeout(t, rig); timeout < time.Minute {
		t.Fatalf("the re-armed wait has --timeout %s, want a real wait, not a spin", timeout)
	}

	pane.setStatus(t, "idle")
	rig.node.relayBusyManager().release(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times after idle, want 1 (typed=%q)", got, pane.typed(t))
	}
	if got := pane.calls(t, "pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys return called %d times, want 1", got)
	}
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want one proven relay.delivered", kinds)
	}
	if reason := payloads["relay.delivered"].Reason; !strings.Contains(reason, "nonce_echo+status_working") {
		t.Fatalf("relay.delivered reason=%q, want the echo-and-working proof", reason)
	}
	if typed := pane.typed(t); strings.Contains(typed[0], "[대기 만료") {
		t.Fatalf("typed=%q, want no expiry prefix: the row was delivered on idle, not on expiry", typed)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("the proven row stayed undelivered: %+v", row)
	}
	if rows := t3384HeldRows(t, rig); len(rows) != 0 {
		t.Fatalf("held rows left: %+v", rows)
	}
}

// Acceptance (b): three rows held past the old max_wait go out as three
// injects in the order they were held, one per idle wake.
func TestCmdHoldThreeRowsInOrderAfterIdle_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective, "idle", "max_wait=60")
	manager := rig.node.relayBusyManager()

	t3411ExpireWorking(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3411AssertNothingTyped(t, pane)
	if rows := t3384HeldRows(t, rig); len(rows) != len(held) {
		t.Fatalf("held rows=%+v, want all %d kept past the old max_wait", rows, len(held))
	}

	for turn := 1; turn <= len(held); turn++ {
		pane.setStatus(t, "idle")
		manager.release(t.Context(), "w1:pW", false)
		if got := pane.calls(t, "pane send-text"); got != turn {
			t.Fatalf("after idle wake %d: %d injects, want %d (typed=%q)", turn, got, turn, pane.typed(t))
		}
	}
	typed := pane.typed(t)
	if len(typed) != len(held) {
		t.Fatalf("typed %d texts, want %d: %q", len(typed), len(held), typed)
	}
	for index, directive := range held {
		nonce := relayNonce(relayHeld{EventID: directive.EventID, Text: directive.Text})
		if !strings.Contains(typed[index], nonce) || len(relayNoncesIn(typed[index])) != 1 {
			t.Fatalf("inject %d=%q, want only row %d (%s)", index+1, typed[index], directive.EventID, nonce)
		}
		if row := rig.row(directive.EventID); row.DeliveredAt == "" {
			t.Fatalf("row %d was not proven delivered", directive.EventID)
		}
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.delivered") != len(held) || t3276Count(kinds, "relay.dropped") != 0 {
		t.Fatalf("events=%v, want %d relay.delivered and no drop", kinds, len(held))
	}
}

// Acceptance (c): the pane disappears while the row is held past the old
// max_wait. The row is dropped with the lease gate's stale reason, nothing is
// typed, and the durable row stays undelivered.
func TestCmdHoldDroppedWhenPaneGone_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	t3411ExpireWorking(t, rig, clock, defaultRelayMaxWait+time.Minute)
	if rows := t3384HeldRows(t, rig); len(rows) != 1 {
		t.Fatalf("held rows=%+v, want the row kept before the pane goes", rows)
	}

	gone := t.TempDir()
	installFakeHerdr(t, gone, `#!/bin/sh
echo "$1 $2" >> '`+pane.dir+`/calls'
echo '{"error":{"code":"agent_not_found","message":"no agent in pane"}}'
exit 1
`)
	rig.node.relayBusyManager().release(t.Context(), "w1:pW", false)

	t3411AssertNothingTyped(t, pane)
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.dropped") != 1 || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v, want one relay.dropped", kinds)
	}
	if reason := payloads["relay.dropped"].Reason; reason != "pane_occupant_gone" {
		t.Fatalf("relay.dropped reason=%q, want pane_occupant_gone", reason)
	}
	if rows := t3384HeldRows(t, rig); len(rows) != 0 {
		t.Fatalf("held rows=%+v after the pane went, want none", rows)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("a dropped row was marked delivered: %+v", row)
	}
}

// The job that runs in the lane ended while its pane kept working: the row is
// dropped as job_ended at the next expiry instead of waiting for the ceiling.
func TestCmdHoldDroppedWhenJobEnded_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	root := t.TempDir()
	rig.node.jobsInboxRoot = root
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	events := filepath.Join(root, "jobs", t3276Worker, "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, kind string) {
		body, _ := json.Marshal(map[string]any{"kind": kind, "job_id": t3276Worker, "payload": map[string]any{"agent_label": t3276Worker}})
		if err := os.WriteFile(filepath.Join(events, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("00001-job.claim.json", "job.claim")

	t3411ExpireWorking(t, rig, clock, defaultRelayMaxWait+time.Minute)
	if rows := t3384HeldRows(t, rig); len(rows) != 1 {
		t.Fatalf("held rows=%+v, want the row kept while the job is live", rows)
	}

	write("00002-job.completed.json", "job.completed")
	t3411ExpireWorking(t, rig, clock, relayCmdHoldRecheck+time.Minute)

	t3411AssertNothingTyped(t, pane)
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.dropped") != 1 || payloads["relay.dropped"].Reason != relayDroppedJobEnded || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v dropped=%+v, want one relay.dropped %s", kinds, payloads["relay.dropped"], relayDroppedJobEnded)
	}
	if rows := t3384HeldRows(t, rig); len(rows) != 0 {
		t.Fatalf("held rows=%+v after the job ended, want none", rows)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("a dropped row was marked delivered: %+v", row)
	}
}

// Acceptance (d): the relay.cmd_hold_max ceiling (default 24h) ends the hold
// the way max_wait used to: nothing is typed, the row is told unconfirmed and
// dropped as working_wait_expired, the design lane hears once, and the
// durable row stays undelivered. An hour before it the row is still held.
func TestCmdHoldCeilingExpires_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)

	t3411ExpireWorking(t, rig, clock, defaultRelayCmdHoldMax-time.Hour)
	if rows := t3384HeldRows(t, rig); len(rows) != 1 {
		t.Fatalf("held rows=%+v an hour before the ceiling, want the row kept", rows)
	}
	if kinds, _ := rig.drainEvents(); len(kinds) != 0 {
		t.Fatalf("events=%v before the ceiling, want none", kinds)
	}

	clock.advance(time.Hour + time.Minute)
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)

	t3411AssertNothingTyped(t, pane)
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.unconfirmed") != 1 || t3276Count(kinds, "relay.dropped") != 1 || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v, want one relay.unconfirmed and one relay.dropped", kinds)
	}
	for _, kind := range []string{"relay.unconfirmed", "relay.dropped"} {
		if reason := payloads[kind].Reason; reason != relayDroppedWorkingExpired {
			t.Fatalf("%s reason=%q, want %s", kind, reason, relayDroppedWorkingExpired)
		}
	}
	var escalations []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the escalation to the design lane", func() bool {
		escalations = append(escalations, rig.escalations()...)
		return len(escalations) > 0
	})
	if len(escalations) != 1 || !strings.Contains(escalations[0].Text, relayDroppedWorkingExpired) {
		t.Fatalf("design lane escalations=%+v, want one naming %s", escalations, relayDroppedWorkingExpired)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("an undelivered row was marked delivered: %+v", row)
	}
	if rows := t3384HeldRows(t, rig); len(rows) != 0 {
		t.Fatalf("held rows=%+v after the ceiling, want none", rows)
	}
}

// Acceptance (e): a claude pane is unchanged. Its idle row past max_wait is
// typed into the working pane with the expiry prefix, and its deadline is not
// moved.
func TestClaudeHoldUnchanged_t3411(t *testing.T) {
	rig, clock := t3411Start(t)
	pane := t3384Install(t, "claude", "working")
	held := t3384Hold(t, rig, "idle")
	rows := t3384HeldRows(t, rig)
	if len(rows) != 1 || rows[0].MaxWait != defaultRelayMaxWait {
		t.Fatalf("held rows=%+v, want one idle row with the default max_wait", rows)
	}
	clock.advance(defaultRelayMaxWait + time.Minute)

	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)

	if got := pane.calls(t, "agent prompt"); got != 1 {
		t.Fatalf("agent prompt called %d times, want the expired row typed once", got)
	}
	if typed := pane.typed(t); len(typed) != 1 || !strings.HasPrefix(typed[0], "[대기 만료 ") {
		t.Fatalf("typed=%q, want one inject with the expiry prefix", typed)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("the claude row was not delivered: %+v", row)
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want no drop for a claude pane", kinds)
	}
}

// t3411IdleWake is the idle-wake observer's route request for the worker pane
// with state change sequence seq.
func t3411IdleWake(seq uint64) idleWakeRouteRequest {
	return idleWakeRouteRequest{
		EventID: idleWakeEventID(strings.Repeat("a", 32), "w1:pW", seq), Pane: "w1:pW", State: "idle",
		ChangedAt: time.Now().UTC().Format(time.RFC3339Nano), StateChangeSeq: seq,
	}
}

// t3411WorkerRelays drains the directives the hub queued for the worker lane.
func t3411WorkerRelays(rig *t3276Rig) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case directive := <-rig.dest.relays:
			if directive.Lane == t3276Worker {
				out = append(out, directive)
			}
		default:
			return out
		}
	}
}

// Acceptance (f): rows a node dropped as working_wait_expired (here through a
// ceiling already passed) are re-delivered at the pane's next idle wake:
// oldest first, one per idle wake, once each, and typed into the idle pane.
func TestCmdWorkingExpiredRowRedeliveredOnIdle_t3411(t *testing.T) {
	rig := t3276Start(t)
	rig.node.relayControl.cmdHoldMax = time.Nanosecond
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective, relayDeliverDirective)
	manager := rig.node.relayBusyManager()
	manager.releaseWorking(t.Context(), "w1:pW", true)
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != 2 || t3276Count(kinds, "relay.unconfirmed") != 2 {
		t.Fatalf("events=%v, want both rows dropped as %s", kinds, relayDroppedWorkingExpired)
	}
	t3411AssertNothingTyped(t, pane)
	pane.setStatus(t, "idle")

	var typed []string
	var nonces []string
	for turn, directive := range held {
		t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(uint64(turn+1)))
		var redelivered []hubRelayInjectEvent
		eventually(t, 3*time.Second, "the idle re-delivery", func() bool {
			redelivered = append(redelivered, t3411WorkerRelays(rig)...)
			return len(redelivered) > 0
		})
		// A window that proves the same idle wake queues no second row.
		time.Sleep(200 * time.Millisecond)
		redelivered = append(redelivered, t3411WorkerRelays(rig)...)
		if len(redelivered) != 1 || redelivered[0].EventID != directive.EventID {
			t.Fatalf("idle wake %d re-delivered %+v, want only row %d (oldest first)", turn+1, redelivered, directive.EventID)
		}
		if !strings.HasPrefix(redelivered[0].Text, "[held while this pane was working") {
			t.Fatalf("re-delivered text=%q, want the idle re-delivery label", redelivered[0].Text)
		}
		nonces = append(nonces, relayNonce(relayHeld{EventID: redelivered[0].EventID, Text: redelivered[0].Text}))
		manager.offer(t.Context(), t3276Message(redelivered[0]))
		if got := pane.calls(t, "pane send-text"); got != turn+1 {
			t.Fatalf("after idle wake %d: %d injects, want %d (typed=%q)", turn+1, got, turn+1, pane.typed(t))
		}
		typed = pane.typed(t)
		pane.setStatus(t, "idle")
	}
	for index, directive := range held {
		if !strings.Contains(typed[index], nonces[index]) {
			t.Fatalf("inject %d=%q, want row %d", index+1, typed[index], directive.EventID)
		}
		if row := rig.row(directive.EventID); row.DeliveredAt == "" {
			t.Fatalf("row %d is still open after its one re-delivery: %+v", directive.EventID, row)
		}
	}

	// Once each: later idle wakes, and a retry of one already used, bring
	// nothing more.
	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(1))
	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(9))
	// A window that proves nothing more is queued.
	time.Sleep(300 * time.Millisecond)
	if more := t3411WorkerRelays(rig); len(more) != 0 {
		t.Fatalf("idle wakes after both rows re-delivered queued %+v, want nothing", more)
	}
}

// A retry of an older idle wake that arrives after a newer one re-delivers
// nothing, even with another dropped row still waiting: only a later idle turn
// may bring the next row.
func TestCmdIdleRedeliveryIgnoresOlderWakeRetry_t3411(t *testing.T) {
	rig := t3276Start(t)
	rig.node.relayControl.cmdHoldMax = time.Nanosecond
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective, relayDeliverDirective, relayDeliverDirective)
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != len(held) {
		t.Fatalf("events=%v, want all %d rows dropped as %s", kinds, len(held), relayDroppedWorkingExpired)
	}
	t3411AssertNothingTyped(t, pane)

	for turn, seq := range []uint64{1, 2} {
		t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(seq))
		var redelivered []hubRelayInjectEvent
		eventually(t, 3*time.Second, "the idle re-delivery", func() bool {
			redelivered = append(redelivered, t3411WorkerRelays(rig)...)
			return len(redelivered) > 0
		})
		if len(redelivered) != 1 || redelivered[0].EventID != held[turn].EventID {
			t.Fatalf("idle wake %d re-delivered %+v, want only row %d", seq, redelivered, held[turn].EventID)
		}
	}

	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(1))
	// A window that proves the stale retry queues nothing.
	time.Sleep(300 * time.Millisecond)
	if more := t3411WorkerRelays(rig); len(more) != 0 {
		t.Fatalf("a retry of idle wake 1 after wake 2 re-delivered %+v, want nothing", more)
	}

	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(3))
	var third []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the third row at the next idle wake", func() bool {
		third = append(third, t3411WorkerRelays(rig)...)
		return len(third) > 0
	})
	if len(third) != 1 || third[0].EventID != held[2].EventID {
		t.Fatalf("idle wake 3 re-delivered %+v, want only row %d", third, held[2].EventID)
	}
}

// An idle wake whose replay spends nothing (here the durable retire write
// fails, so nothing is queued) keeps the row for the next idle wake instead of
// stranding it until a hub restart.
func TestCmdIdleRedeliveryRetriesUnspentRow_t3411(t *testing.T) {
	rig := t3276Start(t)
	rig.node.relayControl.cmdHoldMax = time.Nanosecond
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != 1 {
		t.Fatalf("events=%v, want the row dropped as %s", kinds, relayDroppedWorkingExpired)
	}
	t3411AssertNothingTyped(t, pane)
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = map[int64]bool{held[0].EventID: true}
	rig.fake.mu.Unlock()

	retirePath := "/v1/relay/events/" + strconv.FormatInt(held[0].EventID, 10) + "/delivered"
	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(1))
	// The row sits in the hub's memory until the wake picks it, so first wait
	// for the retire write the replay attempts, then for the row to come back.
	eventually(t, 3*time.Second, "the replay's failed retire write", func() bool {
		rig.fake.mu.Lock()
		defer rig.fake.mu.Unlock()
		for _, call := range rig.fake.calls {
			if call.Method == "POST" && call.Path == retirePath {
				return true
			}
		}
		return false
	})
	eventually(t, 3*time.Second, "the failed re-delivery to put the row back", func() bool {
		rig.hub.mu.Lock()
		defer rig.hub.mu.Unlock()
		_, kept := rig.hub.relayControl.workingExpired[held[0].EventID]
		return kept
	})
	if got := t3411WorkerRelays(rig); len(got) != 0 {
		t.Fatalf("a replay whose retire failed queued %+v, want nothing", got)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("the row closed although its retire failed: %+v", row)
	}

	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = nil
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(2))
	var redelivered []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the re-delivery at the next idle wake", func() bool {
		redelivered = append(redelivered, t3411WorkerRelays(rig)...)
		return len(redelivered) > 0
	})
	if len(redelivered) != 1 || redelivered[0].EventID != held[0].EventID {
		t.Fatalf("idle wake 2 re-delivered %+v, want row %d once", redelivered, held[0].EventID)
	}
}

// The job ends while its cmd pane is still working, then the pane turns idle:
// the held row is dropped as job_ended, not typed into the ended job's pane.
func TestCmdHoldDroppedWhenJobEndedBeforeIdle_t3411(t *testing.T) {
	rig := t3276Start(t)
	root := t.TempDir()
	rig.node.jobsInboxRoot = root
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	events := filepath.Join(root, "jobs", t3276Worker, "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, kind := range map[string]string{"00001-job.claim.json": "job.claim", "00002-job.completed.json": "job.completed"} {
		body, _ := json.Marshal(map[string]any{"kind": kind, "job_id": t3276Worker, "payload": map[string]any{"agent_label": t3276Worker}})
		if err := os.WriteFile(filepath.Join(events, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pane.setStatus(t, "idle")

	rig.node.relayBusyManager().release(t.Context(), "w1:pW", false)

	t3411AssertNothingTyped(t, pane)
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.dropped") != 1 || payloads["relay.dropped"].Reason != relayDroppedJobEnded || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v dropped=%+v, want one relay.dropped %s and no delivery", kinds, payloads["relay.dropped"], relayDroppedJobEnded)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("a dropped row was marked delivered: %+v", row)
	}
}

// A row the node held up to the ceiling was received more than the replay
// window ago. Its idle re-delivery is bounded by the drop, not the receipt, so
// it still reaches the pane instead of being retired as stale.
func TestCmdIdleRedeliveryOfRowHeldToCeiling_t3411(t *testing.T) {
	rig := t3276Start(t)
	rig.node.relayControl.cmdHoldMax = time.Nanosecond
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	rig.fake.mu.Lock()
	for _, row := range rig.fake.rows {
		if row.ID == held[0].EventID {
			row.ReceivedAt = time.Now().UTC().Add(-relayReplayMaxAge - time.Hour).Format(time.RFC3339Nano)
		}
	}
	rig.fake.mu.Unlock()
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != 1 {
		t.Fatalf("events=%v, want the row dropped as %s", kinds, relayDroppedWorkingExpired)
	}
	pane.setStatus(t, "idle")

	t650Send(t, rig.hub, "host-a", rig.dest, "idle-wake.route.request", t3411IdleWake(1))
	var redelivered []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the idle re-delivery of the old row", func() bool {
		redelivered = append(redelivered, t3411WorkerRelays(rig)...)
		return len(redelivered) > 0
	})
	if len(redelivered) != 1 || redelivered[0].EventID != held[0].EventID {
		t.Fatalf("re-delivered %+v, want row %d", redelivered, held[0].EventID)
	}
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(redelivered[0]))
	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("the re-delivered row was typed %d times, want 1", got)
	}
}
