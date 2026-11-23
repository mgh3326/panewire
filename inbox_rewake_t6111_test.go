package panewire

// hk 6111 (decision 481, option A): an inbox-only lane whose unread set is
// never announced again gets the wake line again, while its pane is idle, up
// to relay.inbox_rewake_max lines per set, no sooner than
// relay.inbox_rewake_interval after its last line. Every test drives the pass
// on the rig's injected clock; none starts the runInbox loop.

import (
	"strings"
	"testing"
	"time"
)

const rewakePane = "w1:design"

// rewakeSweep runs the re-wake pass as the runInbox tick does. The rig's node
// snapshot is refreshed to the injected clock first: a snapshot older than
// sessionsFindSnapshotMaxAge would read as no idle state at all.
func rewakeSweep(r *t3602Rig) {
	r.h.mu.Lock()
	if node := r.h.nodes["host-a"]; node != nil && node.sessionSnapshot != nil {
		node.sessionSnapshot.receivedAt = r.clock.Now()
	}
	r.h.mu.Unlock()
	r.h.rewakeInboxLanes()
}

// rewakeSetPaneStatus sets the status the node reports for one pane.
func rewakeSetPaneStatus(r *t3602Rig, pane, status string) {
	r.h.mu.Lock()
	defer r.h.mu.Unlock()
	snap := r.h.nodes["host-a"].sessionSnapshot
	for i := range snap.sessions {
		if snap.sessions[i].PaneID == pane {
			snap.sessions[i].Status = status
		}
	}
}

// rewakeNoLine fails unless nothing was typed into the lane's pane since the
// last check.
func rewakeNoLine(t *testing.T, r *t3602Rig, when string) {
	t.Helper()
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("%s: injections=%+v, want none", when, got)
	}
}

// rewakeOneLine fails unless exactly one wake line text went to the lane's
// pane since the last check.
func rewakeOneLine(t *testing.T, r *t3602Rig, when, text string) {
	t.Helper()
	got := t3426Injected(r.agent)
	if len(got) != 1 || got[0].Pane != rewakePane || got[0].Text != text || got[0].EventID != 0 {
		t.Fatalf("%s: injections=%+v, want exactly one %q in %s", when, got, text, rewakePane)
	}
}

// rewakeFileInfo files one unread info item (a MERGED record), which never
// wakes the lane by itself, and fails if it does.
func rewakeFileInfo(t *testing.T, r *t3602Rig) {
	t.Helper()
	if got := r.fileQuiet(t, t3602Event(t3602Design, "merged", "[mergewake] MERGED #400 hk 6111 into main"), r.agent); len(got) != 0 {
		t.Fatalf("info arrival typed %+v", got)
	}
}

// (a) An idle pane with one unread info item: the first line 30 m after the set
// is first seen, then at 60 m and 90 m, then none: relay.inbox_rewake_max
// (default 3) is spent for this set.
func TestInboxRewakeIdleLaneGetsThreeLinesThenNone(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	rewakeNoLine(t, r, "the set's first sweep")
	r.clock.advance(29 * time.Minute)
	rewakeSweep(r)
	rewakeNoLine(t, r, "29 m after the set was first seen")
	r.clock.advance(time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "30 m", t3602Wake(t3602Design, 1, "info"))
	for _, at := range []string{"60 m", "90 m"} {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		rewakeOneLine(t, r, at, t3602Wake(t3602Design, 1, "info"))
	}
	r.clock.advance(30 * time.Minute)
	rewakeSweep(r)
	rewakeNoLine(t, r, "120 m: three re-wakes already went out for this set")
}

// (b) A new unread item after the third line starts a new set, so the count
// resets and the lane gets its three re-wakes again.
func TestInboxRewakeNewArrivalStartsANewSet(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	for _, at := range []string{"30 m", "60 m", "90 m"} {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		rewakeOneLine(t, r, at, t3602Wake(t3602Design, 1, "info"))
	}
	r.clock.advance(10 * time.Minute)
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 6111: ship the re-wake?"), r.agent)
	t3602OneWake(t, got, rewakePane, t3602Wake(t3602Design, 2, "decision"))
	rewakeSweep(r)
	r.clock.advance(20 * time.Minute)
	rewakeSweep(r)
	rewakeNoLine(t, r, "20 m after the new arrival's line")
	r.clock.advance(10 * time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "30 m after the new arrival's line", t3602Wake(t3602Design, 2, "decision"))
	for _, at := range []string{"60 m", "90 m"} {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		rewakeOneLine(t, r, "new set, "+at, t3602Wake(t3602Design, 2, "decision"))
	}
	r.clock.advance(30 * time.Minute)
	rewakeSweep(r)
	rewakeNoLine(t, r, "new set exhausted")
}

// (c) Acking every unread item stops the lines: nothing unread, nothing sent.
func TestInboxRewakeAckOfEveryItemStopsTheLines(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	r.clock.advance(30 * time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "30 m", t3602Wake(t3602Design, 1, "info"))
	if n, err := r.h.inbox.store.ack(0, t3602Design, true, r.clock.Now()); err != nil || n != 1 {
		t.Fatalf("ack all = %d, %v; want 1 item acked", n, err)
	}
	for _, at := range []string{"60 m", "90 m", "120 m"} {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		rewakeNoLine(t, r, "acked lane at "+at)
	}
}

// (d) A working pane is never woken, however long the set waits; the first
// line comes once the pane reports idle again.
func TestInboxRewakeWorkingPaneIsNeverWoken(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	rewakeSetPaneStatus(r, rewakePane, "working")
	for _, at := range []string{"30 m", "60 m"} {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		rewakeNoLine(t, r, "working pane at "+at)
	}
	rewakeSetPaneStatus(r, rewakePane, "idle")
	r.clock.advance(time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "idle again at 61 m", t3602Wake(t3602Design, 1, "info"))
}

// (e) A lane whose coalescing window is open gets no re-wake until the window
// closes: the window's own line (or the owed one) announces the set.
func TestInboxRewakeWaitsForAnOpenWindow(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	r.clock.advance(30 * time.Minute)
	r.h.inbox.wakes.mu.Lock()
	r.h.inbox.wakes.lanes = map[string]*inboxWakeLane{t3602Design: {windowEnd: r.clock.Now().Add(defaultInboxWakeWindow)}}
	r.h.inbox.wakes.mu.Unlock()
	rewakeSweep(r)
	rewakeNoLine(t, r, "30 m, inside the window")
	r.clock.advance(time.Minute)
	rewakeSweep(r)
	rewakeNoLine(t, r, "31 m, still inside the window")
	r.clock.advance(4 * time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "35 m, window closed", t3602Wake(t3602Design, 1, "info"))
}

// (f) Every re-wake is one inbox_wake fleet event: outcome sent, reason rewake,
// the lane, the unread count and the line number within the set (hk 5509
// counts these).
func TestInboxRewakeEachLineIsAFleetEvent(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	rewakeSweep(r)
	for range 2 {
		r.clock.advance(30 * time.Minute)
		rewakeSweep(r)
		t3426Injected(r.agent)
	}
	ev := r.wakeEvents()
	if len(ev) != 2 {
		t.Fatalf("inbox_wake fleet events=%+v, want one per re-wake", ev)
	}
	for i, want := range []string{
		"lane=" + t3602Design + " class=design unread=1 most_urgent=info machine=host-a pane=" + rewakePane + " rewake=1",
		"lane=" + t3602Design + " class=design unread=1 most_urgent=info machine=host-a pane=" + rewakePane + " rewake=2",
	} {
		if e := ev[i]; e.Outcome != "sent" || e.Reason != "rewake" || e.Job != t3602Design || e.Detail != want {
			t.Fatalf("re-wake event %d = %+v, want sent/rewake with detail %q", i+1, e, want)
		}
	}
}

// The re-wake covers every unread kind, not only info: an unread READY and a
// MERGED record both count, and the line names the READY as most urgent.
func TestInboxRewakeAnnouncesReadyAndMergedToo(t *testing.T) {
	r := newT3602Rig(t, "default")
	rewakeFileInfo(t, r)
	r.fileQuiet(t, t3602Event(t3602Design, "ready", "READY hk 6111 PR #7 head abc1234"), r.agent)
	t3426Injected(r.agent)
	rewakeSweep(r)
	r.clock.advance(30 * time.Minute)
	rewakeSweep(r)
	rewakeOneLine(t, r, "30 m", t3602Wake(t3602Design, 2, "ready"))
}

// The two relay keys parse, take their defaults when absent, and a value out
// of range is refused, as the other relay values are.
func TestInboxRewakeConfigBounds(t *testing.T) {
	const anchor = "relay:\n  design_lanes: [\"*-design\"]\n"
	base := string(defaultDeliveryPolicy)
	if !strings.Contains(base, anchor) {
		t.Fatal("shipped policy has no relay block to extend")
	}
	parse := func(extra string) (*deliveryPolicy, error) {
		return parseDeliveryPolicy([]byte(strings.Replace(base, anchor, anchor+extra, 1)))
	}
	p, err := parse("")
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if p.Relay.rewakeInterval != 30*time.Minute || p.Relay.rewakeMax != 3 {
		t.Fatalf("defaults = %v, %d; want 30m, 3", p.Relay.rewakeInterval, p.Relay.rewakeMax)
	}
	for _, extra := range []string{
		"  inbox_rewake_interval: 5m\n  inbox_rewake_max: 1\n",
		"  inbox_rewake_interval: 24h\n  inbox_rewake_max: 20\n",
	} {
		if _, err := parse(extra); err != nil {
			t.Fatalf("in-range %q refused: %v", extra, err)
		}
	}
	for _, extra := range []string{
		"  inbox_rewake_interval: 4m\n",
		"  inbox_rewake_interval: 25h\n",
		"  inbox_rewake_interval: soon\n",
		"  inbox_rewake_max: 0\n",
		"  inbox_rewake_max: 21\n",
	} {
		if _, err := parse(extra); err == nil {
			t.Fatalf("out-of-range %q accepted", extra)
		}
	}
}
