package panewire

// hk 3602 review fixes: an inbox-only lane is never typed a body, whatever
// path reaches its pane -- a hub digest or hold batch, a record an older hub
// stored as an interrupt, or an event whose id merely looks like a chat turn
// -- and its one-line wake follows the window rules across a trailing flush
// with nothing unread, a trial that ends, and a hub restart.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// t3602NoBody fails unless every injection is a wake line for lane: no body,
// no batch, no handoffkeep row id.
func t3602NoBody(t *testing.T, got []hubRelayInjectEvent, lane string) {
	t.Helper()
	for _, injected := range got {
		if injected.EventID != 0 || !strings.HasPrefix(injected.Text, "[inbox] "+lane+": ") {
			t.Fatalf("a body was typed into inbox-only lane %s: %+v", lane, injected)
		}
	}
}

func t3602Unread(t *testing.T, h *HubServer, lane, kind, jobID string) laneInboxItem {
	t.Helper()
	item := t3426Item(t, h, lane, kind, jobID)
	if item.Class != "inbox" || item.ReadAt != 0 || item.AckedAt != 0 || item.DueAt != 0 || item.DigestAt != 0 || item.HoldUntil != 0 {
		t.Fatalf("%s %s stored as %+v, want an unread inbox item with no deadline, digest or hold", kind, jobID, item)
	}
	return item
}

// Finding 1: a digest or hold batch for a design lane or a desk lane -- the
// personal desk, whose inbox_summary override already kept its batches out,
// and a per-fleet desk with no override -- is never typed. Its items are
// filed unread instead.
func TestInboxWakeBatchesAreNeverTyped(t *testing.T) {
	for _, lane := range []string{t3602Design, "operator-desk", "work-operator-desk"} {
		for _, batch := range []string{"digest", "hold"} {
			t.Run(lane+"/"+batch, func(t *testing.T) {
				r := newT3602Rig(t, "default")
				// An item an older hub stored for a digest or a working hold.
				e := t3602Event(lane, "", "ESC stored before hk 3602: which order do we ship?")
				window, hold := 30*time.Second, time.Duration(0)
				if batch == "hold" {
					window, hold = 0, time.Minute
				}
				if _, err := r.h.inbox.store.putFrom("", relayEventDedupeKey("lane.event", e), lane, "lane.event", "interrupt", e, r.clock.Now().UTC(), 0, window, hold); err != nil {
					t.Fatal(err)
				}
				r.clock.advance(2 * time.Minute)
				r.h.flushInbox()
				r.h.flushInbox()
				t3602NoBody(t, t3426Injected(r.agent), lane)
				t3602Unread(t, r.h, lane, "lane.event", e.JobID)
			})
		}
	}
}

// Finding 2: a record an older hub stored as an interrupt for a design lane
// is never typed, whether its producer retries it, its job event is resent,
// or the hub replays its undelivered handoffkeep row. It is filed unread, and
// the lane gets its wake instead.
func TestInboxWakeStoredInterruptIsNeverTyped(t *testing.T) {
	t.Run("producer retry", func(t *testing.T) {
		r := newT3602Rig(t, "default")
		e := t3602Event(t3602Design, "", "ESC stored before hk 3602: which order do we ship?")
		if _, err := r.h.inbox.store.putFrom("host-a", relayEventDedupeKey("lane.event", e), t3602Design, "lane.event", "interrupt", e, r.clock.Now().UTC(), 0, 0); err != nil {
			t.Fatal(err)
		}
		res := r.h.relayLaneEvent(e, r.agent)
		t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "decision"))
		t3602Unread(t, r.h, t3602Design, "lane.event", e.JobID)
		if res.ID != 0 && r.fake.deliveredToFor(res.ID) == "" {
			t.Fatalf("the filed row %d stays undelivered for replay", res.ID)
		}
	})
	t.Run("job event resend", func(t *testing.T) {
		r := newT3602Rig(t, "default")
		c := t3426Completion("consult-1", t3602Design, "")
		if _, err := r.h.inbox.store.putFrom("host-a", relayEventDedupeKey("job.completed", c), t3602Design, "job.completed", "interrupt", c, r.clock.Now().UTC(), 0, 0); err != nil {
			t.Fatal(err)
		}
		r.h.relayJobCompletionFrom("host-a", c)
		t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "ready"))
		t3602Unread(t, r.h, t3602Design, "job.completed", c.JobID)
	})
	t.Run("replay", func(t *testing.T) {
		r := newT3602Rig(t, "default")
		e := t3602Event(t3602Design, "", "ESC stored before hk 3602: the older hub never delivered this")
		if _, err := r.h.inbox.store.putFrom("host-a", relayEventDedupeKey("lane.event", e), t3602Design, "lane.event", "interrupt", e, r.clock.Now().UTC(), 0, 0); err != nil {
			t.Fatal(err)
		}
		r.fake.seedUndelivered(handoffkeepRelayEvent{ID: 801, Kind: "lane.event", JobID: e.JobID, Epoch: 1, OwnerLane: t3602Design, EventID: e.EventID, Text: e.Text, ReceivedAt: r.clock.Now().Format(time.RFC3339Nano)})
		r.h.replayUndeliveredRelayEvents(context.Background())
		t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "decision"))
		t3602Unread(t, r.h, t3602Design, "lane.event", e.JobID)
		if r.fake.deliveredToFor(801) == "" {
			t.Fatal("the filed row stays undelivered for the next replay")
		}
	})
}

// Finding 4: a trailing flush that finds nothing unread closes the window, so
// the next wake-worthy arrival is announced at once.
func TestInboxWakeTrailingFlushWithNothingUnreadClosesTheWindow(t *testing.T) {
	r := newT3602Rig(t, "default")
	t3602OneWake(t, r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?"), r.agent), "w1:design", t3602Wake(t3602Design, 1, "decision"))
	r.clock.advance(time.Minute)
	if got := r.fileQuiet(t, t3602Event(t3602Design, "blocked", "BLOCKED hk 3602: CI needs an operator secret"), r.agent); len(got) != 0 {
		t.Fatalf("an arrival inside the window typed %+v", got)
	}
	// The session reads its inbox before the window ends.
	if _, err := r.h.inbox.store.ack(0, t3602Design, true, r.clock.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	r.clock.advance(4*time.Minute + time.Second)
	r.h.flushInboxWakes()
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("a trailing flush with nothing unread typed %+v", got)
	}
	r.clock.advance(time.Second)
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: and the hub first?"), r.agent)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "decision"))
}

// Finding 5: a trial wake still owed when relay.worker_inbox_trial_until
// passes is dropped: the lane is a worker lane again.
func TestInboxWakeTrialPendingAcrossUntilIsDropped(t *testing.T) {
	r := newT3602Rig(t, t3602TrialPolicy(t))
	until, _ := time.Parse(time.RFC3339, t3602Until)
	r.clock.advance(until.Sub(r.clock.Now()) - 2*time.Minute)
	t3602OneWake(t, r.fileQuiet(t, t3602Event("trial-3404", "findings", "FINDINGS #12: 2 blocking"), nil), "w1:trial", t3602Wake("trial-3404", 1, "findings"))
	r.clock.advance(time.Minute)
	if got := r.fileQuiet(t, t3602Event("trial-3404", "findings", "FINDINGS #12 round 2: 1 blocking"), nil); len(got) != 0 {
		t.Fatalf("an arrival inside the window typed %+v", got)
	}
	r.clock.advance(4*time.Minute + time.Second)
	r.h.flushInboxWakes()
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("a trial wake was sent after the trial ended: %+v", got)
	}
	if ev := r.wakeEvents(); len(ev) != 1 {
		t.Fatalf("inbox_wake fleet events=%+v, want only the first trial wake", ev)
	}
}

// Finding 6: a hub restart gives every inbox-only lane with wake-worthy
// unread items one wake, recorded with reason startup. A lane whose unread
// items are info only gets none, and the window keeps it to one line.
func TestInboxWakeStartupWakesEachLaneOnce(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	start, _ := time.Parse(time.RFC3339, t3602Start)
	clock := &t3602Clock{now: start}
	r := t3602Hub(t, path, "default", fake, hk, clock)
	r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?"), r.agent)
	r.fileQuiet(t, t3602Event("operator-desk", "decision", "Decision required: choose the release"), nil)
	r.fileQuiet(t, t3602Event("operator-desk", "automation-failed", "[panewire] SPAWN_FAILED job-9 request=r9 host=none profile=none rc=70 stage=launch replay=no reason=spawn_failed fix=Check the checkout."), nil)
	r.fileQuiet(t, t3602Event("work-operator-desk", "merged", "[mergewake] MERGED #400 hk 3602 into main"), r.agent)
	r.h.Close()

	clock.advance(time.Hour)
	r2 := t3602Hub(t, path, "default", fake, hk, clock)
	r2.h.flushInboxWakes()
	if got := t3426Injected(r2.agent); len(got) != 0 {
		t.Fatalf("startup wake sent before its window ended: %+v", got)
	}
	clock.advance(time.Minute)
	r2.h.Close()
	r3 := t3602Hub(t, path, "default", fake, hk, clock)
	r3.h.flushInboxWakes()
	if got := t3426Injected(r3.agent); len(got) != 0 {
		t.Fatalf("second restart sent before its window ended: %+v", got)
	}
	clock.advance(5 * time.Minute)
	r3.h.flushInboxWakes()
	got := t3426Injected(r3.agent)
	want := map[string]string{
		"w1:design": t3602Wake(t3602Design, 1, "decision"),
		"w1:desk":   t3602Wake("operator-desk", 2, "decision"),
	}
	if len(got) != len(want) {
		t.Fatalf("startup injections=%+v, want one wake for %d lanes", got, len(want))
	}
	for _, injected := range got {
		if want[injected.Pane] != injected.Text || injected.EventID != 0 {
			t.Fatalf("startup injection %+v, want %q", injected, want[injected.Pane])
		}
		delete(want, injected.Pane)
	}
	startup := 0
	for _, ev := range r3.wakeEvents() {
		if ev.Outcome == "sent" && ev.Reason == "startup" {
			startup++
		}
	}
	if startup != 2 {
		t.Fatalf("inbox_wake fleet events=%+v, want two sent with reason startup", r3.wakeEvents())
	}
}

func TestInboxWakeFiledBatchWakeSurvivesQuarantineFailure(t *testing.T) {
	r := newT3602Rig(t, "default")
	e := t3602Event(t3602Design, "decision", "ESC hk 3602: quarantine failure still owes a wake")
	if _, err := r.h.inbox.store.putFrom("", relayEventDedupeKey("lane.event", e), t3602Design, "lane.event", "interrupt", e, r.clock.Now().UTC(), 0, 0); err != nil {
		t.Fatal(err)
	}
	const batchID = "quiet-digest-test"
	if _, err := r.h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`, batchID, t3602Design, "batch"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.h.inbox.store.db.Exec(`UPDATE lane_inbox SET digest_batch=? WHERE lane=?`, batchID, t3602Design); err != nil {
		t.Fatal(err)
	}
	if _, err := r.h.inbox.store.db.Exec(`CREATE TRIGGER fail_batch_quarantine BEFORE INSERT ON lane_inbox_batch_quarantine BEGIN SELECT RAISE(FAIL, 'fixture quarantine failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := r.h.fileInboxOnlyBatch(t3602Design, "design", batchID); err == nil {
		t.Fatal("fileInboxOnlyBatch succeeded despite quarantine failure")
	}
	t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "decision"))
	t3602Unread(t, r.h, t3602Design, "lane.event", e.JobID)
}
