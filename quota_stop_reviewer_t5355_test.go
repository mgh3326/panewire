package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A reviewer on a quota stop (hk 5690): a stop is announced for the hub to carry
// on to another pool, unless its reset is short: then it may wait and be resumed
// once, after its watcher adopted the wait (hk 5355, hk 5749).
// The rig is the implementer rig with the spawn receipt naming a reviewer. The
// manager runs in shadow mode, and a reviewer is still acted on.

const reviewerStopTask = roleStopTask // priority 3, expected 4h

func newReviewerStopRig(t *testing.T, startedAgo time.Duration, task, screenPath string) *roleStopRig {
	t.Helper()
	return newRoleStopRigScreen(t, "reviewer", startedAgo, task, screenPath, "")
}

// reviewerLedgerHas reports whether the job's ledger holds an event of kind.
func reviewerLedgerHas(t *testing.T, rig *roleStopRig, kind string) bool {
	t.Helper()
	dir := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), `"kind":"`+kind+`"`) {
			return true
		}
	}
	return false
}

// decisionFor returns the quota decision recorded for one stop, by its event id.
func decisionFor(t *testing.T, rig *roleStopRig, eventID string) (decision, reason string) {
	t.Helper()
	if err := rig.store.db.QueryRow(`SELECT decision,reason FROM quota_stop_decisions WHERE event_id=? AND decision<>''`, eventID).Scan(&decision, &reason); err != nil {
		t.Fatalf("no decision for stop %s: %v", eventID, err)
	}
	return
}

// A reviewer whose reset is beyond the reviewer wait is handed over at once
// (hk 5690): announced once for the hub to carry on to another pool. It is never
// resumed, and no wait is requested of its watcher (hk 5749,
// quota_stop_reviewer_wait_t5749_test.go: a short reset may wait, behind the
// watcher's acknowledgement).
func TestQuotaReviewerLongResetIsHandedOver(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2h.txt")
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_beyond_reviewer_wait" {
		t.Fatalf("decision=%s reason=%s, want handover reset_beyond_reviewer_wait", d, reason)
	}
	if rig.sends != 0 {
		t.Fatalf("a reviewer was resumed: sends=%d", rig.sends)
	}
	if len(rig.announce) != 1 || rig.announce[0].Pool != "devin-free" {
		t.Fatalf("announced=%+v, want one devin-free announcement", rig.announce)
	}
	if reviewerLedgerHas(t, rig, "job.quota_stop_wait_request") {
		t.Fatal("a wait was requested of a reviewer's watcher for a long reset")
	}
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
	if len(rig.announce) != 1 {
		t.Fatalf("the reviewer was announced again: %d", len(rig.announce))
	}
}

// The quota-stop path acts only on a proven stop (mergewake review, hk 5355): a
// reviewer hand-over recorded for a failed screen read (pool unknown) must not
// reach the watcher's intent, even with --quota-stop-act on.
func TestQuotaReviewerUnprovenHandoverIsNotEndedWhenActing(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt")
	rig.q.act = true
	now := rig.now
	if _, err := rig.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,role,created) VALUES('evt-unproven','job-active','workspace:pane-1','worker-synthetic','unknown','unknown','handover','screen_read_timeout','reviewer',?)`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rig.q.tick(context.Background(), now, false)
	if len(rig.announce) != 0 {
		t.Fatalf("an unproven reviewer hand-over ended the reviewer: %+v", rig.announce)
	}
}

// hk 5690 review: the pane state a manager hand-over announces is herdr's own
// state_change_seq of the stop, not the wake table's local counter (which counts
// from 1 and is independent). A pane first seen working at herdr sequence 3 and
// stopped at 4 has local sequence 2; the announcement must carry 4, or the hub's
// fenced close refuses an unchanged stopped pane.
func TestManagerHandOverAnnouncesHerdrsSequenceNotTheLocalCounter(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2h.txt")
	if err := rig.manager.ObserveSnapshot(context.Background(), []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "working", Revision: 1, SourceStateChangeSeq: 3}}, rig.now); err != nil {
		t.Fatal(err)
	}
	if err := rig.manager.ObserveSnapshot(context.Background(), []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Revision: 2, SourceStateChangeSeq: 4}}, rig.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	if len(rig.announce) != 1 {
		t.Fatalf("announcements=%d, want one", len(rig.announce))
	}
	if got := rig.announce[0].PaneSeq; got != 4 {
		t.Fatalf("announced PaneSeq=%d, want herdr's 4 (the local counter is not it)", got)
	}
}

// hk 5690 review: a stall examination's candidate carries no local sequence (it
// keeps it in quota_stall_wakes), so resolving the sequence from the candidate
// alone stored zero and the announcement went out unfenced. The reservation reads
// the stall wake's sequence and stores herdr's.
func TestStallExaminationDecisionRecordsHerdrsSequence(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2h.txt")
	for _, st := range []struct {
		status string
		rev    int64
		seq    int64
	}{{"working", 1, 3}, {"idle", 2, 4}} {
		if err := rig.manager.ObserveSnapshot(context.Background(), []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: st.status, Revision: st.rev, SourceStateChangeSeq: st.seq}}, rig.now); err != nil {
			t.Fatal(err)
		}
	}
	var local int64
	if err := rig.store.db.QueryRow(`SELECT state_change_seq FROM idle_wake_panes WHERE pane_id='workspace:pane-1'`).Scan(&local); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.store.db.Exec(`INSERT INTO quota_stall_wakes(event_id,pane_id,state_change_seq) VALUES('stall-ev','workspace:pane-1',?)`, local); err != nil {
		t.Fatal(err)
	}
	job := HubActiveJob{JobID: "job-active", AgentLabel: "worker-synthetic", Role: "reviewer", OwnerLane: "owner"}
	// A stall candidate has no StateChangeSeq of its own.
	if _, _, err := rig.q.reserveDecision(context.Background(), idleWakeCandidate{PaneID: "workspace:pane-1", EventID: "stall-ev"}, job, rig.now); err != nil {
		t.Fatal(err)
	}
	if got := rig.q.observedSeq(context.Background(), "stall-ev"); got != 4 {
		t.Fatalf("recorded herdr sequence %d, want 4 (local %d)", got, local)
	}
}
