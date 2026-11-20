package panewire

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// The acknowledgement of a reviewer's wait is judged against the bound that
// starts at the decision (hk 5803). Two cases a restart or a whole-second ledger
// timestamp used to get wrong: the request is in the ledger but its
// wait_requested write was lost, and the ack lands inside the bound's second.

// appendAck writes the watcher's acknowledgement of the manager's request through
// the arbiter's real append, with the store's clock at at. The ledger stores
// created_at to the whole second, as the watcher's writes do; legacy leaves out
// adopted_at, the field the watcher now records to the millisecond.
func (rig *waitRig) appendAck(t *testing.T, at time.Time, legacy bool) {
	t.Helper()
	req, ok := rig.ledgerEvent(t, "job.quota_stop_wait_request")
	if !ok {
		t.Fatal("the manager wrote no wait request")
	}
	until := req["until"].(string)
	untilAt, err := time.Parse(time.RFC3339, until)
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"pane_id": req["pane_id"], "attempt": req["attempt"], "pool": req["pool"], "until": until, "deadline": untilAt.Add(time.Minute).Format(time.RFC3339Nano)}
	if !legacy {
		payload["adopted_at"] = at.UTC().Format(time.RFC3339Nano)
	}
	store := &arbiter.Store{DB: arbiter.StatePath(t.TempDir()), Inbox: filepath.Join(rig.manager.inboxRoot, "jobs"), Now: func() time.Time { return at }}
	if _, err := store.AppendEvent(context.Background(), "job-active", "job.quota_stop_wait_adopted", payload, nil); err != nil {
		t.Fatal(err)
	}
}

// decisionCreated is the creation time, in milliseconds, of the rig's one stop.
func (rig *waitRig) decisionCreated(t *testing.T) int64 {
	t.Helper()
	var created int64
	if err := rig.store.db.QueryRow(`SELECT created FROM quota_stop_decisions WHERE job='job-active' AND decision<>''`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	return created
}

// waitAdopted reports whether the decision row records the watcher's acknowledgement.
func (rig *waitRig) waitAdopted(t *testing.T) bool {
	t.Helper()
	var adopted int64
	if err := rig.store.db.QueryRow(`SELECT wait_adopted FROM quota_stop_decisions WHERE job='job-active' AND decision<>''`).Scan(&adopted); err != nil {
		t.Fatal(err)
	}
	return adopted != 0
}

// The daemon exits between the request's ledger append and the manager's
// wait_requested write. The watcher acknowledges on time, and the daemon is back
// more than 30 s after the decision: the wait is kept, the reviewer is not handed
// over, and it is resumed at the reset. (Base: the cutoff finds wait_requested=0
// and hands the stop over without looking for the ack.)
func TestQuotaReviewerWaitRestartKeepsAnOnTimeAck(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	created := time.UnixMilli(rig.decisionCreated(t))
	if _, err := rig.store.db.Exec(`UPDATE quota_stop_decisions SET wait_requested=0`); err != nil {
		t.Fatal(err)
	}
	rig.appendAck(t, created.Add(5*time.Second), false)
	rig.manager.Tick(context.Background(), created.Add(31*time.Second))
	if !rig.waitAdopted(t) {
		t.Fatal("the on-time acknowledgement did not adopt the wait after the restart")
	}
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	if len(rig.announce) != 0 {
		t.Fatalf("handed over after an on-time acknowledgement: announced=%d", len(rig.announce))
	}
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want the waited-out resume and no hand-over", rig.sends, len(rig.announce))
	}
}

// The bound falls at .250 of a second. The ack is written inside that same second
// at .800 (precise), or at .800 without adopted_at (legacy): the ack is late and
// the reviewer is handed over. An ack at .200 of that second is on time. (Base:
// the ledger stores every ack at its whole second, which is inside the bound.)
func TestQuotaReviewerWaitAckTimeIsJudgedToTheMillisecond(t *testing.T) {
	for name, tc := range map[string]struct {
		at     time.Duration
		legacy bool
		want   string
	}{
		"precise ack after the bound":   {800 * time.Millisecond, false, "handover"},
		"legacy ack in the same second": {800 * time.Millisecond, true, "handover"},
		"precise ack before the bound":  {200 * time.Millisecond, false, "wait"},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newWaitRig(t)
			rig.decide()
			second := rig.now.Add(10 * time.Second)
			// The decision's bound is created+30 s: place it at .250 of second.
			if _, err := rig.store.db.Exec(`UPDATE quota_stop_decisions SET created=? WHERE job='job-active' AND decision<>''`, second.Add(250*time.Millisecond).Add(-quotaWaitAdoptBound).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			rig.appendAck(t, second.Add(tc.at), tc.legacy)
			rig.manager.Tick(context.Background(), second.Add(time.Second))
			rig.manager.Tick(context.Background(), second.Add(2*time.Second))
			if tc.want == "handover" {
				rig.wantDecision(t, "handover", "wait_not_adopted")
				if rig.waitAdopted(t) {
					t.Fatal("a late acknowledgement adopted the wait")
				}
				return
			}
			if !rig.waitAdopted(t) {
				t.Fatal("an acknowledgement within the bound did not adopt the wait")
			}
			rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
		})
	}
}
