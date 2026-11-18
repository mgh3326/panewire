package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A decision is reserved with the attempt the stop was observed in. A ledger
// that cannot be read reserves nothing: an unbound row would let a retried
// hand-over pass for the attempt a re-prompt started later (#699 review).
func TestReserveDecisionBindsTheAttemptAndRefusesAnUnreadableLedger_t5224(t *testing.T) {
	rig := newRoleStopRig(t, "implementer", time.Hour, "42")
	events := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
	// The rig's synthetic events carry a hub-style type; the fleet ledger
	// reader needs the kind the arbiter writes.
	for name, kind := range map[string]string{"00001-job.claimed.json": "job.claimed", "00002-job.spawned.json": "job.spawned"} {
		body := `{"kind":"` + kind + `","payload":{"pane_id":"workspace:pane-1","task_id":"42","owner_lane":"owner"}}`
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	job := HubActiveJob{JobID: "job-active", AgentLabel: "worker-synthetic"}
	broken := filepath.Join(events, "00003-job.note.json")
	if err := os.Symlink(filepath.Join(events, "missing"), broken); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.q.reserveDecision(context.Background(), idleWakeCandidate{EventID: "e-unreadable", PaneID: "workspace:pane-1"}, job, rig.now); err == nil {
		t.Fatal("reserved a decision on an unreadable ledger")
	}
	var rows int
	if err := rig.store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE event_id='e-unreadable'`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows=%d err=%v, want no reservation", rows, err)
	}
	if err := os.Remove(broken); err != nil {
		t.Fatal(err)
	}
	if fresh, _, err := rig.q.reserveDecision(context.Background(), idleWakeCandidate{EventID: "e-ok", PaneID: "workspace:pane-1"}, job, rig.now); err != nil || !fresh {
		t.Fatalf("fresh=%v err=%v", fresh, err)
	}
	var attempt string
	if err := rig.store.db.QueryRow(`SELECT attempt FROM quota_stop_decisions WHERE event_id='e-ok'`).Scan(&attempt); err != nil || attempt != "00002-job.spawned.json" {
		t.Fatalf("attempt=%q err=%v, want the spawn the stop was observed in", attempt, err)
	}
}
