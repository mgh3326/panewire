package panewire

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 5799: the quota-stop manager reads the pane of a live job. A completed
// job is not read, so a limit stop on its re-prompted pane goes unseen. The
// pane sweep's revive (internal/fleet/completed_revive.go) writes
// job.reprompted after job.completed; with that ledger the manager decides the
// same stop as for any live implementer. The revive itself is proven in
// internal/fleet (TestCompletedPaneRevivedWhenBusyAgain_hk5799).
func TestCompletedJobsLimitStopIsObservedOnlyOnceRevived_hk5799(t *testing.T) {
	for name, revived := range map[string]bool{"completed, not revived": false, "completed, revived": true} {
		t.Run(name, func(t *testing.T) {
			rig := newRoleStopRig(t, "implementer", time.Hour, roleStopTask)
			events := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
			completed := rig.now.Add(-30 * time.Minute).UTC().Format(time.RFC3339)
			writeRigEvent(t, events, "00003-job.completed.json", `{"kind":"job.completed","type":"job.completed","created_at":"`+completed+`"}`)
			if revived {
				at := rig.now.Add(-10 * time.Minute).UTC().Format(time.RFC3339)
				writeRigEvent(t, events, "00004-job.reprompted.json", `{"kind":"job.reprompted","type":"job.reprompted","created_at":"`+at+`","reason":"completed_pane_busy","pane_id":"workspace:pane-1"}`)
			}
			rig.run()
			var decided int
			if err := rig.store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE decision<>''`).Scan(&decided); err != nil {
				t.Fatal(err)
			}
			if !revived {
				if decided != 0 || rig.sends != 0 || len(rig.announce) != 0 {
					t.Fatalf("the manager read a completed job's pane: decisions=%d sends=%d announced=%d", decided, rig.sends, len(rig.announce))
				}
				return
			}
			if decided == 0 {
				t.Fatalf("no decision for the revived job; manager log:\n%s", rig.logs.String())
			}
			if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_expected" {
				t.Fatalf("revived job's stop decision=%s reason=%s, want wait reset_within_expected", d, reason)
			}
		})
	}
}

// writeRigEvent writes one job event file of a rig job, in the rig's own shape.
func writeRigEvent(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
