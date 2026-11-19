package fleet

import (
	"context"
	"testing"
)

// hk 3704: the task pane sweep's reap (reapSweptPaneHeld, pane_sweep.go) is
// where a job's end is written; it queues the at-end dir delete once
// job.reaped is on record, and a failed write queues nothing.
func TestReapSweptPaneQueuesTheAtEndDelete(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.unreap("j1")
	j, err := r.a.job("j1")
	if err != nil {
		t.Fatal(err)
	}
	s := &TaskPaneSweeper{RelayOwed: relayOwes(true, nil)}
	s.reapSweptPaneHeld(context.Background(), r.a, j, j, "", paneSweepJobEnd(j, ""), jobSession{}, false, r.now)

	if _, queued := s.endedDirs["j1"]; !queued {
		t.Fatalf("j1 was not queued for the at-end delete after job.reaped was written (queued=%v)", s.endedDirs)
	}
	if after, err := r.a.job("j1"); err != nil || !after.Reaped {
		t.Fatalf("job.reaped not on record after the reap: reaped=%v err=%v", after.Reaped, err)
	}
	if !r.exists("j1") {
		t.Fatal("the reap itself deleted the dir; the delete belongs to the queued attempt")
	}
}
