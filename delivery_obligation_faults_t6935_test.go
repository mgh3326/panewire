package panewire

// hk 6935: a job.* row whose pane stayed working is not owed a budget copy. The
// design (v4 §6) gives working_wait_expired no budget retry, and an idle wake
// revives only lane.event rows, so the row ends exhausted with nothing typed.
// Killed by: giving a job.* working expiry the one budget copy a maybe-typed
// job.* fault earns.

import (
	"testing"
	"time"
)

func TestJobWorkingExpiryEarnsNoBudgetCopy(t *testing.T) {
	rig := newT6384Rig(t)
	rig.hub.relayJobEventFrom("host-a", "job.escalate", hubJobEventPayload{
		JobID: "job-6935-working", Epoch: 1, OwnerLane: "worker-a", EventID: "escalation-6935",
		Question: "which way?", ReportPath: "report.md",
	})
	d := rig.injected()
	t6820Report(t, rig, d, t6820Working)
	t6820AwaitDropExpiry(t, rig, d)
	rig.pass(0)
	rig.pass(time.Minute)
	if got := t3426Injected(rig.agent); len(got) != 0 {
		t.Fatalf("a job.* row whose pane stayed working was copied again: %+v", got)
	}
	if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Fault != relayDroppedWorkingExpired {
		t.Fatalf("job row after its working expiry: %+v", o)
	}
}
