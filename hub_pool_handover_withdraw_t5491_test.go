package panewire

import (
	"context"
	"strings"
	"testing"
)

// hk 5491: a hand-over successor still queued (accepted, unbound, unclaimed by
// any host) is withdrawn once its job no longer needs it, through the requester
// withdrawal's release. The row ends withdrawn with the cause; the owner hears
// one info line, or nothing for a terminal task; nothing says SPAWN_FAILED.

// queuedHandover runs the hk 4633 hand-over to the point where the successor is
// submitted and queued: no pool has room, so it stays accepted.
func queuedHandover(t *testing.T, job string) (*quotaReplaceFixture, hubSpawnTask) {
	t.Helper()
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettled(t, f)
	successor := readOnlyPoolAction(t, f.hub).Data.Handover.Job
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), successor)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "accepted" {
		t.Fatalf("successor %s status=%s, want queued accepted", successor, task.Status)
	}
	return f, task
}

// successorNotices returns the lane texts that name the successor's request.
func successorNotices(t *testing.T, f *quotaReplaceFixture, request string) []string {
	t.Helper()
	var texts []string
	for _, event := range deliveredSpawnTexts(t, f.hub, f.destination) {
		if strings.Contains(event.Text, request) {
			texts = append(texts, event.Text)
		}
	}
	return texts
}

// withHandoverInputs replaces the task read the sweep takes (bound job, state).
func withHandoverInputs(f *quotaReplaceFixture, job, state string) {
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1, bound: job, state: state}
	}
}

// assertHandoverWithdrawn checks the successor ended cancelled with the cause
// and the hand-over row ended withdrawn, not failed.
func assertHandoverWithdrawn(t *testing.T, f *quotaReplaceFixture, job string, task hubSpawnTask, cause string) {
	t.Helper()
	saved, err := f.hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "cancelled" || saved.Outcome.Reason != "handover_withdrawn: "+cause {
		t.Fatalf("successor status=%s reason=%q, want cancelled with handover_withdrawn: %s", saved.Status, saved.Outcome.Reason, cause)
	}
	rows, err := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil || len(rows) != 1 || rows[0].Phase != poolActionDone || rows[0].Data.Handover.Outcome != handoverWithdrawn || rows[0].Data.Handover.Reason != cause {
		t.Fatalf("rows=%+v err=%v, want one done row withdrawn for %s", rows, err, cause)
	}
}

// The successor is queued, then the task moves on to another job (the incident:
// -h2 rebound it while q1 waited): the successor is withdrawn, owner told once.
func TestPoolHandoverWithdrawsQueuedSuccessorWhenTaskIsRebound_t5491(t *testing.T) {
	const job = "rebound-job"
	f, task := queuedHandover(t, job)
	withHandoverInputs(f, job+"-h2", "developing")
	finishSettled(t, f)
	assertHandoverWithdrawn(t, f, job, task, "task_rebound")
	lines := successorNotices(t, f, task.Request.RequestID)
	if len(lines) != 1 || !strings.Contains(lines[0], "[panewire] WITHDRAWN") || !strings.Contains(lines[0], "task_rebound") {
		t.Fatalf("owner lines=%q, want one info line naming the withdrawal", lines)
	}
	if strings.Contains(lines[0], "SPAWN_FAILED") || strings.Contains(lines[0], "handover_failed") {
		t.Fatalf("owner line %q says failure for a withdrawn successor", lines[0])
	}
}

// The task reaches a terminal hk state while the successor waits: withdrawn,
// and the owner hears nothing about it.
func TestPoolHandoverWithdrawsQueuedSuccessorWhenTaskIsTerminal_t5491(t *testing.T) {
	const job = "terminal-job"
	f, task := queuedHandover(t, job)
	withHandoverInputs(f, job, "merged")
	finishSettled(t, f)
	assertHandoverWithdrawn(t, f, job, task, "task_terminal")
	if lines := successorNotices(t, f, task.Request.RequestID); len(lines) != 0 {
		t.Fatalf("owner lines=%q, want none for a terminal task", lines)
	}
}

// The owner closes the successor's job record: withdrawn with that cause.
func TestPoolHandoverWithdrawsQueuedSuccessorWhenItsJobIsClosed_t5491(t *testing.T) {
	const job = "closed-job"
	f, task := queuedHandover(t, job)
	f.jobs.mu.Lock()
	f.jobs.records[task.Job] = map[string]any{"state": "lost", "end_reason": "closed by owner"}
	f.jobs.mu.Unlock()
	finishSettled(t, f)
	assertHandoverWithdrawn(t, f, job, task, "successor_closed")
}

// A successor whose task still names the old job (or the successor itself) is
// legitimately waiting: it stays queued, and it is placed when a pool frees.
func TestPoolHandoverQueuedSuccessorStaysWhileTaskUnchanged_t5491(t *testing.T) {
	const job = "waiting-job"
	f, task := queuedHandover(t, job)
	withHandoverInputs(f, job, "developing")
	finishSettled(t, f)
	saved, err := f.hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "accepted" {
		t.Fatalf("queued successor status=%s, want accepted while its task is unchanged", saved.Status)
	}
	placeSuccessor(t, f, task.Job)
	finishSettled(t, f)
	rows, err := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil || len(rows) != 1 || rows[0].Data.Handover.Outcome != handoverPlaced {
		t.Fatalf("rows=%+v err=%v, want one row placed once a pool freed", rows, err)
	}
}
