package panewire

import (
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5573: mergewake's round-5 call names the job its task is bound to. After a
// quota stop or a pool hand-over that job is a successor ("<job>-q<n>") whose
// submission names --parent-job and no --task, and the step refused it
// job_mismatch ("not the task's original implementer"), so a chain that had one
// hop never got its ladder step. The chain head, the job the task's binding
// names, is the author the step replaces, whatever its place in the chain.

// handoverFixtureRequest is the head's submission as the pool hand-over builds
// it: handoverSuccessorRequest itself, so the test follows its checkout mode.
func handoverFixtureRequest(job, parent string) fleet.Submission {
	old := escalateHolderRequest(parent)
	next, err := handoverSuccessorRequest(old, parent, job, "claude", "machine-b", window5h(), nodeHandoverCheckResult{Branch: "wk/task", Top: "/work/" + parent + "-checkout", Pushed: strings.Repeat("a", 40)}, nil)
	if err != nil {
		panic(err)
	}
	return next
}

// stepsChainHead checks one successful step from a chain head and returns the
// successor's stored submission.
func (f *escalateFixture) stepsChainHead(t *testing.T) fleet.Submission {
	t.Helper()
	placed := placedAnywhere
	result, _ := f.escalate(t, &placed)
	want, _, _ := escalateAuthorJob(f.job)
	if result.Code != "" || result.Job != want || result.Rung != "sonnet@xhigh" || result.AuthorEnded {
		t.Fatalf("the chain head must step: %+v", result)
	}
	successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	request := successor.Request
	if got := hubSpawnArg(request.Args, "--parent-job"); got != f.job || slices.Contains(request.Args, "--task") {
		t.Fatalf("the ladder successor must name the head %q as its parent and no task: %v", f.job, request.Args)
	}
	if got := hubSpawnArg(request.Args, "--containment"); got != "hk:42" {
		t.Fatalf("containment=%q: the step must carry the task, not the successor's empty --task", got)
	}
	if _, id, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || id != want {
		t.Fatalf("the ladder successor must hold the task role: job=%q err=%v", id, err)
	}
	return request
}

// A quota-stop successor holds the task: the step goes ahead on it.
func TestEscalateAuthorStepsAQuotaStopSuccessor_t5573(t *testing.T) {
	f := newEscalateChainFixture(t, "pw-task-80-q1", "pw-task-80", nil)
	request := f.stepsChainHead(t)
	if request.Repository.Checkout == nil || request.Repository.Checkout.Dir != "pw-task-80-q1-checkout" {
		t.Fatalf("the successor keeps the head's worktree: %+v", request.Repository)
	}
}

// A pool hand-over successor is stored in the checkout's successor mode; the
// ladder successor reuses its live worktree and branch as a branch checkout.
func TestEscalateAuthorStepsAHandoverSuccessor_t5573(t *testing.T) {
	f := newEscalateChainFixture(t, "pw-task-81-q1", "pw-task-81", handoverFixtureRequest)
	held, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil || held.Request.Repository.Checkout.Mode != fleet.HubCheckoutSuccessor {
		t.Fatalf("the fixture must hold a hand-over shaped head: %v %+v", err, held.Request.Repository.Checkout)
	}
	request := f.stepsChainHead(t)
	checkout := request.Repository.Checkout
	if checkout == nil || checkout.Mode != "branch" || checkout.Fetch != "" || checkout.Dir != "pw-task-81-q1-checkout" || checkout.Branch != "wk/task" || !checkout.VerifyReuse || checkout.Sha != strings.Repeat("b", 40) {
		t.Fatalf("the ladder successor must reuse the live worktree: %+v", checkout)
	}
}

// A successor whose predecessor had completed claimed no task role at the
// hand-over; the step claims it fresh instead of refusing.
func TestEscalateAuthorStepsASuccessorThatHoldsNoRole_t5573(t *testing.T) {
	f := newEscalateChainFixture(t, "pw-task-82-q1", "pw-task-82", nil)
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	// Free the role the way a hand-over from a completed predecessor left it.
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_reservations SET task_role=NULL WHERE request_id=?`, holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatal("the fixture's role must be free")
	}
	f.stepsChainHead(t)
}

// A request that names an older link of the chain is refused, naming the job
// that holds the task. Nothing is touched and no successor is staged.
func TestEscalateAuthorRefusesANonHolderAndNamesTheHolder_t5573(t *testing.T) {
	f := newEscalateChainFixture(t, "pw-task-83-q1", "pw-task-83", nil)
	request := f.request()
	request.Job = "pw-task-83"
	result, err := f.hub.escalateAuthor(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Code != "job_mismatch" || result.AuthorEnded || !strings.Contains(result.Reason, "pw-task-83-q1") || strings.Contains(result.Reason, "not pw-task-83-q1") {
		t.Fatalf("the original is no longer the holder: %+v", result)
	}
	if ops := commandOps(f.take()); len(ops) != 0 {
		t.Fatalf("a refusal must not touch the pane: %v", ops)
	}
	for _, job := range []string{"pw-task-83-l1", "pw-task-83-q1-l1"} {
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job); err == nil {
			t.Fatalf("no successor %s may be staged", job)
		}
	}
	if _, id, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || id == "" {
		t.Fatalf("the task role stays with the head: %v", err)
	}
}

// A successor of another task's chain is no holder of this one.
func TestEscalateAuthorRefusesASuccessorOfAnotherTask_t5573(t *testing.T) {
	f := newEscalateChainFixture(t, "pw-task-84-q1", "pw-task-84", nil)
	request := f.request()
	request.Task = "43"
	result, err := f.hub.escalateAuthor(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Code != "job_mismatch" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if ops := commandOps(f.take()); len(ops) != 0 {
		t.Fatalf("a refusal must not touch the pane: %v", ops)
	}
}

// ladderStepsAllowed walks a job chain the way the policy does: it takes ladder
// steps while escalateAuthorJob allows one, and a hop (quota-stop or hand-over
// replacement, named by quotaReplacementJob) falls in front of every step
// named by hopBefore.
func ladderStepsAllowed(start string, hopBefore ...int) int {
	job, steps := start, 0
	for guard := 0; guard < 8; guard++ {
		if slices.Contains(hopBefore, steps) {
			hopped, _, ok := quotaReplacementJob(job)
			if !ok {
				break
			}
			job = hopped
		}
		next, _, ok := escalateAuthorJob(job)
		if !ok {
			break
		}
		job, steps = next, steps+1
	}
	return steps
}

// The step limit counts once per logical chain: a hop before the step, after it
// or both neither resets nor bypasses it.
func TestEscalateAuthorStepLimitCountsOverTheWholeChain_t5573(t *testing.T) {
	base := ladderStepsAllowed("pw-task-85")
	if base != escalateAuthorMaxStep {
		t.Fatalf("a chain with no hop gets %d step(s), want %d", base, escalateAuthorMaxStep)
	}
	for name, hops := range map[string][]int{"before the step": {0}, "after the step": {1}, "before and after": {0, 1}} {
		if got := ladderStepsAllowed("pw-task-85", hops...); got != base {
			t.Fatalf("a hop %s: %d step(s), want %d", name, got, base)
		}
	}
	if got := ladderStepsAllowed("pw-task-85-q1"); got != base {
		t.Fatalf("a chain that starts on a hop head: %d step(s), want %d", got, base)
	}
}

// End to end: a hop head steps once, and the ladder successor it started (and a
// later hop of that) has no step left, as for a chain with no hop.
func TestEscalateAuthorSecondStepOfAHoppedChainHasNoRung_t5573(t *testing.T) {
	for _, head := range []string{"pw-task-86-q1-l1", "pw-task-87-l1", "pw-task-88-l1-q1"} {
		parent, _, _ := strings.Cut(head, "-q")
		if strings.HasSuffix(head, "-l1") {
			parent = strings.TrimSuffix(head, "-l1")
		}
		f := newEscalateChainFixture(t, head, parent, nil)
		result, err := f.hub.escalateAuthor(t.Context(), f.request())
		if err != nil {
			t.Fatal(err)
		}
		if result.Code != "no_rung" || result.AuthorEnded {
			t.Fatalf("%s: result=%+v", head, result)
		}
		if ops := commandOps(f.take()); len(ops) != 0 {
			t.Fatalf("%s: a refusal must not touch the pane: %v", head, ops)
		}
	}
}
