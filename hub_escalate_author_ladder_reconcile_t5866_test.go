package panewire

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The ladder close reconciler (hk 5866, docs/escalate-author.md): one table
// decides what a node verdict does to a staged step, for the handler, the
// replay, the expiry pass and every cancellation.

func TestDecideLadderIsTheTable(t *testing.T) {
	unanswered := ladderVerdict{}
	verdict := func(state string) ladderVerdict { return ladderVerdict{state: state, answered: true} }
	for _, tc := range []struct {
		name string
		v    ladderVerdict
		want [2]ladderAction // predecessor completed, predecessor live
	}{
		{"unavailable", unanswered, [2]ladderAction{ladderKeep, ladderKeep}},
		{nodeLadderUnknown, verdict(nodeLadderUnknown), [2]ladderAction{ladderKeep, ladderKeep}},
		{nodeLadderClosing, verdict(nodeLadderClosing), [2]ladderAction{ladderKeep, ladderKeep}},
		{nodeLadderClosed, verdict(nodeLadderClosed), [2]ladderAction{ladderActivate, ladderActivate}},
		{nodeLadderGone, verdict(nodeLadderGone), [2]ladderAction{ladderRelease, ladderActivate}},
		{nodeLadderRefused, verdict(nodeLadderRefused), [2]ladderAction{ladderRelease, ladderRelease}},
		{nodeLadderFenced, verdict(nodeLadderFenced), [2]ladderAction{ladderRelease, ladderRelease}},
	} {
		for live, want := range tc.want {
			if got := decideLadder(tc.v, live == 1, false); got != want {
				t.Errorf("%s live=%d: action %d, want %d", tc.name, live, got, want)
			}
			// A cancellation reason releases a settled K and never an ambiguous one.
			wantCancel := ladderRelease
			if want == ladderKeep {
				wantCancel = ladderKeep
			}
			if got := decideLadder(tc.v, live == 1, true); got != wantCancel {
				t.Errorf("%s live=%d cancelled: action %d, want %d", tc.name, live, got, wantCancel)
			}
		}
	}
}

// stageBoundLiveSuccessor stages the successor of a live author with its close
// binding frozen, as a step that sent its close and stopped leaves it.
func (f *ladderCloseFixture) stageBoundLiveSuccessor(t *testing.T) {
	t.Helper()
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.Repository.Checkout.Sha = strings.Repeat("b", 40)
	next.Repository.Checkout.VerifyReuse = true
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), holder.Request.RequestID, "42:implementer", "staged", ""); err != nil {
		t.Fatal(err)
	}
	requestID, err := fleet.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.bindStagedClose(t.Context(), next.RequestID, fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolV1,
		RequestID: requestID, Node: "machine-a", Epoch: 3, Attempt: "attempt-1", Pane: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/" + f.job + "-checkout"}); err != nil {
		t.Fatal(err)
	}
}

type ladderStageOutcome string

const (
	stageKept      ladderStageOutcome = "kept"
	stageReleased  ladderStageOutcome = "released"
	stageActivated ladderStageOutcome = "activated"
)

func (f *ladderCloseFixture) stageOutcome(t *testing.T) ladderStageOutcome {
	t.Helper()
	stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	switch {
	case err != nil:
		return stageReleased
	case stage.Status == "staged":
		return stageKept
	}
	return stageActivated
}

// Every verdict reaches the same action whoever reads it: the handler (a live
// author's close), the replay of a completed author's step, the expiry pass
// (its author gone, and still present) and a cancellation (the task moved).
func TestLadderReconcilerEveryVerdictEveryCaller(t *testing.T) {
	const ambiguous = stageKept
	type want struct {
		outcome ladderStageOutcome
		code    string // the answer's code; "" for an expiry pass, which answers nobody
	}
	// callers lists, per verdict, what the table gives each caller.
	callers := []string{"handler", "replay", "expiry", "expiry_present", "cancel"}
	table := map[string][5]want{
		"none":            {{ambiguous, "pending"}, {ambiguous, "pending"}, {ambiguous, ""}, {ambiguous, ""}, {ambiguous, "pending"}},
		nodeLadderUnknown: {{ambiguous, "pending"}, {ambiguous, "pending"}, {ambiguous, ""}, {ambiguous, ""}, {ambiguous, "pending"}},
		nodeLadderClosing: {{ambiguous, "pending"}, {ambiguous, "pending"}, {ambiguous, ""}, {ambiguous, ""}, {ambiguous, "pending"}},
		nodeLadderClosed:  {{stageActivated, ""}, {stageActivated, ""}, {stageActivated, ""}, {stageReleased, ""}, {stageReleased, "job_mismatch"}},
		nodeLadderGone:    {{stageActivated, ""}, {stageReleased, "parent_ended"}, {stageReleased, ""}, {stageReleased, ""}, {stageReleased, "job_mismatch"}},
		nodeLadderRefused: {{stageReleased, "pane_not_closed"}, {stageReleased, "parent_ended"}, {stageReleased, ""}, {stageReleased, ""}, {stageReleased, "job_mismatch"}},
		nodeLadderFenced:  {{stageReleased, "pane_not_closed"}, {stageReleased, "parent_ended"}, {stageReleased, ""}, {stageReleased, ""}, {stageReleased, "job_mismatch"}},
	}
	for verdict, row := range table {
		for i, caller := range callers {
			want := row[i]
			t.Run(caller+"/"+verdict, func(t *testing.T) {
				f := newLadderCloseFixture(t, fmt.Sprintf("pw-task-5866-%s-%s", strings.ReplaceAll(caller, "_", "-"), verdict), true)
				var placed *hubSpawnOutcome
				if want.outcome == stageActivated {
					value := placedAnywhere
					placed = &value
				}
				code := ""
				switch caller {
				case "handler":
					f.answer = verdict
					result, _ := f.escalate(t, placed)
					code = result.Code
				case "replay":
					f.fenceAnswer = verdict
					f.stageBoundCompletedSuccessor(t)
					f.paneAbsent = true
					result, _ := f.escalate(t, placed)
					code = result.Code
				case "expiry", "expiry_present":
					f.fenceAnswer = verdict
					if caller == "expiry" {
						f.stageBoundCompletedSuccessor(t)
						f.paneAbsent = true
					} else {
						f.stageBoundLiveSuccessor(t)
					}
					f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
					f.hub.expireStagedLadderSteps(t.Context())
				case "cancel":
					f.fenceAnswer = verdict
					f.stageBoundLiveSuccessor(t)
					f.mu.Lock()
					f.taskBound = "another-job"
					f.mu.Unlock()
					result, _ := f.escalate(t, nil)
					code = result.Code
				}
				if got := f.stageOutcome(t); got != want.outcome {
					t.Fatalf("stage %s, want %s", got, want.outcome)
				}
				if want.code != "" && code != want.code {
					t.Fatalf("answer code %q, want %q", code, want.code)
				}
				if want.outcome == ambiguous {
					// One decision event, however often the reconciler runs.
					f.awaitInfo(t, "unresolved")
					if caller == "expiry" || caller == "expiry_present" {
						f.hub.expireStagedLadderSteps(t.Context())
					} else {
						f.escalate(t, nil)
					}
					// A window that proves nothing more is filed: the repeat shares
					// the first event's id and awaitInfo drained the first one.
					time.Sleep(200 * time.Millisecond)
					for _, text := range f.infoTexts() {
						if strings.Contains(text, "unresolved") {
							t.Fatalf("a second decision event: %q", text)
						}
					}
				}
			})
		}
	}
}
