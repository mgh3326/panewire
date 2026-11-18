package panewire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// The hub finishes a pool close or hand-over row for a missing job only on the
// node's missing-job proof (hk 5296, docs/node-commands.md "A missing job").

func missingProofReply(epoch uint64) poolActionReply {
	return poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "", "missing_epoch": epoch}}
}

func bareAbsenceReply() poolActionReply {
	return poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": ""}}
}

// handoverRowWithSavedClose is the handover row the unreadable-ledger test
// uses: close arguments saved, the node holding no readable attempt.
func handoverRowWithSavedClose(t *testing.T, job string) (*quotaReplaceFixture, poolAction) {
	t.Helper()
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := readOnlyPoolAction(t, f.hub)
	checkout := action.Data.Handover.Successor.Repository.Checkout
	action.Data.Args = &nodePaneCloseOpArgs{RequestID: action.ID, Job: job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/project.checkout", EndReason: action.Data.EndReason, Handover: &nodeHandoverCloseGuard{Origin: checkout.Origin, Branch: checkout.Branch, Commit: checkout.Sha}}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return f, action
}

// storedPoolAction reads the row whatever its phase: a finished row is no
// longer active.
func storedPoolAction(t *testing.T, f *quotaReplaceFixture, job string) poolAction {
	t.Helper()
	rows, err := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	return rows[0]
}

func assertJobFence(t *testing.T, f *quotaReplaceFixture, job, owner string) {
	t.Helper()
	if got, err := f.hub.inbox.store.poolActionOwns(t.Context(), job); err != nil || got != owner {
		t.Fatalf("fence owner=%q err=%v, want %q", got, err, owner)
	}
}

func grantCloseMissing(f *quotaReplaceFixture) {
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseMissing] = true
	f.hub.mu.Unlock()
}

// stepArgs plays one reconcile pass, answers the first command with reply and
// returns the arguments that command carried.
func stepAskedStatus(t *testing.T, f *quotaReplaceFixture, action poolAction, reply poolActionReply) (json.RawMessage, error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), action) }()
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"`+nodeCommandPaneCloseStatus+`"` {
		t.Fatalf("first command=%s, want %s", command["op"], nodeCommandPaneCloseStatus)
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply.result})
	return command["args"], <-done
}

func TestPoolActionMissingJobProofFinishesTheRow_t5296(t *testing.T) {
	f, action := poolActionFixture(t)
	grantCloseMissing(f)
	assertPoolActionFence(t, f, action, false)
	args, err := stepAskedStatus(t, f, action, missingProofReply(1))
	if err != nil {
		t.Fatal(err)
	}
	var asked nodeCloseOpRefArgs
	if json.Unmarshal(args, &asked) != nil || asked.Epoch != action.Epoch || asked.RequestID != action.ID {
		t.Fatalf("status args=%s, want the row's epoch %d", args, action.Epoch)
	}
	if row := storedPoolAction(t, f, action.Job); row.Phase != poolActionRefused {
		t.Fatalf("row phase=%s, want refused", row.Phase)
	}
	assertPoolActionFence(t, f, action, true)
	assertJobFence(t, f, action.Job, "")
	lines := ownerLines(t, f.hub, "pool-action-missing:")
	if len(lines) != 1 || !strings.Contains(lines[0], action.Job) || !strings.Contains(lines[0], "machine-a") || !strings.Contains(lines[0], action.ID) {
		t.Fatalf("owner notice=%q, want one line naming job, host and row", lines)
	}
}

func TestPoolHandoverMissingJobProofFinishesHandoverFailed_t5296(t *testing.T) {
	const job = "missing-proved-job"
	f, action := handoverRowWithSavedClose(t, job)
	grantCloseMissing(f)
	if _, err := stepAskedStatus(t, f, action, missingProofReply(1)); err != nil {
		t.Fatal(err)
	}
	row := storedPoolAction(t, f, job)
	if row.Phase != poolActionRefused || row.Data.Handover.Outcome != handoverFailed || row.Data.Handover.Reason != handoverJobMissing {
		t.Fatalf("row=%+v handover=%+v, want refused handover_failed job_missing", row, row.Data.Handover)
	}
	assertJobFence(t, f, job, "")
	lines := ownerLines(t, f.hub, "pool-handover:")
	if len(lines) != 1 || !strings.Contains(lines[0], job) || !strings.Contains(lines[0], handoverJobMissing) {
		t.Fatalf("owner notice=%q, want one hand-over failed line", lines)
	}
}

// Bare absence, a proof from a lower epoch, and a proof from a node that never
// advertised the answer all leave the row held, with the 30-minute notice.
func TestPoolActionMissingJobWithoutProofIsHeld_t5296(t *testing.T) {
	for _, tc := range []struct {
		name       string
		capability bool
		reply      poolActionReply
		epoch      uint64
	}{
		{"bare absence from a node that can prove", true, bareAbsenceReply(), 1},
		{"proof from a lower epoch", true, missingProofReply(1), 2},
		{"proof from a node that did not advertise it", false, missingProofReply(1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, action := poolActionFixture(t)
			if tc.capability {
				grantCloseMissing(f)
			}
			action.Epoch = tc.epoch
			start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
			now := start
			f.hub.now = func() time.Time { return now }
			step := func() {
				t.Helper()
				replies := []poolActionReply{tc.reply}
				if tc.reply.result["missing_epoch"] == nil {
					// The older flow: close attempt, then the release that proves nothing.
					replies = append(replies,
						poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCaptureNotAJob}},
						poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false}})
				} else if !tc.capability {
					replies = append(replies,
						poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCaptureNotAJob}},
						poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false}})
				}
				err := runPoolActionStep(t, f.hub, f.node, action, replies...)
				if tc.capability && tc.reply.result["missing_epoch"] != nil {
					// A proof below the row's epoch is an error, not a hold on a later step.
					if err == nil || !strings.Contains(err.Error(), "missing-job proof") {
						t.Fatalf("reconcile error=%v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "release not proved") {
					t.Fatalf("reconcile error=%v", err)
				}
				if row := readOnlyPoolAction(t, f.hub); row.Phase != poolActionPending {
					t.Fatalf("row phase=%s, want pending", row.Phase)
				}
				action.Epoch = tc.epoch
				assertPoolActionFence(t, f, action, false)
			}
			step()
			if lines := ownerLines(t, f.hub, "pool-action-missing:"); len(lines) != 0 {
				t.Fatalf("a missing-job notice without proof: %q", lines)
			}
		})
	}
}

// An older node never gets the epoch argument and never finishes the row.
func TestPoolActionOlderNodeKeepsTheHoldAndNotice_t5296(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	now := start
	f.hub.now = func() time.Time { return now }
	step := func() {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)) }()
		for _, reply := range []poolActionReply{bareAbsenceReply(),
			{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCaptureNotAJob}},
			{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false}}} {
			command := nodeCommandRead(t, f.node)
			if reply.op == nodeCommandPaneCloseStatus && strings.Contains(string(command["args"]), "epoch") {
				t.Errorf("an older node was sent the epoch argument: %s", command["args"])
			}
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply.result})
		}
		if err := <-done; err == nil || !strings.Contains(err.Error(), "release not proved") {
			t.Fatalf("reconcile error=%v", err)
		}
		assertPoolActionFence(t, f, action, false)
	}
	step()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 0 {
		t.Fatalf("early notice=%q", lines)
	}
	now = start.Add(30 * time.Minute)
	step()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 {
		t.Fatalf("notices after 30 minutes=%q, want one", lines)
	}
}

// A node that can prove still holds a handover row whose attempt is
// unreadable: the answer carries no proof, and the saved close stays held.
func TestPoolHandoverUnreadableAttemptStaysHeldOnAProvingNode_t5296(t *testing.T) {
	f, action := handoverRowWithSavedClose(t, "unreadable-on-proving-node")
	grantCloseMissing(f)
	if _, err := stepAskedStatus(t, f, action, bareAbsenceReply()); err == nil || !strings.Contains(err.Error(), "attempt unavailable") {
		t.Fatalf("error=%v", err)
	}
	if row := readOnlyPoolAction(t, f.hub); row.Phase != poolActionPending {
		t.Fatalf("row=%+v", row)
	}
	assertJobFence(t, f, action.Job, action.ID)
}
