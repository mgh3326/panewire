package panewire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5324: a dirty or unpushed worktree on a node that advertises
// handover_snapshot_v1 is snapshotted and verified on origin before the
// close; the successor's checkout starts from the snapshot commit; the close
// binds the worktree to what the snapshot holds.

var handoverSnapCommit = strings.Repeat("c", 40)

func handoverSnapshotFixture(t *testing.T, job string, priority int, expected time.Duration) *quotaReplaceFixture {
	t.Helper()
	f := handoverFixture(t, job, priority, expected)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverSnapshot] = true
	f.hub.mu.Unlock()
	return f
}

// handoverSnapshotReply is a node's ok snapshot of a dirty worktree.
func handoverSnapshotReply(job string) map[string]any {
	return map[string]any{
		"outcome":   handoverSnapshotOK,
		"top":       "/work/" + job + "-checkout",
		"branch":    "wk/task",
		"ref":       handoverSnapshotRef(job, handoverSnapCommit),
		"commit":    handoverSnapCommit,
		"tree":      strings.Repeat("d", 40),
		"parent":    handoverPushed,
		"untracked": []string{".env", "notes.txt"},
	}
}

// sweepHandoverSnapshot runs the sweep for f's job: the pane and worktree
// checks, then the snapshot for a dirty or unpushed answer. The snapshot must
// come after the check and before anything that could close.
func sweepHandoverSnapshot(t *testing.T, f *quotaReplaceFixture, exhaustion poolExhaustion, check, snapshot map[string]any) poolJobResult {
	t.Helper()
	f.holder.Outcome.AccountKey = exhaustion.Account
	done := make(chan poolJobResult, 1)
	go func() { done <- f.hub.sweepPoolJob(t.Context(), exhaustion, f.holder) }()
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"job.result"` {
		t.Fatalf("first node command %s, want job.result", command["op"])
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"found": false, "quota_resumes": 0}})
	checked := false
	for check != nil || snapshot != nil {
		select {
		case result := <-done:
			return result
		default:
		}
		command := nodeCommandRead(t, f.node)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
		case `"worktree.handover_check"`:
			if checked {
				t.Fatal("handover_check ran twice")
			}
			checked = true
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": check})
			check = nil
		case `"worktree.handover_snapshot"`:
			if !checked {
				t.Fatal("snapshot before the worktree check")
			}
			var args nodeHandoverSnapshotArgs
			if err := json.Unmarshal(command["args"], &args); err != nil || args.Job != f.job || args.Epoch != 1 || args.Branch != "wk/task" || args.Cwd != "/work/project.checkout" {
				t.Fatalf("snapshot args=%s", command["args"])
			}
			if code, failed := snapshot["error"]; failed {
				nodeCommandReplyTo(t, f.node, command, map[string]any{"error": code})
			} else {
				nodeCommandReplyTo(t, f.node, command, map[string]any{"result": snapshot})
			}
			snapshot = nil
		default:
			// Nothing may close before the snapshot is stored and verified.
			t.Fatalf("unexpected node command during the sweep: %s", command["op"])
		}
	}
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("sweep did not finish")
	}
	return poolJobResult{}
}

func TestPoolHandoverSnapshotsADirtyWorktreeBeforeClose(t *testing.T) {
	const job, next = "dirty-stutter-5324", "dirty-stutter-5324-q1"
	f := handoverSnapshotFixture(t, job, 1, 3*time.Hour)
	exhaustion := window5h()
	result := sweepHandoverSnapshot(t, f, exhaustion, map[string]any{"outcome": handoverDirty}, handoverSnapshotReply(job))
	if result.action != poolJobQueued || result.why != "urgent" {
		t.Fatalf("sweep=%+v", result)
	}
	action := readOnlyPoolAction(t, f.hub)
	handover := action.Data.Handover
	snapshot := handover.Snapshot
	if action.Data.Kind != poolActionKindHandover || handover == nil || snapshot == nil ||
		snapshot.Ref != handoverSnapshotRef(job, handoverSnapCommit) || snapshot.Commit != handoverSnapCommit ||
		snapshot.Parent != handoverPushed || snapshot.Tree != strings.Repeat("d", 40) ||
		len(snapshot.Untracked) != 2 {
		t.Fatalf("stored snapshot=%+v", snapshot)
	}
	checkout := handover.Successor.Repository.Checkout
	if checkout == nil || checkout.Mode != fleet.HubCheckoutSuccessor || checkout.Branch != "wk/task" ||
		checkout.Sha != handoverSnapCommit || checkout.Fetch != snapshot.Ref {
		t.Fatalf("successor checkout=%+v", checkout)
	}
	brief := handover.Successor.Brief
	for _, want := range []string{snapshot.Ref, handoverSnapCommit, ".env", "notes.txt", "may still be at machine-a:/work/" + job + "-checkout until cleanup", "abandoned-worktree.patch", "Ignored files are not preserved"} {
		if !strings.Contains(brief, want) {
			t.Fatalf("brief misses %q:\n%s", want, brief)
		}
	}
	if strings.Contains(brief, "kept") {
		t.Fatalf("brief promises the old worktree is kept:\n%s", brief)
	}
	if _, err := fleet.ValidateSubmission(handover.Successor); err != nil {
		t.Fatalf("stored successor is not a valid submission: %v", err)
	}
	action = closeHandover(t, f, action)
	guard := action.Data.Args.Handover
	if guard == nil || guard.Commit != handoverSnapCommit || guard.Snapshot == nil ||
		guard.Snapshot.Tree != snapshot.Tree || guard.Snapshot.Parent != snapshot.Parent {
		t.Fatalf("close guard=%+v", guard)
	}
	finishSettled(t, f)
	placeSuccessor(t, f, next)
	finishSettled(t, f)
	if actions, _ := f.hub.inbox.store.activePoolActions(t.Context()); len(actions) != 0 {
		t.Fatalf("row not final after placement: %+v", actions)
	}
	lines := ownerLines(t, f.hub, "")
	want := "[panewire] handed " + job + " over to " + next + " on machine-b/sonnet (claude window:5h)"
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("owner lines=%q, want [%q]", lines, want)
	}
}

// A snapshot that fails or is refused means no close, no row, and an owner
// notice naming the reason.
func TestPoolHandoverSnapshotFailureSendsANoticeAndNoClose(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   map[string]any
		wantWhy string
	}{
		{"push refused", map[string]any{"outcome": handoverSnapshotPushFail}, "snapshot_push_failed"},
		{"unproven on origin", map[string]any{"outcome": handoverSnapshotUnproven}, "snapshot_origin_mismatch"},
		{"refused state", map[string]any{"outcome": handoverSnapshotRefused, "reason": handoverRefusedUnmerged}, "snapshot_refused_unmerged_index"},
		{"epoch behind the node's", map[string]any{"outcome": handoverEpochMismatch}, "snapshot_epoch_mismatch"},
		{"node error", map[string]any{"error": "snapshot_failed"}, "snapshot_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := handoverSnapshotFixture(t, "snapfail-job", 1, 0)
			result := sweepHandoverSnapshot(t, f, window5h(), map[string]any{"outcome": handoverUnpushed}, tc.reply)
			if result.action != poolJobNotified || result.why != tc.wantWhy {
				t.Fatalf("sweep=%+v want why=%s", result, tc.wantWhy)
			}
			if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), f.job); len(rows) != 0 {
				t.Fatalf("rows=%+v", rows)
			}
			lines := ownerLines(t, f.hub, "pool-exhausted:")
			if len(lines) != 1 || !strings.Contains(lines[0], tc.wantWhy) {
				t.Fatalf("notice=%q", lines)
			}
		})
	}
}

// A node that never advertised handover_snapshot_v1 keeps the clean-only
// behavior: the check's dirty or unpushed is the refusal itself.
func TestPoolHandoverWithoutSnapshotCapabilityKeepsCleanOnly(t *testing.T) {
	f := handoverFixture(t, "old-node-job", 1, 0)
	result := sweepHandover(t, f, window5h(), map[string]any{"outcome": handoverDirty})
	if result.action != poolJobNotified || result.why != handoverDirty {
		t.Fatalf("sweep=%+v", result)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), f.job); len(rows) != 0 {
		t.Fatalf("rows=%+v", rows)
	}
	lines := ownerLines(t, f.hub, "pool-exhausted:")
	if len(lines) != 1 || !strings.Contains(lines[0], handoverDirty) {
		t.Fatalf("notice=%q", lines)
	}
}

// A dirty worktree on an older node is refused; a capable node snapshots it.
func TestPoolHandoverSnapshotNoteNeverPromisesKept(t *testing.T) {
	old := quotaHolderRequest("implementer", "job-old")
	old.Repository.Checkout.Origin = "git@host:repo"
	snapshot := &poolHandoverSnapshot{
		Ref: handoverSnapshotRef("job-old", handoverSnapCommit), Commit: handoverSnapCommit,
		Tree: strings.Repeat("d", 40), Parent: handoverPushed, Untracked: []string{".env"}, UntrackedMore: 3,
	}
	read := nodeHandoverCheckResult{Outcome: handoverCheckOK, Top: "/work/job-old-checkout", Branch: "wk/task", Pushed: handoverSnapCommit}
	next, err := handoverSuccessorRequest(old, "job-old", "job-old-q1", "claude", "machine-a", window5h(), read, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	brief := next.Brief
	if strings.Contains(brief, "kept") {
		t.Fatalf("note promises the worktree is kept:\n%s", brief)
	}
	for _, want := range []string{snapshot.Ref, "(and 3 more)", "may still be at machine-a:/work/job-old-checkout until cleanup"} {
		if !strings.Contains(brief, want) {
			t.Fatalf("note misses %q:\n%s", want, brief)
		}
	}
	if _, err := fleet.ValidateSubmission(next); err != nil {
		t.Fatalf("successor invalid: %v", err)
	}
	if next.Repository.Checkout.Fetch != snapshot.Ref || next.Repository.Checkout.Sha != handoverSnapCommit {
		t.Fatalf("checkout=%+v", next.Repository.Checkout)
	}
}

// The fetch a successor checkout may name is the hand-over ref alone.
func TestSuccessorCheckoutFetchGrammar(t *testing.T) {
	base := func(fetch string) hubSpawnCheckout {
		return hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: fleet.HubCheckoutSuccessor, Branch: "wk/task", Sha: handoverSnapCommit, Fetch: fetch, Dir: "job-a-checkout"}
	}
	if !validHubSpawnCheckout(base("")) {
		t.Fatal("successor without fetch refused")
	}
	if !validHubSpawnCheckout(base(handoverSnapshotRef("job-a", handoverSnapCommit))) {
		t.Fatal("hand-over ref fetch refused")
	}
	for _, fetch := range []string{"refs/heads/main", "refs/panewire/handover/", "refs/panewire/handover/job-a", "refs/panewire/handover/job-a/notasha", "main:refs/heads/main"} {
		if validHubSpawnCheckout(base(fetch)) {
			t.Fatalf("fetch %q accepted", fetch)
		}
	}
	raw := `{"origin":"https://github.com/acme/repo.git","mode":"successor","branch":"wk/task","sha":"` + handoverSnapCommit + `","fetch":"` + handoverSnapshotRef("job-a", handoverSnapCommit) + `","dir":"job-a-checkout"}`
	if _, ok := decodeNodeCommandArgs(nodeCommandWorktreePrepare, json.RawMessage(raw)); !ok {
		t.Fatal("node refuses a successor prepare with the hand-over fetch")
	}
	bad := strings.Replace(raw, `"fetch":"`+handoverSnapshotRef("job-a", handoverSnapCommit), `"fetch":"refs/heads/main`, 1)
	if _, ok := decodeNodeCommandArgs(nodeCommandWorktreePrepare, json.RawMessage(bad)); ok {
		t.Fatal("node accepts an arbitrary fetch ref in successor mode")
	}
}
