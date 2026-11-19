package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5324: the close of a snapshot hand-over binds the worktree to what the
// snapshot holds — HEAD is still the snapshot's parent and the tracked state
// still matches the snapshot commit — and proves the ref on origin still
// names it. A worktree that changed since the snapshot refuses the close.

// snapshotBoundClose takes a real snapshot of a dirty worktree and returns
// the close-op arguments bound to it.
func snapshotBoundClose(t *testing.T, client *HubClient, inbox, work, origin string) (map[string]any, nodeHandoverSnapshotResult) {
	t.Helper()
	event := filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt)
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
	if err := os.WriteFile(event, receipt, 0600); err != nil {
		t.Fatal(err)
	}
	raw, code := nodeRun(t, client, nodeCommandHandoverSnapshot, map[string]any{"job": closeOpJob, "epoch": 1, "cwd": work, "origin": origin, "branch": "wk/task"})
	if code != "" || raw["outcome"] != handoverSnapshotOK {
		t.Fatalf("snapshot=%v code=%q", raw, code)
	}
	var snap nodeHandoverSnapshotResult
	if err := json.Unmarshal(mustJSON(raw), &snap); err != nil {
		t.Fatal(err)
	}
	args := closeOpArgs("op-handover-snapshot")
	args["cwd"] = work
	args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": snap.Commit,
		"snapshot": map[string]any{"tree": snap.Tree, "parent": snap.Parent}}
	return args, snap
}

func dirtySnapshotWorktree(t *testing.T, work string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("saved by the snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "untracked.txt"), []byte("listed only"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCloseOpBindsHandoverWorktreeToItsSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name            string
		change          func(*testing.T, string, string)
		outcome, reason string
		closes          int
	}{
		{"unchanged", func(*testing.T, string, string) {}, fleet.CloseOpClosed, "", 1},
		{"tracked edit after snapshot", func(t *testing.T, work, _ string) {
			if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("after the snapshot"), 0600); err != nil {
				t.Fatal(err)
			}
		}, fleet.CloseOpRefused, handoverDirty, 0},
		{"new commit after snapshot", func(t *testing.T, work, _ string) {
			if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("after the snapshot"), 0600); err != nil {
				t.Fatal(err)
			}
			handoverGit(t, work, "commit", "-am", "late work")
		}, fleet.CloseOpRefused, handoverUnpushed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, _ := handoverRepo(t)
			dirtySnapshotWorktree(t, work)
			inbox := t.TempDir()
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
			client := closeOpNode(t, inbox, fake)
			args, _ := snapshotBoundClose(t, client, inbox, work, origin)
			tc.change(t, work, origin)
			result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
			if code != "" || result["outcome"] != tc.outcome || fake.called("pane", "close") != tc.closes {
				t.Fatalf("close=%v code=%q calls=%v", result, code, fake.calls)
			}
			if tc.reason != "" && result["reason"] != tc.reason {
				t.Fatalf("close=%v want reason=%s", result, tc.reason)
			}
		})
	}
}

// The ref-delete helper above deletes under the job prefix; keep the exact
// ref deletion honest.
func TestCloseOpSnapshotBoundRefDeleted(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	dirtySnapshotWorktree(t, work)
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	args, snap := snapshotBoundClose(t, client, inbox, work, origin)
	handoverGit(t, origin, "update-ref", "-d", snap.Ref)
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if code != "" || result["outcome"] != fleet.CloseOpRefused || result["reason"] != handoverGitFailed || fake.called("pane", "close") != 0 {
		t.Fatalf("close=%v code=%q calls=%v", result, code, fake.calls)
	}
}

// Without a snapshot bound, the guard still binds the pushed tip alone: the
// snapshot fields are a mutation the validator and the recheck both reject
// being skipped silently.
func TestCloseOpSnapshotBoundArgsAreValidated(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	dirtySnapshotWorktree(t, work)
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	args, _ := snapshotBoundClose(t, client, inbox, work, origin)
	guard := args["handover"].(map[string]any)
	guard["snapshot"] = map[string]any{"tree": "notasha", "parent": handoverPushed}
	_, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if code == "" {
		t.Fatal("a malformed snapshot bound was accepted")
	}
}
