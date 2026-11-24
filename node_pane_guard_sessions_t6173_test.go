package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// sessionPaneFake points `herdr pane list` at a fake that answers per herdr
// session the way a real herdr scopes its panes by HERDR_SESSION: session S
// lists the panes in lists[S], and a session absent from lists cannot be read
// (the fake exits non-zero). An absolute path, never PATH, so no real herdr runs.
func sessionPaneFake(t *testing.T, lists map[string]string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake needs a POSIX shell")
	}
	panes := t.TempDir()
	for session, body := range lists {
		if err := os.WriteFile(filepath.Join(panes, session+".json"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/bin/sh\nf=\"" + panes + "/$HERDR_SESSION.json\"\n[ -f \"$f\" ] || exit 1\ncat \"$f\"\n"
	fake := filepath.Join(t.TempDir(), "herdr")
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := nodeHerdrExecutable
	nodeHerdrExecutable = fake
	t.Cleanup(func() { nodeHerdrExecutable = previous })
}

// herdrPanesAt is `herdr pane list` output with one working pane in cwd.
func herdrPanesAt(t *testing.T, cwd string) string {
	t.Helper()
	out, err := json.Marshal(map[string]any{"result": map[string]any{"panes": []map[string]any{
		{"pane_id": "w9:p1", "cwd": cwd, "agent_status": "working"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

const herdrNoPanes = `{"result":{"panes":[]}}`

// guardFixture is a node whose spawns use herdr session spawn-a, with one job
// bound to a pane in session job-b under its inbox root (hk 5883).
func guardFixture(t *testing.T) (*nodeWorktreeFixture, *HubClient) {
	t.Helper()
	fixture := newNodeWorktreeFixture(t)
	contents, err := json.Marshal(map[string]any{"work_root": fixture.workRoot, "workspace": "w", "enabled": true, "herdr_session": "spawn-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.config, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-b", "w9:p1", "job-b")
	return fixture, &HubClient{spawnConfigPath: fixture.config, jobsInboxRoot: inbox}
}

// A held branch is kept while a pane in the job's session works in it. With
// both sessions readable and empty the branch is released: that positive
// control proves a refusal comes from the pane, not from an earlier guard.
func TestNodePaneGuardHeldBranchAcrossSessions6173(t *testing.T) {
	cases := []struct {
		name string
		// jobPanes is the job session's answer for a holder at held; nil means
		// the job session cannot be read.
		jobPanes func(t *testing.T, held string) string
		wantKept bool
	}{
		{name: "both sessions readable and empty", jobPanes: func(*testing.T, string) string { return herdrNoPanes }, wantKept: false},
		{name: "live pane in the job session", jobPanes: func(t *testing.T, held string) string {
			return herdrPanesAt(t, filepath.Join(held, "sub"))
		}, wantKept: true},
		{name: "job session unreadable", jobPanes: nil, wantKept: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture, client := guardFixture(t)
			held := fixture.holdBranch(t, "repo.parent", "feat")
			lists := map[string]string{"spawn-a": herdrNoPanes}
			if tc.jobPanes != nil {
				lists["job-b"] = tc.jobPanes(t, held)
			}
			sessionPaneFake(t, lists)

			ctx := boundedTestContext(t)
			if tc.jobPanes == nil {
				inUse, err := client.nodePaneInUse(ctx, held)
				if !inUse || err == nil || !strings.Contains(err.Error(), "job-b") {
					t.Fatalf("unreadable job session: inUse=%v err=%v, want in use with job-b named", inUse, err)
				}
			}

			released := client.nodeReleaseHeldBranch(ctx, client.nodeGitRunner(), fixture.clone, nodeWorktreeEntry{Path: held, Branch: "feat"}, fixture.feat)
			if released == tc.wantKept {
				t.Fatalf("release=%v with wantKept=%v", released, tc.wantKept)
			}
			if _, err := os.Stat(held); (err == nil) != tc.wantKept {
				t.Fatalf("holder %s present=%v, want kept=%v (err=%v)", held, err == nil, tc.wantKept, err)
			}
		})
	}
}

// worktree.remove refuses a worktree a pane in any session the node reads is
// working in, and an unreadable session refuses too; with every session
// readable and empty the clean worktree is removed.
func TestNodePaneGuardWorktreeRemoveAcrossSessions6173(t *testing.T) {
	cases := []struct {
		name    string
		lists   func(t *testing.T, dir string) map[string]string
		wantOut string
	}{
		{name: "both sessions readable and empty", lists: func(*testing.T, string) map[string]string {
			return map[string]string{"spawn-a": herdrNoPanes, "job-b": herdrNoPanes}
		}, wantOut: nodeRemoveRemoved},
		{name: "live pane in the job session", lists: func(t *testing.T, dir string) map[string]string {
			return map[string]string{"spawn-a": herdrNoPanes, "job-b": herdrPanesAt(t, dir)}
		}, wantOut: nodeRemovePaneInUse},
		{name: "job session unreadable", lists: func(*testing.T, string) map[string]string {
			return map[string]string{"spawn-a": herdrNoPanes}
		}, wantOut: nodeRemovePaneScanFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture, client := guardFixture(t)
			fixture.cloneRepo(t)
			dir, head := fixture.addWorktree(t, "guard")
			sessionPaneFake(t, tc.lists(t, dir))

			result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil))
			if result["outcome"] != tc.wantOut {
				t.Fatalf("worktree.remove outcome=%v, want %q", result["outcome"], tc.wantOut)
			}
			_, err := os.Stat(dir)
			if removed := err != nil; removed != (tc.wantOut == nodeRemoveRemoved) {
				t.Fatalf("worktree %s removed=%v for outcome %v (err=%v)", dir, removed, result["outcome"], err)
			}
		})
	}
}
