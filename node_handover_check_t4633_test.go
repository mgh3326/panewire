package panewire

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func handoverGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var values []string
	for _, value := range os.Environ() {
		if name, _, _ := strings.Cut(value, "="); !nodeGitLocationEnv[name] {
			values = append(values, value)
		}
	}
	cmd.Env = append(values, "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func handoverRepo(t *testing.T) (work, origin, commit string) {
	t.Helper()
	root := t.TempDir()
	origin, work = filepath.Join(root, "origin.git"), filepath.Join(root, "work")
	handoverGit(t, root, "init", "--bare", "-b", "main", origin)
	handoverGit(t, root, "clone", origin, work)
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "a.txt")
	handoverGit(t, work, "commit", "-m", "base")
	handoverGit(t, work, "checkout", "-b", "wk/task")
	handoverGit(t, work, "push", "origin", "wk/task")
	return work, origin, handoverGit(t, work, "rev-parse", "HEAD")
}

func TestNodeHandoverEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, string)
		want   string
	}{
		{"clean and pushed", nil, handoverCheckOK},
		{"tracked change", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "a.txt"), []byte("changed"), 0600); err != nil {
				t.Fatal(err)
			}
		}, handoverDirty},
		{"untracked file", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "new.txt"), []byte("new"), 0600); err != nil {
				t.Fatal(err)
			}
		}, handoverDirty},
		{"unpushed commit", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "a.txt"), []byte("committed"), 0600); err != nil {
				t.Fatal(err)
			}
			handoverGit(t, w, "commit", "-am", "local")
		}, handoverUnpushed},
		{"never pushed", func(t *testing.T, w string) { handoverGit(t, w, "push", "origin", ":refs/heads/wk/task") }, handoverUnpushed},
		{"assume unchanged", func(t *testing.T, w string) {
			handoverGit(t, w, "update-index", "--assume-unchanged", "a.txt")
			if err := os.WriteFile(filepath.Join(w, "a.txt"), []byte("hidden"), 0600); err != nil {
				t.Fatal(err)
			}
		}, handoverDirty},
		{"skip worktree absent", func(t *testing.T, w string) {
			handoverGit(t, w, "update-index", "--skip-worktree", "a.txt")
			if err := os.Remove(filepath.Join(w, "a.txt")); err != nil {
				t.Fatal(err)
			}
		}, handoverDirty},
		{"both flags absent", func(t *testing.T, w string) {
			handoverGit(t, w, "update-index", "--assume-unchanged", "--skip-worktree", "a.txt")
			if err := os.Remove(filepath.Join(w, "a.txt")); err != nil {
				t.Fatal(err)
			}
		}, handoverDirty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, commit := handoverRepo(t)
			if tc.change != nil {
				tc.change(t, work)
			}
			inbox := t.TempDir()
			events := filepath.Join(inbox, "jobs", "job-a", "events")
			if err := os.MkdirAll(events, 0700); err != nil {
				t.Fatal(err)
			}
			receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"cwd": work, "role": "implementer"}})
			if err := os.WriteFile(filepath.Join(events, "00001-job.spawned.json"), receipt, 0600); err != nil {
				t.Fatal(err)
			}
			result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverCheck, map[string]any{"job": "job-a", "cwd": work, "origin": origin, "branch": "wk/task"})
			if code != "" || result["outcome"] != tc.want {
				t.Fatalf("result=%v code=%q want=%s", result, code, tc.want)
			}
			if tc.want == handoverCheckOK && result["pushed"] != commit {
				t.Fatalf("result=%v want pushed=%s", result, commit)
			}
			if _, ok := canonicalNodeCommandResult(nodeCommandHandoverCheck, mustJSON(result)); !ok {
				t.Fatalf("invalid result %v", result)
			}
		})
	}
}

func TestNodeHandoverChecksPopulatedSubmodules(t *testing.T) {
	for _, flag := range []string{"--assume-unchanged", "--skip-worktree"} {
		t.Run(flag, func(t *testing.T) {
			work, _, _ := handoverRepo(t)
			sub, subOrigin, _ := handoverRepo(t)
			handoverGit(t, work, "-c", "protocol.file.allow=always", "submodule", "add", "-b", "wk/task", subOrigin, "sub")
			handoverGit(t, work, "commit", "-am", "add submodule")
			handoverGit(t, work, "push", "origin", "wk/task")
			_ = sub
			handoverGit(t, filepath.Join(work, "sub"), "update-index", flag, "a.txt")
			if err := os.WriteFile(filepath.Join(work, "sub", "a.txt"), []byte("hidden"), 0600); err != nil {
				t.Fatal(err)
			}
			origin := handoverGit(t, work, "remote", "get-url", "origin")
			check := (&HubClient{}).handoverCheck(t.Context(), nodeHandoverCheckArgs{Job: "job-a", Cwd: work, Origin: origin, Branch: "wk/task"})
			if check.Outcome != handoverDirty {
				t.Fatalf("check=%+v", check)
			}
		})
	}
}

func TestNodeHandoverChecksTheReceiptWorktree(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	other := filepath.Join(t.TempDir(), "work")
	handoverGit(t, filepath.Dir(other), "clone", "--branch", "wk/task", origin, other)
	inbox := t.TempDir()
	events := filepath.Join(inbox, "jobs", "job-a", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"cwd": work, "role": "implementer"}})
	if err := os.WriteFile(filepath.Join(events, "00001-job.spawned.json"), receipt, 0600); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverCheck, map[string]any{"job": "job-a", "cwd": other, "origin": origin, "branch": "wk/task"})
	if code != "" || result["outcome"] != handoverGitFailed {
		t.Fatalf("result=%v code=%q", result, code)
	}
}

// The worktree is checked during preparation and checked again within the
// close. Refusal reasons survive a node restart and lost acknowledgement.
func TestCloseOpRechecksHandoverWorktree(t *testing.T) {
	for _, tc := range []struct {
		name            string
		change          func(*testing.T, string)
		outcome, reason string
		closes          int
	}{
		{"clean", nil, fleet.CloseOpClosed, "", 1},
		{"dirtied", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "new.txt"), []byte("new work"), 0600); err != nil {
				t.Fatal(err)
			}
		}, fleet.CloseOpRefused, handoverDirty, 0},
		{"new unpushed commit", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "a.txt"), []byte("new work"), 0600); err != nil {
				t.Fatal(err)
			}
			handoverGit(t, w, "commit", "-am", "new work")
		}, fleet.CloseOpRefused, handoverUnpushed, 0},
		{"pushed tip changed", func(t *testing.T, w string) {
			if err := os.WriteFile(filepath.Join(w, "a.txt"), []byte("new work"), 0600); err != nil {
				t.Fatal(err)
			}
			handoverGit(t, w, "commit", "-am", "new work")
			handoverGit(t, w, "push", "origin", "wk/task")
		}, fleet.CloseOpRefused, handoverUnpushed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, commit := handoverRepo(t)
			inbox := t.TempDir()
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
			client := closeOpNode(t, inbox, fake)
			event := filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt)
			receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
			if err := os.WriteFile(event, receipt, 0600); err != nil {
				t.Fatal(err)
			}
			check := client.handoverCheck(t.Context(), nodeHandoverCheckArgs{Job: closeOpJob, Cwd: work, Origin: origin, Branch: "wk/task"})
			if check.Outcome != handoverCheckOK {
				t.Fatalf("initial check=%+v", check)
			}
			if tc.change != nil {
				tc.change(t, work)
			}
			args := closeOpArgs("op-handover")
			args["cwd"] = work
			args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
			result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
			if code != "" || result["outcome"] != tc.outcome || fake.called("pane", "close") != tc.closes {
				t.Fatalf("close=%v code=%q calls=%v", result, code, fake.calls)
			}
			if tc.reason != "" && result["reason"] != tc.reason {
				t.Fatalf("close=%v want reason=%s", result, tc.reason)
			}
			restarted := closeOpNode(t, inbox, fake)
			status, code := nodeRun(t, restarted, nodeCommandPaneCloseStatus, closeOpRef("op-handover"))
			if code != "" || status["state"] != tc.outcome || tc.reason != "" && status["reason"] != tc.reason {
				t.Fatalf("status=%v code=%q", status, code)
			}
		})
	}
}

// A write during the final pane look is caught by the last worktree check.
// A write racing pane.close itself remains on disk and is named in the note.
func TestCloseOpHandoverChecksLastAndNamesLateWork(t *testing.T) {
	for _, tc := range []struct {
		name, writeAt, outcome string
		closes                 int
	}{
		{"write during final pane look", "agent list", fleet.CloseOpRefused, 0},
		{"write racing close", "pane close", fleet.CloseOpClosed, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, commit := handoverRepo(t)
			inbox := t.TempDir()
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
			client := closeOpNode(t, inbox, fake)
			receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
			if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt), receipt, 0600); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(work, "after-check.txt")
			reads := 0
			client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
				command := strings.Join(args[:2], " ")
				if command == "agent list" {
					reads++
				}
				if command == tc.writeAt && (command != "agent list" || reads == 2) {
					if err := os.WriteFile(file, []byte("later work"), 0600); err != nil {
						return nil, err
					}
				}
				return fake.run(ctx, args...)
			}
			args := closeOpArgs("op-late-work")
			args["cwd"] = work
			args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
			result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
			if code != "" || result["outcome"] != tc.outcome || fake.called("pane", "close") != tc.closes {
				t.Fatalf("close=%v code=%q close calls=%d", result, code, fake.called("pane", "close"))
			}
			if data, err := os.ReadFile(file); err != nil || string(data) != "later work" {
				t.Fatalf("later work=%q err=%v", data, err)
			}
			old := quotaHolderRequest("implementer", closeOpJob)
			old.Repository.Checkout.Origin = origin
			successor, err := handoverSuccessorRequest(old, closeOpJob, closeOpJob+"-q1", "claude", "machine-a", window5h(), nodeHandoverCheckResult{Outcome: handoverCheckOK, Top: work, Branch: "wk/task", Pushed: commit}, nil)
			if err != nil || !strings.Contains(successor.Brief, "may still be at machine-a:"+work+" until cleanup") || !strings.Contains(successor.Brief, "jobs/"+closeOpJob+"/abandoned-worktree.patch") || !strings.Contains(successor.Brief, "Ignored files are not preserved") || strings.Contains(successor.Brief, "worktree is kept") {
				t.Fatalf("successor note=%q err=%v", successor.Brief, err)
			}
		})
	}
}

func TestCloseOpRefusesPaneResumingDuringHandoverRemoteRead(t *testing.T) {
	work, origin, commit := handoverRepo(t)
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
	if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt), receipt, 0600); err != nil {
		t.Fatal(err)
	}
	git := client.nodeGitRunner()
	readRemote := false
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if slices.Contains(args, "ls-remote") {
			readRemote = true
			fake.mu.Lock()
			fake.agents = []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", work))}
			fake.mu.Unlock()
		}
		return git(ctx, args...)
	}
	args := closeOpArgs("op-resumed")
	args["cwd"] = work
	args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if !readRemote || code != "" || result["outcome"] != fleet.CloseOpRefused || fake.called("pane", "close") != 0 {
		t.Fatalf("remote=%v close=%v code=%q close calls=%d", readRemote, result, code, fake.called("pane", "close"))
	}
}

func TestCloseOpRefusesHandoverLocalCheckTimeout(t *testing.T) {
	work, origin, commit := handoverRepo(t)
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", work))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
	if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt), receipt, 0600); err != nil {
		t.Fatal(err)
	}
	git := client.nodeGitRunner()
	shortDeadline := false
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if slices.Contains(args, "status") {
			deadline, ok := ctx.Deadline()
			shortDeadline = ok && time.Until(deadline) <= 2*time.Second
			// Simulate a slow local Git read until its deadline expires.
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return git(ctx, args...)
	}
	args := closeOpArgs("op-local-timeout")
	args["cwd"] = work
	args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if !shortDeadline || code != "" || result["outcome"] != fleet.CloseOpRefused || result["reason"] != handoverGitFailed || fake.called("pane", "close") != 0 {
		t.Fatalf("short deadline=%v close=%v code=%q close calls=%d", shortDeadline, result, code, fake.called("pane", "close"))
	}
}

// hk 4633: a hub close of an implementer keeps pane.close's open-question
// guard, so an idle implementer waiting on an ESC answer is not handed over.
func TestCloseOpKeepsTheQuestionGuardForAnImplementer(t *testing.T) {
	for _, tc := range []struct {
		role, want string
	}{{"implementer", fleet.CloseOpRefused}, {"reviewer", fleet.CloseOpClosed}} {
		t.Run(tc.role, func(t *testing.T) {
			inbox := t.TempDir()
			dir := filepath.Join(inbox, "jobs", closeOpJob, "events")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			for name, body := range map[string]string{
				"00001-job.claim.json":   `{"kind":"job.claim","created_at":"2026-11-14T00:00:00Z","payload":{"owner_lane":"owner"}}`,
				"00002-job.spawned.json": `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"` + tc.role + `","herdr_session":"fleet"}}`,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			at := time.Now().Add(-time.Minute)
			writeLaneEventFile(t, inbox, "0001.json", at, map[string]any{"kind": "lane.event", "pane_id": "w1:p1", "event_id": "q1", "notify_type": "decision", "text": "Which design?"})
			fake := idlePaneHerdr()
			client := &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
			wantCloseOp(t, client, "op-q", tc.want, false)
			if closes := fake.called("pane", "close"); (closes == 0) != (tc.want == fleet.CloseOpRefused) {
				t.Fatalf("closes=%d for %s", closes, tc.role)
			}
		})
	}
}
