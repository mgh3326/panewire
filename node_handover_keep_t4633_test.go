package panewire

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4633: worktree.handover_keep reads the old worktree's branch and start
// point and, at the close, lists the work origin lacks and marks the
// worktree kept. Nothing is committed or pushed (a ref snapshot is hk 5259).

func handoverGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var env []string
	for _, value := range os.Environ() {
		// A fixture git must never act on the caller's repository or index.
		if name, _, _ := strings.Cut(value, "="); !nodeGitLocationEnv[name] {
			env = append(env, value)
		}
	}
	cmd.Env = append(env, "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// handoverRepo is a clone of a bare origin with main pushed and branch
// wk/task checked out. push says whether wk/task has a pushed commit.
func handoverRepo(t *testing.T, push bool) (work, pushed string) {
	t.Helper()
	root := t.TempDir()
	origin := filepath.Join(root, "origin.git")
	work = filepath.Join(root, "work")
	handoverGit(t, root, "init", "--bare", "-b", "main", origin)
	handoverGit(t, root, "clone", origin, work)
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "a.txt")
	handoverGit(t, work, "commit", "-m", "base")
	handoverGit(t, work, "push", "origin", "main")
	handoverGit(t, work, "remote", "set-head", "origin", "main")
	handoverGit(t, work, "checkout", "-b", "wk/task")
	if push {
		if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("two\n"), 0600); err != nil {
			t.Fatal(err)
		}
		handoverGit(t, work, "commit", "-am", "pushed work")
		handoverGit(t, work, "push", "origin", "wk/task")
		pushed = handoverGit(t, work, "rev-parse", "HEAD")
	}
	// Uncommitted and untracked work the successor's host cannot see.
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("three\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "new.txt"), []byte("new\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return work, pushed
}

// handoverKeepRun runs the command for job-a from cwd.
func handoverKeepRun(t *testing.T, cwd string, keep bool) (map[string]any, string) {
	t.Helper()
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverKeep, map[string]any{"job": "job-a", "cwd": cwd, "branch": "wk/task", "keep": keep})
	if code != "" {
		t.Fatalf("code=%q", code)
	}
	if _, ok := canonicalNodeCommandResult(nodeCommandHandoverKeep, mustJSON(result)); !ok {
		t.Fatalf("hub refuses the node's answer: %v", result)
	}
	return result, filepath.Join(inbox, "jobs", "job-a")
}

// At the close the node lists every uncommitted path and the unpushed HEAD,
// marks the worktree kept, and pushes nothing: a 0600 runtime.env never
// leaves the host. A pane in a subdirectory reads from the top level.
func TestNodeHandoverKeepMarksTheWorktreeAndPushesNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		push bool
	}{{"pushed", true}, {"never pushed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			work, pushed := handoverRepo(t, tc.push)
			if err := os.WriteFile(filepath.Join(work, "runtime.env"), []byte("TOKEN=secret\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(work, "live"), 0700); err != nil {
				t.Fatal(err)
			}
			refsBefore := handoverGit(t, work, "ls-remote", "origin")
			statusBefore := handoverGit(t, work, "status", "--porcelain")
			result, jobDir := handoverKeepRun(t, filepath.Join(work, "live"), true)
			if result["outcome"] != nodeHandoverKeepOK || result["dirty"] != true || result["kept"] != true || result["top"] != work || result["head"] != handoverGit(t, work, "rev-parse", "HEAD") {
				t.Fatalf("result=%v", result)
			}
			if !tc.push && (result["pushed"] != nil || result["base_ref"] != "refs/heads/main") {
				t.Fatalf("never-pushed branch must name its base: %v", result)
			}
			if tc.push && result["pushed"] != pushed {
				t.Fatalf("pushed=%v, want %s", result["pushed"], pushed)
			}
			if got := fmt.Sprint(result["uncommitted"]); got != "[a.txt new.txt runtime.env]" {
				t.Fatalf("uncommitted=%s", got)
			}
			if refs := handoverGit(t, work, "ls-remote", "origin"); refs != refsBefore {
				t.Fatalf("the hand-over pushed: %q, was %q", refs, refsBefore)
			}
			if status := handoverGit(t, work, "status", "--porcelain"); status != statusBefore {
				t.Fatalf("the worktree changed: %q, was %q", status, statusBefore)
			}
			body, err := os.ReadFile(filepath.Join(jobDir, fleet.HandoverKeepFile))
			if err != nil || strings.TrimSpace(string(body)) != work {
				t.Fatalf("keep mark=%q err=%v", body, err)
			}
		})
	}
}

// Without keep the command only reads: no mark, no listing.
func TestNodeHandoverKeepReadsWithoutChanging(t *testing.T) {
	work, pushed := handoverRepo(t, true)
	result, jobDir := handoverKeepRun(t, work, false)
	if result["outcome"] != nodeHandoverKeepOK || result["pushed"] != pushed || result["kept"] != false || result["uncommitted"] != nil {
		t.Fatalf("result=%v", result)
	}
	if _, err := os.Stat(filepath.Join(jobDir, fleet.HandoverKeepFile)); !os.IsNotExist(err) {
		t.Fatalf("a read marked the worktree: %v", err)
	}
}

func TestNodeHandoverKeepRefusesAnotherBranch(t *testing.T) {
	work, _ := handoverRepo(t, true)
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverKeep, map[string]any{"job": "job-a", "cwd": work, "branch": "wk/other", "keep": true})
	if code != "" || result["outcome"] != nodeHandoverKeepBranch {
		t.Fatalf("result=%v code=%q", result, code)
	}
}

// Many long uncommitted names never push the result past the node's budget:
// the list is cut by bytes and the rest counted.
func TestNodeHandoverKeepListFitsTheResultBudget(t *testing.T) {
	work, _ := handoverRepo(t, true)
	// About 600 bytes per path (a name component stops at 255): fifty of
	// them alone would pass the 28 KiB result budget.
	deep := filepath.Join(strings.Repeat(strings.Repeat("d", 200)+"/", 2), strings.Repeat("e", 150))
	if err := os.MkdirAll(filepath.Join(work, deep), 0700); err != nil {
		t.Fatal(err)
	}
	for i := range 60 {
		name := filepath.Join(deep, fmt.Sprintf("%03d-%s", i, strings.Repeat("n", 40)))
		if err := os.WriteFile(filepath.Join(work, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	result, _ := handoverKeepRun(t, work, true)
	if raw := mustJSON(result); len(raw) > nodeCommandMaxResultBytes {
		t.Fatalf("result is %d bytes, over %d", len(raw), nodeCommandMaxResultBytes)
	}
	listed, _ := result["uncommitted"].([]any)
	more, _ := result["uncommitted_more"].(float64)
	// a.txt, new.txt and the sixty deep files.
	if len(listed)+int(more) != 62 || more == 0 {
		t.Fatalf("listed=%d more=%v", len(listed), more)
	}
}

// worktree.remove refuses a worktree a hand-over keeps, whichever job's mark
// names it.
func TestNodeWorktreeRemoveRefusesAHandedOverWorktree(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	client := fixture.client()
	client.jobsInboxRoot = t.TempDir()
	dir, head := fixture.addWorktree(t, "kept")
	jobDir := filepath.Join(client.jobsInboxRoot, "jobs", "old-job")
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	// The jobs root also holds plain files (close-op flags): skipped.
	if err := os.WriteFile(filepath.Join(client.jobsInboxRoot, "jobs", ".close-ops-enabled"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, fleet.HandoverKeepFile), []byte(dir+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveHandoverKept {
		t.Fatalf("outcome=%v", result["outcome"])
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the kept worktree is gone: %v", err)
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
