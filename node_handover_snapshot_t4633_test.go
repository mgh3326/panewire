package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4633: worktree.handover_snapshot reads the old worktree's branch and
// start point, and at the close commits the whole worktree through a
// temporary index and pushes it to refs/panewire/handover/<job>.

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

// handoverSnapshotRun runs the command for job-a from cwd.
func handoverSnapshotRun(t *testing.T, cwd string, snapshot bool) (map[string]any, string) {
	t.Helper()
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverSnapshot, map[string]any{"job": "job-a", "cwd": cwd, "branch": "wk/task", "snapshot": snapshot})
	if code != "" {
		t.Fatalf("code=%q", code)
	}
	if _, ok := canonicalNodeCommandResult(nodeCommandHandoverSnapshot, mustJSON(result)); !ok {
		t.Fatalf("hub refuses the node's answer: %v", result)
	}
	return result, filepath.Join(inbox, "jobs", "job-a")
}

// worktreeFiles reads every file under dir except .git, byte for byte.
func worktreeFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Name() == ".git" {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		body, err := os.ReadFile(path)
		files[rel] = body
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// The snapshot carries untracked, binary and non-UTF-8 files from a pane in a
// subdirectory, byte for byte, without touching the user's index; the
// successor's note restores it on a fresh checkout of the start point.
func TestNodeHandoverSnapshotRestoresTheWorktreeByteForByte(t *testing.T) {
	for _, tc := range []struct {
		name string
		push bool
	}{{"pushed", true}, {"never pushed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			work, pushed := handoverRepo(t, tc.push)
			if err := os.MkdirAll(filepath.Join(work, "live", "deep"), 0700); err != nil {
				t.Fatal(err)
			}
			for name, body := range map[string][]byte{
				"live/new.go":       []byte("package live\n"),
				"live/deep/bin.dat": {0, 1, 2, 0xff, 0, 'x', '\n', 0},
				"latin1.txt":        {'c', 'a', 'f', 0xe9, '\n'},
			} {
				if err := os.WriteFile(filepath.Join(work, name), body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// A staged edit and an unpushed commit: the snapshot carries both,
			// and the user's index is left as it was.
			if err := os.WriteFile(filepath.Join(work, "staged.txt"), []byte("staged\n"), 0600); err != nil {
				t.Fatal(err)
			}
			// Staged new files are tracked in the user's index: they go in.
			handoverGit(t, work, "add", "staged.txt", "live", "latin1.txt")
			indexBefore := handoverGit(t, work, "diff", "--cached", "--name-status")
			statusBefore := handoverGit(t, work, "status", "--porcelain")
			want := worktreeFiles(t, work)
			// new.txt is untracked: it stays on the host and is only named.
			delete(want, "new.txt")

			result, _ := handoverSnapshotRun(t, filepath.Join(work, "live"), true)
			ref := handoverSnapshotRefPrefix + "job-a"
			if result["outcome"] != nodeHandoverSnapshotSaved || result["dirty"] != true || result["ref"] != ref || result["kept"] != true || result["top"] != work || fmt.Sprint(result["untracked"]) != "[new.txt]" {
				t.Fatalf("result=%v", result)
			}
			if got := handoverGit(t, work, "diff", "--cached", "--name-status"); got != indexBefore {
				t.Fatalf("the user's index changed: %q, was %q", got, indexBefore)
			}
			if got := handoverGit(t, work, "status", "--porcelain"); got != statusBefore {
				t.Fatalf("the worktree status changed: %q, was %q", got, statusBefore)
			}
			start := pushed
			if !tc.push {
				start = result["base"].(string)
			}
			// The successor: a fresh clone at the start point, then the note's
			// restore commands.
			origin := handoverGit(t, work, "remote", "get-url", "origin")
			next := filepath.Join(t.TempDir(), "next")
			handoverGit(t, filepath.Dir(next), "clone", origin, next)
			handoverGit(t, next, "checkout", "--detach", start)
			handoverGit(t, next, "fetch", "origin", ref)
			if got := handoverGit(t, next, "rev-parse", "FETCH_HEAD"); got != result["commit"] {
				t.Fatalf("origin ref=%s, want the reported commit %v", got, result["commit"])
			}
			handoverGit(t, next, "checkout", "-B", "wk/task", "FETCH_HEAD")
			handoverGit(t, next, "reset", "--mixed", "HEAD~1")
			got := worktreeFiles(t, next)
			if len(got) != len(want) {
				t.Fatalf("files=%d, want %d", len(got), len(want))
			}
			for name, body := range want {
				if !bytes.Equal(got[name], body) {
					t.Fatalf("%s=%q, want %q", name, got[name], body)
				}
			}
			if head := handoverGit(t, next, "rev-parse", "HEAD"); head != handoverGit(t, work, "rev-parse", "HEAD") {
				t.Fatalf("successor HEAD=%s, want the old HEAD", head)
			}
		})
	}
}

// Without snapshot the command only reads: nothing is committed or pushed.
func TestNodeHandoverSnapshotReadsWithoutChanging(t *testing.T) {
	work, pushed := handoverRepo(t, true)
	result, _ := handoverSnapshotRun(t, work, false)
	if result["outcome"] != nodeHandoverSnapshotSaved || result["pushed"] != pushed || result["commit"] != nil || result["dirty"] != false {
		t.Fatalf("result=%v", result)
	}
	if refs := handoverGit(t, work, "ls-remote", "origin", handoverSnapshotRefPrefix+"*"); refs != "" {
		t.Fatalf("a read pushed a snapshot: %q", refs)
	}
}

func TestNodeHandoverSnapshotRefusesAnotherBranch(t *testing.T) {
	work, _ := handoverRepo(t, true)
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverSnapshot, map[string]any{"job": "job-a", "cwd": work, "branch": "wk/other", "snapshot": true})
	if code != "" || result["outcome"] != nodeHandoverSnapshotBranch {
		t.Fatalf("result=%v code=%q", result, code)
	}
}

// A push origin refuses keeps the old worktree: the job is marked, the
// result says so, and no ref is named.
func TestNodeHandoverSnapshotKeepsTheWorktreeWhenThePushFails(t *testing.T) {
	work, _ := handoverRepo(t, true)
	origin := handoverGit(t, work, "remote", "get-url", "origin")
	hook := filepath.Join(origin, "hooks", "pre-receive")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	result, jobDir := handoverSnapshotRun(t, work, true)
	if result["outcome"] != nodeHandoverSnapshotSaved || result["dirty"] != true || result["kept"] != true || result["ref"] != nil || result["commit"] != nil {
		t.Fatalf("result=%v", result)
	}
	body, err := os.ReadFile(filepath.Join(jobDir, fleet.HandoverKeepFile))
	if err != nil || strings.TrimSpace(string(body)) != work {
		t.Fatalf("keep mark=%q err=%v", body, err)
	}
}

// A dirty submodule is not carried by the commit: the worktree is kept too.
func TestNodeHandoverSnapshotKeepsTheWorktreeForADirtySubmodule(t *testing.T) {
	work, _ := handoverRepo(t, true)
	sub, _ := handoverRepo(t, true)
	handoverGit(t, work, "-c", "protocol.file.allow=always", "submodule", "add", sub, "sub")
	handoverGit(t, work, "commit", "-m", "add sub")
	if err := os.WriteFile(filepath.Join(work, "sub", "a.txt"), []byte("sub edit\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result, jobDir := handoverSnapshotRun(t, work, true)
	if result["submodule"] != true || result["kept"] != true {
		t.Fatalf("result=%v", result)
	}
	if _, err := os.Stat(filepath.Join(jobDir, fleet.HandoverKeepFile)); err != nil {
		t.Fatal(err)
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

// Round 4 (PR #687 head 22178c8d).

// The snapshot push sends only its ref: no annotated tag follows it (a v* tag
// starts a release), the ref reads back on origin, and the worktree is kept
// whatever the push did, since the commit cannot carry LFS objects, clean
// filters or late writes.
func TestNodeHandoverSnapshotPushesOnlyItsRefAndKeepsTheWorktree(t *testing.T) {
	work, _ := handoverRepo(t, true)
	handoverGit(t, work, "tag", "-a", "v9.9.9", "-m", "unpublished")
	handoverGit(t, work, "config", "push.followTags", "true")
	handoverGit(t, work, "config", "push.recurseSubmodules", "only")
	result, jobDir := handoverSnapshotRun(t, work, true)
	if result["ref"] != handoverSnapshotRefPrefix+"job-a" || result["kept"] != true {
		t.Fatalf("result=%v", result)
	}
	if tags := handoverGit(t, work, "ls-remote", "--tags", "origin"); tags != "" {
		t.Fatalf("the snapshot push published tags: %q", tags)
	}
	if got := handoverGit(t, work, "ls-remote", "origin", handoverSnapshotRefPrefix+"job-a"); !strings.HasPrefix(got, result["commit"].(string)) {
		t.Fatalf("origin ref=%q, want %v", got, result["commit"])
	}
	if _, err := os.Stat(filepath.Join(jobDir, fleet.HandoverKeepFile)); err != nil {
		t.Fatalf("a pushed snapshot left the worktree unkept: %v", err)
	}
}

// Operator decision on r4613533212: the ref carries tracked changes and
// unpushed commits only. A non-ignored untracked file (a 0600 runtime.env
// holding a token) is never committed or pushed; the successor's note names
// it, and it stays in the kept old worktree.
func TestNodeHandoverSnapshotNeverPushesUntrackedFiles(t *testing.T) {
	work, _ := handoverRepo(t, true)
	if err := os.WriteFile(filepath.Join(work, "runtime.env"), []byte("TOKEN=secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result, jobDir := handoverSnapshotRun(t, work, true)
	commit, _ := result["commit"].(string)
	if commit == "" {
		t.Fatalf("result=%v, want a snapshot of the tracked edit", result)
	}
	tree := handoverGit(t, work, "ls-tree", "-r", "--name-only", commit)
	for _, name := range []string{"runtime.env", "new.txt"} {
		if strings.Contains("\n"+tree+"\n", "\n"+name+"\n") {
			t.Fatalf("untracked %s is in the snapshot commit: %q", name, tree)
		}
	}
	if !strings.Contains("\n"+tree+"\n", "\na.txt\n") {
		t.Fatalf("the tracked edit is missing: %q", tree)
	}
	if blob := handoverGit(t, work, "show", commit+":a.txt"); blob != "three" {
		t.Fatalf("a.txt in the snapshot=%q", blob)
	}
	var snap nodeHandoverSnapshotResult
	if err := json.Unmarshal(mustJSON(result), &snap); err != nil {
		t.Fatal(err)
	}
	note := handoverNote("job-a", "machine-a", poolExhaustion{Pool: "claude", Cause: "window:5h"}, snap)
	if !strings.Contains(note, `"runtime.env"`) || !strings.Contains(note, "never pushed") || !strings.Contains(note, "kept at machine-a:"+work) {
		t.Fatalf("note=%q", note)
	}
	if _, err := os.Stat(filepath.Join(jobDir, fleet.HandoverKeepFile)); err != nil {
		t.Fatal(err)
	}
}
