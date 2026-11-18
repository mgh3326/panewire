package panewire

import (
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4633: worktree.handover_patch reads the old worktree without changing it:
// the branch's last pushed commit through ls-remote, and git status, the
// untracked list and git diff saved into the job directory and to hk.

func handoverGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid", "GIT_CONFIG_NOSYSTEM=1")
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

func TestNodeHandoverPatchSavesDirtyWorkAndReadsThePushedCommit(t *testing.T) {
	for _, tc := range []struct {
		name string
		push bool
	}{{"pushed", true}, {"never pushed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			hk := captureDocHK(t, 0, "")
			work, pushed := handoverRepo(t, tc.push)
			inbox := t.TempDir()
			if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
				t.Fatal(err)
			}
			result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverPatch, map[string]any{"job": "job-a", "cwd": work, "branch": "wk/task"})
			if code != "" || result["outcome"] != nodeHandoverPatchSaved || result["dirty"] != true {
				t.Fatalf("result=%v code=%q", result, code)
			}
			if tc.push && result["pushed"] != pushed {
				t.Fatalf("pushed=%v, want %s", result["pushed"], pushed)
			}
			if !tc.push && (result["pushed"] != nil || result["base_ref"] != "refs/heads/main" || result["base"] != handoverGit(t, work, "rev-parse", "main")) {
				t.Fatalf("never-pushed branch must name its base: %v", result)
			}
			saved := filepath.Join(inbox, "jobs", "job-a", fleet.HandoverPatchFile)
			body, err := os.ReadFile(saved)
			if err != nil || result["saved"] != saved {
				t.Fatalf("saved=%v err=%v", result["saved"], err)
			}
			for _, want := range []string{"## git status --porcelain", " M a.txt", "?? new.txt", "## untracked files", "new.txt", "+three"} {
				if !strings.Contains(string(body), want) {
					t.Fatalf("patch lacks %q:\n%s", want, body)
				}
			}
			if info, _ := os.Stat(saved); info.Mode().Perm() != 0600 {
				t.Fatalf("patch mode %v", info.Mode())
			}
			if result["doc"] != fleet.HandoverPatchDocKey("job-a") {
				t.Fatalf("doc=%v", result["doc"])
			}
			put := false
			for _, r := range hk.requests() {
				put = put || r.Route() == http.MethodPut+" /v1/documents/"+url.PathEscape(fleet.HandoverPatchDocKey("job-a"))
			}
			if !put {
				t.Fatalf("hk routes=%q, want the patch document PUT", hk.routes())
			}
			// The worktree is read, never changed.
			if status := handoverGit(t, work, "status", "--porcelain"); !strings.Contains(status, "M a.txt") || !strings.Contains(status, "?? new.txt") {
				t.Fatalf("worktree changed: %q", status)
			}
			raw, _ := os.ReadFile(saved)
			if _, ok := canonicalNodeCommandResult(nodeCommandHandoverPatch, mustJSON(result)); !ok || len(raw) == 0 {
				t.Fatalf("hub refuses the node's answer: %v", result)
			}
		})
	}
}

func TestNodeHandoverPatchRefusesAnotherBranch(t *testing.T) {
	work, _ := handoverRepo(t, true)
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverPatch, map[string]any{"job": "job-a", "cwd": work, "branch": "wk/other"})
	if code != "" || result["outcome"] != nodeHandoverPatchBranch {
		t.Fatalf("result=%v code=%q", result, code)
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs", "job-a", fleet.HandoverPatchFile)); !os.IsNotExist(err) {
		t.Fatalf("a refused patch wrote a file: %v", err)
	}
}
