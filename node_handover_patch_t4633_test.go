package panewire

import (
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4633: worktree.handover_patch reads the old worktree without changing it:
// the branch's last pushed commit through ls-remote, and git status, the
// untracked list and git diff saved into the job directory and to hk.

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
			for _, want := range []string{"## git status --porcelain", " M a.txt", "?? new.txt", "+three", "+++ b/new.txt", "+new"} {
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

// handoverPatchRun runs the command for job-a on work and returns the result.
func handoverPatchRun(t *testing.T, work string) (map[string]any, string) {
	t.Helper()
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverPatch, map[string]any{"job": "job-a", "cwd": work, "branch": "wk/task"})
	if code != "" {
		t.Fatalf("code=%q", code)
	}
	return result, filepath.Join(inbox, "jobs", "job-a", fleet.HandoverPatchFile)
}

// The patch restores the whole old worktree, untracked file contents
// included, on a fresh checkout of the pushed commit.
func TestNodeHandoverPatchAppliesOnTheSuccessorCheckout(t *testing.T) {
	captureDocHK(t, 0, "")
	work, pushed := handoverRepo(t, true)
	result, saved := handoverPatchRun(t, work)
	if result["outcome"] != nodeHandoverPatchSaved {
		t.Fatalf("result=%v", result)
	}
	body, err := os.ReadFile(saved)
	if err != nil {
		t.Fatal(err)
	}
	_, patch, ok := strings.Cut(string(body), "## patch: apply everything below with git apply at the checkout's top level\n")
	if !ok {
		t.Fatalf("no patch heading:\n%s", body)
	}
	successor := filepath.Join(t.TempDir(), "successor")
	origin := handoverGit(t, work, "remote", "get-url", "origin")
	handoverGit(t, filepath.Dir(successor), "clone", origin, successor)
	handoverGit(t, successor, "checkout", "-B", "wk/task", pushed)
	file := filepath.Join(t.TempDir(), "p.patch")
	if err := os.WriteFile(file, []byte(patch), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, successor, "apply", file)
	for name, want := range map[string]string{"a.txt": "three\n", "new.txt": "new\n"} {
		if got, _ := os.ReadFile(filepath.Join(successor, name)); string(got) != want {
			t.Fatalf("%s=%q, want %q", name, got, want)
		}
	}
}

func TestNodeHandoverPatchRefusesWhatItCannotCarry(t *testing.T) {
	t.Run("origin ahead of the worktree", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, _ := handoverRepo(t, true)
		other := filepath.Join(t.TempDir(), "other")
		handoverGit(t, filepath.Dir(other), "clone", "-b", "wk/task", handoverGit(t, work, "remote", "get-url", "origin"), other)
		if err := os.WriteFile(filepath.Join(other, "b.txt"), []byte("elsewhere\n"), 0600); err != nil {
			t.Fatal(err)
		}
		handoverGit(t, other, "add", "b.txt")
		handoverGit(t, other, "commit", "-m", "pushed elsewhere")
		handoverGit(t, other, "push", "origin", "wk/task")
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchDiverged {
			t.Fatalf("result=%v, want diverged", result)
		}
	})
	t.Run("over hk's cap", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, _ := handoverRepo(t, true)
		if err := os.WriteFile(filepath.Join(work, "big.txt"), []byte(strings.Repeat("x\n", fleet.PaneScreenDocMaxBytes)), 0600); err != nil {
			t.Fatal(err)
		}
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchTooLarge {
			t.Fatalf("result=%v, want too_large", result)
		}
	})
	t.Run("not UTF-8", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, _ := handoverRepo(t, true)
		if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("caf\xe9\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchUnportable {
			t.Fatalf("result=%v, want unportable", result)
		}
	})
	t.Run("hk did not store it", func(t *testing.T) {
		captureDocHK(t, http.StatusInternalServerError, `{"error":"internal"}`)
		work, _ := handoverRepo(t, true)
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchUploadFailed {
			t.Fatalf("result=%v, want upload_failed", result)
		}
	})
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

// applyHandoverPatch captures from cwd, applies the saved patch at the top of
// a fresh checkout of the pushed commit, and checks the files there.
func applyHandoverPatch(t *testing.T, work, cwd, pushed string, want map[string]string) map[string]any {
	t.Helper()
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox}, nodeCommandHandoverPatch, map[string]any{"job": "job-a", "cwd": cwd, "branch": "wk/task"})
	if code != "" || result["outcome"] != nodeHandoverPatchSaved {
		t.Fatalf("result=%v code=%q", result, code)
	}
	body, err := os.ReadFile(filepath.Join(inbox, "jobs", "job-a", fleet.HandoverPatchFile))
	if err != nil {
		t.Fatal(err)
	}
	_, patch, _ := strings.Cut(string(body), "## patch: apply everything below with git apply at the checkout's top level\n")
	successor := filepath.Join(t.TempDir(), "successor")
	handoverGit(t, filepath.Dir(successor), "clone", handoverGit(t, work, "remote", "get-url", "origin"), successor)
	handoverGit(t, successor, "checkout", "-B", "wk/task", pushed)
	file := filepath.Join(t.TempDir(), "p.patch")
	if err := os.WriteFile(file, []byte(patch), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, successor, "apply", file)
	for name, content := range want {
		if got, _ := os.ReadFile(filepath.Join(successor, name)); string(got) != content {
			t.Fatalf("%s=%q, want %q", name, got, content)
		}
	}
	return result
}

// Review round 2: the patch applies at the top level whatever the pane's
// directory, the user's diff config or the inherited git environment.
func TestNodeHandoverPatchIsMachineApplicable(t *testing.T) {
	t.Run("pane in a subdirectory", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, pushed := handoverRepo(t, true)
		if err := os.MkdirAll(filepath.Join(work, "live"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(work, "live", "lane.txt"), []byte("lane\n"), 0600); err != nil {
			t.Fatal(err)
		}
		result := applyHandoverPatch(t, work, filepath.Join(work, "live"), pushed, map[string]string{"live/lane.txt": "lane\n", "new.txt": "new\n", "a.txt": "three\n"})
		if result["top"] != work {
			t.Fatalf("top=%v, want %s", result["top"], work)
		}
	})
	t.Run("color, prefix and textconv config", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, pushed := handoverRepo(t, true)
		for _, kv := range [][2]string{{"color.ui", "always"}, {"color.diff", "always"}, {"diff.noprefix", "true"}, {"diff.mnemonicPrefix", "true"}, {"diff.upper.textconv", "tr a-z A-Z"}} {
			handoverGit(t, work, "config", kv[0], kv[1])
		}
		if err := os.WriteFile(filepath.Join(work, ".gitattributes"), []byte("*.txt diff=upper\n"), 0600); err != nil {
			t.Fatal(err)
		}
		applyHandoverPatch(t, work, work, pushed, map[string]string{"a.txt": "three\n", "new.txt": "new\n", ".gitattributes": "*.txt diff=upper\n"})
	})
	t.Run("inherited repository variables", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, pushed := handoverRepo(t, true)
		t.Setenv("GIT_INDEX_FILE", filepath.Join(t.TempDir(), "elsewhere-index"))
		t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "elsewhere"))
		applyHandoverPatch(t, work, work, pushed, map[string]string{"a.txt": "three\n", "new.txt": "new\n"})
	})
	t.Run("untracked files hidden from status", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, pushed := handoverRepo(t, true)
		handoverGit(t, work, "checkout", "--", "a.txt")
		handoverGit(t, work, "config", "status.showUntrackedFiles", "no")
		result := applyHandoverPatch(t, work, work, pushed, map[string]string{"new.txt": "new\n"})
		if result["dirty"] != true || result["doc"] != fleet.HandoverPatchDocKey("job-a") {
			t.Fatalf("untracked-only work must be dirty and uploaded: %v", result)
		}
	})
}

func TestNodeHandoverPatchRefusesSubmoduleAndHugeTrackedWork(t *testing.T) {
	t.Run("changed submodule", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, _ := handoverRepo(t, true)
		lib := filepath.Join(t.TempDir(), "lib")
		handoverGit(t, filepath.Dir(lib), "init", "-b", "main", lib)
		if err := os.WriteFile(filepath.Join(lib, "l.txt"), []byte("l\n"), 0600); err != nil {
			t.Fatal(err)
		}
		handoverGit(t, lib, "add", "l.txt")
		handoverGit(t, lib, "commit", "-m", "lib")
		handoverGit(t, work, "-c", "protocol.file.allow=always", "submodule", "add", lib, "deps/lib")
		handoverGit(t, work, "commit", "-m", "submodule")
		if err := os.WriteFile(filepath.Join(work, "deps", "lib", "l.txt"), []byte("edited\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchSubmodule {
			t.Fatalf("result=%v, want submodule_changed", result)
		}
	})
	t.Run("tracked diff over the cap", func(t *testing.T) {
		captureDocHK(t, 0, "")
		work, _ := handoverRepo(t, true)
		if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte(strings.Repeat("y\n", fleet.PaneScreenDocMaxBytes)), 0600); err != nil {
			t.Fatal(err)
		}
		if result, _ := handoverPatchRun(t, work); result["outcome"] != nodeHandoverPatchTooLarge {
			t.Fatalf("result=%v, want too_large", result)
		}
	})
}
