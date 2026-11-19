package panewire

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandoverSnapshotTreeCapturesTrackedOnly(t *testing.T) {
	work, _, _ := handoverRepo(t)
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("modified"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "new*.txt"), []byte("staged"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "new*.txt")
	if err := os.WriteFile(filepath.Join(work, "new-secret.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "removed.txt"), []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "removed.txt")
	handoverGit(t, work, "commit", "-m", "track removed")
	handoverGit(t, work, "rm", "--cached", "--", "removed.txt")
	if err := os.WriteFile(filepath.Join(work, "removed.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "another.txt"), []byte("staged new"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "another.txt")
	if err := os.WriteFile(filepath.Join(work, "staged*.txt"), []byte("literal"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "--literal-pathspecs", "add", "--", "staged*.txt")
	if err := os.WriteFile(filepath.Join(work, "staged-secret.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	replaced := filepath.Join(work, "replaced")
	if err := os.WriteFile(replaced, []byte("staged"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "replaced")
	if err := os.Remove(replaced); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replaced, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(replaced, "private"), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	before := handoverGit(t, work, "status", "--porcelain=v1")
	tree, err := handoverSnapshotTree(t.Context(), work, t.TempDir(), handoverGit(t, work, "rev-parse", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if got := handoverGit(t, work, "show", tree+":a.txt"); got != "modified" {
		t.Fatalf("tracked content = %q", got)
	}
	if got := handoverGit(t, work, "show", tree+":another.txt"); got != "staged new" {
		t.Fatalf("staged content = %q", got)
	}
	if got := handoverGit(t, work, "show", tree+":staged*.txt"); got != "literal" {
		t.Fatalf("literal staged content = %q", got)
	}
	for _, name := range []string{"removed.txt", "new-secret.txt", "staged-secret.txt", "replaced/private"} {
		cmd := exec.CommandContext(t.Context(), "git", "-C", work, "cat-file", "-e", tree+":"+name)
		if err := cmd.Run(); err == nil {
			t.Fatalf("untracked path %q captured", name)
		}
	}
	if after := handoverGit(t, work, "status", "--porcelain=v1"); before != after {
		t.Fatalf("user index changed: before %q after %q", before, after)
	}
}

// snapshotClient is a node whose job-a has a spawn receipt for work.
func snapshotClient(t *testing.T, work string) *HubClient {
	t.Helper()
	inbox := t.TempDir()
	events := filepath.Join(inbox, "jobs", "job-a", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"cwd": work, "role": "implementer"}})
	if err := os.WriteFile(filepath.Join(events, "00001-job.spawned.json"), receipt, 0600); err != nil {
		t.Fatal(err)
	}
	return &HubClient{jobsInboxRoot: inbox}
}

func snapshotArgs(t *testing.T, work string) map[string]any {
	t.Helper()
	return map[string]any{"job": "job-a", "epoch": 1, "cwd": work, "origin": handoverGit(t, work, "remote", "get-url", "origin"), "branch": "wk/task"}
}

func writeSnapshotFile(t *testing.T, work, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func originHasNoSnapshotRef(t *testing.T, origin string) {
	t.Helper()
	if refs := handoverGit(t, origin, "for-each-ref", "refs/panewire"); refs != "" {
		t.Fatalf("origin holds snapshot refs: %s", refs)
	}
}

func TestNodeHandoverSnapshotPublishesTrackedWorkAndListsUntracked(t *testing.T) {
	work, origin, base := handoverRepo(t)
	// An unpushed commit, a tracked edit, a staged new file, an untracked
	// credential, an ignored file and an intent-to-add entry.
	writeSnapshotFile(t, work, "a.txt", "committed")
	handoverGit(t, work, "commit", "-am", "local")
	local := handoverGit(t, work, "rev-parse", "HEAD")
	writeSnapshotFile(t, work, "a.txt", "edited")
	writeSnapshotFile(t, work, "staged.txt", "staged")
	handoverGit(t, work, "add", "--", "staged.txt")
	writeSnapshotFile(t, work, ".env", "TOKEN=secret")
	writeSnapshotFile(t, work, ".gitignore", "ignored.log\n")
	writeSnapshotFile(t, work, "ignored.log", "build")
	writeSnapshotFile(t, work, "intent.txt", "intent")
	handoverGit(t, work, "add", "-N", "--", "intent.txt")
	before := handoverGit(t, work, "status", "--porcelain=v1")

	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "" || result["outcome"] != handoverSnapshotOK {
		t.Fatalf("result=%v code=%q", result, code)
	}
	commit, _ := result["commit"].(string)
	ref, _ := result["ref"].(string)
	if ref != "refs/panewire/handover/job-a/"+commit || result["parent"] != local || result["top"] == "" {
		t.Fatalf("result=%v want parent %s", result, local)
	}
	if got := handoverGit(t, origin, "rev-parse", ref); got != commit {
		t.Fatalf("origin ref = %q, want %q", got, commit)
	}
	if got := handoverGit(t, origin, "rev-parse", ref+"^{tree}"); got != result["tree"] {
		t.Fatalf("origin tree = %q, result %v", got, result["tree"])
	}
	if got := handoverGit(t, origin, "rev-parse", ref+"^"); got != local || local == base {
		t.Fatalf("parent = %q, want unpushed commit %q", got, local)
	}
	for name, want := range map[string]string{"a.txt": "edited", "staged.txt": "staged"} {
		if got := handoverGit(t, origin, "show", ref+":"+name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{".env", "ignored.log", "intent.txt"} {
		if err := exec.CommandContext(t.Context(), "git", "-C", origin, "cat-file", "-e", ref+":"+name).Run(); err == nil {
			t.Fatalf("%s was pushed", name)
		}
	}
	listed := map[string]bool{}
	for _, path := range result["untracked"].([]any) {
		listed[path.(string)] = true
	}
	if !listed[".env"] || !listed["intent.txt"] || listed["ignored.log"] || listed["a.txt"] {
		t.Fatalf("untracked = %v", result["untracked"])
	}
	if after := handoverGit(t, work, "status", "--porcelain=v1"); before != after {
		t.Fatalf("worktree changed: before %q after %q", before, after)
	}
	// The ref restores the work: a fresh clone fetches it and checks it out.
	restored := filepath.Join(t.TempDir(), "restored")
	handoverGit(t, filepath.Dir(restored), "clone", "--no-checkout", origin, restored)
	handoverGit(t, restored, "fetch", "origin", ref)
	handoverGit(t, restored, "checkout", "--detach", "FETCH_HEAD")
	if got := handoverGit(t, restored, "show", "HEAD:a.txt"); got != "edited" {
		t.Fatalf("restored a.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(restored, ".env")); err == nil {
		t.Fatal("restored worktree holds the untracked credential")
	}
}

func TestNodeHandoverSnapshotListIsBounded(t *testing.T) {
	work, _, _ := handoverRepo(t)
	for i := range nodeHandoverSnapshotMaxListed + 7 {
		writeSnapshotFile(t, work, "untracked-"+strings.Repeat("x", i%5)+string(rune('a'+i%26))+string(rune('a'+i/26))+".txt", "u")
	}
	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "" || result["outcome"] != handoverSnapshotOK {
		t.Fatalf("result=%v code=%q", result, code)
	}
	listed, more := len(result["untracked"].([]any)), result["untracked_more"].(float64)
	if listed != nodeHandoverSnapshotMaxListed || int(more) != 7 {
		t.Fatalf("listed %d more %v", listed, more)
	}
}

func TestNodeHandoverSnapshotPushFailureLeavesNoRef(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "a.txt", "edited")
	handoverGit(t, work, "config", "remote.origin.pushurl", filepath.Join(t.TempDir(), "missing"))
	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "" || result["outcome"] != handoverSnapshotPushFail || result["ref"] != nil {
		t.Fatalf("result=%v code=%q", result, code)
	}
	originHasNoSnapshotRef(t, origin)
}

// A push that reports success to a place origin does not read is not proof:
// the command answers only after ls-remote on origin names the commit.
func TestNodeHandoverSnapshotRequiresOriginReadBack(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "a.txt", "edited")
	elsewhere := filepath.Join(t.TempDir(), "elsewhere.git")
	handoverGit(t, filepath.Dir(elsewhere), "init", "--bare", elsewhere)
	handoverGit(t, work, "config", "remote.origin.pushurl", elsewhere)
	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "" || result["outcome"] != handoverSnapshotUnproven || result["ref"] != nil {
		t.Fatalf("result=%v code=%q", result, code)
	}
	originHasNoSnapshotRef(t, origin)
}

func TestNodeHandoverSnapshotCapturesAssumeUnchangedEdits(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	handoverGit(t, work, "update-index", "--assume-unchanged", "a.txt")
	writeSnapshotFile(t, work, "a.txt", "hidden edit")
	if status := handoverGit(t, work, "status", "--porcelain=v1"); status != "" {
		t.Fatalf("fixture: git sees the edit: %q", status)
	}
	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "" || result["outcome"] != handoverSnapshotOK {
		t.Fatalf("result=%v code=%q", result, code)
	}
	if got := handoverGit(t, origin, "show", result["ref"].(string)+":a.txt"); got != "hidden edit" {
		t.Fatalf("a.txt = %q: the assume-unchanged edit was dropped", got)
	}
}

func TestNodeHandoverSnapshotSkipWorktree(t *testing.T) {
	t.Run("absent path keeps its index entry", func(t *testing.T) {
		work, origin, _ := handoverRepo(t)
		handoverGit(t, work, "update-index", "--skip-worktree", "a.txt")
		if err := os.Remove(filepath.Join(work, "a.txt")); err != nil {
			t.Fatal(err)
		}
		result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
		if code != "" || result["outcome"] != handoverSnapshotOK {
			t.Fatalf("result=%v code=%q", result, code)
		}
		if got := handoverGit(t, origin, "show", result["ref"].(string)+":a.txt"); got != "one" {
			t.Fatalf("a.txt = %q: a sparse-excluded file became a deletion", got)
		}
	})
	t.Run("present path is refused", func(t *testing.T) {
		work, origin, _ := handoverRepo(t)
		handoverGit(t, work, "update-index", "--skip-worktree", "a.txt")
		writeSnapshotFile(t, work, "a.txt", "edit git ignores")
		result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
		if code != "" || result["outcome"] != handoverSnapshotRefused || result["reason"] != handoverRefusedSkipWorktree {
			t.Fatalf("result=%v code=%q", result, code)
		}
		originHasNoSnapshotRef(t, origin)
	})
}

func TestNodeHandoverSnapshotRefusesWhatItCannotCarry(t *testing.T) {
	withSubmodule := func(t *testing.T) (work, origin string) {
		work, origin, _ = handoverRepo(t)
		_, subOrigin, _ := handoverRepo(t)
		handoverGit(t, work, "-c", "protocol.file.allow=always", "submodule", "add", "-b", "wk/task", subOrigin, "sub")
		handoverGit(t, work, "commit", "-am", "add submodule")
		handoverGit(t, work, "push", "origin", "wk/task")
		return work, origin
	}
	for _, tc := range []struct {
		name   string
		reason string
		setup  func(t *testing.T) (work, origin string)
	}{
		{"submodule tracked edit", handoverRefusedSubmodule, func(t *testing.T) (string, string) {
			work, origin := withSubmodule(t)
			writeSnapshotFile(t, filepath.Join(work, "sub"), "a.txt", "edited in the submodule")
			return work, origin
		}},
		{"submodule edit under assume-unchanged", handoverRefusedSubmodule, func(t *testing.T) (string, string) {
			work, origin := withSubmodule(t)
			handoverGit(t, filepath.Join(work, "sub"), "update-index", "--assume-unchanged", "a.txt")
			writeSnapshotFile(t, filepath.Join(work, "sub"), "a.txt", "hidden")
			return work, origin
		}},
		{"submodule commit", handoverRefusedSubmodule, func(t *testing.T) (string, string) {
			work, origin := withSubmodule(t)
			sub := filepath.Join(work, "sub")
			writeSnapshotFile(t, sub, "a.txt", "committed in the submodule")
			handoverGit(t, sub, "commit", "-am", "sub change")
			return work, origin
		}},
		{"staged gitlink", handoverRefusedGitlink, func(t *testing.T) (string, string) {
			work, origin, _ := handoverRepo(t)
			nested := filepath.Join(work, "nested")
			handoverGit(t, work, "init", nested)
			writeSnapshotFile(t, nested, "f.txt", "f")
			handoverGit(t, nested, "add", "f.txt")
			handoverGit(t, nested, "commit", "-m", "nested")
			handoverGit(t, work, "add", "nested")
			return work, origin
		}},
		{"unmerged index", handoverRefusedUnmerged, func(t *testing.T) (string, string) {
			work, origin, _ := handoverRepo(t)
			handoverGit(t, work, "checkout", "-b", "other")
			writeSnapshotFile(t, work, "a.txt", "other")
			handoverGit(t, work, "commit", "-am", "other")
			handoverGit(t, work, "checkout", "wk/task")
			writeSnapshotFile(t, work, "a.txt", "task")
			handoverGit(t, work, "commit", "-am", "task")
			merge := exec.CommandContext(t.Context(), "git", "-C", work, "merge", "other")
			merge.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid")
			if err := merge.Run(); err == nil || handoverGit(t, work, "ls-files", "-u") == "" {
				t.Fatalf("fixture: the merge did not leave a conflict: %v", err)
			}
			return work, origin
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin := tc.setup(t)
			result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
			if code != "" || result["outcome"] != handoverSnapshotRefused || result["reason"] != tc.reason {
				t.Fatalf("result=%v code=%q", result, code)
			}
			originHasNoSnapshotRef(t, origin)
		})
	}
}

func TestNodeHandoverSnapshotTimesOut(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "a.txt", "edited")
	// A remote that never answers: git's ssh helper sleeps (simulated
	// latency, bounded so a stray child ends by itself).
	helper := filepath.Join(t.TempDir(), "slow-ssh")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nexec sleep 4\n"), 0700); err != nil {
		t.Fatal(err)
	}
	// GIT_SSH_COMMAND wins over GIT_SSH and over a runner's own setting.
	t.Setenv("GIT_SSH_COMMAND", helper)
	hanging := "ssh://remote.invalid/repo.git"
	handoverGit(t, work, "config", "remote.origin.pushurl", hanging)
	previous := nodeHandoverSnapshotRunTimeout
	nodeHandoverSnapshotRunTimeout = 700 * time.Millisecond
	t.Cleanup(func() { nodeHandoverSnapshotRunTimeout = previous })
	started := time.Now()
	result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
	if code != "snapshot_failed" || result != nil {
		t.Fatalf("result=%v code=%q", result, code)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the command ran %v past its budget", elapsed)
	}
	originHasNoSnapshotRef(t, origin)
}

func TestNodeHandoverSnapshotRefusesStaleOrWrongRequests(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "a.txt", "edited")
	client := snapshotClient(t, work)
	client.assignedJobs = map[string]uint64{"job-a": 3}
	args := snapshotArgs(t, work)
	args["epoch"] = 2
	if result, code := nodeRun(t, client, nodeCommandHandoverSnapshot, args); code != "" || result["outcome"] != handoverEpochMismatch {
		t.Fatalf("stale epoch: result=%v code=%q", result, code)
	}
	args = snapshotArgs(t, work)
	args["epoch"] = 3
	args["branch"] = "wk/other"
	if result, code := nodeRun(t, client, nodeCommandHandoverSnapshot, args); code != "" || result["outcome"] != handoverBranchMismatch {
		t.Fatalf("branch: result=%v code=%q", result, code)
	}
	args = snapshotArgs(t, work)
	args["epoch"] = 3
	args["job"] = "job-b"
	if result, code := nodeRun(t, client, nodeCommandHandoverSnapshot, args); code != "" || result["outcome"] != nodeCaptureNotAJob {
		t.Fatalf("job: result=%v code=%q", result, code)
	}
	originHasNoSnapshotRef(t, origin)
	for name, bad := range map[string]map[string]any{
		"no epoch":   {"job": "job-a", "cwd": work, "origin": "o", "branch": "wk/task"},
		"zero epoch": {"job": "job-a", "epoch": 0, "cwd": work, "origin": "o", "branch": "wk/task"},
		"extra":      {"job": "job-a", "epoch": 1, "cwd": work, "origin": "o", "branch": "wk/task", "force": true},
	} {
		if _, code := nodeRun(t, client, nodeCommandHandoverSnapshot, bad); code != "invalid_args" {
			t.Fatalf("%s: code=%q", name, code)
		}
	}
}

func TestHandoverSnapshotPushPublishesVerifiedTrackedWork(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "a.txt", "snapshot")
	writeSnapshotFile(t, work, "private.txt", "local")
	snapshot, err := handoverSnapshotPush(t.Context(), work, t.TempDir(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := handoverGit(t, origin, "rev-parse", snapshot.Ref); got != snapshot.Commit {
		t.Fatalf("origin ref = %q, want %q", got, snapshot.Commit)
	}
	if err := exec.CommandContext(t.Context(), "git", "-C", origin, "cat-file", "-e", snapshot.Ref+":private.txt").Run(); err == nil {
		t.Fatal("untracked file was published")
	}
}

func TestHandoverSnapshotPushRefusesFailedOrigin(t *testing.T) {
	work, _, _ := handoverRepo(t)
	handoverGit(t, work, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "missing"))
	snapshot, err := handoverSnapshotPush(t.Context(), work, t.TempDir(), "job-a")
	if err == nil || snapshot != (handoverSnapshotCommit{}) {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

// The implementer keeps running: it stops tracking a credential file and
// commits that, leaving the file on disk, while the snapshot holds the older
// HEAD. The path must stay out of the tree, and a change that lands while the
// tree is being built is refused rather than snapshotted.
func TestHandoverSnapshotIgnoresPathsUntrackedSinceTheCapturedHead(t *testing.T) {
	work, _, _ := handoverRepo(t)
	writeSnapshotFile(t, work, "runtime.env", "tracked once")
	handoverGit(t, work, "add", "runtime.env")
	handoverGit(t, work, "commit", "-m", "track env")
	captured := handoverGit(t, work, "rev-parse", "HEAD")
	handoverGit(t, work, "rm", "--cached", "runtime.env")
	handoverGit(t, work, "commit", "-m", "stop tracking env")
	writeSnapshotFile(t, work, "runtime.env", "TOKEN=secret")
	tree, err := handoverSnapshotTree(t.Context(), work, t.TempDir(), captured)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.CommandContext(t.Context(), "git", "-C", work, "cat-file", "-e", tree+":runtime.env").Run(); err == nil {
		t.Fatal("a path the user's index no longer tracks was captured")
	}
}

func TestHandoverSnapshotRefusesAChangeDuringTheBuild(t *testing.T) {
	for name, change := range map[string]func(t *testing.T, work string){
		"commit": func(t *testing.T, work string) {
			writeSnapshotFile(t, work, "a.txt", "raced")
			handoverGit(t, work, "commit", "-am", "raced")
		},
		"index": func(t *testing.T, work string) {
			writeSnapshotFile(t, work, "late.txt", "late")
			handoverGit(t, work, "add", "late.txt")
		},
	} {
		t.Run(name, func(t *testing.T) {
			work, origin, _ := handoverRepo(t)
			writeSnapshotFile(t, work, "a.txt", "edited")
			handoverSnapshotBeforeRecheck = func() { change(t, work) }
			t.Cleanup(func() { handoverSnapshotBeforeRecheck = nil })
			result, code := nodeRun(t, snapshotClient(t, work), nodeCommandHandoverSnapshot, snapshotArgs(t, work))
			if code != "" || result["outcome"] != handoverSnapshotRefused || result["reason"] != handoverRefusedChanged {
				t.Fatalf("result=%v code=%q", result, code)
			}
			originHasNoSnapshotRef(t, origin)
		})
	}
}
