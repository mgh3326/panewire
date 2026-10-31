package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// nodeWorktreeFixture is one real-git rig: a bare "remote" at
// <root>/fleet/repo (a plain path remote so no network is involved), a
// spawn.json naming <root>/work as work_root, and a herdr fake with no
// panes. nodeGit owns the actual repository mutations.
type nodeWorktreeFixture struct {
	root     string
	workRoot string
	origin   string
	clone    string
	config   string
	main     string // tip of origin/main
	feat     string // tip of origin/feat, also pushed as refs/pull/1/head
	herdr    *nodeFakeHerdr
}

func newNodeWorktreeFixture(t *testing.T) *nodeWorktreeFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture := &nodeWorktreeFixture{root: root, origin: filepath.Join(root, "fleet", "repo")}
	if err := os.MkdirAll(fixture.origin, 0700); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, root, "init", "-q", "--bare", fixture.origin)
	seed := filepath.Join(root, "seed")
	if err := os.Mkdir(seed, 0700); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, seed, "init", "-q")
	nodeGit(t, seed, "commit", "-q", "--allow-empty", "-m", "base")
	nodeGit(t, seed, "checkout", "-q", "-b", "feat")
	nodeGit(t, seed, "commit", "-q", "--allow-empty", "-m", "feat")
	nodeGit(t, seed, "remote", "add", "origin", fixture.origin)
	nodeGit(t, seed, "push", "-q", "origin", "main", "feat")
	nodeGit(t, seed, "push", "-q", "origin", "main:refs/heads/other")
	nodeGit(t, seed, "push", "-q", "origin", "feat:refs/pull/1/head")
	fixture.main = nodeGit(t, seed, "rev-parse", "main")
	fixture.feat = nodeGit(t, seed, "rev-parse", "feat")

	fixture.workRoot = filepath.Join(root, "work")
	if err := os.MkdirAll(fixture.workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	fixture.clone = filepath.Join(fixture.workRoot, "repo")
	fixture.config = filepath.Join(root, "spawn.json")
	contents, err := json.Marshal(map[string]any{"work_root": fixture.workRoot, "enabled": false})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.config, contents, 0600); err != nil {
		t.Fatal(err)
	}
	fixture.herdr = &nodeFakeHerdr{panes: `{"result":{"panes":[]}}`}
	return fixture
}

func (fixture *nodeWorktreeFixture) client() *HubClient {
	return &HubClient{spawnConfigPath: fixture.config, nodeHerdr: fixture.herdr.run}
}

func (fixture *nodeWorktreeFixture) prepareArgs(mode, branch, sha, fetch, dir string) map[string]any {
	return map[string]any{"origin": fixture.origin, "mode": mode, "branch": branch, "sha": sha, "fetch": fetch, "dir": dir}
}

// cloneRepo materializes <work_root>/repo as a real clone of the fixture
// remote; prepare tests that need an existing clone call it directly.
func (fixture *nodeWorktreeFixture) cloneRepo(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(fixture.clone); err == nil {
		return
	}
	nodeGit(t, fixture.root, "clone", "-q", fixture.origin, fixture.clone)
}

// addWorktree registers a fresh linked worktree of the clone on a new
// branch forked from main, and returns its path and head.
func (fixture *nodeWorktreeFixture) addWorktree(t *testing.T, name string) (string, string) {
	t.Helper()
	dir := filepath.Join(fixture.workRoot, "repo."+name)
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "-b", "job/"+name, dir, "main")
	return dir, nodeGit(t, dir, "rev-parse", "HEAD")
}

func (fixture *nodeWorktreeFixture) removeArgs(cwd, head string, regenerable []string) map[string]any {
	globs := make([]any, len(regenerable))
	for i, glob := range regenerable {
		globs[i] = glob
	}
	return map[string]any{"cwd": cwd, "expect_head": head, "base": "main", "regenerable": globs}
}

func TestNodeWorktreePrepareDetached(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "" || worktree["head"] != fixture.feat || worktree["common"] != filepath.Join(fixture.clone, ".git") {
		t.Fatalf("worktree=%v", worktree)
	}

	result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if result["outcome"] != nodePrepareReused {
		t.Fatalf("second prepare outcome=%v", result["outcome"])
	}

	if err := os.WriteFile(filepath.Join(dir, "scratch.txt"), []byte("wip"), 0600); err != nil {
		t.Fatal(err)
	}
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, args); result["outcome"] != nodeOutcomeDirty {
		t.Fatalf("dirty worktree outcome=%v", result["outcome"])
	}
	if err := os.Remove(filepath.Join(dir, "scratch.txt")); err != nil {
		t.Fatal(err)
	}

	// Detached at a different commit is not a reuse either.
	mismatch := fixture.prepareArgs("detached", "", fixture.main, "refs/heads/main", "repo.job")
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, mismatch); result["outcome"] != nodePrepareBranchElsewhere {
		t.Fatalf("detached mismatch outcome=%v", result["outcome"])
	}

	missing := fixture.prepareArgs("detached", "", strings.Repeat("0", 40), "refs/heads/main", "repo.other")
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, missing); result["outcome"] != nodePrepareShaMissing {
		t.Fatalf("unknown sha outcome=%v", result["outcome"])
	}

	badFetch := fixture.prepareArgs("detached", "", fixture.main, "refs/heads/no-such-branch", "repo.other")
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, badFetch); result["outcome"] != nodeOutcomeGitFailed {
		t.Fatalf("unfetchable ref outcome=%v", result["outcome"])
	}

	mismatchedFetch := fixture.prepareArgs("detached", "", fixture.main, "refs/heads/feat", "repo.mismatch")
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, mismatchedFetch); result["outcome"] != nodePrepareShaMissing {
		t.Fatalf("fetched-ref mismatch outcome=%v", result["outcome"])
	}
}

func TestNodeWorktreePrepareBranch(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	dir := filepath.Join(fixture.workRoot, "repo.feat")
	args := fixture.prepareArgs("branch", "feat", "", "", "repo.feat")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "feat" || worktree["head"] != fixture.feat {
		t.Fatalf("worktree=%v", worktree)
	}
	if upstream := nodeGit(t, dir, "rev-parse", "--abbrev-ref", "feat@{upstream}"); upstream != "origin/feat" {
		t.Fatalf("the new branch must track origin/feat, got %q", upstream)
	}

	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, args); result["outcome"] != nodePrepareReused {
		t.Fatalf("second prepare outcome=%v", result["outcome"])
	}

	// The branch is checked out in repo.feat; a second directory for it is
	// refused, and so is a dir parked on another branch.
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "feat", "", "", "repo.second")); result["outcome"] != nodePrepareBranchElsewhere {
		t.Fatalf("checked-out-elsewhere outcome=%v", result["outcome"])
	}
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "main", "", "", "repo.feat")); result["outcome"] != nodePrepareBranchElsewhere {
		t.Fatalf("dir on another branch outcome=%v", result["outcome"])
	}

	// With no local branch, origin/<branch> must still match a stated
	// expected tip: a moved remote branch is refused, not tracked as-is.
	if result, _ := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "other", fixture.feat, "", "repo.o1")); result["outcome"] != nodePrepareBranchElsewhere {
		t.Fatalf("moved remote tip outcome=%v", result["outcome"])
	}
	if result, _ := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "other", fixture.main, "", "repo.o1")); result["outcome"] != nodePreparePrepared {
		t.Fatalf("matching remote tip outcome=%v", result["outcome"])
	}

	// A local branch that points at another commit than the expected tip is
	// refused rather than silently tracked.
	nodeGit(t, fixture.clone, "branch", "localdev", fixture.feat)
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "localdev", fixture.main, "", "repo.dev")); result["outcome"] != nodePrepareBranchElsewhere {
		t.Fatalf("diverged local branch outcome=%v", result["outcome"])
	}
	// With no expected tip at all the local branch is taken as-is.
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "localdev", "", "", "repo.dev")); result["outcome"] != nodePreparePrepared {
		t.Fatalf("local branch prepare outcome=%v", result["outcome"])
	}
}

func TestNodeWorktreePrepareOriginMismatch(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	client := fixture.client()
	// Same repo name, different owner: the clone's own origin is its
	// identity, and a fork sharing the name is refused.
	args := map[string]any{"origin": filepath.Join(fixture.root, "other", "repo"), "mode": "detached", "branch": "", "sha": fixture.main, "fetch": "refs/heads/main", "dir": "repo.job"}
	if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodePrepareOriginMismatch {
		t.Fatalf("outcome=%v code=%q", result["outcome"], code)
	}
}

func TestNodeWorktreePrepareLock(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	args := fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job")
	if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("setup prepare outcome=%v code=%q", result["outcome"], code)
	}

	// With the per-clone lock held, a second op waits out its budget and
	// answers busy with a retry hint instead of colliding with git.
	release, err := tryNodeGitOpLock(fixture.clone + ".git-op.lock")
	if err != nil {
		t.Fatal(err)
	}
	client.nodeGitLockWait = 50 * time.Millisecond
	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodeOutcomeBusy {
		t.Fatalf("held-lock outcome=%v code=%q", result["outcome"], code)
	}
	if retry, _ := result["retry_after_s"].(float64); retry < 1 {
		t.Fatalf("busy must carry retry_after_s: %v", result)
	}
	release()

	// Two concurrent prepares on one clone serialize on the lock: one
	// prepares, the other reuses — never git-failed.
	client.nodeGitLockWait = 10 * time.Second
	parallel := fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.pair")
	type runResult struct {
		outcome any
		code    string
		err     error
	}
	run := func() runResult {
		raw, err := json.Marshal(parallel)
		if err != nil {
			return runResult{err: err}
		}
		result, code := client.runNodeCommand(context.Background(), nodeCommandWorktreePrepare, raw)
		if code != "" {
			return runResult{code: code}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return runResult{err: err}
		}
		canonical, valid := canonicalNodeCommandResult(nodeCommandWorktreePrepare, encoded)
		if !valid {
			return runResult{err: errors.New("result outside the documented shape")}
		}
		var decoded map[string]any
		if err := json.Unmarshal(canonical, &decoded); err != nil {
			return runResult{err: err}
		}
		return runResult{outcome: decoded["outcome"]}
	}
	var wg sync.WaitGroup
	results := make(chan runResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- run()
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.code != "" {
			t.Fatalf("concurrent prepare code=%q", result.code)
		}
		if result.outcome != nodePreparePrepared && result.outcome != nodePrepareReused {
			t.Fatalf("concurrent prepare outcome=%v", result.outcome)
		}
	}
}

func TestNodeWorktreePrepareArgsValidation(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	base := fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job")
	cases := map[string]map[string]any{
		"userinfo":     {"origin": "https://user:secret@example.invalid/fleet/repo.git", "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job"},
		"ssh-userinfo": {"origin": "git@example.invalid:fleet/repo.git", "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job"},
		"multi-seg":    {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo/job"},
		"dotdot":       {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": ".."},
		"work-root":    {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job", "work_root": "/tmp/evil"},
		"missing-sha":  {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": "", "fetch": "pull/1/head", "dir": "repo.job"},
		"short-sha":    {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": fixture.feat[:12], "fetch": "pull/1/head", "dir": "repo.job"},
		"bad-fetch":    {"origin": fixture.origin, "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "refs/tags/v1", "dir": "repo.job"},
		"branch-set":   {"origin": fixture.origin, "mode": "detached", "branch": "feat", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job"},
		"mode":         {"origin": fixture.origin, "mode": "weird", "branch": "", "sha": "", "fetch": "", "dir": "repo.job"},
		"branch-fetch": {"origin": fixture.origin, "mode": "branch", "branch": "feat", "sha": "", "fetch": "refs/heads/feat", "dir": "repo.job"},
		"bad-branch":   {"origin": fixture.origin, "mode": "branch", "branch": "feat..x", "sha": "", "fetch": "", "dir": "repo.job"},
		"origin-dot":   {"origin": "https://example.invalid/fleet/../repo.git", "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job"},
		"origin-path":  {"origin": "/srv/fleet/./repo", "mode": "detached", "branch": "", "sha": fixture.feat, "fetch": "pull/1/head", "dir": "repo.job"},
	}
	for name, args := range cases {
		if _, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "invalid_args" {
			t.Fatalf("%s: code=%q", name, code)
		}
	}
	// A caller-supplied work root is refused outright, and the configured
	// one is the only root in play.
	if _, code := nodeRun(t, client, nodeCommandWorktreePrepare, base); code != "" {
		t.Fatalf("baseline args must stay valid: %q", code)
	}
}

func TestNodeWorktreePrepareNoWorkRoot(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	args := fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job")
	if _, code := nodeRun(t, &HubClient{}, nodeCommandWorktreePrepare, args); code != "work_root_unset" {
		t.Fatalf("unset config code=%q", code)
	}
	contents, err := json.Marshal(map[string]any{"enabled": true, "workspace": "/w", "herdr_session": "s"})
	if err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(fixture.root, "spawn-empty.json")
	if err := os.WriteFile(bare, contents, 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: bare}
	if _, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "work_root_unset" {
		t.Fatalf("missing work_root code=%q", code)
	}
}

func TestNodeWorktreeOpsGitTooOld(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	old := func(_ context.Context, args ...string) ([]byte, error) {
		if len(args) == 1 && args[0] == "--version" {
			return []byte("git version 2.20.1\n"), nil
		}
		return nil, errors.New("old git stub")
	}
	client := fixture.client()
	client.nodeGit = old
	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job"))
	if code != "" || result["outcome"] != nodeOutcomeGitTooOld {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	dir, head := fixture.addDetachedWorktree(t)
	result, code = nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil))
	if code != "" || result["outcome"] != nodeOutcomeGitTooOld {
		t.Fatalf("remove outcome=%v code=%q", result["outcome"], code)
	}
}

func (fixture *nodeWorktreeFixture) addDetachedWorktree(t *testing.T) (string, string) {
	t.Helper()
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.det")
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "--detach", dir, fixture.main)
	return dir, nodeGit(t, dir, "rev-parse", "HEAD")
}

func TestNodeWorktreeRemove(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	client := fixture.client()

	dir, head := fixture.addWorktree(t, "gone")
	result, code := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil))
	if code != "" || result["outcome"] != nodeRemoveRemoved {
		t.Fatalf("remove outcome=%v code=%q", result["outcome"], code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "job/gone" || worktree["head"] != head {
		t.Fatalf("worktree=%v", worktree)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree dir must be gone: %v", err)
	}
	if err := exec.Command("git", "-C", fixture.clone, "show-ref", "--verify", "--quiet", "refs/heads/job/gone").Run(); err == nil {
		t.Fatal("the job branch must be deleted")
	}
}

func TestNodeWorktreeRemoveRefusals(t *testing.T) {
	newFixture := func(t *testing.T) (*nodeWorktreeFixture, *HubClient) {
		fixture := newNodeWorktreeFixture(t)
		fixture.cloneRepo(t)
		return fixture, fixture.client()
	}

	t.Run("worktree-gone", func(t *testing.T) {
		fixture, client := newFixture(t)
		ordinary := filepath.Join(fixture.workRoot, "repo.plain")
		if err := os.Mkdir(ordinary, 0700); err != nil {
			t.Fatal(err)
		}
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(ordinary, fixture.main, nil)); result["outcome"] != nodeRemoveWorktreeGone {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("worktree-changed", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, _ := fixture.addWorktree(t, "moved")
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, fixture.feat, nil)); result["outcome"] != nodeRemoveWorktreeChanged {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("outside-work-root", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir := filepath.Join(fixture.root, "repo.outside")
		nodeGit(t, fixture.clone, "worktree", "add", "-q", "-b", "job/outside", dir, "main")
		head := nodeGit(t, dir, "rev-parse", "HEAD")
		before := nodeGit(t, fixture.clone, "worktree", "list", "--porcelain")
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveOutsideWorkRoot {
			t.Fatalf("outcome=%v", result["outcome"])
		}
		if after := nodeGit(t, fixture.clone, "worktree", "list", "--porcelain"); after != before {
			t.Fatalf("worktree list changed after refusal\nbefore: %s\nafter: %s", before, after)
		}
	})

	t.Run("common-outside-work-root", func(t *testing.T) {
		fixture, client := newFixture(t)
		foreign := filepath.Join(fixture.root, "foreign")
		nodeGit(t, fixture.root, "clone", "-q", fixture.origin, foreign)
		dir := filepath.Join(fixture.workRoot, "repo.foreign-job")
		nodeGit(t, foreign, "worktree", "add", "-q", "-b", "job/foreign", dir, "main")
		head := nodeGit(t, dir, "rev-parse", "HEAD")
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveOutsideWorkRoot {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("unregistered", func(t *testing.T) {
		fixture, client := newFixture(t)
		// Moving the worktree leaves the clone's metadata pointing at the
		// old path: the moved dir still resolves as a linked worktree to
		// git, but `worktree list` names a different path for it.
		dir, head := fixture.addWorktree(t, "stray")
		moved := filepath.Join(fixture.workRoot, "repo.stray-moved")
		if err := os.Rename(dir, moved); err != nil {
			t.Fatal(err)
		}
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(moved, head, nil)); result["outcome"] != nodeRemoveUnregistered {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("locked", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addWorktree(t, "held")
		nodeGit(t, fixture.clone, "worktree", "lock", dir)
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveLocked {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("detached", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addDetachedWorktree(t)
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveDetached {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("dirty", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addWorktree(t, "messy")
		if err := os.WriteFile(filepath.Join(dir, "wip.txt"), []byte("wip"), 0600); err != nil {
			t.Fatal(err)
		}
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("ignored-files", func(t *testing.T) {
		fixture, client := newFixture(t)
		exclude := filepath.Join(fixture.clone, ".git", "info", "exclude")
		if err := os.WriteFile(exclude, []byte("keep.log\n"), 0600); err != nil {
			t.Fatal(err)
		}
		dir, head := fixture.addWorktree(t, "logs")
		if err := os.WriteFile(filepath.Join(dir, "keep.log"), []byte("notes"), 0600); err != nil {
			t.Fatal(err)
		}
		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, []string{"other.tmp"}))
		if result["outcome"] != nodeRemoveIgnoredFiles {
			t.Fatalf("outcome=%v", result["outcome"])
		}
		if files, _ := result["files"].([]any); len(files) != 1 || files[0] != "keep.log" {
			t.Fatalf("files=%v", result["files"])
		}
		// The same file is expendable once the caller names it regenerable.
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, []string{"keep.log"})); result["outcome"] != nodeRemoveRemoved {
			t.Fatalf("regenerable outcome=%v", result["outcome"])
		}
	})

	t.Run("unmerged-or-unpushed", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, _ := fixture.addWorktree(t, "wip")
		nodeGit(t, dir, "commit", "-q", "--allow-empty", "-m", "wip")
		head := nodeGit(t, dir, "rev-parse", "HEAD")
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemoveUnmerged {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("pane-in-use", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addWorktree(t, "busy")
		fixture.herdr.panes = `{"result":{"panes":[{"pane_id":"w1:p9","cwd":` + strconv.Quote(dir) + `,"agent_status":"idle"}]}}`
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemovePaneInUse {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("pane-in-use-resolved", func(t *testing.T) {
		fixture := newNodeWorktreeFixture(t)
		linked := filepath.Join(fixture.root, "work-link")
		if err := os.Symlink(fixture.workRoot, linked); err != nil {
			t.Fatal(err)
		}
		fixture.workRoot = linked
		fixture.clone = filepath.Join(linked, "repo")
		contents, err := json.Marshal(map[string]any{"work_root": linked, "enabled": false})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixture.config, contents, 0600); err != nil {
			t.Fatal(err)
		}
		fixture.cloneRepo(t)
		client := fixture.client()
		dir, head := fixture.addWorktree(t, "linked")
		resolved, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatal(err)
		}
		fixture.herdr.panes = `{"result":{"panes":[{"pane_id":"w1:p10","cwd":` + strconv.Quote(resolved) + `,"agent_status":"idle"}]}}`
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemovePaneInUse {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("pane-scan-failed", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addWorktree(t, "scan")
		fixture.herdr.panesErr = errors.New("herdr down")
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeRemovePaneScanFailed {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})

	t.Run("busy", func(t *testing.T) {
		fixture, client := newFixture(t)
		dir, head := fixture.addWorktree(t, "wait")
		release, err := tryNodeGitOpLock(fixture.clone + ".git-op.lock")
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		client.nodeGitLockWait = 50 * time.Millisecond
		if result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)); result["outcome"] != nodeOutcomeBusy {
			t.Fatalf("outcome=%v", result["outcome"])
		}
	})
}

func TestNodeWorktreeRemoveChangedCommon(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	cwd := filepath.Join(fixture.workRoot, "repo.job")
	head := strings.Repeat("a", 40)
	inspections := 0
	client.nodeGit = func(_ context.Context, args ...string) ([]byte, error) {
		switch {
		case len(args) == 1 && args[0] == "--version":
			return []byte("git version 2.31.0\n"), nil
		case len(args) == 7 && args[0] == "-C" && args[1] == cwd && args[2] == "rev-parse" && args[3] == "--path-format=absolute":
			inspections++
			clone := filepath.Join(fixture.workRoot, "repo")
			if inspections == 2 {
				clone = filepath.Join(fixture.workRoot, "other")
			}
			common := filepath.Join(clone, ".git")
			gitDir := filepath.Join(common, "worktrees", "repo.job")
			return []byte(gitDir + "\n" + common + "\n" + cwd + "\n"), nil
		case len(args) == 5 && args[0] == "-C" && args[1] == cwd && args[2] == "branch" && args[3] == "--show-current":
			return []byte("job/changed\n"), nil
		case len(args) == 4 && args[0] == "-C" && args[1] == cwd && args[2] == "rev-parse" && args[3] == "HEAD":
			return []byte(head + "\n"), nil
		case len(args) == 5 && args[0] == "-C" && args[1] == cwd && args[2] == "remote" && args[3] == "get-url" && args[4] == "origin":
			return []byte(fixture.origin + "\n"), nil
		}
		return nil, errors.New("unexpected git argv")
	}
	result, code := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(cwd, head, nil))
	if code != "" || result["outcome"] != nodeRemoveWorktreeChanged {
		t.Fatalf("outcome=%v code=%q", result["outcome"], code)
	}
}

// TestNodeWorktreeRemoveNeverForces proves the removal path cannot be talked
// into --force or -D: every git argv a successful remove issues is recorded.
func TestNodeWorktreeRemoveNeverForces(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir, head := fixture.addWorktree(t, "safe")
	var mu sync.Mutex
	var calls [][]string
	client := fixture.client()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		mu.Lock()
		calls = append(calls, append([]string(nil), args...))
		mu.Unlock()
		return exec.CommandContext(ctx, "git", args...).Output()
	}
	result, code := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil))
	if code != "" || result["outcome"] != nodeRemoveRemoved {
		t.Fatalf("outcome=%v code=%q", result["outcome"], code)
	}
	sawRemove, sawBranchDelete := false, false
	for _, call := range calls {
		for _, arg := range call {
			if arg == "--force" || arg == "-f" || arg == "-D" {
				t.Fatalf("forced argv %v must never reach git", call)
			}
		}
		joined := strings.Join(call, " ")
		if strings.Contains(joined, "worktree remove") {
			sawRemove = true
		}
		if strings.Contains(joined, "branch -d") {
			sawBranchDelete = true
		}
	}
	if !sawRemove || !sawBranchDelete {
		t.Fatalf("expected worktree remove and branch -d in %v", calls)
	}
}

func TestNodeQuotaRefresh(t *testing.T) {
	var calls [][]string
	client := &HubClient{nodePacelever: func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), args...))
		return []byte("refreshed\n"), nil
	}}
	result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "pool-a"})
	if code != "" || result["outcome"] != nodeQuotaRefreshed {
		t.Fatalf("outcome=%v code=%q", result["outcome"], code)
	}
	if len(calls) != 1 || strings.Join(calls[0], " ") != "refresh pool-a" {
		t.Fatalf("refresh must build exactly `pacelever refresh <pool>`: %v", calls)
	}

	for _, pool := range []string{"", "POOL", "pool a", "pool;rm", "-pool", "pool-", "../pool", "pool/x"} {
		if _, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": pool}); code != "invalid_args" {
			t.Fatalf("pool %q code=%q", pool, code)
		}
	}

	failing := &HubClient{nodePacelever: func(context.Context, ...string) ([]byte, error) {
		return nil, errors.New("pacelever down")
	}}
	if _, code := nodeRun(t, failing, nodeCommandQuotaRefresh, map[string]any{"pool": "pool-a"}); code != "refresh_failed" {
		t.Fatalf("failing refresh code=%q", code)
	}
}

func TestNodeWorktreePrepareResultNeedsHead(t *testing.T) {
	for _, outcome := range []string{nodePreparePrepared, nodePrepareReused} {
		empty := json.RawMessage(`{"outcome":"` + outcome + `","worktree":{"path":"/w/repo.job","branch":"","head":"","origin_url":"","common":""}}`)
		if _, valid := canonicalNodeCommandResult(nodeCommandWorktreePrepare, empty); valid {
			t.Fatalf("%s with empty head must be rejected", outcome)
		}
		full := json.RawMessage(`{"outcome":"` + outcome + `","worktree":{"path":"/w/repo.job","branch":"","head":"` + strings.Repeat("a", 40) + `","origin_url":"","common":""}}`)
		if _, valid := canonicalNodeCommandResult(nodeCommandWorktreePrepare, full); !valid {
			t.Fatalf("%s with a head must stay valid", outcome)
		}
	}
}

func TestNodeCloneRemoteRepoAcceptsSSHForms(t *testing.T) {
	for raw, want := range map[string]string{
		"git@example.invalid:fleet/repo.git":       "fleet/repo",
		"ssh://git@example.invalid/fleet/repo.git": "fleet/repo",
		"https://example.invalid/fleet/repo.git":   "fleet/repo",
	} {
		if got, ok := nodeCloneRemoteRepo(raw); !ok || got != want {
			t.Fatalf("%s: got %q %v", raw, got, ok)
		}
	}
	// Caller-supplied origins keep refusing userinfo.
	if _, ok := nodeOriginRepo("git@example.invalid:fleet/repo.git"); ok {
		t.Fatal("caller origin with userinfo must stay refused")
	}
}

func TestNodeGitOpLockPathCanonical(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(real, link); err != nil {
		t.Skip(err)
	}
	clone := filepath.Join(real, "repo")
	if err := os.MkdirAll(clone, 0o700); err != nil {
		t.Fatal(err)
	}
	// prepare names the clone through the configured (symlinked) root;
	// remove names it through git's resolved common dir parent.
	if a, b := nodeGitOpLockPath(filepath.Join(link, "repo")), nodeGitOpLockPath(nodeResolvedPath(clone)); a != b {
		t.Fatalf("lock paths differ: %s vs %s", a, b)
	}
	// Before the clone exists, the root is still resolved.
	if a, b := nodeGitOpLockPath(filepath.Join(link, "fresh")), filepath.Join(nodeResolvedPath(real), "fresh")+".git-op.lock"; a != b {
		t.Fatalf("fresh lock path %s, want %s", a, b)
	}
}
