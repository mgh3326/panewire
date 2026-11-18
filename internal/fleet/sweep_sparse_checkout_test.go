package fleet

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// hk 5098: removal is unchanged for a sparse worktree. Both hk 5091 paths —
// the sweep's job-owned checkout tail and the node's checkout.sweep pass —
// remove one through the same guarded removal a full worktree gets.

// isolateGitRepositoryEnv removes the variables that make git read a caller's
// repository or index instead of the -C target. Without it a host that exports
// GIT_DIR would have these fixture commits land in the caller's checkout, and
// the sweep's own git calls would read the wrong repository (hk 5098 review).
func isolateGitRepositoryEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_COMMON_DIR", "GIT_NAMESPACE"} {
		value, set := os.LookupEnv(name)
		if !set {
			continue
		}
		t.Cleanup(func() { _ = os.Setenv(name, value) })
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
}

// requireSparseGit skips a test whose fixture needs the sparse checkout
// commands on a git older than their floor.
func requireSparseGit(t *testing.T) {
	t.Helper()
	output, err := exec.Command("git", "--version").Output()
	if err != nil {
		t.Skip("git is not installed")
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		t.Skip("git --version is unreadable")
	}
	parts := strings.Split(fields[2], ".")
	if len(parts) < 2 {
		t.Skip("git --version is unreadable")
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || major < 2 || (major == 2 && minor < 37) {
		t.Skip("git is older than 2.37, the floor for the sparse checkout commands")
	}
}

func sparseGit(t *testing.T, dir, input string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false"}, args...)...)
	command.Env = append(gitFixtureEnv(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// sparseSweepCheckout builds a real clone under workRoot and the sparse
// worktree a spec spawn for job would leave there. It returns the worktree
// path and proves the exclusion took effect before the caller sweeps.
func sparseSweepCheckout(t *testing.T, workRoot, job string) string {
	t.Helper()
	root := filepath.Dir(workRoot)
	origin := filepath.Join(root, "origin.git")
	if err := os.MkdirAll(origin, 0700); err != nil {
		t.Fatal(err)
	}
	sparseGit(t, root, "", "init", "-q", "--bare", origin)
	seed := filepath.Join(root, "seed")
	if err := os.MkdirAll(seed, 0700); err != nil {
		t.Fatal(err)
	}
	sparseGit(t, seed, "", "init", "-q")
	for name, body := range map[string]string{
		"README.md":       "# project\n",
		"Assets/Logo.bin": "binary logo\n",
		"src/app.go":      "package main\n",
	} {
		path := filepath.Join(seed, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	sparseGit(t, seed, "", "add", "-A")
	sparseGit(t, seed, "", "commit", "-q", "-m", "tree")
	sha := sparseGit(t, seed, "", "rev-parse", "HEAD")
	sparseGit(t, seed, "", "remote", "add", "origin", origin)
	sparseGit(t, seed, "", "push", "-q", "origin", "main")

	clone := filepath.Join(workRoot, "repo")
	sparseGit(t, root, "", "clone", "-q", origin, clone)
	dir := filepath.Join(workRoot, checkoutWorktreeName(job))
	sparseGit(t, clone, "", "worktree", "add", "--no-checkout", "--detach", dir, sha)
	sparseGit(t, dir, "/*\n!/Assets\n", "sparse-checkout", "set", "--no-cone", "--stdin")
	sparseGit(t, dir, "", "checkout", "--detach", sha)
	if _, err := os.Stat(filepath.Join(dir, "Assets", "Logo.bin")); !os.IsNotExist(err) {
		t.Fatalf("the fixture worktree is not sparse: %v", err)
	}
	return dir
}

// realGitRunner sends every git command to the real binary; the sweep fixtures
// fake everything else.
// gitFixtureEnv is os.Environ without the repository-location variables, so a
// fixture command can only touch the -C target.
func gitFixtureEnv() []string {
	drop := map[string]bool{"GIT_DIR": true, "GIT_WORK_TREE": true, "GIT_INDEX_FILE": true, "GIT_OBJECT_DIRECTORY": true, "GIT_ALTERNATE_OBJECT_DIRECTORIES": true, "GIT_COMMON_DIR": true, "GIT_NAMESPACE": true}
	var env []string
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if !drop[name] {
			env = append(env, value)
		}
	}
	return env
}

func realGitRunner(f *paneSweepFixture) {
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "git" {
			return (ExecRunner{}).Run(context.Background(), c)
		}
		return base(c)
	}
}

func TestSweepRemovesASparseJobCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	isolateGitRepositoryEnv(t)
	requireSparseGit(t)
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	realGitRunner(f)
	id := "consult-9-sparse"
	cwd := sparseSweepCheckout(t, g.workRoot, id)
	f.job(id, "consult", "", map[string]any{"cwd": cwd})
	putEvent(t, f.a, id, "job.completed", map[string]any{})

	f.endedSweep(61, time.Hour)
	if _, err := os.Stat(cwd); !os.IsNotExist(err) {
		t.Fatalf("the sparse job checkout survived the sweep: %v", err)
	}
	if !f.infoHas("task worktree removed", "worktree="+filepath.Base(cwd)) {
		t.Fatalf("info lines = %q", f.info)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, id, AbandonedWorktreeFile)); err != nil {
		t.Fatalf("the sparse checkout's patch was not saved: %v", err)
	}
}

func TestCheckoutSweepRemovesASparseJobCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	isolateGitRepositoryEnv(t)
	requireSparseGit(t)
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	realGitRunner(f)
	sweep := &CheckoutSweep{WorkRoot: g.workRoot, OrphanOwner: func(string) string { return "" }, Info: f.sweeper.Info}
	id := "consult-9-sweep1"
	cwd := sparseSweepCheckout(t, g.workRoot, id)
	pane := f.job(id, "consult", "", map[string]any{"cwd": cwd})
	putEvent(t, f.a, id, "job.completed", map[string]any{})
	delete(f.status, pane)

	rows, err := sweep.Pass(t.Context(), f.a)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, row := range rows {
		if sweepSamePath(row.Path, cwd) {
			found = true
			if row.Outcome != "removed" || row.Job != id {
				t.Fatalf("sparse checkout row = %+v", row)
			}
		}
	}
	if !found {
		t.Fatalf("the sparse checkout was not listed: %+v", rows)
	}
	if _, err := os.Stat(cwd); !os.IsNotExist(err) {
		t.Fatalf("the sparse checkout survived checkout.sweep: %v", err)
	}
}
