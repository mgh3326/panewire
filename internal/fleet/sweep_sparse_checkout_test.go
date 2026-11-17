package fleet

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 5098: removal is unchanged for a sparse worktree. Both hk 5091 paths —
// the sweep's job-owned checkout tail and the node's checkout.sweep pass —
// remove one through the same guarded removal a full worktree gets.

func sparseGit(t *testing.T, dir, input string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false"}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
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
