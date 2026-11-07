package fleet

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

func TestSweepKeepsHarnessState(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	for _, state := range []string{"merged", "dropped"} {
		t.Run(state, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, "job-a")
			if out, err := exec.Command("git", "-C", cwd, "init", "-q").CombinedOutput(); err != nil {
				t.Fatalf("git init: %s: %v", out, err)
			}
			if err := os.WriteFile(filepath.Join(cwd, ".git", "info", "exclude"), []byte(".commandcode/\n"), 0600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(cwd, ".commandcode", "taste")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, "taste.md")
			if err := os.WriteFile(file, nil, 0600); err != nil {
				t.Fatal(err)
			}
			base := f.runner.Do
			f.runner.Do = func(c Command) Result {
				if c.Name == "git" && slices.Contains(c.Args, "--ignored") {
					return (ExecRunner{}).Run(context.Background(), c)
				}
				return base(c)
			}
			f.sweptJob("a", "42", cwd)
			f.tasks["42"] = state
			f.at(0)
			f.at(11)
			if _, err := os.Stat(file); err != nil {
				t.Fatalf("harness state lost: %v", err)
			}
			if len(g.removed) != 0 {
				t.Fatalf("removed worktrees = %v", g.removed)
			}
		})
	}
}
