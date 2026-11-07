package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeWorktreePrepareHarnessStateExclude(t *testing.T) {
	for _, mode := range []string{"branch", "detached"} {
		t.Run(mode, func(t *testing.T) {
			f := newNodeWorktreeFixture(t)
			f.cloneRepo(t)
			exclude := filepath.Join(f.clone, ".git", "info", "exclude")
			existing := "# preserved\nother-pattern\n"
			if err := os.WriteFile(exclude, []byte(existing), 0600); err != nil {
				t.Fatal(err)
			}
			args := f.prepareArgs("branch", "feat", "", "", "repo.job")
			if mode == "detached" {
				args = f.prepareArgs("detached", "", f.feat, "pull/1/head", "repo.job")
			}
			client := f.client()
			if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodePreparePrepared {
				t.Fatalf("create outcome=%v code=%q", result["outcome"], code)
			}
			dir := filepath.Join(f.workRoot, "repo.job")
			state := filepath.Join(dir, ".commandcode", "taste")
			if err := os.MkdirAll(state, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(state, "taste.md"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodePrepareReused {
				t.Fatalf("harness state reuse outcome=%v code=%q", result["outcome"], code)
			}
			data, err := os.ReadFile(exclude)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(data), existing) || strings.Count("\n"+string(data), "\n.commandcode/\n") != 1 {
				t.Fatalf("exclude contents=%q", data)
			}
			if err := os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodeOutcomeDirty {
				t.Fatalf("untracked file outcome=%v code=%q", result["outcome"], code)
			}
		})
	}
}
