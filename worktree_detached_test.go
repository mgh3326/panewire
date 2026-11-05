package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestNodeWorktreeRemoveDetached(t *testing.T) {
	for _, tc := range []struct {
		name, dir, want                         string
		branch, dirty, locked, changed, omitted bool
	}{
		{name: "removed", dir: "mwr-review.1", want: "removed"},
		{name: "dirty", dir: "mwr-dirty", dirty: true, want: "dirty"},
		{name: "changed", dir: "mwr-changed", changed: true, want: "worktree-changed"},
		{name: "branch", dir: "mwr-branch", branch: true, want: "not_detached"},
		{name: "name", dir: "review", want: "outside-work-root"},
		{name: "nested", dir: "nested/mwr-review", want: "outside-work-root"},
		{name: "invalid-name", dir: "mwr-Upper", want: "outside-work-root"},
		{name: "locked", dir: "mwr-locked", locked: true, want: "locked"},
		{name: "default", dir: "mwr-default", omitted: true, want: "detached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodeWorktreeFixture(t)
			f.cloneRepo(t)
			dir := filepath.Join(f.workRoot, tc.dir)
			if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
				t.Fatal(err)
			}
			if tc.branch {
				nodeGit(t, f.clone, "worktree", "add", "-q", "-b", "job/reviewer", dir, "main")
			} else {
				nodeGit(t, f.clone, "worktree", "add", "-q", "--detach", dir, f.main)
			}
			if tc.dirty {
				if err := os.WriteFile(filepath.Join(dir, "wip"), []byte("wip"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.locked {
				nodeGit(t, f.clone, "worktree", "lock", dir)
			}
			head := f.main
			if tc.changed {
				head = f.feat
			}
			args := f.removeArgs(dir, head, nil)
			if !tc.omitted {
				args["detached"] = true
			}
			result, code := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args)
			if code != "" || result["outcome"] != tc.want {
				t.Fatalf("outcome=%v code=%q, want %s", result["outcome"], code, tc.want)
			}
			_, err := os.Stat(dir)
			if tc.want == "removed" {
				if !os.IsNotExist(err) {
					t.Fatalf("directory remains: %v", err)
				}
			} else if err != nil {
				t.Fatalf("refused directory missing: %v", err)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := canonicalNodeCommandResult(nodeCommandWorktreeRemove, raw); !ok {
				t.Fatalf("result rejected by hub: %s", raw)
			}
		})
	}
}

func TestNodeWorktreeRemoveDetachedStrictDecode(t *testing.T) {
	f := newNodeWorktreeFixture(t)
	args := f.removeArgs(filepath.Join(f.workRoot, "mwr-x"), f.main, nil)
	for _, bad := range []any{"true", nil, 1} {
		args["detached"] = bad
		if _, code := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args); code != "invalid_args" {
			t.Fatalf("detached=%v code=%q", bad, code)
		}
	}
}

func TestNodeWorktreeRemoveDetachedIgnoredAndPane(t *testing.T) {
	f := newNodeWorktreeFixture(t)
	f.cloneRepo(t)
	dir := filepath.Join(f.workRoot, "mwr-ignore")
	nodeGit(t, f.clone, "worktree", "add", "-q", "--detach", dir, f.main)
	if err := os.WriteFile(filepath.Join(f.clone, ".git", "info", "exclude"), []byte("cache.log\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cache.log"), []byte("cache"), 0600); err != nil {
		t.Fatal(err)
	}
	args := f.removeArgs(dir, f.main, nil)
	args["detached"] = true
	if result, _ := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args); result["outcome"] != nodeRemoveIgnoredFiles {
		t.Fatalf("ignored outcome=%v", result["outcome"])
	}
	args["regenerable"] = []string{"cache.log"}
	f.herdr.panes = `{"result":{"panes":[{"pane_id":"w1:p9","cwd":` + strconv.Quote(dir) + `,"agent_status":"idle"}]}}`
	if result, _ := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args); result["outcome"] != nodeRemovePaneInUse {
		t.Fatalf("pane outcome=%v", result["outcome"])
	}
	f.herdr.panes = `{"result":{"panes":[]}}`
	// Removing the remote proves detached removal does not need a fetch.
	nodeGit(t, f.clone, "remote", "remove", "origin")
	if result, _ := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args); result["outcome"] != nodeRemoveRemoved {
		t.Fatalf("regenerable outcome=%v", result["outcome"])
	}
}
