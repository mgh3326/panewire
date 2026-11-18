package panewire

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func newDisposableTestFixture(t *testing.T) (*nodeWorktreeFixture, *HubClient) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	return fixture, fixture.client()
}

func TestNodeWorktreeRemoveDisposableAcceptance(t *testing.T) {
	t.Run("root disposable file removed", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-disposable")
		draftPath := filepath.Join(dir, "report.draft.md")
		if err := os.WriteFile(draftPath, []byte("# Draft Report\n"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, code := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if code != "" {
			t.Fatalf("code=%q", code)
		}
		if result["outcome"] != nodeRemoveRemoved {
			t.Fatalf("outcome=%v", result["outcome"])
		}
		disp, ok := result["disposable"].([]any)
		if !ok || len(disp) != 1 || disp[0] != "report.draft.md" {
			t.Fatalf("disposable=%v", result["disposable"])
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("worktree directory still exists after removal: %v", err)
		}
	})

	t.Run("without disposable list refused as dirty", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-nodisp")
		if err := os.WriteFile(filepath.Join(dir, "report.draft.md"), []byte("draft"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty", result["outcome"])
		}
	})

	t.Run("disposable in subdirectory refused as dirty", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-subdir")
		subDir := filepath.Join(dir, "sub")
		if err := os.Mkdir(subDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(subDir, "report.draft.md"), []byte("sub draft"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty", result["outcome"])
		}
	})

	t.Run("another untracked file next to disposable refused as dirty", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-extra-untracked")
		if err := os.WriteFile(filepath.Join(dir, "report.draft.md"), []byte("draft"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "extra.txt"), []byte("extra"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty", result["outcome"])
		}
	})

	t.Run("tracked modification with disposable refused as dirty", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-tracked-mod")
		if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("v1"), 0600); err != nil {
			t.Fatal(err)
		}
		nodeGit(t, dir, "add", "tracked.txt")
		nodeGit(t, dir, "commit", "-q", "-m", "add tracked")
		head = nodeGit(t, dir, "rev-parse", "HEAD")
		if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("v2"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "report.draft.md"), []byte("draft"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty", result["outcome"])
		}
	})

	t.Run("untracked directory at root refused as dirty", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-dir-dirty")
		subDir := filepath.Join(dir, "report.draft.md")
		if err := os.Mkdir(subDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(subDir, "inner.txt"), []byte("inner"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty", result["outcome"])
		}
	})
}

func TestNodeWorktreeRemoveDisposableValidation(t *testing.T) {
	hub, server := t14Hub(t)
	_ = t14Node(t, hub, server, true)

	over16 := make([]string, 17)
	for i := range over16 {
		over16[i] = "file" + strconv.Itoa(i) + ".tmp"
	}

	invalidEntries := []struct {
		name       string
		disposable []string
		wantIndex  string
	}{
		{"slash in name", []string{"a/b"}, "entry 1"},
		{"glob asterisk", []string{"*.md"}, "entry 1"},
		{"dotdot", []string{".."}, "entry 1"},
		{"empty string", []string{""}, "entry 1"},
		{"17 entries", over16, "16"},
		{"second entry invalid", []string{"ok.md", "bad/path"}, "entry 2"},
		{"glob question mark", []string{"report?.md"}, "entry 1"},
		{"glob bracket", []string{"report[0-9].md"}, "entry 1"},
		{"single dot", []string{"."}, "entry 1"},
		{"over 255 bytes", []string{strings.Repeat("a", 256)}, "entry 1"},
	}

	for _, tc := range invalidEntries {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"op": "worktree.remove",
				"args": map[string]any{
					"cwd":         "/work/repo",
					"expect_head": "0123456789012345678901234567890123456789",
					"base":        "main",
					"regenerable": []any{},
					"disposable":  tc.disposable,
				},
			}
			status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, body)
			if status != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400; body=%s", status, contents)
			}
			var decoded map[string]any
			if err := json.Unmarshal(contents, &decoded); err != nil {
				t.Fatalf("json unmarshal failed: %v", err)
			}
			if decoded["error"] != "invalid_request" {
				t.Fatalf("error=%v, want invalid_request", decoded["error"])
			}
			reason, _ := decoded["reason"].(string)
			if !strings.Contains(reason, tc.wantIndex) {
				t.Fatalf("reason %q does not name expected index token %q", reason, tc.wantIndex)
			}

			// Also verify nodeRun rejects with invalid_args before running.
			raw, _ := json.Marshal(body["args"])
			if _, valid := decodeNodeCommandArgs("worktree.remove", raw); valid {
				t.Fatalf("decodeNodeCommandArgs accepted invalid disposable: %v", tc.disposable)
			}
		})
	}
}

func TestNodeWorktreeRemoveDisposableMutants(t *testing.T) {
	// Mutant 1: match by suffix instead of exact name.
	t.Run("mutant suffix matching rejected", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-mutant-suffix")
		// "prefix-report.draft.md" ends with "report.draft.md", but is not an exact match.
		if err := os.WriteFile(filepath.Join(dir, "prefix-report.draft.md"), []byte("suffix test"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty (mutant matched by suffix)", result["outcome"])
		}
	})

	// Mutant 2: allow subdirectories.
	t.Run("mutant subdirectory allowed rejected", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-mutant-subdir")
		sub := filepath.Join(dir, "sub")
		if err := os.Mkdir(sub, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, "report.draft.md"), []byte("subdir test"), 0600); err != nil {
			t.Fatal(err)
		}
		args := fixture.removeArgs(dir, head, nil)
		args["disposable"] = []string{"report.draft.md"}

		result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, args)
		if result["outcome"] != nodeOutcomeDirty {
			t.Fatalf("outcome=%v, want dirty (mutant allowed subdirectory)", result["outcome"])
		}
	})

	// Mutant 3: accept globs.
	t.Run("mutant globs accepted rejected", func(t *testing.T) {
		for _, glob := range []string{"*.md", "report.*", "report?.draft.md", "[a-z]"} {
			raw, _ := json.Marshal(map[string]any{
				"cwd":         "/work/repo",
				"expect_head": "0123456789012345678901234567890123456789",
				"base":        "main",
				"regenerable": []any{},
				"disposable":  []string{glob},
			})
			if _, valid := decodeNodeCommandArgs("worktree.remove", raw); valid {
				t.Fatalf("mutant accepted glob pattern %q in disposable", glob)
			}
		}
	})
}

func TestNodeWorktreeRemoveDisposableStrictAndDetached(t *testing.T) {
	t.Run("strict decoding rejects bad fields", func(t *testing.T) {
		fixture, client := newDisposableTestFixture(t)
		dir, head := fixture.addWorktree(t, "feat-strict")
		base := fixture.removeArgs(dir, head, nil)

		for _, bad := range []any{nil, "report.draft.md", 123, []any{123}} {
			args := make(map[string]any)
			for k, v := range base {
				args[k] = v
			}
			args["disposable"] = bad
			if _, code := nodeRun(t, client, nodeCommandWorktreeRemove, args); code != "invalid_args" {
				t.Fatalf("disposable=%v code=%q, want invalid_args", bad, code)
			}
		}

		args := make(map[string]any)
		for k, v := range base {
			args[k] = v
		}
		args["disposable"] = []string{"report.draft.md"}
		args["extra_field"] = "bad"
		if _, code := nodeRun(t, client, nodeCommandWorktreeRemove, args); code != "invalid_args" {
			t.Fatalf("extra_field code=%q, want invalid_args", code)
		}
	})

	t.Run("detached worktree with disposable removed", func(t *testing.T) {
		f := newNodeWorktreeFixture(t)
		f.cloneRepo(t)
		dir := filepath.Join(f.workRoot, "mwr-disp")
		nodeGit(t, f.clone, "worktree", "add", "-q", "--detach", dir, f.main)
		if err := os.WriteFile(filepath.Join(dir, "report.draft.md"), []byte("draft"), 0600); err != nil {
			t.Fatal(err)
		}
		args := f.removeArgs(dir, f.main, nil)
		args["detached"] = true
		args["disposable"] = []string{"report.draft.md"}

		result, code := nodeRun(t, f.client(), nodeCommandWorktreeRemove, args)
		if code != "" {
			t.Fatalf("code=%q", code)
		}
		if result["outcome"] != nodeRemoveRemoved {
			t.Fatalf("outcome=%v", result["outcome"])
		}
		disp, ok := result["disposable"].([]any)
		if !ok || len(disp) != 1 || disp[0] != "report.draft.md" {
			t.Fatalf("disposable=%v", result["disposable"])
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("detached worktree directory still exists after removal: %v", err)
		}
	})
}
