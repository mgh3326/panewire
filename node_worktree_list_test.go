package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestNodeWorktreeList(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(root, "main")
	if err := os.Mkdir(main, 0755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command("git", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return out
	}
	git("init", main)
	git("-C", main, "config", "user.email", "fixture@example.invalid")
	git("-C", main, "config", "user.name", "fixture")
	if err := os.WriteFile(filepath.Join(main, "tracked"), []byte("base"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(main, ".gitignore"), []byte("ignored\n"), 0644); err != nil {
		t.Fatal(err)
	}
	git("-C", main, "add", ".")
	git("-C", main, "commit", "-m", "fixture")
	paths := []string{main}
	for _, name := range []string{"dirty", "extra", "detached", "missing"} {
		p := filepath.Join(root, name)
		paths = append(paths, p)
		git("-C", main, "worktree", "add", "--detach", p)
	}
	for _, file := range []string{filepath.Join(root, "dirty", "tracked"), filepath.Join(root, "extra", "untracked"), filepath.Join(root, "extra", "ignored")} {
		if err := os.WriteFile(file, []byte("changed"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.RemoveAll(filepath.Join(root, "missing")); err != nil {
		t.Fatal(err)
	}
	before := git("-C", main, "worktree", "list", "--porcelain")
	mtimes := map[string]int64{}
	heads := map[string]string{}
	for _, p := range paths[:4] {
		index := strings.TrimSpace(string(git("-C", p, "rev-parse", "--path-format=absolute", "--git-path", "index")))
		info, err := os.Stat(index)
		if err != nil {
			t.Fatal(err)
		}
		mtimes[p] = info.ModTime().UnixNano()
		heads[p] = string(git("-C", p, "rev-parse", "HEAD"))
	}
	client := &HubClient{}
	cursor := ""
	var got []string
	for page := 0; page < 3; page++ {
		result, code := nodeRun(t, client, nodeCommandWorktreeList, map[string]any{"common": main, "cursor": cursor, "limit": 2})
		if code != "" {
			t.Fatal(code)
		}
		if result["total"] != float64(5) {
			t.Fatal(result)
		}
		for _, v := range result["worktrees"].([]any) {
			row := v.(map[string]any)
			p := row["path"].(string)
			got = append(got, p)
			status := row["status"].(map[string]any)
			switch filepath.Base(p) {
			case "dirty":
				if status["dirty"] != true {
					t.Fatal(row)
				}
			case "extra":
				if status["untracked"] != float64(1) || status["ignored"] != float64(1) {
					t.Fatal(row)
				}
			case "missing":
				if status["error"] == nil {
					t.Fatal(row)
				}
			case "detached":
				if row["detached"] != true || row["branch"] != "" {
					t.Fatal(row)
				}
			case "main":
				if status["dirty"] != false || row["branch"] == "" || row["head"] == "" || row["detached"] != false {
					t.Fatal(row)
				}
			}
		}
		cursor = result["next_cursor"].(string)
		if page < 2 && cursor == "" || page == 2 && cursor != "" {
			t.Fatal(result)
		}
	}
	sort.Strings(paths)
	if strings.Join(got, "\n") != strings.Join(paths, "\n") {
		t.Fatalf("got %v want %v", got, paths)
	}
	result, _ := nodeRun(t, client, nodeCommandWorktreeList, map[string]any{"common": main, "limit": 2})
	stale := result["next_cursor"].(string)
	if _, code := nodeRun(t, client, nodeCommandWorktreeList, map[string]any{"common": main, "cursor": "forged"}); code != "invalid_args" {
		t.Fatal(code)
	}
	git("-C", main, "worktree", "add", "--detach", filepath.Join(root, "new"))
	if _, code := nodeRun(t, client, nodeCommandWorktreeList, map[string]any{"common": main, "cursor": stale}); code != "invalid_args" {
		t.Fatal(code)
	}
	git("-C", main, "worktree", "remove", filepath.Join(root, "new"))
	if after := git("-C", main, "worktree", "list", "--porcelain"); !bytes.Equal(before, after) {
		t.Fatalf("worktree list changed")
	}
	for p, mtime := range mtimes {
		index := strings.TrimSpace(string(git("-C", p, "rev-parse", "--path-format=absolute", "--git-path", "index")))
		info, err := os.Stat(index)
		if err != nil || info.ModTime().UnixNano() != mtime || heads[p] != string(git("-C", p, "rev-parse", "HEAD")) {
			t.Fatalf("changed %s", p)
		}
	}
	for _, raw := range []string{`{"common":"relative"}`, `{"common":"/tmp","unknown":1}`, `{"common":"/tmp","limit":0}`, `{"common":"/tmp","limit":null}`} {
		if _, ok := decodeNodeCommandArgs(nodeCommandWorktreeList, json.RawMessage(raw)); ok {
			t.Fatal(raw)
		}
	}
}

func TestNodeWorktreeListPageSize(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var listing strings.Builder
	for i := 0; i < 40; i++ {
		p := root
		for j := 0; j < 4; j++ {
			p = filepath.Join(p, strings.Repeat(string(rune('a'+j)), 180))
		}
		p = filepath.Join(p, strings.Repeat("x", 100)+string(rune('A'+i)))
		if err := os.MkdirAll(p, 0755); err != nil {
			t.Fatal(err)
		}
		listing.WriteString("worktree " + p + "\x00HEAD abc\x00detached\x00\x00")
	}
	runner := func(_ context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "worktree list") {
			return []byte(listing.String()), nil
		}
		return nil, nil
	}
	limit := 100
	result, code := nodeWorktreeList(t.Context(), runner, nodeWorktreeListArgs{Common: root, Limit: &limit})
	if code != "" {
		t.Fatal(code)
	}
	page := result.(nodeWorktreeListResult)
	encoded, _ := json.Marshal(page)
	if len(encoded) > nodeCommandMaxResultBytes || page.NextCursor == "" || len(page.Worktrees) >= 40 {
		t.Fatalf("size=%d page=%d cursor=%q", len(encoded), len(page.Worktrees), page.NextCursor)
	}
	result, code = nodeWorktreeList(t.Context(), runner, nodeWorktreeListArgs{Common: root, Limit: &limit, Cursor: page.NextCursor})
	if code != "" || len(result.(nodeWorktreeListResult).Worktrees)+len(page.Worktrees) != 40 {
		t.Fatal(code, result)
	}
}

func TestNodeWorktreeListScope(t *testing.T) {
	_, server := pw2Hub(t, map[string]string{"inspect": "node.command:worktree.inspect@node-a", "other": "node.command:worktree.inspect@node-b"}, "", nil)
	for _, tc := range []struct {
		id   string
		want int
	}{{"inspect", 503}, {"other", 403}} {
		caller := pw2Caller{id: tc.id, token: pw2ServiceToken(tc.id)}
		status, body := caller.do(t, server, "POST", "/v1/nodes/node-a/command", `{"op":"worktree.list","args":{"common":"/tmp"}}`)
		if status != tc.want {
			t.Fatalf("%s: %d %s", tc.id, status, body)
		}
	}
}

func TestNodeWorktreeListCLICollect(t *testing.T) {
	calls := 0
	call := func(args nodeWorktreeListArgs) (nodeWorktreeListResult, error) {
		calls++
		if args.Common != "/repo" {
			t.Fatal(args)
		}
		switch args.Cursor {
		case "":
			return nodeWorktreeListResult{Worktrees: []nodeListedWorktree{{Path: "/a"}}, NextCursor: "next"}, nil
		case "next":
			return nodeWorktreeListResult{Worktrees: []nodeListedWorktree{{Path: "/b"}}}, nil
		default:
			t.Fatal(args)
			return nodeWorktreeListResult{}, nil
		}
	}
	rows, err := collectNodeWorktrees("/repo", true, call)
	if err != nil || calls != 2 || len(rows) != 2 || rows[1].Path != "/b" {
		t.Fatal(rows, calls, err)
	}
	calls = 0
	rows, err = collectNodeWorktrees("/repo", false, call)
	if err != nil || calls != 1 || len(rows) != 1 {
		t.Fatal(rows, calls, err)
	}
}

func TestNodeWorktreeListSymlink(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	called := false
	status := nodeListStatus(t.Context(), func(context.Context, ...string) ([]byte, error) { called = true; return nil, nil }, link)
	if called || status.Error == "" {
		t.Fatal(status, called)
	}
}
