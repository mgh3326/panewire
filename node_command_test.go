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

// nodeFakeHerdr answers the herdr subcommands node commands run. The agent
// list may change after the first read so a re-read can be exercised.
type nodeFakeHerdr struct {
	mu        sync.Mutex
	agents    []string // successive `agent list` outputs; the last one repeats
	panes     string
	panesErr  error
	closeErr  error
	read      string
	calls     [][]string
	agentRead int
}

func (fake *nodeFakeHerdr) run(_ context.Context, args ...string) ([]byte, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, append([]string(nil), args...))
	switch strings.Join(args[:2], " ") {
	case "agent list":
		index := min(fake.agentRead, len(fake.agents)-1)
		fake.agentRead++
		return []byte(fake.agents[index]), nil
	case "pane list":
		return []byte(fake.panes), fake.panesErr
	case "pane close":
		return nil, fake.closeErr
	case "pane read":
		return []byte(fake.read), nil
	}
	return nil, errors.New("unexpected herdr call")
}

func (fake *nodeFakeHerdr) called(prefix ...string) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	count := 0
	for _, call := range fake.calls {
		if len(call) >= len(prefix) && strings.Join(call[:len(prefix)], " ") == strings.Join(prefix, " ") {
			count++
		}
	}
	return count
}

func nodeAgentList(rows ...string) string {
	return `{"result":{"agents":[` + strings.Join(rows, ",") + `],"extra":1}}`
}

func nodeAgentJSON(pane, name, status, cwd string) string {
	return `{"pane_id":"` + pane + `","name":"` + name + `","agent_status":"` + status + `","agent":"claude","cwd":"` + cwd + `","tab_id":"t1"}`
}

func nodeRun(t *testing.T, client *HubClient, op string, args any) (map[string]any, string) {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	result, code := client.runNodeCommand(t.Context(), op, raw)
	if code != "" {
		return nil, code
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := canonicalNodeCommandResult(op, encoded); !valid {
		t.Fatalf("%s result %s is outside the documented shape", op, encoded)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded, ""
}

func TestNodeCommandAgentsList(t *testing.T) {
	fake := &nodeFakeHerdr{
		agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))},
		panes:  `{"result":{"panes":[{"pane_id":"w1:p1","label":"x","agent_status":"idle","cwd":"/work/a"},{"pane_id":"w1:p2","agent_status":"","cwd":"/work/b"}]}}`,
	}
	client := &HubClient{nodeHerdr: fake.run}
	result, code := nodeRun(t, client, nodeCommandAgentsList, map[string]any{})
	if code != "" {
		t.Fatal(code)
	}
	agents := result["agents"].([]any)
	row := agents[0].(map[string]any)
	if len(agents) != 1 || row["pane_id"] != "w1:p1" || row["name"] != "job-a" || row["agent_status"] != "idle" || row["cwd"] != "/work/a" || row["agent"] != "claude" || row["tab_id"] != nil {
		t.Fatalf("agents=%v", agents)
	}
	if panes := result["panes"].([]any); len(panes) != 2 || result["panes_ok"] != true || panes[1].(map[string]any)["cwd"] != "/work/b" {
		t.Fatalf("panes=%v ok=%v", result["panes"], result["panes_ok"])
	}

	fake.panesErr = errors.New("pane list failed")
	result, _ = nodeRun(t, client, nodeCommandAgentsList, map[string]any{})
	if result["panes_ok"] != false || result["panes"] != nil {
		t.Fatalf("a failed pane list must read as unknown usage: %v", result)
	}
	if _, code := nodeRun(t, &HubClient{nodeHerdr: (&nodeFakeHerdr{agents: []string{`{"result":{}}`}}).run}, nodeCommandAgentsList, map[string]any{}); code != "herdr_failed" {
		t.Fatalf("missing agents list code=%q", code)
	}
	if _, code := nodeRun(t, client, nodeCommandAgentsList, map[string]any{"extra": 1}); code != "invalid_args" {
		t.Fatalf("unknown argument code=%q", code)
	}
}

func TestNodeCommandPaneCloseExactMatch(t *testing.T) {
	expected := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"}
	cases := []struct {
		name    string
		listed  string
		outcome string
	}{
		{"match", nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"), nodePaneClosed},
		{"status changed", nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"), nodePaneChanged},
		{"cwd changed", nodeAgentJSON("w1:p1", "job-a", "idle", "/work/b"), nodePaneChanged},
		{"name changed", nodeAgentJSON("w1:p1", "job-b", "idle", "/work/a"), nodePaneChanged},
		{"gone", nodeAgentJSON("w1:p9", "job-a", "idle", "/work/a"), nodePaneGone},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(test.listed)}}
			result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}, nodeCommandPaneClose, expected)
			if code != "" || result["outcome"] != test.outcome {
				t.Fatalf("result=%v code=%q", result, code)
			}
			closes := fake.called("pane", "close", "w1:p1")
			if (test.outcome == nodePaneClosed) != (closes == 1) || fake.called("pane", "close") != closes {
				t.Fatalf("outcome=%s but closes=%d calls=%v", test.outcome, closes, fake.calls)
			}
		})
	}

	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, closeErr: errors.New("close failed")}
	if _, code := nodeRun(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}, nodeCommandPaneClose, expected); code != "close_failed" {
		t.Fatalf("failed close code=%q", code)
	}
	for _, bad := range []map[string]any{
		{"pane_id": "w1:p1", "name": "", "agent_status": "idle", "cwd": "/work/a"},
		{"pane_id": "w1:p1", "name": "../x", "agent_status": "idle", "cwd": "/work/a"},
		{"pane_id": "w1 p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"},
		{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "work/a"},
		{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a"},
		{"pane_id": "w1:p1", "name": "job-a", "agent_status": "working", "cwd": "/work/a"},
		{"pane_id": "w1:p1", "name": "job-a", "agent_status": "blocked", "cwd": "/work/a"},
	} {
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList()}}
		if _, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandPaneClose, bad); code != "invalid_args" || len(fake.calls) != 0 {
			t.Fatalf("args=%v code=%q calls=%v", bad, code, fake.calls)
		}
	}
}

func TestNodeCommandPaneCapture(t *testing.T) {
	inbox := t.TempDir()
	jobDir := filepath.Join(inbox, "jobs", "job-a")
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	listed := nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "done", "/work/a"))
	args := func(name string, mergedAfter int64, write bool) map[string]any {
		return map[string]any{"pane_id": "w1:p1", "name": name, "cwd": "/work/a", "merged_after": mergedAfter, "write": write}
	}
	run := func(fake *nodeFakeHerdr, request map[string]any) (map[string]any, string) {
		return nodeRun(t, &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run}, nodeCommandPaneCapture, request)
	}

	fake := &nodeFakeHerdr{agents: []string{listed}}
	if result, code := run(fake, args("job-missing", 0, true)); code != "" || result["outcome"] != nodeCaptureNotAJob || len(fake.calls) != 0 {
		t.Fatalf("missing job dir result=%v code=%q calls=%v", result, code, fake.calls)
	}

	report := filepath.Join(jobDir, "report.md")
	if err := os.WriteFile(report, []byte("report"), 0600); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1_800_000_000, 0)
	if err := os.Chtimes(report, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	if result, _ := run(fake, args("job-a", mtime.Unix()-1, true)); result["outcome"] != nodeCaptureReport || result["saved"] != report {
		t.Fatalf("fresh report result=%v", result)
	}
	screen := filepath.Join(jobDir, nodeCaptureScreenTxt)
	// A report not newer than the merge is stale: the screen is captured.
	if result, _ := run(fake, args("job-a", mtime.Unix(), false)); result["outcome"] != nodeCaptureWould || result["saved"] != screen || len(fake.calls) != 0 {
		t.Fatalf("shadow capture result=%v calls=%v", result, fake.calls)
	}
	if _, err := os.Stat(screen); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shadow capture wrote the screen: %v", err)
	}

	gone := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-b", "done", "/work/a"))}, read: "other pane"}
	if result, _ := run(gone, args("job-a", 0, true)); result["outcome"] != nodePaneChanged || gone.called("pane", "read") != 0 {
		t.Fatalf("renamed pane result=%v calls=%v", result, gone.calls)
	}

	fake = &nodeFakeHerdr{agents: []string{listed}, read: "last screen\n"}
	result, code := run(fake, args("job-a", 0, true))
	if code != "" || result["outcome"] != nodeCaptureScreen || result["saved"] != screen || fake.called("pane", "read", "w1:p1") != 1 {
		t.Fatalf("screen capture result=%v code=%q calls=%v", result, code, fake.calls)
	}
	contents, err := os.ReadFile(screen)
	if err != nil || string(contents) != "last screen\n" {
		t.Fatalf("screen=%q err=%v", contents, err)
	}
	if info, err := os.Stat(screen); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("screen mode=%v err=%v", info.Mode(), err)
	}

	if _, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandPaneCapture, args("job-a", 0, true)); code != "jobs_root_unset" {
		t.Fatalf("unset jobs root code=%q", code)
	}
	if _, code := run(fake, args("job-a", -1, true)); code != "invalid_args" {
		t.Fatalf("negative merged_after code=%q", code)
	}
}

func nodeGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false"}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestNodeCommandWorktreeInspect(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(root, "main")
	if err := os.Mkdir(main, 0700); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, main, "init", "-q")
	nodeGit(t, main, "commit", "-q", "--allow-empty", "-m", "base")
	nodeGit(t, main, "remote", "add", "origin", "https://user:secret-token@example.invalid/owner/name.git")
	linked := filepath.Join(root, "linked")
	nodeGit(t, main, "worktree", "add", "-q", "-b", "feat/x", linked)
	head := nodeGit(t, linked, "rev-parse", "HEAD")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	client := &HubClient{}
	result, code := nodeRun(t, client, nodeCommandWorktreeInspect, map[string]any{"cwd": filepath.Join(linked)})
	if code != "" {
		t.Fatal(code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != linked || worktree["branch"] != "feat/x" || worktree["head"] != head || worktree["common"] != filepath.Join(main, ".git") {
		t.Fatalf("worktree=%v", worktree)
	}
	if origin := worktree["origin_url"]; origin != "https://example.invalid/owner/name.git" {
		t.Fatalf("origin userinfo must not leave the node: %v", origin)
	}
	// From a subdirectory a relative common dir still resolves correctly.
	sub := filepath.Join(linked, "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}
	if result, _ := nodeRun(t, client, nodeCommandWorktreeInspect, map[string]any{"cwd": sub}); result["worktree"].(map[string]any)["common"] != filepath.Join(main, ".git") || result["worktree"].(map[string]any)["path"] != linked {
		t.Fatalf("subdirectory worktree=%v", result["worktree"])
	}
	// git is asked for absolute paths; a relative answer (an older git that
	// ignored the request, or a stub) is never resolved by guessing a base.
	var asked []string
	relative := nodeWorktreeInspect(t.Context(), func(_ context.Context, args ...string) ([]byte, error) {
		if args[2] == "rev-parse" && len(args) == 7 {
			asked = args
			return []byte("/w/repo/.git/worktrees/x\n../../repo/.git\n/w/x\n"), nil
		}
		return nil, errors.New("not needed")
	}, "/w/x/a/b")
	if relative.Worktree != nil || strings.Join(asked, " ") != "-C /w/x/a/b rev-parse --path-format=absolute --absolute-git-dir --git-common-dir --show-toplevel" {
		t.Fatalf("relative common dir must not be guessed: %+v asked=%v", relative.Worktree, asked)
	}
	for raw, want := range map[string]string{
		"https://user:tok@example.invalid/owner/name.git?token=abc#frag": "https://example.invalid/owner/name.git",
		"git@example.invalid:owner/name.git":                             "example.invalid:owner/name.git",
		"token@example.invalid:owner/name.git":                           "example.invalid:owner/name.git",
		"user:secret@example.invalid:owner/name.git":                     "example.invalid:owner/name.git",
		"a@b:c@example.invalid:owner/name.git":                           "example.invalid:owner/name.git",
		"example.invalid:owner/name.git":                                 "example.invalid:owner/name.git",
		"/srv/git/name.git":                                              "/srv/git/name.git",
		"../repos/x@y/name.git":                                          "../repos/x@y/name.git",
	} {
		if got := redactNodeOriginURL(raw); got != want {
			t.Fatalf("redact %q = %q, want %q", raw, got, want)
		}
	}
	for _, cwd := range []string{main, root, filepath.Join(root, "missing")} {
		if result, code := nodeRun(t, client, nodeCommandWorktreeInspect, map[string]any{"cwd": cwd}); code != "" || result["worktree"] != nil {
			t.Fatalf("cwd=%s result=%v code=%q", cwd, result, code)
		}
	}
}

func TestNodeCommandWireParsing(t *testing.T) {
	valid := `{"type":"node.command","request_id":"cmd-1","op":"pane.close","args":{"pane_id":"w1:p1"}}`
	message, ok := parseHubOutbound([]byte(valid))
	if !ok || message.CommandOp != nodeCommandPaneClose || message.RequestID != "cmd-1" || string(message.CommandArgs) != `{"pane_id":"w1:p1"}` {
		t.Fatalf("message=%+v ok=%v", message, ok)
	}
	for _, payload := range []string{
		`{"type":"node.command","request_id":"cmd-1","op":"shell.run","args":{}}`,
		`{"type":"node.command","request_id":"cmd 1","op":"agents.list","args":{}}`,
		`{"type":"node.command","request_id":"cmd-1","op":"agents.list"}`,
		`{"type":"node.command","request_id":"cmd-1","op":"agents.list","args":{},"extra":1}`,
	} {
		if _, ok := parseHubOutbound([]byte(payload)); ok {
			t.Fatalf("accepted %s", payload)
		}
	}

	for _, payload := range []string{
		`{"type":"node.command.result","request_id":"cmd-1","op":"pane.close","result":{"outcome":"deleted"}}`,
		`{"type":"node.command.result","request_id":"cmd-1","op":"pane.close","error":"rm_failed"}`,
		`{"type":"node.command.result","request_id":"cmd-1","op":"pane.capture","result":{"outcome":"screen","saved":"relative"}}`,
		`{"type":"node.command.result","request_id":"cmd-1","op":"agents.list","result":{"agents":null,"panes":[],"panes_ok":true}}`,
		`{"type":"node.command.result","request_id":"cmd-1","op":"pane.close","result":{"outcome":"closed","extra":1}}`,
	} {
		if _, ok := parseHubNodeCommandResult([]byte(payload)); ok {
			t.Fatalf("hub accepted %s", payload)
		}
	}
	result, ok := parseHubNodeCommandResult([]byte(`{"type":"node.command.result","request_id":"cmd-1","op":"pane.close","result":{"outcome":"closed"}}`))
	if !ok || string(result.Result) != `{"outcome":"closed"}` {
		t.Fatalf("result=%+v ok=%v", result, ok)
	}
}

// TestNodeCommandPaneCaptureStaysInsideJobs pins the jobs-root confinement:
// no symlink at the job dir, report.md or pane-screen.txt can make capture
// read a foreign report or write outside <inbox>/jobs.
func TestNodeCommandPaneCaptureStaysInsideJobs(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	inbox := filepath.Join(base, "inbox")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{filepath.Join(inbox, "jobs", "job-a"), outside} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	victim := filepath.Join(outside, "pane-screen.txt")
	foreignReport := filepath.Join(outside, "report.md")
	for _, path := range []string{victim, foreignReport} {
		if err := os.WriteFile(path, []byte("outside"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	listed := func(name string) *nodeFakeHerdr {
		return &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", name, "done", "/work/a"))}, read: "screen\n"}
	}
	capture := func(root string, fake *nodeFakeHerdr, name string, mergedAfter int64, write bool) (map[string]any, string) {
		return nodeRun(t, &HubClient{jobsInboxRoot: root, nodeHerdr: fake.run}, nodeCommandPaneCapture, map[string]any{"pane_id": "w1:p1", "name": name, "cwd": "/work/a", "merged_after": mergedAfter, "write": write})
	}
	assertOutsideUntouched := func() {
		t.Helper()
		for _, path := range []string{victim, foreignReport} {
			if contents, err := os.ReadFile(path); err != nil || string(contents) != "outside" {
				t.Fatalf("%s changed: %q %v", path, contents, err)
			}
		}
		if entries, _ := os.ReadDir(outside); len(entries) != 2 {
			t.Fatalf("capture created files outside jobs: %v", entries)
		}
	}

	// A symlinked job dir is not a fleet job, whatever it points at.
	if err := os.Symlink(outside, filepath.Join(inbox, "jobs", "job-link")); err != nil {
		t.Fatal(err)
	}
	fake := listed("job-link")
	if result, code := capture(inbox, fake, "job-link", 1, true); code != "" || result["outcome"] != nodeCaptureNotAJob || len(fake.calls) != 0 {
		t.Fatalf("symlinked job dir result=%v code=%q calls=%v", result, code, fake.calls)
	}
	assertOutsideUntouched()

	// A symlinked report.md never counts as the job's fresh report.
	if err := os.Symlink(foreignReport, filepath.Join(inbox, "jobs", "job-a", "report.md")); err != nil {
		t.Fatal(err)
	}
	if result, _ := capture(inbox, listed("job-a"), "job-a", 1, false); result["outcome"] != nodeCaptureWould {
		t.Fatalf("symlinked report result=%v", result)
	}

	// A pane-screen.txt symlink is replaced, never written through.
	screen := filepath.Join(inbox, "jobs", "job-a", nodeCaptureScreenTxt)
	if err := os.Symlink(victim, screen); err != nil {
		t.Fatal(err)
	}
	fake = listed("job-a")
	if result, code := capture(inbox, fake, "job-a", 1, true); code != "" || result["outcome"] != nodeCaptureScreen || result["saved"] != screen {
		t.Fatalf("screen result=%v code=%q", result, code)
	}
	assertOutsideUntouched()
	info, err := os.Lstat(screen)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("pane-screen.txt mode=%v err=%v", info.Mode(), err)
	}
	if contents, _ := os.ReadFile(screen); string(contents) != "screen\n" {
		t.Fatalf("pane-screen.txt=%q", contents)
	}
	if entries, _ := os.ReadDir(filepath.Dir(screen)); len(entries) != 2 {
		t.Fatalf("temporary file left behind: %v", entries)
	}

	// An operator-provisioned symlink above the jobs root is still honoured.
	linkedInbox := filepath.Join(base, "inbox-link")
	if err := os.Symlink(inbox, linkedInbox); err != nil {
		t.Fatal(err)
	}
	if result, code := capture(linkedInbox, listed("job-a"), "job-a", 0, true); code != "" || result["outcome"] != nodeCaptureScreen {
		t.Fatalf("symlinked inbox result=%v code=%q", result, code)
	}
	assertOutsideUntouched()
}

func TestNodeCommandArgsRequireEveryField(t *testing.T) {
	full := map[string]any{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a", "merged_after": 0, "write": false}
	encode := func(value any) json.RawMessage {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, valid := decodeNodeCommandArgs(nodeCommandPaneCapture, encode(full)); !valid {
		t.Fatal("complete capture args rejected")
	}
	for field := range full {
		missing := map[string]any{}
		nulled := map[string]any{}
		for key, value := range full {
			nulled[key] = value
			if key != field {
				missing[key] = value
			}
		}
		nulled[field] = nil
		for label, args := range map[string]map[string]any{"missing": missing, "null": nulled} {
			if _, valid := decodeNodeCommandArgs(nodeCommandPaneCapture, encode(args)); valid {
				t.Fatalf("%s %s accepted", label, field)
			}
		}
	}
	for op, raw := range map[string]string{
		nodeCommandAgentsList:      `null`,
		nodeCommandPaneClose:       `{"pane_id":"w1:p1","name":"job-a","agent_status":"idle"}`,
		nodeCommandWorktreeInspect: `{}`,
	} {
		if _, valid := decodeNodeCommandArgs(op, json.RawMessage(raw)); valid {
			t.Fatalf("%s accepted %s", op, raw)
		}
	}
	client := &HubClient{nodeHerdr: (&nodeFakeHerdr{}).run}
	if _, code := client.runNodeCommand(t.Context(), nodeCommandPaneCapture, encode(map[string]any{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a"})); code != "invalid_args" {
		t.Fatalf("node accepted capture without merged_after/write: %q", code)
	}
}

func nodeAgentSeqJSON(pane, status string, seq int) string {
	return `{"pane_id":"` + pane + `","name":"job-a","agent_status":"` + status + `","cwd":"/work/a","state_change_seq":` + strconv.Itoa(seq) + `}`
}

// TestNodeCommandPaneCloseSettles pins the close guard herdr cannot give
// atomically: two matching re-reads a settle interval apart, the second
// immediately before the close, with an unchanged state_change_seq.
func TestNodeCommandPaneCloseSettles(t *testing.T) {
	expected := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"}
	cases := []struct {
		name    string
		reads   []string
		outcome string
	}{
		{"steady", []string{nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5)), nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5))}, nodePaneClosed},
		{"no seq reported", []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, nodePaneClosed},
		{"went back to work", []string{nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5)), nodeAgentList(nodeAgentSeqJSON("w1:p1", "working", 6))}, nodePaneChanged},
		{"bounced back to idle", []string{nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5)), nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 7))}, nodePaneChanged},
		{"gone during settle", []string{nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5)), nodeAgentList()}, nodePaneGone},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fake := &nodeFakeHerdr{agents: test.reads}
			settle := 40 * time.Millisecond
			started := time.Now()
			result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: settle}, nodeCommandPaneClose, expected)
			if code != "" || result["outcome"] != test.outcome {
				t.Fatalf("result=%v code=%q", result, code)
			}
			if elapsed := time.Since(started); elapsed < settle {
				t.Fatalf("closed after %v, before the %v settle", elapsed, settle)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			var sequence []string
			for _, call := range fake.calls {
				sequence = append(sequence, strings.Join(call[:2], " "))
			}
			want := "agent list,agent list"
			if test.outcome == nodePaneClosed {
				want += ",pane close"
			}
			if strings.Join(sequence, ",") != want {
				t.Fatalf("herdr calls %v, want %s", sequence, want)
			}
		})
	}

	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentSeqJSON("w1:p1", "idle", 5))}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	raw, _ := json.Marshal(expected)
	if _, code := (&HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Hour}).runNodeCommand(ctx, nodeCommandPaneClose, raw); code != "herdr_failed" || fake.called("pane", "close") != 0 {
		t.Fatalf("cancelled settle code=%q calls=%v", code, fake.calls)
	}
}
