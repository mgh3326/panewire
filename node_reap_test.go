package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	nodeReapScreenSecret = "screen-text-not-for-logs"
	nodeReapDivider      = "────────────────────"
)

func nodeReapRow(pane, name, agent string, focused bool) string {
	return `{"pane_id":"` + pane + `","name":"` + name + `","agent_status":"idle","agent":"` + agent + `","cwd":"/work/a","focused":` + strconv.FormatBool(focused) + `}`
}

func nodeReapScreen(composer string) string {
	return "transcript line\n" + nodeReapDivider + "\n" + composer + "\n" + nodeReapDivider + "\n  footer\n"
}

var nodeReapReadArgs = nodePaneReadArgs{PaneID: "w1:p1", Name: "job-a", Cwd: "/work/a"}

func TestNodePaneReadComposerAndFocus(t *testing.T) {
	cases := []struct {
		name     string
		agent    string
		screen   string
		readErr  error
		composer string
	}{
		{name: "claude empty", agent: "claude", screen: nodeReapScreen("❯ "), composer: nodeComposerEmpty},
		{name: "claude crlf empty", agent: "claude", screen: strings.ReplaceAll(nodeReapScreen("❯ "), "\n", "\r\n"), composer: nodeComposerEmpty},
		{name: "claude typed", agent: "claude", screen: nodeReapScreen("❯ half a thought"), composer: nodeComposerHasText},
		{name: "claude typed second line", agent: "claude", screen: nodeReapScreen("❯ \n  more"), composer: nodeComposerHasText},
		{name: "devin empty", agent: "devin", screen: nodeReapScreen("❭ "), composer: nodeComposerEmpty},
		{name: "devin typed", agent: "devin", screen: nodeReapScreen("❭ draft"), composer: nodeComposerHasText},
		{name: "codex empty", agent: "codex", screen: "output\n› \n\n  footer\n", composer: nodeComposerEmpty},
		{name: "codex placeholder remains text", agent: "codex", screen: "output\n› Ask Codex to do anything\n\n  footer\n", composer: nodeComposerHasText},
		{name: "claude glyph-only draft", agent: "claude", screen: nodeReapScreen("❯ \n >>"), composer: nodeComposerHasText},
		{name: "codex typed", agent: "codex", screen: "output\n› draft\n\n  footer\n", composer: nodeComposerHasText},
		{name: "codex wrapped", agent: "codex", screen: "output\n› \n  wrapped\n\n  footer\n", composer: nodeComposerHasText},
		{name: "unreadable", agent: "claude", screen: nodeReapScreen("❯ "), readErr: errors.New("read failed"), composer: nodeComposerUnknown},
		{name: "no composer on screen", agent: "claude", screen: "just output\n", composer: nodeComposerUnknown},
		{name: "codex no prompt", agent: "codex", screen: "just output\n", composer: nodeComposerUnknown},
		{name: "unknown harness", agent: "grok", screen: nodeReapScreen("❯ "), composer: nodeComposerUnknown},
		{name: "no harness", agent: "", screen: nodeReapScreen("❯ "), composer: nodeComposerUnknown},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			for _, focused := range []bool{false, true} {
				fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-a", test.agent, focused))}, read: test.screen, readErr: test.readErr}
				result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandPaneRead, nodeReapReadArgs)
				if code != "" {
					t.Fatal(code)
				}
				if result["outcome"] != nodePaneReadOK || result["composer"] != test.composer || result["agent_status"] != "idle" || result["focused"] != focused {
					t.Fatalf("focused=%v result=%v", focused, result)
				}
				if result["screen_tail"] != nil {
					t.Fatalf("tail_lines 0 must return no screen: %v", result)
				}
				if fake.called("pane", "read", "w1:p1", "--source", "visible") != 1 {
					t.Fatalf("calls=%v", fake.calls)
				}
			}
		})
	}
}

// A herdr that does not report focus must not read as "not focused", which is
// the answer that lets a caller close the pane.
func TestNodePaneReadRefusesUnreportedFocus(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, read: nodeReapScreen("❯ ")}
	if _, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandPaneRead, nodeReapReadArgs); code != "herdr_failed" {
		t.Fatalf("code=%q", code)
	}
}

func TestNodePaneReadTailIsBounded(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-a", "claude", false))}, read: "one\ntwo\nthree\n"}
	client := &HubClient{nodeHerdr: fake.run}
	args := nodeReapReadArgs
	args.TailLines = 2
	result, code := nodeRun(t, client, nodeCommandPaneRead, args)
	if code != "" || result["screen_tail"] != "two\nthree" {
		t.Fatalf("code=%q result=%v", code, result)
	}

	// Every byte of this screen JSON-escapes to six, and a rune straddles the
	// cut: the reply must still fit and stay valid UTF-8.
	fake.read = strings.Repeat("<", 2*nodePaneReadMaxTailBytes) + "\n" + strings.Repeat("─", 2*nodePaneReadMaxTailBytes)
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	reply, code := client.runNodeCommand(t.Context(), nodeCommandPaneRead, raw)
	if code != "" {
		t.Fatal(code)
	}
	if tail := reply.(PaneReadResult).ScreenTail; len(tail) == 0 || len(tail) > nodePaneReadMaxTailBytes || !strings.HasSuffix(tail, "─") {
		t.Fatalf("tail length %d", len(tail))
	}
	fake.read = strings.Repeat("<", 2*nodePaneReadMaxTailBytes)
	reply, _ = client.runNodeCommand(t.Context(), nodeCommandPaneRead, raw)
	encoded, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > nodeCommandMaxResultBytes {
		t.Fatalf("worst-case reply is %d bytes", len(encoded))
	}

	args.TailLines = nodePaneReadMaxTailLines + 1
	if _, code := nodeRun(t, client, nodeCommandPaneRead, args); code != "invalid_args" {
		t.Fatalf("over-long tail code=%q", code)
	}
	args.TailLines = -1
	if _, code := nodeRun(t, client, nodeCommandPaneRead, args); code != "invalid_args" {
		t.Fatalf("negative tail code=%q", code)
	}
}

// nodeReapRuntug installs a fake runtug that appends its argv to a file and
// runs script, and returns a function counting its runs.
func nodeReapRuntug(t *testing.T, script string) (runs func() int, argvPath string) {
	t.Helper()
	argvPath = filepath.Join(t.TempDir(), "runtug-argv")
	t14FakeRuntug(t, `printf '%s\n' "$*" >> '`+argvPath+`'`+"\n"+script+"\n")
	return func() int {
		contents, err := os.ReadFile(argvPath)
		if errors.Is(err, os.ErrNotExist) {
			return 0
		} else if err != nil {
			t.Fatal(err)
		}
		return strings.Count(string(contents), "\n")
	}, argvPath
}

const nodeReapRuntugOK = `echo "warning for stderr" >&2; echo "progress"; echo "OK job=$2 report=$4 kind=job.completed"`

func nodeReapJobDir(t *testing.T, job, report string) (*HubClient, string) {
	t.Helper()
	inbox := t.TempDir()
	dir := filepath.Join(inbox, "jobs", job)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, report)
	if err := os.WriteFile(path, []byte("final report\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return &HubClient{jobsInboxRoot: inbox}, path
}

func TestNodeJobDoneRunsRuntugOnce(t *testing.T) {
	runs, argvPath := nodeReapRuntug(t, nodeReapRuntugOK)
	client, report := nodeReapJobDir(t, "job-a", nodeCaptureScreenTxt)
	args := nodeJobDoneArgs{Job: "job-a", Report: report}

	result, code := nodeRun(t, client, nodeCommandJobDone, args)
	wantLine := "OK job=job-a report=" + report + " kind=job.completed"
	if code != "" || result["outcome"] != nodeJobDoneOK || result["line"] != wantLine {
		t.Fatalf("code=%q result=%v", code, result)
	}
	argv, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(argv) != "done job-a --report "+report+"\n" {
		t.Fatalf("argv=%q", argv)
	}
	info, err := os.Stat(filepath.Join(filepath.Dir(report), nodeJobDoneMarker))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("marker info=%v err=%v", info, err)
	}

	result, code = nodeRun(t, client, nodeCommandJobDone, args)
	if code != "" || result["outcome"] != nodeJobAlreadyDone || result["line"] != wantLine {
		t.Fatalf("second call code=%q result=%v", code, result)
	}
	if runs() != 1 {
		t.Fatalf("runtug ran %d times", runs())
	}
}

func TestNodeJobDoneRefusesWhatIsNotThisJob(t *testing.T) {
	runs, _ := nodeReapRuntug(t, nodeReapRuntugOK)
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	dir := filepath.Dir(report)
	jobs := filepath.Dir(dir)

	other := filepath.Join(jobs, "job-b")
	if err := os.MkdirAll(other, 0700); err != nil {
		t.Fatal(err)
	}
	otherReport := filepath.Join(other, nodeJobReportFile)
	secret := filepath.Join(t.TempDir(), "secret.env")
	draft := filepath.Join(dir, "notes.md")
	for _, path := range []string{otherReport, secret, draft} {
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(dir, filepath.Join(jobs, "job-link")); err != nil {
		t.Fatal(err)
	}
	// This link stays inside the job, so only the report's own check refuses it.
	if err := os.Symlink(filepath.Base(draft), filepath.Join(dir, nodeCaptureScreenTxt)); err != nil {
		t.Fatal(err)
	}
	escaping := filepath.Join(jobs, "job-c")
	if err := os.MkdirAll(escaping, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(escaping, nodeJobReportFile)); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name, job, report, outcome string
	}{
		{"missing job dir", "job-gone", filepath.Join(jobs, "job-gone", nodeJobReportFile), nodeCaptureNotAJob},
		{"symlinked job dir", "job-link", filepath.Join(jobs, "job-link", nodeJobReportFile), nodeCaptureNotAJob},
		{"file outside the inbox", "job-a", secret, nodeJobReportInvalid},
		{"another job's report", "job-a", otherReport, nodeJobReportInvalid},
		{"traversal", "job-a", dir + "/../job-b/report.md", nodeJobReportInvalid},
		{"other file in the job", "job-a", draft, nodeJobReportInvalid},
		{"report symlinked inside the job", "job-a", filepath.Join(dir, nodeCaptureScreenTxt), nodeJobReportInvalid},
		{"report symlinked outside the inbox", "job-c", filepath.Join(escaping, nodeJobReportFile), nodeJobReportInvalid},
		{"report not written", "job-b", filepath.Join(other, nodeCaptureScreenTxt), nodeJobReportInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, code := nodeRun(t, client, nodeCommandJobDone, nodeJobDoneArgs{Job: test.job, Report: test.report})
			if code != "" || result["outcome"] != test.outcome || result["line"] != nil {
				t.Fatalf("code=%q result=%v", code, result)
			}
		})
	}
	if runs() != 0 {
		t.Fatalf("runtug ran %d times for a refusal", runs())
	}
	for _, name := range []string{"", "../job-a", "-rf", ".hidden", "a b"} {
		if _, code := nodeRun(t, client, nodeCommandJobDone, nodeJobDoneArgs{Job: name, Report: report}); code != "invalid_args" {
			t.Fatalf("job %q code=%q", name, code)
		}
	}
	if _, code := nodeRun(t, client, nodeCommandJobDone, nodeJobDoneArgs{Job: "job-a", Report: "report.md"}); code != "invalid_args" {
		t.Fatalf("relative report code=%q", code)
	}
	if _, code := nodeRun(t, &HubClient{}, nodeCommandJobDone, nodeJobDoneArgs{Job: "job-a", Report: report}); code != "jobs_root_unset" {
		t.Fatalf("unset root code=%q", code)
	}
}

func TestNodeJobDoneFailureIsRetried(t *testing.T) {
	flag := filepath.Join(t.TempDir(), "healthy")
	runs, _ := nodeReapRuntug(t, `[ -e '`+flag+`' ] || exit 3`+"\n"+nodeReapRuntugOK)
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	args := nodeJobDoneArgs{Job: "job-a", Report: report}
	if _, code := nodeRun(t, client, nodeCommandJobDone, args); code != "done_failed" {
		t.Fatalf("code=%q", code)
	}
	if err := os.WriteFile(flag, nil, 0600); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandJobDone, args)
	if code != "" || result["outcome"] != nodeJobDoneOK {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if runs() != 2 {
		t.Fatalf("runtug ran %d times", runs())
	}
}

// A lead that already ran runtug done leaves no marker; runtug's own duplicate
// answer is what reports it.
func TestNodeJobDoneReportsRuntugDuplicate(t *testing.T) {
	runs, _ := nodeReapRuntug(t, `echo "OK job=$2 report=$4 duplicate=yes"`)
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	result, code := nodeRun(t, client, nodeCommandJobDone, nodeJobDoneArgs{Job: "job-a", Report: report})
	if code != "" || result["outcome"] != nodeJobAlreadyDone {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if runs() != 1 {
		t.Fatalf("runtug ran %d times", runs())
	}
}

// A released job id can be claimed again, so a new report must still complete
// that later round instead of being swallowed by the earlier marker.
func TestNodeJobDoneNewReportRunsAgain(t *testing.T) {
	runs, _ := nodeReapRuntug(t, nodeReapRuntugOK)
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	args := nodeJobDoneArgs{Job: "job-a", Report: report}
	if _, code := nodeRun(t, client, nodeCommandJobDone, args); code != "" {
		t.Fatal(code)
	}
	if err := os.WriteFile(report, []byte("second round\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandJobDone, args)
	if code != "" || result["outcome"] != nodeJobDoneOK || runs() != 2 {
		t.Fatalf("code=%q result=%v runs=%d", code, result, runs())
	}
}

// Refused arguments must not reach a caller in any form, however far decoding
// got before it refused them.
func TestNodeCommandArgsRefusedAreNotReturned(t *testing.T) {
	for _, test := range []struct{ op, raw string }{
		{nodeCommandPaneRead, `{"pane_id":"bad id","name":"job-a","cwd":"/work/a","tail_lines":0}`},
		{nodeCommandJobDone, `{"job":"../job-a","report":"/work/report.md"}`},
		{nodeCommandPaneClose, `{"pane_id":"bad id","name":"job-a","agent_status":"idle","cwd":"/work/a"}`},
		{nodeCommandWorktreeInspect, `{"cwd":"relative"}`},
	} {
		if decoded, valid := decodeNodeCommandArgs(test.op, json.RawMessage(test.raw)); valid || decoded != nil {
			t.Errorf("%s %s: decoded=%+v valid=%v", test.op, test.raw, decoded, valid)
		}
	}
}

func TestNodeReapResultShapes(t *testing.T) {
	for _, test := range []struct {
		op, raw string
		valid   bool
	}{
		{nodeCommandPaneRead, `{"outcome":"read","agent_status":"idle","focused":false,"composer":"empty"}`, true},
		{nodeCommandPaneRead, `{"outcome":"read","agent_status":"idle","focused":false,"composer":"maybe"}`, false},
		{nodeCommandPaneRead, `{"outcome":"read","focused":false,"composer":"empty"}`, false},
		{nodeCommandPaneRead, `{"outcome":"read","agent_status":"idle","focused":false,"composer":"empty","cwd":"/x"}`, false},
		{nodeCommandPaneRead, `{"outcome":"read","agent_status":"idle","focused":false,"composer":"empty","screen_tail":"` + strings.Repeat("x", nodePaneReadMaxTailBytes+1) + `"}`, false},
		{nodeCommandPaneRead, `{"outcome":"pane-gone"}`, true},
		{nodeCommandPaneRead, `{"outcome":"pane-gone","focused":true}`, false},
		{nodeCommandPaneRead, `{"outcome":"pane-changed","screen_tail":"x"}`, false},
		{nodeCommandPaneRead, `{"outcome":"closed"}`, false},
		{nodeCommandJobDone, `{"outcome":"done","line":"OK job=a"}`, true},
		{nodeCommandJobDone, `{"outcome":"already-done"}`, true},
		{nodeCommandJobDone, `{"outcome":"not-a-job"}`, true},
		{nodeCommandJobDone, `{"outcome":"not-a-job","line":"x"}`, false},
		{nodeCommandJobDone, `{"outcome":"report-invalid","line":"x"}`, false},
		{nodeCommandJobDone, `{"outcome":"done","line":"` + strings.Repeat("x", nodeJobDoneMaxLine+1) + `"}`, false},
		{nodeCommandJobDone, `{"outcome":"deleted"}`, false},
	} {
		if _, valid := canonicalNodeCommandResult(test.op, json.RawMessage(test.raw)); valid != test.valid {
			t.Errorf("%s %.80s valid=%v", test.op, test.raw, valid)
		}
	}
}

// nodeReapNode runs a real HubClient for machine with a fake herdr and connects
// it to hub.
func nodeReapNode(t *testing.T, hub *HubServer, server *httptest.Server, machine, token, inbox string, fake *nodeFakeHerdr) {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{
		URL: t14WSURL(server.URL, ""), MachineID: machine, Token: token, AllowInsecureForTests: true, PingInterval: time.Hour, JobsInboxRoot: inbox,
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, errors.New("fixture") },
	})
	if err != nil {
		t.Fatal(err)
	}
	client.nodeHerdr = fake.run
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	eventually(t, 2*time.Second, "node connected", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes[machine]
		return record != nil && record.agent != nil && record.state == "connected"
	})
}

const nodeReapScopes = "node.command:pane.read@*,node.command:job.done@*"

func nodeReapCaller(client string) pw2Caller {
	return pw2Caller{name: client, id: client, token: pw2ServiceToken(client)}
}

type nodeReapFixture struct {
	hub      *HubServer
	server   *httptest.Server
	hubLogs  *pw2LockedBuffer
	nodeLogs *pw2LockedBuffer
	herdrA   *nodeFakeHerdr
	herdrB   *nodeFakeHerdr
	report   string
	readBody string
	doneBody string
	runs     func() int
}

// nodeReapSetup connects two real nodes to a real hub. node-a holds the job and
// its pane; node-b holds a pane whose id collides with it, as host-local ids
// can, under another name.
func nodeReapSetup(t *testing.T, scopes map[string]string) *nodeReapFixture {
	t.Helper()
	runs, _ := nodeReapRuntug(t, nodeReapRuntugOK)
	fixture := &nodeReapFixture{hubLogs: &pw2LockedBuffer{}, nodeLogs: &pw2LockedBuffer{}, runs: runs}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(fixture.nodeLogs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	fixture.hub, fixture.server = pw2Hub(t, scopes, "", fixture.hubLogs)

	screen := "transcript " + nodeReapScreenSecret + "\n" + nodeReapDivider + "\n❯ \n" + nodeReapDivider + "\n"
	fixture.herdrA = &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-a", "claude", false))}, read: screen}
	fixture.herdrB = &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-z", "claude", false))}, read: screen}
	jobClient, report := nodeReapJobDir(t, "job-a", nodeCaptureScreenTxt)
	fixture.report = report
	nodeReapNode(t, fixture.hub, fixture.server, "node-a", pw2NodeAToken, jobClient.jobsInboxRoot, fixture.herdrA)
	nodeReapNode(t, fixture.hub, fixture.server, "node-b", pw2NodeBToken, t.TempDir(), fixture.herdrB)

	fixture.readBody = `{"op":"pane.read","args":{"pane_id":"w1:p1","name":"job-a","cwd":"/work/a","tail_lines":5}}`
	encoded, err := json.Marshal(map[string]any{"op": "job.done", "args": nodeJobDoneArgs{Job: "job-a", Report: report}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.doneBody = string(encoded)
	return fixture
}

func TestNodeReapCommandsAdmitOnlyTheReapService(t *testing.T) {
	fixture := nodeReapSetup(t, map[string]string{
		"svc-reap":    nodeReapScopes,
		"svc-a-only":  "node.command:pane.read@node-a,node.command:job.done@node-a",
		"svc-read":    "node.command:pane.read@*",
		"svc-classic": "nodes.agents.read,node.command:agents.list@*,node.command:pane.capture@*,node.command:pane.close@*",
	})
	nodeA, nodeB := "/v1/nodes/node-a/command", "/v1/nodes/node-b/command"
	operator := pw2Caller{name: "operator", token: pw2OperatorToken}
	node := pw2Caller{name: "node", id: "node-a", token: pw2NodeAToken}
	for _, test := range []struct {
		name   string
		caller pw2Caller
		path   string
		body   string
		status int
	}{
		{"operator pane.read", operator, nodeA, fixture.readBody, http.StatusForbidden},
		{"operator job.done", operator, nodeA, fixture.doneBody, http.StatusForbidden},
		{"node pane.read", node, nodeA, fixture.readBody, http.StatusUnauthorized},
		{"node job.done", node, nodeA, fixture.doneBody, http.StatusUnauthorized},
		{"other service pane.read", nodeReapCaller("svc-classic"), nodeA, fixture.readBody, http.StatusForbidden},
		{"other service job.done", nodeReapCaller("svc-classic"), nodeA, fixture.doneBody, http.StatusForbidden},
		{"read grant job.done", nodeReapCaller("svc-read"), nodeA, fixture.doneBody, http.StatusForbidden},
		{"other machine pane.read", nodeReapCaller("svc-a-only"), nodeB, fixture.readBody, http.StatusForbidden},
		{"other machine job.done", nodeReapCaller("svc-a-only"), nodeB, fixture.doneBody, http.StatusForbidden},
		{"no credential", pw2Caller{name: "none"}, nodeA, fixture.readBody, http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			if status, body := test.caller.do(t, fixture.server, http.MethodPost, test.path, test.body); status != test.status {
				t.Fatalf("status=%d body=%s", status, body)
			}
		})
	}
	if fixture.herdrA.called("pane", "read") != 0 || fixture.herdrA.called("agent", "list") != 0 || fixture.herdrB.called("agent", "list") != 0 {
		t.Fatalf("a refused request reached a node: %v %v", fixture.herdrA.calls, fixture.herdrB.calls)
	}
	if fixture.runs() != 0 {
		t.Fatalf("a refused request ran runtug %d times", fixture.runs())
	}
	// Operator rights over every other op are unchanged.
	if status, body := operator.do(t, fixture.server, http.MethodPost, nodeA, `{"op":"agents.list"}`); status == http.StatusForbidden || status == http.StatusUnauthorized {
		t.Fatalf("operator agents.list status=%d body=%s", status, body)
	}
}

func TestNodeReapEndToEnd(t *testing.T) {
	fixture := nodeReapSetup(t, map[string]string{"svc-reap": nodeReapScopes})
	reaper := nodeReapCaller("svc-reap")
	nodeA, nodeB := "/v1/nodes/node-a/command", "/v1/nodes/node-b/command"
	decode := func(t *testing.T, status int, body string) map[string]any {
		t.Helper()
		if status != http.StatusOK {
			t.Fatalf("status=%d body=%s", status, body)
		}
		return nodeCommandDecode(t, []byte(body))["result"].(map[string]any)
	}

	status, body := reaper.do(t, fixture.server, http.MethodPost, nodeA, fixture.readBody)
	result := decode(t, status, body)
	if result["outcome"] != nodePaneReadOK || result["composer"] != nodeComposerEmpty || result["focused"] != false || !strings.Contains(result["screen_tail"].(string), nodeReapScreenSecret) {
		t.Fatalf("result=%v", result)
	}

	// The same id on another node is a different pane: nothing of it comes back.
	status, body = reaper.do(t, fixture.server, http.MethodPost, nodeB, fixture.readBody)
	if result = decode(t, status, body); len(result) != 2 || result["outcome"] != nodePaneChanged {
		t.Fatalf("colliding id on another node: %v", result)
	}
	if fixture.herdrB.called("pane", "read") != 0 {
		t.Fatalf("node-b read its pane: %v", fixture.herdrB.calls)
	}
	fixture.herdrB.mu.Lock()
	fixture.herdrB.agents = []string{nodeAgentList(nodeReapRow("w9:p9", "job-z", "claude", false))}
	fixture.herdrB.mu.Unlock()
	status, body = reaper.do(t, fixture.server, http.MethodPost, nodeB, fixture.readBody)
	if result = decode(t, status, body); result["outcome"] != nodePaneGone {
		t.Fatalf("pane absent from another node: %v", result)
	}
	// Likewise a job that lives on node-a is not a job on node-b.
	status, body = reaper.do(t, fixture.server, http.MethodPost, nodeB, fixture.doneBody)
	if result = decode(t, status, body); result["outcome"] != nodeCaptureNotAJob {
		t.Fatalf("job on another node: %v", result)
	}
	if fixture.runs() != 0 {
		t.Fatalf("runtug ran %d times for another node's job", fixture.runs())
	}

	status, body = reaper.do(t, fixture.server, http.MethodPost, nodeA, fixture.doneBody)
	if result = decode(t, status, body); result["outcome"] != nodeJobDoneOK || !strings.HasPrefix(result["line"].(string), "OK job=job-a ") {
		t.Fatalf("job.done: %v", result)
	}
	status, body = reaper.do(t, fixture.server, http.MethodPost, nodeA, fixture.doneBody)
	if result = decode(t, status, body); result["outcome"] != nodeJobAlreadyDone {
		t.Fatalf("second job.done: %v", result)
	}
	if fixture.runs() != 1 {
		t.Fatalf("runtug ran %d times", fixture.runs())
	}

	// Each side logged every call by pane, job and outcome, and neither logged
	// the screen, the runtug line or a credential.
	operator := pw2Caller{name: "operator", token: pw2OperatorToken}
	if status, _ := operator.do(t, fixture.server, http.MethodPost, nodeA, fixture.readBody); status != http.StatusForbidden {
		t.Fatalf("operator status=%d", status)
	}
	if status, _ := reaper.do(t, fixture.server, http.MethodPost, nodeA, `{"op":"pane.read","args":{"pane_id":"w1:p1"}}`); status != http.StatusBadRequest {
		t.Fatalf("malformed status=%d", status)
	}
	// Every field is present but none is valid, so the decoder fills them in
	// before it refuses them. None of that text is a validated subject.
	const forged = "forged-subject-not-for-logs"
	for _, body := range []string{
		`{"op":"pane.read","args":{"pane_id":"` + forged + ` x","name":"` + forged + `/x","cwd":"/work/a","tail_lines":0}}`,
		`{"op":"job.done","args":{"job":"` + forged + `/x","report":"/work/report.md"}}`,
	} {
		if status, _ := reaper.do(t, fixture.server, http.MethodPost, nodeA, body); status != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d", body, status)
		}
	}
	hubLogs := fixture.hubLogs.String()
	for _, want := range []string{
		"client=svc-reap machine=node-a op=pane.read pane=w1:p1 job=job-a outcome=read",
		"client=svc-reap machine=node-b op=pane.read pane=w1:p1 job=job-a outcome=pane-changed",
		"client=svc-reap machine=node-b op=pane.read pane=w1:p1 job=job-a outcome=pane-gone",
		"client=svc-reap machine=node-b op=job.done pane=\"\" job=job-a outcome=not-a-job",
		"client=svc-reap machine=node-a op=job.done pane=\"\" job=job-a outcome=done",
		"client=svc-reap machine=node-a op=job.done pane=\"\" job=job-a outcome=already-done",
		"client=operator machine=node-a op=pane.read pane=w1:p1 job=job-a outcome=scope_forbidden",
		"client=svc-reap machine=node-a op=pane.read pane=\"\" job=\"\" outcome=invalid_request",
		"client=svc-reap machine=node-a op=job.done pane=\"\" job=\"\" outcome=invalid_request",
	} {
		if !strings.Contains(hubLogs, want) {
			t.Errorf("hub log lacks %q:\n%s", want, hubLogs)
		}
	}
	nodeLogs := fixture.nodeLogs.String()
	for _, want := range []string{
		"op=pane.read pane=w1:p1 job=job-a outcome=read",
		"op=pane.read pane=w1:p1 job=job-a outcome=pane-changed",
		"op=job.done pane=\"\" job=job-a outcome=done",
		"op=job.done pane=\"\" job=job-a outcome=already-done",
	} {
		if !strings.Contains(nodeLogs, want) {
			t.Errorf("node log lacks %q:\n%s", want, nodeLogs)
		}
	}
	for _, forbidden := range []string{nodeReapScreenSecret, "OK job=", forged, pw2ServiceToken("svc-reap"), pw2OperatorToken, pw2NodeAToken} {
		if strings.Contains(hubLogs+nodeLogs, forbidden) {
			t.Errorf("logs hold %q:\n%s\n%s", forbidden, hubLogs, nodeLogs)
		}
	}
}

func TestNodeReapClient(t *testing.T) {
	fixture := nodeReapSetup(t, map[string]string{"svc-reap": nodeReapScopes, "svc-none": "nodes.agents.read"})
	deps := hubCLIDeps{AllowInsecureForTests: true}
	client, err := newNodeReapClient(fixture.server.URL, hubTokenEnv{MachineID: "svc-reap", Token: pw2ServiceToken("svc-reap")}, hubCFAccessEnv{}, deps)
	if err != nil {
		t.Fatal(err)
	}
	read, err := client.PaneRead(t.Context(), "node-a", "w1:p1", "job-a", "/work/a", 1)
	if err != nil || read.Outcome != nodePaneReadOK || read.Composer != nodeComposerEmpty || read.AgentStatus != "idle" || read.Focused || read.ScreenTail == "" {
		t.Fatalf("read=%+v err=%v", read, err)
	}
	done, err := client.JobDone(t.Context(), "node-a", "job-a", fixture.report)
	if err != nil || done.Outcome != nodeJobDoneOK || done.Line == "" {
		t.Fatalf("done=%+v err=%v", done, err)
	}
	if _, err := client.PaneRead(t.Context(), "node-c", "w1:p1", "job-a", "/work/a", 0); err == nil {
		t.Fatal("expected an error for an unknown node")
	} else {
		var hubErr *NodeReapError
		if !errors.As(err, &hubErr) || hubErr.Code != "node_unavailable" {
			t.Fatalf("err=%v", err)
		}
	}
	unscoped, err := newNodeReapClient(fixture.server.URL, hubTokenEnv{MachineID: "svc-none", Token: pw2ServiceToken("svc-none")}, hubCFAccessEnv{}, deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unscoped.JobDone(t.Context(), "node-a", "job-a", fixture.report); !errors.Is(err, ErrNodeReapRefused) {
		t.Fatalf("err=%v", err)
	}

	if _, err := newNodeReapClient(fixture.server.URL, hubTokenEnv{MachineID: hubOperatorMachineID, Token: pw2OperatorToken}, hubCFAccessEnv{}, deps); err == nil {
		t.Fatal("the operator credential must be refused")
	}
	if _, err := NewNodeReapClient(fixture.server.URL, "svc-reap", pw2ServiceToken("svc-reap"), "", ""); err == nil {
		t.Fatal("the exported client must refuse a loopback origin")
	}
	if _, err := NewNodeReapClient("https://"+hubOperatorProductionHost, "svc-reap", pw2ServiceToken("svc-reap"), "", ""); err != nil {
		t.Fatalf("no Access pair: %v", err)
	}
	if _, err := NewNodeReapClient("https://"+hubOperatorProductionHost, "svc-reap", pw2ServiceToken("svc-reap"), "client-id", ""); err == nil {
		t.Fatal("half an Access pair must be refused")
	}
}

func TestNodeReapCLI(t *testing.T) {
	fixture := nodeReapSetup(t, map[string]string{"svc-reap": nodeReapScopes})
	deps := hubCLIDeps{AllowInsecureForTests: true}
	directory := t.TempDir()
	envFile := func(name, contents string) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	service := envFile("service.env", "HUB_MACHINE_ID=svc-reap\nHUB_TOKEN="+pw2ServiceToken("svc-reap")+"\n")
	operator := envFile("operator.env", "HUB_MACHINE_ID=operator\nHUB_TOKEN="+pw2OperatorToken+"\n")
	common := []string{"--hub-url", fixture.server.URL, "--hub-token-env", service, "--machine", "node-a"}

	var stdout, stderr bytes.Buffer
	readArgs := append(append([]string{}, common...), "--pane-id", "w1:p1", "--name", "job-a", "--cwd", "/work/a")
	if code := runNodeCLI(append([]string{"pane-read"}, readArgs...), &stdout, &stderr, deps); code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	var read PaneReadResult
	if err := json.Unmarshal(stdout.Bytes(), &read); err != nil || read.Outcome != nodePaneReadOK || read.Composer != nodeComposerEmpty {
		t.Fatalf("stdout=%s err=%v", stdout.String(), err)
	}

	stdout.Reset()
	doneArgs := append(append([]string{}, common...), "--job", "job-a", "--report", fixture.report)
	if code := runNodeCLI(append([]string{"job-done"}, doneArgs...), &stdout, &stderr, deps); code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	var done JobDoneResult
	if err := json.Unmarshal(stdout.Bytes(), &done); err != nil || done.Outcome != nodeJobDoneOK {
		t.Fatalf("stdout=%s err=%v", stdout.String(), err)
	}

	for _, args := range [][]string{
		{"pane-read"},
		{"pane-read", "--hub-url", fixture.server.URL},
		append(append([]string{"pane-read"}, readArgs...), "--tail-lines", "9999"),
		append(append([]string{"pane-read"}, readArgs...), "extra"),
		{"job-done", "--job", "job-a"},
		append([]string{"job-done"}, common...),
		{"unknown"},
	} {
		if code := runNodeCLI(args, &stdout, &stderr, deps); code != ExitUsage {
			t.Errorf("args=%v code=%d", args, code)
		}
	}
	operatorArgs := []string{"pane-read", "--hub-url", fixture.server.URL, "--hub-token-env", operator, "--machine", "node-a", "--pane-id", "w1:p1", "--name", "job-a", "--cwd", "/work/a"}
	if code := runNodeCLI(operatorArgs, &stdout, &stderr, deps); code != ExitConditionInvalid {
		t.Fatalf("operator env code=%d", code)
	}
	unavailable := []string{"pane-read", "--hub-url", fixture.server.URL, "--hub-token-env", service, "--machine", "node-c", "--pane-id", "w1:p1", "--name", "job-a", "--cwd", "/work/a"}
	stderr.Reset()
	if code := runNodeCLI(unavailable, &stdout, &stderr, deps); code != ExitDaemonUnavailable || !strings.Contains(stderr.String(), "node_unavailable") {
		t.Fatalf("unavailable code=%d stderr=%s", code, stderr.String())
	}
}
