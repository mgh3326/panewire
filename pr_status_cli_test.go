package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const prCLIToken = "svc-pr-cli-token-secret-value"

var prCLINow = time.Date(2026, 11, 6, 0, 15, 6, 0, time.UTC) // three minutes after the fixture

// prCLIStub is a hub that answers every POST /v1/nodes/oci/command with a
// fixed reply and records the requests it saw.
type prCLIStub struct {
	*httptest.Server
	mu       sync.Mutex
	bodies   []string
	headers  []http.Header
	status   int
	response string
}

func newPRCLIStub(t *testing.T, status int, response string) *prCLIStub {
	t.Helper()
	stub := &prCLIStub{status: status, response: response}
	stub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contents, _ := io.ReadAll(r.Body)
		stub.mu.Lock()
		stub.bodies = append(stub.bodies, string(contents))
		stub.headers = append(stub.headers, r.Header.Clone())
		stub.mu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/v1/nodes/oci/command" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(stub.status)
		_, _ = w.Write([]byte(stub.response))
	}))
	t.Cleanup(stub.Close)
	return stub
}

func prNodeReply(t *testing.T, result nodePRStatusResult) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"request_id": "r", "machine": "oci", "op": "pr.status", "result": result})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func prFixtureResult(t *testing.T, hk nodePRStatusHK) nodePRStatusResult {
	t.Helper()
	var snapshot PRStatusSnapshot
	if err := json.Unmarshal([]byte(prStatusFixture), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = boundPRStatusSnapshot(snapshot)
	return nodePRStatusResult{Mergewake: nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot}, HK: hk}
}

var (
	prHKNotDeployed = nodePRStatusHK{Outcome: prStatusNotFound, Status: 404}
	prBothDown      = nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonExit},
		HK:        nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonRequest},
	}
)

// prCLIConnection writes the credential env file and returns the connection
// flags for url. The credential is in the file, never in the arguments.
func prCLIConnection(t *testing.T, url string) []string {
	t.Helper()
	for _, name := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_PR_STATUS_HOST"} {
		t.Setenv(name, "")
	}
	env := writeMode0600(t, t.TempDir(), "hub.env", "HUB_MACHINE_ID=svc-pr\nHUB_TOKEN="+prCLIToken+"\n")
	return []string{"--hub-url", url, "--hub-token-env", env}
}

type prCLIRun struct {
	code           int
	stdout, stderr string
}

func runPR(t *testing.T, args ...string) prCLIRun {
	t.Helper()
	var stdout, stderr bytes.Buffer
	deps := hubCLIDeps{AllowInsecureForTests: true, Now: func() time.Time { return prCLINow }}
	code := runPRCLI(args, &stdout, &stderr, deps)
	run := prCLIRun{code: code, stdout: stdout.String(), stderr: stderr.String()}
	if strings.Contains(run.stdout+run.stderr, prCLIToken) {
		t.Fatalf("credential leaked into output: %q %q", run.stdout, run.stderr)
	}
	return run
}

// fakeGH installs a gh that records its argv and answers `pr view` and
// `api graphql` from the given JSON; "" makes that call exit 1.
func fakeGH(t *testing.T, view, graphql string) (argvLog string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake gh is a POSIX shell script")
	}
	dir := t.TempDir()
	argvLog = filepath.Join(dir, "gh-argv")
	files := map[string]string{"view": filepath.Join(dir, "view.json"), "graphql": filepath.Join(dir, "graphql.json")}
	for name, contents := range map[string]string{"view": view, "graphql": graphql} {
		if err := os.WriteFile(files[name], []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> '` + argvLog + `'
case "$1" in
  pr) [ -s '` + files["view"] + `' ] && cat '` + files["view"] + `' || exit 1 ;;
  api) [ -s '` + files["graphql"] + `' ] && cat '` + files["graphql"] + `' || exit 1 ;;
  *) exit 2 ;;
esac
`
	path := filepath.Join(dir, "gh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := prStatusGHExecutable
	prStatusGHExecutable = path
	t.Cleanup(func() { prStatusGHExecutable = previous })
	return argvLog
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

const (
	ghViewFixture    = `{"state":"OPEN","headRefOid":"1c4231f99c574ac6da029dd038756a9190e6a3cf","isDraft":false,"mergeable":"MERGEABLE","statusCheckRollup":[{"__typename":"CheckRun","name":"test","status":"COMPLETED","conclusion":"SUCCESS"},{"__typename":"CheckRun","name":"vet","status":"COMPLETED","conclusion":"FAILURE"},{"__typename":"StatusContext","context":"ci","state":"PENDING"}]}`
	ghThreadsFixture = `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[
	  {"id":"T1","isResolved":false,"path":"a/b.go","line":42,"comments":{"totalCount":2,"nodes":[{"body":"first line\nsecond line","url":"https://github.com/owner/name/pull/152#r1"}]}},
	  {"id":"T2","isResolved":true,"path":"c.go","line":1,"comments":{"totalCount":1,"nodes":[{"body":"resolved","url":"https://github.com/owner/name/pull/152#r2"}]}}]}}}}}`
)

func TestPRStatusSummaryLine(t *testing.T) {
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, prHKNotDeployed)))
	run := runPR(t, append([]string{"status", "owner/name", "152"}, prCLIConnection(t, stub.URL)...)...)
	if run.code != ExitOK || run.stdout != "owner/name#152 blocked head=1c4231f blocking=1 checks=2/2 green observed=3m ago reasons=threads_open:1\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	// hk 404 is a note, never a failure, and gh is not consulted for it.
	if !strings.Contains(run.stderr, "note: hk change record omitted: /v1/changes answered 404") {
		t.Fatalf("stderr=%q", run.stderr)
	}
	if log := readLog(t, gh); log != "" {
		t.Fatalf("gh ran although a source answered: %q", log)
	}
	var sent struct {
		Op   string         `json:"op"`
		Args map[string]any `json:"args"`
	}
	if err := json.Unmarshal([]byte(stub.bodies[0]), &sent); err != nil || sent.Op != "pr.status" || sent.Args["repo"] != "owner/name" || sent.Args["pr"] != float64(152) || sent.Args["fresh"] != false {
		t.Fatalf("request %s", stub.bodies[0])
	}
	// The credential is the bearer header from the env file, nowhere else.
	if got := stub.headers[0].Get("Authorization"); got != "Bearer "+prCLIToken {
		t.Fatalf("authorization %q", got)
	}
	if strings.Contains(stub.bodies[0], prCLIToken) {
		t.Fatal("token in request body")
	}
}

func TestPRStatusFlagsMayFollowPositionals(t *testing.T) {
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, prHKNotDeployed)))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"status", "owner/name", "152", "--fresh", "--json"}, connection...)...)
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "{") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if !strings.Contains(stub.bodies[0], `"fresh":true`) {
		t.Fatalf("--fresh did not reach the node op: %s", stub.bodies[0])
	}
	// findings has no --fresh: the snapshot is the last gate decision.
	if run := runPR(t, append([]string{"findings", "owner/name", "152", "--fresh"}, connection...)...); run.code != ExitUsage {
		t.Fatalf("findings --fresh code=%d", run.code)
	}
}

func TestPRStatusJSONMergedView(t *testing.T) {
	change := json.RawMessage(`{"pr":"https://github.com/owner/name/pull/152","verdicts":[]}`)
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: change})))
	run := runPR(t, append([]string{"status", "owner/name", "152", "--json"}, prCLIConnection(t, stub.URL)...)...)
	var merged map[string]any
	if err := json.Unmarshal([]byte(run.stdout), &merged); err != nil || run.code != ExitOK {
		t.Fatalf("code=%d stdout=%q err=%v", run.code, run.stdout, err)
	}
	if merged["source"] != "panewire" || merged["repo"] != "owner/name" || merged["pr"] != float64(152) || merged["gh"] != nil {
		t.Fatalf("merged %v", merged)
	}
	if merged["mergewake"].(map[string]any)["state"] != "blocked" || merged["hk"].(map[string]any)["pr"] != "https://github.com/owner/name/pull/152" {
		t.Fatalf("merged %v", merged)
	}
	if len(merged["notes"].([]any)) != 0 || strings.TrimSpace(run.stderr) != "" {
		t.Fatalf("notes %v stderr %q", merged["notes"], run.stderr)
	}

	// hk 404: the hk part is omitted and the JSON says so.
	stub.response = prNodeReply(t, prFixtureResult(t, prHKNotDeployed))
	run = runPR(t, append([]string{"status", "owner/name", "152", "--json"}, prCLIConnection(t, stub.URL)...)...)
	merged = nil
	if err := json.Unmarshal([]byte(run.stdout), &merged); err != nil || merged["hk"] != nil || merged["mergewake"] == nil {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	if notes := merged["notes"].([]any); len(notes) != 1 || !strings.Contains(notes[0].(string), "404") {
		t.Fatalf("notes %v", notes)
	}
}

func TestPRFindingsListing(t *testing.T) {
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, prHKNotDeployed)))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
	want := "high\tblocking\tinternal/prstatus/handler.go:42\tfresh read overwrites a newer gate decision\tunreplied\thttps://github.com/owner/name/pull/152#discussion_r1\n" +
		"low\tnonblocking\tdocs/pr-status.md:10\ttypo in field table\treplied\thttps://github.com/owner/name/pull/152#discussion_r2\n"
	if run.code != ExitOK || run.stdout != want {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	run = runPR(t, append([]string{"findings", "owner/name", "152", "--json"}, connection...)...)
	var listed struct {
		Source   string            `json:"source"`
		Head     string            `json:"head"`
		Findings []PRStatusFinding `json:"findings"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &listed); err != nil || listed.Source != "panewire" || len(listed.Findings) != 2 || listed.Findings[0].ID != "PRRT_kwDOexample1" || !strings.HasPrefix(listed.Head, "1c4231f") {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	// A summary can never break the one-finding-per-line shape.
	result := prFixtureResult(t, prHKNotDeployed)
	result.Mergewake.Snapshot.Findings[0].Summary = "two\nlines\twith\ttabs"
	stub.response = prNodeReply(t, result)
	run = runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
	lines := strings.Split(strings.TrimSuffix(run.stdout, "\n"), "\n")
	if len(lines) != 2 || len(strings.Split(lines[0], "\t")) != 6 || !strings.Contains(lines[0], "two lines with tabs") {
		t.Fatalf("stdout=%q", run.stdout)
	}
	// No findings: nothing on stdout, a note on stderr.
	result.Mergewake.Snapshot.Findings = []PRStatusFinding{}
	stub.response = prNodeReply(t, result)
	if run = runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...); run.code != ExitOK || run.stdout != "" || !strings.Contains(run.stderr, "no open findings") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

func TestPRStatusFallsBackToGHOnlyWhenBothSourcesFail(t *testing.T) {
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	stub := newPRCLIStub(t, 200, prNodeReply(t, prBothDown))
	connection := prCLIConnection(t, stub.URL)

	run := runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK {
		t.Fatalf("code=%d stderr=%q", run.code, run.stderr)
	}
	for _, want := range []string{"owner/name#152 open head=1c4231f blocking=? checks=1/3 green pending:1 red:1", "source=gh (fallback: mergewake failed (exit_status), handoffkeep failed (request_failed))"} {
		if !strings.Contains(run.stdout, want) {
			t.Fatalf("stdout %q lacks %q", run.stdout, want)
		}
	}
	if !strings.Contains(run.stderr, "note: fell back to gh: mergewake failed (exit_status)") {
		t.Fatalf("stderr %q", run.stderr)
	}
	if log := readLog(t, gh); log != "pr view 152 --repo owner/name --json state,headRefOid,isDraft,mergeable,statusCheckRollup\n" {
		t.Fatalf("gh argv %q", log)
	}

	run = runPR(t, append([]string{"status", "owner/name", "152", "--json"}, connection...)...)
	var merged map[string]any
	if err := json.Unmarshal([]byte(run.stdout), &merged); err != nil || merged["source"] != "gh" || merged["mergewake"] != nil || merged["hk"] != nil || merged["gh"].(map[string]any)["state"] != "OPEN" || !strings.Contains(merged["fallback"].(string), "mergewake failed") {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}

	run = runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
	want := "-\t?\ta/b.go:42\tfirst line\treplied\thttps://github.com/owner/name/pull/152#r1\n"
	if run.code != ExitOK || run.stdout != want || !strings.Contains(run.stderr, "fell back to gh") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if log := readLog(t, gh); !strings.Contains(log, "api graphql -f query=") || !strings.Contains(log, "-f owner=owner -f name=name -F number=152") {
		t.Fatalf("gh argv %q", log)
	}
}

func TestPRStatusFallbackWhenHubCannotBeAsked(t *testing.T) {
	fakeGH(t, ghViewFixture, ghThreadsFixture)
	down := newPRCLIStub(t, 200, "")
	downURL := down.URL
	down.Close()
	refused := newPRCLIStub(t, 403, `{"error":"scope_forbidden"}`)
	unavailable := newPRCLIStub(t, 503, `{"error":"node_unavailable","state":"disconnected"}`)
	garbled := newPRCLIStub(t, 200, `{"result":{"mergewake":{"outcome":"ok"},"hk":{"outcome":"ok"}}}`)
	for name, tc := range map[string]struct {
		url  string
		want string
	}{
		"unreachable": {downURL, "hub unreachable"},
		"refused":     {refused.URL, "hub refused the credential for pr.status on oci"},
		"unavailable": {unavailable.URL, "hub answered 503 node_unavailable"},
		"garbled":     {garbled.URL, "hub answered an unusable pr.status result"},
	} {
		run := runPR(t, append([]string{"status", "owner/name", "152"}, prCLIConnection(t, tc.url)...)...)
		if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: "+tc.want+")") {
			t.Errorf("%s: code=%d stdout=%q stderr=%q", name, run.code, run.stdout, run.stderr)
		}
	}
	// No hub connection configured: gh answers, and the line says why.
	prCLIConnection(t, "") // clears the environment
	run := runPR(t, "status", "owner/name", "152")
	if run.code != ExitOK || !strings.Contains(run.stdout, "fallback: hub connection is not configured") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

func TestPRStatusGHFailsToo(t *testing.T) {
	fakeGH(t, "", "")
	stub := newPRCLIStub(t, 200, prNodeReply(t, prBothDown))
	run := runPR(t, append([]string{"status", "owner/name", "152"}, prCLIConnection(t, stub.URL)...)...)
	if run.code != ExitDaemonUnavailable || run.stdout != "" || !strings.Contains(run.stderr, "no source answered (mergewake failed (exit_status), handoffkeep failed (request_failed)) and gh failed") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

// One source is enough: the missing one is a note, and gh is never run.
func TestPRStatusOneSourceIsEnough(t *testing.T) {
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	change := json.RawMessage(`{"pr":"https://github.com/owner/name/pull/152"}`)
	onlyHK := nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusNotFound},
		HK:        nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: change},
	}
	stub := newPRCLIStub(t, 200, prNodeReply(t, onlyHK))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || !strings.Contains(run.stdout, "no mergewake snapshot; handoffkeep change record only") || !strings.Contains(run.stderr, "mergewake snapshot omitted: mergewake has no snapshot for this PR") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	run = runPR(t, append([]string{"status", "owner/name", "152", "--json"}, connection...)...)
	var merged map[string]any
	if err := json.Unmarshal([]byte(run.stdout), &merged); err != nil || merged["mergewake"] != nil || merged["hk"] == nil || merged["source"] != "panewire" {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	// Findings come only from mergewake: say so instead of guessing from hk.
	if run = runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...); run.code != ExitDaemonUnavailable || run.stdout != "" || !strings.Contains(run.stderr, "findings need the mergewake snapshot") {
		t.Fatalf("findings code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}

	// mergewake alone, with hk failing for any reason, still answers.
	stub.response = prNodeReply(t, prFixtureResult(t, nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonHTTPStatus}))
	run = runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "owner/name#152 blocked") || !strings.Contains(run.stderr, "hk change record omitted: handoffkeep failed (http_status)") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if log := readLog(t, gh); log != "" {
		t.Fatalf("gh ran: %q", log)
	}
}

func TestPRCLIUsage(t *testing.T) {
	for name, args := range map[string][]string{
		"none":            {},
		"unknown":         {"list"},
		"no args":         {"status"},
		"one arg":         {"status", "owner/name"},
		"three args":      {"status", "owner/name", "1", "2"},
		"bad repo":        {"status", "owner", "1"},
		"traversal":       {"status", "owner/..", "1"},
		"non-numeric":     {"status", "owner/name", "x"},
		"zero":            {"status", "owner/name", "0"},
		"leading zero":    {"status", "owner/name", "01"},
		"unknown flag":    {"status", "owner/name", "1", "--token", "x"},
		"flag no value":   {"status", "owner/name", "1", "--hub-url"},
		"operator target": {"status", "owner/name", "1", "--machine", "operator"},
		"bad machine":     {"status", "owner/name", "1", "--machine", "Bad Host"},
	} {
		var stdout, stderr bytes.Buffer
		if code := runPRCLI(args, &stdout, &stderr, hubCLIDeps{}); code != ExitUsage || stdout.Len() != 0 {
			t.Errorf("%s: code=%d stdout=%q", name, code, stdout.String())
		}
	}
	var stdout bytes.Buffer
	if code := runPRCLI([]string{"--help"}, &stdout, io.Discard, hubCLIDeps{}); code != ExitOK || !strings.Contains(stdout.String(), "panewire pr status") {
		t.Fatalf("help code=%d %q", code, stdout.String())
	}
}

// The whole path with nothing stubbed but mergewake, hk and gh themselves: the
// CLI sends the op over HTTP to a real hub, the hub forwards it to a real node
// connection, and the node runs the fake mergewake and reads the fake hk.
func TestPRStatusEndToEndThroughHubAndNode(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_leak_canary_value")
	fake := installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	hk := newPRStatusHKServer(t, 404, `{"error":"not found"}`)
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	hub, server := prStatusHub(t, "machine-a")
	node, err := NewHubClient(HubClientConfig{
		URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour,
		SpawnConfigPath:     prStatusNodeClient(t, hk.URL).spawnConfigPath,
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, errors.New("fixture") },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		node.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	eventually(t, 2*time.Second, "node connected", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.state == "connected"
	})

	env := writeMode0600(t, t.TempDir(), "svc.env", "HUB_MACHINE_ID=svc-pr\nHUB_TOKEN="+prHubServiceToken+"\n")
	for _, name := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_PR_STATUS_HOST"} {
		t.Setenv(name, "")
	}
	t.Setenv("PANEWIRE_PR_STATUS_HOST", "machine-a")
	t.Setenv("PANEWIRE_HUB_URL", server.URL)
	t.Setenv("PANEWIRE_HUB_TOKEN_ENV", env)

	run := runPR(t, "status", "owner/name", "152")
	if run.code != ExitOK || run.stdout != "owner/name#152 blocked head=1c4231f blocking=1 checks=2/2 green observed=3m ago reasons=threads_open:1\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if !strings.Contains(run.stderr, "hk change record omitted: /v1/changes answered 404") {
		t.Fatalf("stderr=%q", run.stderr)
	}
	run = runPR(t, "findings", "owner/name", "152", "--json")
	var listed struct {
		Findings []PRStatusFinding `json:"findings"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &listed); err != nil || len(listed.Findings) != 2 {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	if argv := fake.read(t, "argv"); !strings.HasPrefix(argv, "pr-status\n--repo\nowner/name\n--pr\n152\n--json") || strings.Contains(fake.read(t, "env"), "ghp_leak_canary_value") {
		t.Fatalf("mergewake ran with argv %q", argv)
	}
	// Zero GitHub requests: gh was never run, and mergewake got no --fresh.
	if log := readLog(t, gh); log != "" || strings.Contains(fake.read(t, "argv"), "--fresh") {
		t.Fatalf("gh=%q", log)
	}
	// A credential for another op is refused by the hub, and the CLI falls back.
	other := writeMode0600(t, t.TempDir(), "other.env", "HUB_MACHINE_ID=svc-other\nHUB_TOKEN="+prHubOtherToken+"\n")
	t.Setenv("PANEWIRE_HUB_TOKEN_ENV", other)
	run = runPR(t, "status", "owner/name", "152")
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: hub refused the credential for pr.status on machine-a)") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

// Observed age reads from the injected clock, in the units a person scans.
func TestPRObservedAge(t *testing.T) {
	observed := "2026-11-06T00:12:06Z"
	for offset, want := range map[time.Duration]string{
		-time.Hour:       "0s ago",
		0:                "0s ago",
		59 * time.Second: "0s ago",
		3 * time.Minute:  "3m ago",
		2 * time.Hour:    "2h ago",
		47 * time.Hour:   "47h ago",
		72 * time.Hour:   "3d ago",
	} {
		at, _ := time.Parse(time.RFC3339, observed)
		if got := prObservedAge(observed, at.Add(offset)); got != want {
			t.Errorf("offset %s: %q want %q", offset, got, want)
		}
	}
	if got := prObservedAge("garbage", prCLINow); got != "unknown" {
		t.Fatal(got)
	}
}

// fakeGHThreadPages installs a gh whose review-thread query answers page 1 until
// it is asked `after=C1`, then page 2; it logs every call.
func fakeGHThreadPages(t *testing.T, page1, page2 string) (argvLog string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake gh is a POSIX shell script")
	}
	dir := t.TempDir()
	argvLog = filepath.Join(dir, "gh-argv")
	for name, contents := range map[string]string{"page1.json": page1, "page2.json": page2} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> '` + argvLog + `'
case "$*" in
  *"after=C1"*) cat '` + filepath.Join(dir, "page2.json") + `' ;;
  *) cat '` + filepath.Join(dir, "page1.json") + `' ;;
esac
`
	path := filepath.Join(dir, "gh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := prStatusGHExecutable
	prStatusGHExecutable = path
	t.Cleanup(func() { prStatusGHExecutable = previous })
	return argvLog
}

func ghThreadsPage(hasNext bool, cursor, threads string) string {
	return fmt.Sprintf(`{"data":{"repository":{"pullRequest":{"reviewThreads":{"pageInfo":{"hasNextPage":%t,"endCursor":%q},"nodes":[%s]}}}}}`, hasNext, cursor, threads)
}

const ghThreadNode = `{"id":%q,"isResolved":false,"path":"f.go","line":%d,"comments":{"totalCount":1,"nodes":[{"body":"finding %s","url":"https://github.com/owner/name/pull/152#r%s"}]}}`

// The gh fallback follows pageInfo, so an unresolved thread past the first 100
// is listed, and a PR the page cap cannot cover says the list is incomplete.
func TestPRStatusGHFallbackPaginatesReviewThreads(t *testing.T) {
	stub := newPRCLIStub(t, 200, prNodeReply(t, prBothDown))
	connection := prCLIConnection(t, stub.URL)

	log := fakeGHThreadPages(t,
		ghThreadsPage(true, "C1", fmt.Sprintf(ghThreadNode, "T1", 1, "one", "1")),
		ghThreadsPage(false, "", fmt.Sprintf(ghThreadNode, "T2", 2, "two", "2")))
	run := runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || strings.Count(run.stdout, "\n") != 2 || !strings.Contains(run.stdout, "f.go:1\tfinding one") || !strings.Contains(run.stdout, "f.go:2\tfinding two") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if strings.Contains(run.stderr, "incomplete") {
		t.Fatalf("complete list reported as incomplete: %q", run.stderr)
	}
	calls := strings.Split(strings.TrimSpace(readLog(t, log)), "\n")
	if len(calls) != 2 || strings.Contains(calls[0], "after=") || !strings.Contains(calls[1], "-f after=C1") {
		t.Fatalf("gh calls %q", calls)
	}

	// Every page claims another one: stop at the cap and say the list is partial.
	log = fakeGHThreadPages(t, ghThreadsPage(true, "C1", fmt.Sprintf(ghThreadNode, "T1", 1, "one", "1")), ghThreadsPage(true, "C1", fmt.Sprintf(ghThreadNode, "T2", 2, "two", "2")))
	run = runPR(t, append([]string{"findings", "owner/name", "152", "--json"}, connection...)...)
	var listed struct {
		Findings []PRStatusFinding `json:"findings"`
		Notes    []string          `json:"notes"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &listed); err != nil || run.code != ExitOK {
		t.Fatalf("code=%d stdout=%q err=%v", run.code, run.stdout, err)
	}
	if got := len(strings.Split(strings.TrimSpace(readLog(t, log)), "\n")); got != prStatusGHThreadPages {
		t.Fatalf("gh called %d times, want the cap %d", got, prStatusGHThreadPages)
	}
	joined := strings.Join(listed.Notes, "|")
	if !strings.Contains(joined, "findings list is incomplete") || !strings.Contains(joined, "fell back to gh") {
		t.Fatalf("notes %v", listed.Notes)
	}
}

// hk answering 200 with an empty changes list holds no record: with mergewake
// missing too, gh answers (finding: empty list suppressed the fallback).
func TestPRStatusEmptyHKChangesFallsBackToGH(t *testing.T) {
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	empty := nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusNotFound},
		HK:        nodePRStatusHK{Outcome: prStatusNotFound, Status: http.StatusOK},
	}
	stub := newPRCLIStub(t, 200, prNodeReply(t, empty))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: mergewake has no snapshot for this PR, handoffkeep") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if run := runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...); run.code != ExitOK || !strings.Contains(run.stdout, "a/b.go:42") {
		t.Fatalf("findings code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if !strings.Contains(readLog(t, gh), "api graphql") {
		t.Fatal("gh was not consulted")
	}
	// With a mergewake snapshot, the empty hk list is only a note.
	stub.response = prNodeReply(t, prFixtureResult(t, nodePRStatusHK{Outcome: prStatusNotFound, Status: http.StatusOK}))
	run = runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "owner/name#152 blocked") || !strings.Contains(run.stderr, "/v1/changes has no change for this PR") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

// A GraphQL answer without the PR (unknown number, no access) or with errors is
// a gh failure, never "no open findings".
func TestPRStatusGHFallbackRejectsIncompleteGraphQL(t *testing.T) {
	stub := newPRCLIStub(t, 200, prNodeReply(t, prBothDown))
	connection := prCLIConnection(t, stub.URL)
	for name, answer := range map[string]string{
		"null pull request": `{"data":{"repository":{"pullRequest":null}}}`,
		"null repository":   `{"data":{"repository":null}}`,
		"null data":         `{"data":null}`,
		"empty object":      `{}`,
		"no threads":        `{"data":{"repository":{"pullRequest":{}}}}`,
		"graphql errors":    `{"errors":[{"message":"Could not resolve to a PullRequest"}],"data":{"repository":{"pullRequest":null}}}`,
		"errors with data":  `{"errors":[{"message":"rate limited"}],"data":{"repository":{"pullRequest":{"reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":""},"nodes":[]}}}}}`,
	} {
		fakeGH(t, ghViewFixture, answer)
		run := runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
		if run.code != ExitDaemonUnavailable || run.stdout != "" || strings.Contains(run.stderr, "no open findings") || !strings.Contains(run.stderr, "gh failed") {
			t.Errorf("%s: code=%d stdout=%q stderr=%q", name, run.code, run.stdout, run.stderr)
		}
	}
	// A real PR with no threads is still an empty list, not a failure.
	fakeGH(t, ghViewFixture, ghThreadsPage(false, "", ""))
	if run := runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...); run.code != ExitOK || !strings.Contains(run.stderr, "no open findings") {
		t.Fatalf("empty PR code=%d stderr=%q", run.code, run.stderr)
	}
}
