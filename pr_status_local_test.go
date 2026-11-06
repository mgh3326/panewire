package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeLocalNode is a daemon socket that answers every request line with a fixed
// localResponse and records the requests it saw.
type fakeLocalNode struct {
	socket   string
	mu       sync.Mutex
	requests []localRequest
}

func newFakeLocalNode(t *testing.T, response localResponse) *fakeLocalNode {
	t.Helper()
	node := &fakeLocalNode{socket: filepath.Join(r20SocketRoot(t), "node.sock")}
	listener, err := net.Listen("unix", node.socket)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			scan := bufio.NewScanner(conn)
			for scan.Scan() {
				var request localRequest
				_ = json.Unmarshal(scan.Bytes(), &request)
				node.mu.Lock()
				node.requests = append(node.requests, request)
				node.mu.Unlock()
				writeLocal(conn, response)
			}
			conn.Close()
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return node
}

// lastRequest is the newest request, after checking the node saw want of them.
func (node *fakeLocalNode) lastRequest(t *testing.T, want int) localRequest {
	t.Helper()
	node.mu.Lock()
	defer node.mu.Unlock()
	if len(node.requests) != want {
		t.Fatalf("local node saw %d requests, want %d: %+v", len(node.requests), want, node.requests)
	}
	return node.requests[want-1]
}

func clearPRConnectionEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_PR_STATUS_HOST"} {
		t.Setenv(name, "")
	}
}

// Acceptance 1: with no connection, the CLI asks the local node and prints the
// node's snapshot, and gh never runs.
func TestPRStatusAsksTheLocalNodeWhenNoConnectionIsGiven(t *testing.T) {
	clearPRConnectionEnv(t)
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	result := prFixtureResult(t, prHKNotDeployed)
	node := newFakeLocalNode(t, localResponse{OK: true, Result: localPRStatusResult{Result: &result}})

	run := runPRWithSocket(t, node.socket, "status", "owner/name", "152")
	want := "owner/name#152 blocked head=1c4231f blocking=1 checks=2/2 green observed=3m ago reasons=threads_open:1\n"
	if run.code != ExitOK || run.stdout != want || strings.Contains(run.stderr, "gh") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if log := readLog(t, gh); log != "" {
		t.Fatalf("gh ran: %q", log)
	}
	request := node.lastRequest(t, 1)
	if request.Op != "pr.status" || request.PRStatus == nil || request.PRStatus.Machine != defaultPRStatusHost || request.PRStatus.Args != (nodePRStatusArgs{Repo: "owner/name", PR: 152}) || request.TimeoutMS <= 0 {
		t.Fatalf("request=%+v %+v", request, request.PRStatus)
	}

	// findings and --json come from the same snapshot, with --machine and --fresh sent on.
	run = runPRWithSocket(t, node.socket, "findings", "owner/name", "152", "--json")
	var listed struct {
		Source   string            `json:"source"`
		Findings []PRStatusFinding `json:"findings"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &listed); err != nil || listed.Source != "panewire" || len(listed.Findings) != 2 {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	runPRWithSocket(t, node.socket, "status", "owner/name", "152", "--fresh", "--machine", "machine-a")
	if sent := node.lastRequest(t, 3).PRStatus; sent.Machine != "machine-a" || !sent.Args.Fresh {
		t.Fatalf("sent=%+v", sent)
	}
}

// Acceptance 2: no local node, gh answers, and the note names the missing node.
func TestPRStatusFallsBackToGHWhenNoLocalNodeRuns(t *testing.T) {
	clearPRConnectionEnv(t)
	fakeGH(t, ghViewFixture, ghThreadsFixture)
	run := runPR(t, "status", "owner/name", "152")
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: local node is not running (no daemon socket))") ||
		!strings.Contains(run.stderr, "note: fell back to gh: local node is not running") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

// A daemon that cannot answer says why, and gh answers instead.
func TestPRStatusFallbackWhenTheLocalNodeCannotAnswer(t *testing.T) {
	clearPRConnectionEnv(t)
	fakeGH(t, ghViewFixture, ghThreadsFixture)
	garbled := prFixtureResult(t, prHKNotDeployed)
	garbled.Mergewake.Outcome = "bogus"
	for name, tc := range map[string]struct {
		response localResponse
		want     string
	}{
		"old daemon":      {localResponse{Code: ExitUsage, Error: "unknown operation"}, "local node does not support pr.status"},
		"refused":         {localResponse{Code: ExitUsage, Error: "pr.status: invalid request"}, "local node refused the pr.status request"},
		"hub says no":     {localResponse{OK: true, Result: localPRStatusResult{Why: "hub unreachable"}}, "hub unreachable"},
		"unusable result": {localResponse{OK: true, Result: localPRStatusResult{Result: &garbled}}, "local node answered an unusable pr.status result"},
	} {
		node := newFakeLocalNode(t, tc.response)
		run := runPRWithSocket(t, node.socket, "status", "owner/name", "152")
		if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: "+tc.want) {
			t.Errorf("%s: code=%d stdout=%q stderr=%q", name, run.code, run.stdout, run.stderr)
		}
	}
}

// Acceptance 3: a connection given on the command line or in the environment
// skips the local node, even when one is listening.
func TestPRStatusExplicitConnectionSkipsTheLocalNode(t *testing.T) {
	node := newFakeLocalNode(t, localResponse{Code: ExitUsage, Error: "must not be asked"})
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, prHKNotDeployed)))
	run := runPRWithSocket(t, node.socket, append([]string{"status", "owner/name", "152"}, prCLIConnection(t, stub.URL)...)...)
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "owner/name#152 blocked") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	node.mu.Lock()
	local := len(node.requests)
	node.mu.Unlock()
	stub.mu.Lock()
	hubCalls := len(stub.bodies)
	stub.mu.Unlock()
	if local != 0 || hubCalls != 1 {
		t.Fatalf("local requests=%d hub requests=%d", local, hubCalls)
	}
}

// The daemon forwards pr.status through the hub with its own node credential,
// to the machine the CLI named, and answers the hub's reply unchanged.
func TestDaemonForwardsPRStatusThroughTheHub(t *testing.T) {
	clearPRConnectionEnv(t)
	var mu sync.Mutex
	var seen []string
	reply := prNodeReply(t, prFixtureResult(t, prHKNotDeployed))
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+" "+r.Header.Get(hubMachineIDHeader)+" "+r.Header.Get("Authorization"))
		code := status
		mu.Unlock()
		w.WriteHeader(code)
		if code == http.StatusOK {
			_, _ = w.Write([]byte(reply))
		} else {
			_, _ = w.Write([]byte(`{"error":"node_unavailable"}`))
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewHubClient(HubClientConfig{URL: strings.Replace(server.URL, "http://", "ws://", 1), MachineID: "host-a", Token: "node-secret-token-9", AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}, Logger: slog.New(slog.DiscardHandler)})
	socket := inboxDaemonSocket(t, daemon)
	fakeGH(t, ghViewFixture, ghThreadsFixture)

	run := runPRWithSocket(t, socket, "status", "owner/name", "152")
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "owner/name#152 blocked head=1c4231f") || strings.Contains(run.stdout+run.stderr, "node-secret-token-9") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	mu.Lock()
	if len(seen) != 1 || seen[0] != "POST /v1/nodes/oci/command host-a Bearer node-secret-token-9" {
		t.Fatalf("hub saw %q", seen)
	}
	status = http.StatusServiceUnavailable
	mu.Unlock()

	run = runPRWithSocket(t, socket, "status", "owner/name", "152")
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: hub answered 503 node_unavailable)") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

// A daemon with no hub connection says so; it is not an op error.
func TestDaemonPRStatusWithoutHubClient(t *testing.T) {
	clearPRConnectionEnv(t)
	fakeGH(t, ghViewFixture, ghThreadsFixture)
	daemon := NewDaemon(Config{Logger: slog.New(slog.DiscardHandler)})
	run := runPRWithSocket(t, inboxDaemonSocket(t, daemon), "status", "owner/name", "152")
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: local node has no hub connection)") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
}

func TestDaemonPRStatusRejectsBadRequests(t *testing.T) {
	daemon := NewDaemon(Config{Logger: slog.New(slog.DiscardHandler)})
	for name, req := range map[string]*localPRStatusRequest{
		"nil":            nil,
		"bad repo":       {Machine: "oci", Args: nodePRStatusArgs{Repo: "../x", PR: 1}},
		"zero pr":        {Machine: "oci", Args: nodePRStatusArgs{Repo: "o/n", PR: 0}},
		"bad machine":    {Machine: "o ci", Args: nodePRStatusArgs{Repo: "o/n", PR: 1}},
		"operator as id": {Machine: hubOperatorMachineID, Args: nodePRStatusArgs{Repo: "o/n", PR: 1}},
	} {
		if _, err := daemon.handlePRStatus(context.Background(), req); ExitCode(err) != ExitUsage {
			t.Errorf("%s: err=%v", name, err)
		}
	}
}

// A node credential may run pr.status on the PR-status host and no other op:
// that is what lets the local daemon ask without any service credential.
func TestHubNodeCredentialMayOnlyRunPRStatus(t *testing.T) {
	hub, server := prStatusHub(t, "machine-a")
	connection := t14Node(t, hub, server, true)
	// machine-b's node credential, asking for machine-a.
	call := func(machine string, body any) (int, []byte) {
		return prStatusServiceCall(t, server, machine, "machine-b", "node-b-token", body)
	}
	done := make(chan struct{})
	var status int
	var contents []byte
	go func() {
		defer close(done)
		status, contents = call("machine-a", prStatusCommandBody)
	}()
	command := nodeCommandRead(t, connection)
	nodeCommandReplyTo(t, connection, command, prStatusNodeAnswer(t))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("node credential's pr.status never answered")
	}
	if status != http.StatusOK || nodeCommandDecode(t, contents)["result"] == nil {
		t.Fatalf("pr.status: status=%d body=%s", status, contents)
	}
	if status, contents := call("machine-a", map[string]any{"op": "agents.list"}); status != http.StatusUnauthorized {
		t.Fatalf("agents.list: status=%d body=%s", status, contents)
	}
	if status, contents := call("machine-a", map[string]any{"op": "job.done", "args": map[string]any{"job": "j", "report": "/r"}}); status != http.StatusUnauthorized {
		t.Fatalf("job.done: status=%d body=%s", status, contents)
	}
	if status, contents := call("machine-b", prStatusCommandBody); status != http.StatusBadRequest || nodeCommandDecode(t, contents)["error"] != "wrong_machine" {
		t.Fatalf("off-host pr.status: status=%d body=%s", status, contents)
	}
	if status, _ := prStatusServiceCall(t, server, "machine-a", "machine-b", "wrong-token", prStatusCommandBody); status != http.StatusUnauthorized {
		t.Fatalf("wrong token: status=%d", status)
	}
}
