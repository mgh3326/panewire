package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

// ownJobsLanes is the fixture's lanes file: the caller's lane on node-a, and
// the lane a job registered on node-b. The job's own lane is what a close
// retires; the caller's lane points at another pane and stays.
var ownJobsCaller = pw2Caller{"node", "node-a", pw2NodeAToken}

const ownJobsLanes = `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"},"job-b":{"machine":"node-b","pane":"w2:p1"}}}`

func ownJobsHub(t *testing.T) (*HubServer, *httptest.Server) {
	t.Helper()
	return ownJobsHubLogs(t, nil)
}

func ownJobsHubLogs(t *testing.T, logs io.Writer) (*HubServer, *httptest.Server) {
	t.Helper()
	hub, server := pw2Hub(t, nil, "", logs)
	if err := os.WriteFile(hub.reportRelayPath, []byte(ownJobsLanes), 0600); err != nil {
		t.Fatal(err)
	}
	return hub, server
}

// ownJobsResult waits for one own-jobs response. A close that retires a lane
// row rewrites the lanes file, and the first such write in a process pays the
// directory's cold-cache cost (seconds on a slow disk), so this budget is
// wider than t14Result's.
func ownJobsResult(t *testing.T, results <-chan t14HTTPResult) t14HTTPResult {
	t.Helper()
	select {
	case result := <-results:
		if result.status < 0 {
			t.Fatal(string(result.body))
		}
		return result
	case <-time.After(30 * time.Second):
		t.Fatal("HTTP response did not arrive")
		return t14HTTPResult{}
	}
}

func ownJobsLaneRows(t *testing.T, hub *HubServer) map[string]reportRelayRoute {
	t.Helper()
	return loadReportRelayRoutes(hub.reportRelayPath)
}

// TestJobsMineListsOneLaneAcrossMachines is acceptance (a): `jobs mine` returns
// only the lane's jobs, and it names each one's machine, pane, state and last
// observation.
func TestJobsMineListsOneLaneAcrossMachines(t *testing.T) {
	hub, server := ownJobsHub(t)
	seen := hub.now().UTC()
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "lane-a", Pane: "w1:p1"}, Node: "node-a", LastSeen: seen}
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b", LastSeen: seen.Add(-time.Minute), Completed: true}
	hub.jobs["job-c"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-c", OwnerLane: "lane-b", Pane: "w1:p2"}, Node: "node-a", LastSeen: seen}

	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-a", "")
	var result struct {
		Jobs []hubOwnJob `json:"jobs"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if len(result.Jobs) != 2 || result.Jobs[0].JobID != "job-a" || result.Jobs[1].JobID != "job-b" {
		t.Fatalf("jobs=%+v", result.Jobs)
	}
	if result.Jobs[0].Machine != "node-a" || result.Jobs[0].Pane != "w1:p1" || result.Jobs[0].State != "active" || result.Jobs[0].Task != "" {
		t.Fatalf("job-a row=%+v", result.Jobs[0])
	}
	if result.Jobs[1].Machine != "node-b" || result.Jobs[1].State != "completed" || result.Jobs[1].LastSeen != seen.Add(-time.Minute).Format(time.RFC3339) {
		t.Fatalf("job-b row=%+v", result.Jobs[1])
	}

	// The operator may name any lane; a service is not a lane.
	if status, _ := (pw2Caller{"operator", "", pw2OperatorToken}).do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-b", ""); status != http.StatusOK {
		t.Fatalf("operator lane-b: %d", status)
	}
	_, svcServer := pw2Hub(t, map[string]string{"svc-jobs": "inbox.read:lane-a"}, "", nil)
	if status, body := (pw2Caller{"svc-jobs", "svc-jobs", pw2ServiceToken("svc-jobs")}).do(t, svcServer, http.MethodGet, "/v1/jobs/mine?lane=lane-a", ""); status != http.StatusForbidden || !strings.Contains(body, "scope_forbidden") {
		t.Fatalf("service: %d %s", status, body)
	}
}

func TestJobsMineRetainsHistoricalDottedLane(t *testing.T) {
	hub, server := ownJobsHub(t)
	if err := os.WriteFile(hub.reportRelayPath, []byte(`{"lanes":{"legacy.parent":{"machine":"node-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub.jobs["job-legacy"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-legacy", OwnerLane: "legacy.parent", Pane: "w1:p1"}, Node: "node-a"}
	status, body := ownJobsCaller.do(t, server, http.MethodGet, "/v1/jobs/mine?lane=legacy.parent", "")
	if status != http.StatusOK || !strings.Contains(body, `"job_id":"job-legacy"`) {
		t.Fatalf("historical own-jobs query status=%d body=%s", status, body)
	}
}

// TestJobsMineNamesTheTaskFromTheSpawnOperation: the task column is the task
// the hub's own spawn operation named, and a job this hub did not spawn through
// its store lists none.
func TestJobsMineNamesTheTaskFromTheSpawnOperation(t *testing.T) {
	hub, server := ownJobsHub(t)
	inbox, err := newHubInbox("default", filepath.Join(t.TempDir(), "inbox.sqlite3"), "")
	if err != nil {
		t.Fatal(err)
	}
	hub.inbox = inbox
	t.Cleanup(func() { inbox.store.close() })
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "lane-a"}, Node: "node-a"}
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a"}, Node: "node-a"}
	submission := fleet.Submission{
		RequestID:       t14RequestID(3968),
		Args:            []string{"-c", "/work/repo", "-p", "/work/brief.md", "-m", "auto", "-w", "worker", "-l", "label-a", "--job", "job-b", "--t", "T1", "--owner", "lane-a", "--task", "3968", "--host", "auto"},
		Brief:           "task",
		Repository:      fleet.Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/repo"},
		OriginMachine:   "node-a",
		DeadlineSeconds: 600,
	}
	if _, _, err := inbox.store.claimSpawn(t.Context(), submission, "job-b", "lane-a", hub.now()); err != nil {
		t.Fatal(err)
	}
	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-a", "")
	var result struct {
		Jobs []hubOwnJob `json:"jobs"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil || len(result.Jobs) != 2 {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if result.Jobs[0].JobID != "job-a" || result.Jobs[0].Task != "" || result.Jobs[1].JobID != "job-b" || result.Jobs[1].Task != "3968" {
		t.Fatalf("jobs=%+v", result.Jobs)
	}
}

// TestJobsMineRefusesALaneOnAnotherMachine is the inbox rule: a node credential
// sees only the lanes routed to its own machine.
func TestJobsMineRefusesALaneOnAnotherMachine(t *testing.T) {
	_, server := ownJobsHub(t)
	remote := pw2Caller{"node-b", "node-b", pw2NodeBToken}
	if status, body := remote.do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-a", ""); status != http.StatusForbidden || !strings.Contains(body, "lane_not_routed_to_caller") {
		t.Fatalf("remote lane: %d %s", status, body)
	}
	if status, _ := (pw2Caller{"anonymous", "", ""}).do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-a", ""); status != http.StatusUnauthorized {
		t.Fatalf("anonymous: %d", status)
	}
	if status, _ := remote.do(t, server, http.MethodGet, "/v1/jobs/mine?lane=bad%2Fname", ""); status != http.StatusBadRequest {
		t.Fatalf("invalid lane: %d", status)
	}
}

// TestJobsCloseClosesOnAnotherMachineAndRetiresTheLane is acceptance (b): the
// job's own machine closes the pane, and the lane route that pointed at it is
// retired while the caller's own lane stays.
func TestJobsCloseClosesOnAnotherMachineAndRetiresTheLane(t *testing.T) {
	var logs pw2LockedBuffer
	hub, server := ownJobsHubLogs(t, &logs)
	connection := t14NamedNodeToken(t, hub, server, "node-b", pw2NodeBToken, false, true)
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}

	results := ownJobsCaller.doAsync(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`)
	command := nodeCommandRead(t, connection)
	if string(command["op"]) != `"job.reap"` || string(command["args"]) != `{"job":"job-b","lane":"lane-a","force":false}` {
		t.Fatalf("node received %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "closed", "pane": "w2:p1"}})
	result := ownJobsResult(t, results)
	body := nodeCommandDecode(t, result.body)
	if result.status != http.StatusOK || body["outcome"] != "closed" || body["machine"] != "node-b" || body["pane"] != "w2:p1" || body["job"] != "job-b" || body["lane"] != "lane-a" {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if released, _ := body["released"].([]any); len(released) != 1 || released[0] != "job-b" {
		t.Fatalf("released=%v", body["released"])
	}
	routes := ownJobsLaneRows(t, hub)
	if _, kept := routes["lane-a"]; !kept {
		t.Fatalf("the caller's own lane was retired: %v", routes)
	}
	if _, gone := routes["job-b"]; gone {
		t.Fatalf("the job's lane route survived: %v", routes)
	}
	// The hub records who closed what.
	for _, want := range []string{"job reaped", "lane=lane-a", "job=job-b", "machine=node-b", "outcome=closed"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("hub log %q missing %q", logs.String(), want)
		}
	}
}

// TestJobsCloseRefusesAnotherLanesJob is acceptance (b): a job owned by another
// lane is refused at the hub, and nothing reaches the node.
func TestJobsCloseRefusesAnotherLanesJob(t *testing.T) {
	hub, server := ownJobsHub(t)
	connection := t14NamedNodeToken(t, hub, server, "node-b", pw2NodeBToken, false, true)
	hub.jobs["job-c"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-c", OwnerLane: "lane-b", Pane: "w2:p2"}, Node: "node-b"}
	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-c","force":false}`)
	if status != http.StatusForbidden || !strings.Contains(body, `"refusal":"owner_mismatch"`) {
		t.Fatalf("status=%d body=%s", status, body)
	}
	// Nothing reached the node: its first command is the one sent below.
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}
	results := ownJobsCaller.doAsync(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":true}`)
	command := nodeCommandRead(t, connection)
	if string(command["args"]) != `{"job":"job-b","lane":"lane-a","force":true}` {
		t.Fatalf("first command the node saw: %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "closed", "pane": "w2:p1"}})
	if result := ownJobsResult(t, results); result.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
}

// TestJobsCloseRefusesANodeThatDoesNotOwnTheLane is acceptance (b): a node
// credential may not act for a lane routed to another machine.
func TestJobsCloseRefusesANodeThatDoesNotOwnTheLane(t *testing.T) {
	hub, server := ownJobsHub(t)
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}
	if status, body := (pw2Caller{"node-b", "node-b", pw2NodeBToken}).do(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`); status != http.StatusForbidden || !strings.Contains(body, "lane_not_routed_to_caller") {
		t.Fatalf("status=%d body=%s", status, body)
	}
	status, body := (pw2Caller{"operator", "", pw2OperatorToken}).do(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-missing","force":false}`)
	if status != http.StatusNotFound || !strings.Contains(body, `"refusal":"job_unknown"`) {
		t.Fatalf("status=%d body=%s", status, body)
	}
}

// TestJobsCloseReportsTheNodeRefusal names the reason a working pane is kept,
// and a refused close retires nothing.
func TestJobsCloseReportsTheNodeRefusal(t *testing.T) {
	hub, server := ownJobsHub(t)
	connection := t14NamedNodeToken(t, hub, server, "node-b", pw2NodeBToken, false, true)
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}
	results := ownJobsCaller.doAsync(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`)
	command := nodeCommandRead(t, connection)
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "job-working", "pane": "w2:p1"}})
	result := ownJobsResult(t, results)
	if result.status != http.StatusConflict || !strings.Contains(string(result.body), `"refusal":"job-working"`) || !strings.Contains(string(result.body), `"pane":"w2:p1"`) {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if _, still := ownJobsLaneRows(t, hub)["job-b"]; !still {
		t.Fatal("a refused close retired the lane route")
	}
}

// TestJobsOwnCLIDaemonParity is the CLI contract on both routes: the local
// node daemon (no token file) and a direct node credential print the same
// lines and exit the same status.
func TestJobsOwnCLIDaemonParity(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	const token = "own-jobs-fixture-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get(hubMachineIDHeader) != "node-a" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/v1/jobs/mine" && r.URL.Query().Get("lane") == "lane-a":
			_, _ = w.Write([]byte(`{"jobs":[{"job_id":"job-a","machine":"node-a","pane":"w1:p1","state":"active","task":"3968","last_seen":"2026-11-11T00:00:00Z"}]}`))
		case r.URL.Path == "/v1/jobs/mine":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"lane_not_routed_to_caller"}`))
		case r.URL.Path == "/v1/jobs/reap":
			var body hubJobReapRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Job == "job-working" {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"refusal":"job-working","outcome":"job-working","machine":"node-b","pane":"w2:p1"}`))
				return
			}
			_, _ = w.Write([]byte(`{"lane":"lane-a","job":"` + body.Job + `","machine":"node-b","pane":"w2:p1","outcome":"closed","released":["job-b"]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "fixture.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := NewHubClient(HubClientConfig{URL: strings.Replace(server.URL, "http://", "ws://", 1), MachineID: "node-a", Token: token, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}})
	deps := hubCLIDeps{AllowInsecureForTests: true, SocketPath: inboxDaemonSocket(t, daemon)}
	cases := []struct {
		args   []string
		code   int
		stdout string
		stderr string
	}{
		{[]string{"mine", "--lane", "lane-a"}, ExitOK, "job-a\tnode-a\tw1:p1\tactive\t3968\t2026-11-11T00:00:00Z\n", ""},
		{[]string{"mine", "--lane", "lane-a", "--json"}, ExitOK, `"job_id":"job-a"`, ""},
		{[]string{"close", "--lane", "lane-a", "--job", "job-b"}, ExitOK, "released=job-b", ""},
		{[]string{"close", "--lane", "lane-a", "--job", "job-b", "--force"}, ExitOK, "outcome=closed", ""},
		{[]string{"close", "--lane", "lane-a", "--job", "job-b", "--json"}, ExitOK, `"outcome":"closed"`, ""},
		{[]string{"close", "--lane", "lane-a", "--job", "job-working"}, jobCloseRefused, "", "use --force"},
		{[]string{"mine", "--lane", "lane-b"}, jobCloseRefused, "", "lane"},
	}
	for _, test := range cases {
		var directOut, directErr, localOut, localErr bytes.Buffer
		directArgs := append(append([]string(nil), test.args...), "--hub-url", server.URL, "--hub-token-env", tokenPath)
		directDeps := deps
		directDeps.SocketPath = filepath.Join(t.TempDir(), "absent.sock")
		directCode := runJobsCLI(directArgs, &directOut, &directErr, directDeps)
		localCode := runJobsCLI(test.args, &localOut, &localErr, deps)
		if directCode != localCode || directOut.String() != localOut.String() || directErr.String() != localErr.String() {
			t.Errorf("%v: direct %d %q %q; daemon %d %q %q", test.args, directCode, directOut.String(), directErr.String(), localCode, localOut.String(), localErr.String())
			continue
		}
		if localCode != test.code || !strings.Contains(localOut.String(), test.stdout) || !strings.Contains(localErr.String(), test.stderr) {
			t.Errorf("%v: %d out=%q err=%q", test.args, localCode, localOut.String(), localErr.String())
		}
	}
}

func TestJobsOwnDaemonUnavailable(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	deps := hubCLIDeps{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}
	for _, args := range [][]string{{"mine", "--lane", "lane-a"}, {"close", "--lane", "lane-a", "--job", "job-a"}} {
		var out, errOut bytes.Buffer
		if code := runJobsCLI(args, &out, &errOut, deps); code != ExitDaemonUnavailable || out.Len() != 0 || strings.Count(errOut.String(), "\n") != 1 {
			t.Errorf("%v: %d %q %q", args, code, out.String(), errOut.String())
		}
	}
}

// TestJobsOperatorCommandsUnchanged is acceptance (d): the operator subcommands
// keep requiring the operator credential and never fall through to the local
// daemon, which would answer as a node.
func TestJobsOperatorCommandsUnchanged(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	deps := hubCLIDeps{SocketPath: inboxDaemonSocket(t, NewDaemon(Config{}))}
	for _, args := range [][]string{{"list", "--lane", "lane-a"}, {"jobs"}, {"orphaned"}, {"reassign", "--job-id", "job-a", "--to", "node-b"}} {
		var out, errOut bytes.Buffer
		if code := runJobsCLI(args, &out, &errOut, deps); code != ExitUsage || out.Len() != 0 {
			t.Errorf("%v: %d %q %q", args, code, out.String(), errOut.String())
		}
	}
	// The subcommand list is still the closed set the operator has.
	var out, errOut bytes.Buffer
	if code := runJobsCLI([]string{"mnie", "--lane", "lane-a"}, &out, &errOut, deps); code != ExitUsage || !strings.Contains(errOut.String(), "expected list, jobs, orphaned, reassign, mine, or close") {
		t.Errorf("unknown subcommand: %d %q", code, errOut.String())
	}
	// Each command owns its own options.
	out.Reset()
	errOut.Reset()
	if code := runJobsCLI([]string{"mine", "--lane", "lane-a", "--force"}, &out, &errOut, deps); code != ExitUsage || !strings.Contains(errOut.String(), "--force belongs to jobs close") {
		t.Errorf("mine --force: %d %q", code, errOut.String())
	}
}

// TestOwnJobsRequestIsClosed keeps the daemon socket contract strict: only the
// two commands, with the fields each one owns.
func TestOwnJobsRequestIsClosed(t *testing.T) {
	for _, test := range []struct {
		req   ownJobsRequest
		valid bool
	}{
		{ownJobsRequest{Command: "mine", Lane: "lane-a"}, true},
		{ownJobsRequest{Command: "mine", Lane: "legacy.parent"}, true},
		{ownJobsRequest{Command: "mine", Lane: "lane-a", JSON: true}, true},
		{ownJobsRequest{Command: "mine", Lane: "lane-a", Job: "job-a"}, false},
		{ownJobsRequest{Command: "mine", Lane: "lane-a", Force: true}, false},
		{ownJobsRequest{Command: "mine"}, false},
		{ownJobsRequest{Command: "close", Lane: "lane-a", Job: "job-a"}, true},
		{ownJobsRequest{Command: "close", Lane: "legacy.parent", Job: "job-a"}, true},
		{ownJobsRequest{Command: "close", Lane: "lane-a", Job: "job-a", Force: true}, true},
		{ownJobsRequest{Command: "close", Lane: "lane-a"}, false},
		{ownJobsRequest{Command: "close", Lane: "bad/name", Job: "job-a"}, false},
		{ownJobsRequest{Command: "list", Lane: "lane-a"}, false},
	} {
		if got := validOwnJobsRequest(test.req); got != test.valid {
			t.Errorf("%+v: %t", test.req, got)
		}
	}
	var out, errOut bytes.Buffer
	socket := inboxDaemonSocket(t, NewDaemon(Config{Hub: HubDaemonConfig{Client: &HubClient{}}}))
	if code := runOwnJobsLocal(ownJobsRequest{Command: "close", Lane: "lane-a"}, &out, &errOut, socket); code != ExitUsage {
		t.Fatalf("invalid daemon request: %d %q", code, errOut.String())
	}
}

// TestJobsCloseReleasesOnlyThePaneTheNodeVerified pins the release to the pane
// the node named. A job whose ledger names no pane proves nothing about the
// hub record's pane — that id may have been reused by a live unrelated pane
// since — so no lane row is retired for it.
func TestJobsCloseReleasesOnlyThePaneTheNodeVerified(t *testing.T) {
	cases := []struct {
		name     string
		reply    map[string]any
		released bool
	}{
		{name: "a verified pane is retired", reply: map[string]any{"outcome": "pane-gone", "pane": "w2:p1"}, released: true},
		{name: "a ledger with no pane retires nothing", reply: map[string]any{"outcome": "pane-gone"}, released: false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			hub, server := ownJobsHub(t)
			connection := t14NamedNodeToken(t, hub, server, "node-b", pw2NodeBToken, false, true)
			hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}
			results := ownJobsCaller.doAsync(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`)
			nodeCommandReplyTo(t, connection, nodeCommandRead(t, connection), map[string]any{"result": test.reply})
			result := ownJobsResult(t, results)
			body := nodeCommandDecode(t, result.body)
			if result.status != http.StatusOK || body["outcome"] != "pane-gone" {
				t.Fatalf("status=%d body=%s", result.status, result.body)
			}
			_, present := ownJobsLaneRows(t, hub)["job-b"]
			if wantPresent := !test.released; present != wantPresent {
				t.Fatalf("lane row present=%t, want %t: body=%s", present, wantPresent, result.body)
			}
			if released, _ := body["released"].([]any); test.released != (len(released) == 1 && released[0] == "job-b") {
				t.Fatalf("released=%v", body["released"])
			}
		})
	}
}

// TestJobsCloseReportsAnUnconfirmedSend: a failed write to the node may still
// have delivered job.reap, so the hub answers close_unconfirmed, never
// node_offline, and the lane is told the outcome is unknown.
func TestJobsCloseReportsAnUnconfirmedSend(t *testing.T) {
	hub, server := ownJobsHub(t)
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b"}
	// A connection the hub still believes in, whose peer is gone: the write
	// fails the way a half-open socket's does.
	hub.mu.Lock()
	hub.nodes["node-b"] = &hubNodeRecord{machineID: "node-b", state: "connected", stateSince: hub.now(), agent: &hubAgent{conn: ownJobsDeadConn(t), machineID: "node-b"}}
	hub.mu.Unlock()
	status, body := ownJobsCaller.do(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`)
	if status != http.StatusServiceUnavailable || !strings.Contains(body, `"refusal":"close_unconfirmed"`) {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if _, still := ownJobsLaneRows(t, hub)["job-b"]; !still {
		t.Fatal("an unconfirmed send retired the lane route")
	}
}

// ownJobsDeadConn is a websocket whose peer has closed, so a write to it fails.
func ownJobsDeadConn(t *testing.T) *websocket.Conn {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, err := websocket.Accept(w, r, nil); err == nil {
			_ = conn.CloseNow()
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/"), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
	return conn
}
