package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var t509Now = time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

// t509Snapshot builds a synthetic session_snapshot object. Sessions default
// to one valid row so the snapshot is complete enough to judge membership.
func t509Snapshot(status string, truncated, stale bool, receivedAt time.Time, sessions string) string {
	return fmt.Sprintf(`{"sessions":%s,"snapshot_status":%q,"truncated":%t,"received_at":%q,"stale":%t}`,
		sessions, status, truncated, receivedAt.UTC().Format(time.RFC3339), stale)
}

func t509Session(paneID string) string {
	return fmt.Sprintf(`{"pane_id":%q,"workspace_id":"w1","label":"agent-x","status":"idle","revision":1,"state_change_seq":1}`, paneID)
}

// t509Node builds one /v1/nodes row. snapshot="" omits the session_snapshot
// key entirely; snapshot="null" writes a JSON null.
func t509Node(machineID, state, snapshot string) lanesAuditNodeWire {
	var raw json.RawMessage
	switch snapshot {
	case "":
		raw = nil
	case "null":
		raw = json.RawMessage(`null`)
	default:
		raw = json.RawMessage(snapshot)
	}
	return lanesAuditNodeWire{MachineID: machineID, State: state, SessionSnapshot: raw}
}

func t509FreshSnapshot(sessions string) string {
	return t509Snapshot(hubSnapshotStatusOK, false, false, t509Now.Add(-10*time.Second), sessions)
}

func t509Lane(lane, machine, pane string) hubLaneProjection {
	return hubLaneProjection{Lane: lane, Machine: machine, Pane: pane}
}

func t509Verdicts(result lanesAuditResult) map[string]lanesAuditLaneRow {
	rows := make(map[string]lanesAuditLaneRow, len(result.Lanes))
	for _, row := range result.Lanes {
		rows[row.Lane] = row
	}
	return rows
}

// TestLanesAuditThreeWayVerdicts pins the closed three-value vocabulary: a
// fresh complete snapshot decides alive/dead, and every observation gap
// stays indeterminate. Folding indeterminate into dead — or dead into
// indeterminate — must fail these assertions, not just exit oddly.
func TestLanesAuditThreeWayVerdicts(t *testing.T) {
	present := t509Session("w1:p1")
	cases := []struct {
		name        string
		lane        hubLaneProjection
		nodes       []lanesAuditNodeWire
		wantVerdict string
		wantReason  string
	}{
		{name: "pane present", lane: t509Lane("lane-alive", "machine-a", "w1:p1"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+present+"]"))},
			wantVerdict: lanesAuditVerdictAlive},
		{name: "pane absent", lane: t509Lane("lane-dead", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+present+"]"))},
			wantVerdict: lanesAuditVerdictDead},
		{name: "pane absent empty snapshot", lane: t509Lane("lane-empty", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictDead},
		{name: "node not returned", lane: t509Lane("lane-gone", "machine-z", "w1:p1"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonNodeNotReturned},
		{name: "snapshot key absent", lane: t509Lane("lane-nosnap", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", "")},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotMissing},
		{name: "snapshot null", lane: t509Lane("lane-nullsnap", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", "null")},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotMissing},
		{name: "snapshot malformed", lane: t509Lane("lane-badsnap", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", `{"sessions":`)},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotInvalid},
		{name: "collector unavailable", lane: t509Lane("lane-coll", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusUnavailable, false, false, t509Now.Add(-10*time.Second), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonCollectorDown},
		{name: "status unknown", lane: t509Lane("lane-stat", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot("bogus", false, false, t509Now.Add(-10*time.Second), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonStatusUnknown},
		{name: "sessions not a list", lane: t509Lane("lane-sessbad", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot(`{"x":1}`))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSessionsInvalid},
		{name: "sessions null", lane: t509Lane("lane-sessnull", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot(`null`))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSessionsNull},
		{name: "received_at missing", lane: t509Lane("lane-norecv", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", `{"sessions":[],"snapshot_status":"ok","truncated":false,"stale":false}`)},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonReceivedAtMissing},
		{name: "node stale", lane: t509Lane("lane-nstale", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "stale", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonNodeStale},
		{name: "node disconnected", lane: t509Lane("lane-ndisc", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "disconnected", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonNodeDisconnected},
		{name: "node presence-only", lane: t509Lane("lane-npres", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "presence-only", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonNodeNotConnected},
		{name: "snapshot stale flag", lane: t509Lane("lane-sstale", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, false, true, t509Now.Add(-10*time.Second), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotStale},
		{name: "received_at future", lane: t509Lane("lane-future", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, false, false, t509Now.Add(time.Minute), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonReceivedAtFuture},
		{name: "snapshot too old", lane: t509Lane("lane-old", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, false, false, t509Now.Add(-sessionsFindSnapshotMaxAge-time.Second), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotAgeExpired},
		{name: "truncated", lane: t509Lane("lane-trunc", "machine-a", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, true, false, t509Now.Add(-10*time.Second), "[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonTruncated},
		{name: "invalid lane projection", lane: t509Lane("lane-inv", "Machine-A", "w1:p9"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonLaneInvalid},
		{name: "nonconforming pane still judged", lane: t509Lane("lane-odd", "machine-a", "not-a-pane"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("[]"))},
			wantVerdict: lanesAuditVerdictDead},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			result := buildLanesAuditResult([]hubLaneProjection{fixture.lane}, fixture.nodes, t509Now)
			rows := t509Verdicts(result)
			row, ok := rows[fixture.lane.Lane]
			if !ok {
				t.Fatalf("lane %q missing from result", fixture.lane.Lane)
			}
			if row.Verdict != fixture.wantVerdict {
				t.Fatalf("verdict=%q want=%q row=%+v", row.Verdict, fixture.wantVerdict, row)
			}
			if row.Reason != fixture.wantReason {
				t.Fatalf("reason=%q want=%q", row.Reason, fixture.wantReason)
			}
		})
	}
}

// TestLanesAuditIndeterminateIsNotDead is the explicit mutant guard: if an
// implementation ever collapses the third verdict into "dead", every
// observation-gap case must read indeterminate — a two-way answer here is
// the failure this feature exists to prevent.
func TestLanesAuditIndeterminateIsNotDead(t *testing.T) {
	lane := t509Lane("lane-x", "machine-a", "w1:p9")
	for _, node := range []lanesAuditNodeWire{
		t509Node("machine-a", "stale", t509FreshSnapshot("[]")),
		t509Node("machine-a", "disconnected", t509FreshSnapshot("[]")),
		t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, true, false, t509Now.Add(-10*time.Second), "[]")),
		t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusUnavailable, false, false, t509Now.Add(-10*time.Second), "[]")),
		t509Node("machine-a", "connected", "null"),
	} {
		result := buildLanesAuditResult([]hubLaneProjection{lane}, []lanesAuditNodeWire{node}, t509Now)
		row := t509Verdicts(result)["lane-x"]
		if row.Verdict == lanesAuditVerdictDead {
			t.Fatalf("observation gap folded into dead: node=%+v row=%+v", node, row)
		}
		if row.Verdict != lanesAuditVerdictIndeterminate || row.Reason == "" {
			t.Fatalf("want indeterminate with reason, got %+v", row)
		}
	}
	// The reverse fold must fail too: a lane judged on a fresh complete
	// snapshot reads dead, never indeterminate.
	live := t509Lane("lane-live", "machine-a", "w1:p1")
	deadLane := t509Lane("lane-dead", "machine-a", "w1:p9")
	nodes := []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+t509Session("w1:p1")+"]"))}
	rows := t509Verdicts(buildLanesAuditResult([]hubLaneProjection{live, deadLane}, nodes, t509Now))
	if rows["lane-dead"].Verdict != lanesAuditVerdictDead {
		t.Fatalf("dead lane folded away: %+v", rows["lane-dead"])
	}
	if rows["lane-live"].Verdict != lanesAuditVerdictAlive {
		t.Fatalf("alive lane misjudged: %+v", rows["lane-live"])
	}
}

// TestLanesAuditSinkSkipped proves sink lanes are never judged: they appear
// only in the skipped count, carry no verdict row, and cannot influence the
// outcome even when their (empty) route would be unjudgeable.
func TestLanesAuditSinkSkipped(t *testing.T) {
	lanes := []hubLaneProjection{
		{Lane: "sink-a", Sink: true},
		t509Lane("lane-a", "machine-a", "w1:p1"),
	}
	nodes := []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+t509Session("w1:p1")+"]"))}
	result := buildLanesAuditResult(lanes, nodes, t509Now)
	if result.Summary.SinkSkipped != 1 || result.Summary.Lanes != 1 {
		t.Fatalf("summary=%+v", result.Summary)
	}
	if _, ok := t509Verdicts(result)["sink-a"]; ok {
		t.Fatal("sink lane produced a verdict row")
	}
	if result.Outcome != lanesAuditOutcomeOK {
		t.Fatalf("outcome=%q", result.Outcome)
	}
}

// TestLanesAuditMatchingAxis pins the join: only the node named by
// lane.machine decides, and only pane_id equality counts — the same pane id
// on another machine, or a matching label on the right machine, proves
// nothing.
func TestLanesAuditMatchingAxis(t *testing.T) {
	// Same pane id lives on machine-b; lane-a's machine has an empty snapshot.
	lane := t509Lane("lane-a", "machine-a", "w1:p1")
	nodes := []lanesAuditNodeWire{
		t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
		t509Node("machine-b", "connected", t509FreshSnapshot("["+t509Session("w1:p1")+"]")),
	}
	row := t509Verdicts(buildLanesAuditResult([]hubLaneProjection{lane}, nodes, t509Now))["lane-a"]
	if row.Verdict != lanesAuditVerdictDead {
		t.Fatalf("cross-machine pane counted as alive: %+v", row)
	}
	// Same pane id but under a different label shape still matches by
	// pane_id only; a session whose label echoes the pane string does not.
	labeled := fmt.Sprintf(`{"pane_id":"w1:p2","workspace_id":"w1","label":"w1:p1","status":"idle","revision":1,"state_change_seq":1}`)
	nodes = []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+labeled+"]"))}
	row = t509Verdicts(buildLanesAuditResult([]hubLaneProjection{lane}, nodes, t509Now))["lane-a"]
	if row.Verdict != lanesAuditVerdictDead {
		t.Fatalf("label matched as pane: %+v", row)
	}
}

func TestLanesAuditOutcomeAndSort(t *testing.T) {
	lanes := []hubLaneProjection{
		t509Lane("lane-z", "machine-a", "w1:p9"),
		t509Lane("lane-a", "machine-a", "w1:p1"),
		t509Lane("lane-m", "machine-gone", "w2:p2"),
	}
	nodes := []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("["+t509Session("w1:p1")+"]"))}
	result := buildLanesAuditResult(lanes, nodes, t509Now)
	if result.Outcome != lanesAuditOutcomePartial {
		t.Fatalf("outcome=%q want partial", result.Outcome)
	}
	if result.Lanes[0].Lane != "lane-a" || result.Lanes[1].Lane != "lane-m" || result.Lanes[2].Lane != "lane-z" {
		t.Fatalf("lanes not sorted: %+v", result.Lanes)
	}
	summary := result.Summary
	if summary.Alive != 1 || summary.Dead != 1 || summary.Indeterminate != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	// All judged, some dead → dead_lanes.
	result = buildLanesAuditResult(lanes[:2], nodes, t509Now)
	if result.Outcome != lanesAuditOutcomeDeadLanes {
		t.Fatalf("outcome=%q want dead_lanes", result.Outcome)
	}
	// All alive → ok.
	result = buildLanesAuditResult(lanes[1:2], nodes, t509Now)
	if result.Outcome != lanesAuditOutcomeOK {
		t.Fatalf("outcome=%q want ok", result.Outcome)
	}
}

// t509Deps pins the audit clock to t509Now so fixture received_at values are
// judged against the same instant the table tests use.
func t509Deps(server *httptest.Server) hubCLIDeps {
	deps := t441Deps(server)
	deps.Now = func() time.Time { return t509Now }
	return deps
}

// t509HubFixture serves the two read-only endpoints the audit consumes and
// records every request so tests can prove nothing else — and no node — was
// contacted.
func t509HubFixture(t *testing.T, recorded chan<- t441RecordedRequest, lanesBody, nodesBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if recorded != nil {
			recorded <- t441RecordedRequest{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery, headers: request.Header.Clone()}
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /v1/lanes":
			_, _ = writer.Write([]byte(lanesBody))
		case "GET /v1/nodes":
			_, _ = writer.Write([]byte(nodesBody))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestLanesAuditCLITwoReads(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	recorded := make(chan t441RecordedRequest, 8)
	snapshot := t509FreshSnapshot("[" + t509Session("w1:p1") + "]")
	nodesBody := `{"nodes":[{"machine_id":"machine-a","state":"connected","alert_class":"presence-only","accepting":false,"remote_meta":{},"session_snapshot":` + snapshot + `}]}`
	lanesBody := `{"lanes":[{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false},{"lane":"lane-b","machine":"machine-a","pane":"w1:p9","parent":"","sink":false},{"lane":"sink-a","machine":"","pane":"","parent":"","sink":true}],"control_epoch":7}`
	server := t509HubFixture(t, recorded, lanesBody, nodesBody)
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := runLanesAuditCLI([]string{"--hub-url", server.URL, "--hub-token-env", tokenEnv, "--json"}, &stdout, &stderr, t509Deps(server))
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result lanesAuditResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("output not JSON: %v %q", err, stdout.String())
	}
	if result.Outcome != lanesAuditOutcomeDeadLanes {
		t.Fatalf("outcome=%q", result.Outcome)
	}
	rows := t509Verdicts(result)
	if rows["lane-a"].Verdict != lanesAuditVerdictAlive || rows["lane-b"].Verdict != lanesAuditVerdictDead {
		t.Fatalf("rows=%+v", rows)
	}
	if result.Summary.SinkSkipped != 1 {
		t.Fatalf("summary=%+v", result.Summary)
	}
	// Exactly two reads, both GETs against the hub's own endpoints. No node
	// command, no write path, no third request of any kind.
	seen := map[string]int{}
	for i := 0; i < 2; i++ {
		got := <-recorded
		seen[got.method+" "+got.path]++
	}
	if len(seen) != 2 || seen["GET /v1/lanes"] != 1 || seen["GET /v1/nodes"] != 1 {
		t.Fatalf("requests=%v", seen)
	}
	select {
	case extra := <-recorded:
		t.Fatalf("unexpected third request: %s %s", extra.method, extra.path)
	default:
	}
}

func TestLanesAuditCLIPartialExit(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	nodesBody := `{"nodes":[{"machine_id":"machine-a","state":"stale","alert_class":"presence-only","accepting":false,"remote_meta":{},"session_snapshot":` + t509FreshSnapshot("[]") + `}]}`
	lanesBody := `{"lanes":[{"lane":"lane-a","machine":"machine-a","pane":"w1:p9","parent":"","sink":false}],"control_epoch":7}`
	server := t509HubFixture(t, nil, lanesBody, nodesBody)
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := runLanesAuditCLI([]string{"--hub-url", server.URL, "--hub-token-env", tokenEnv, "--json"}, &stdout, &stderr, t509Deps(server))
	if code != ExitPartial {
		t.Fatalf("code=%d want=%d stdout=%q", code, ExitPartial, stdout.String())
	}
	var result lanesAuditResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}
	row := t509Verdicts(result)["lane-a"]
	if row.Verdict != lanesAuditVerdictIndeterminate || row.Reason != lanesAuditReasonNodeStale {
		t.Fatalf("row=%+v", row)
	}
}

func TestLanesAuditCLIErrors(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	// Hub unreachable: the deps transport comes from a parked server while the
	// dialed URL points at a closed port.
	parked := httptest.NewServer(http.NotFoundHandler())
	defer parked.Close()
	var stdout, stderr bytes.Buffer
	code := runLanesAuditCLI([]string{"--hub-url", "http://127.0.0.1:1", "--hub-token-env", tokenEnv}, &stdout, &stderr, t509Deps(parked))
	if code == ExitOK {
		t.Fatalf("unreachable hub exited ok: %q", stdout.String())
	}
	// Lanes endpoint errors.
	server := t509HubFixture(t, nil, `{"lanes":"broken"}`, `{"nodes":[]}`)
	defer server.Close()
	stdout.Reset()
	stderr.Reset()
	code = runLanesAuditCLI([]string{"--hub-url", server.URL, "--hub-token-env", tokenEnv}, &stdout, &stderr, t509Deps(server))
	if code == ExitOK {
		t.Fatalf("broken lanes body exited ok: %q", stdout.String())
	}
	// Usage: positional, unknown flag, missing credentials.
	for _, args := range [][]string{
		{"extra", "--hub-url", "http://x", "--hub-token-env", tokenEnv},
		{"--bogus", "--hub-url", "http://x", "--hub-token-env", tokenEnv},
		{"--hub-url", "http://x"},
	} {
		if code := runLanesAuditCLI(args, &stdout, &stderr, t509Deps(server)); code != ExitUsage {
			t.Fatalf("args=%v code=%d want usage", args, code)
		}
	}
}

func TestLanesAuditTextRender(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	nodesBody := `{"nodes":[{"machine_id":"machine-a","state":"connected","alert_class":"presence-only","accepting":false,"remote_meta":{},"session_snapshot":` + t509FreshSnapshot("["+t509Session("w1:p1")+"]") + `}]}`
	lanesBody := `{"lanes":[{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false},{"lane":"lane-b","machine":"machine-a","pane":"w1:p9","parent":"","sink":false}],"control_epoch":7}`
	server := t509HubFixture(t, nil, lanesBody, nodesBody)
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := runLanesAuditCLI([]string{"--hub-url", server.URL, "--hub-token-env", tokenEnv}, &stdout, &stderr, t509Deps(server))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"outcome\tdead_lanes", "verdict=alive", "verdict=dead", "sink_skipped=0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
}
