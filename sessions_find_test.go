package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Synthetic fixture clock: received_at values are placed relative to this
// instant so freshness classification is deterministic.
var t443Now = time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)

const (
	t443Fresh  = "2026-09-18T23:59:00Z" // 60s old
	t443Aged   = "2026-09-18T23:57:00Z" // 180s old, past the 120s max age
	t443Future = "2026-09-19T00:01:00Z" // clock-skewed ahead of now
)

func t443Deps(server *httptest.Server) hubCLIDeps {
	deps := t441Deps(server)
	deps.Now = func() time.Time { return t443Now }
	return deps
}

// t443Session builds one synthetic named-agent HubSession wire object: the
// agent name doubles as the label with label_source=agent_name. Fixture
// identity is fully synthetic: pane/machine/name strings never mirror a real
// session.
func t443Session(pane, workspace, agentName, status string) string {
	return t443SessionProvenance(pane, workspace, agentName, agentName, "agent_name", "", status)
}

// t443SessionProvenance builds a session carrying the additive provenance
// fields explicitly so every mixed wire shape stays expressible. Empty
// agentName/labelSource/displayLabel omit the key entirely.
func t443SessionProvenance(pane, workspace, agentName, label, labelSource, displayLabel, status string) string {
	object := fmt.Sprintf(`{"pane_id":%q,"workspace_id":%q,"label":%q,"status":%q,"revision":1,"state_change_seq":1`, pane, workspace, label, status)
	if agentName != "" {
		object += fmt.Sprintf(`,"agent_name":%q`, agentName)
	}
	if labelSource != "" {
		object += fmt.Sprintf(`,"label_source":%q`, labelSource)
	}
	if displayLabel != "" {
		object += fmt.Sprintf(`,"display_label":%q`, displayLabel)
	}
	return object + "}"
}

// t443SessionLegacy builds a pre-additive payload row: no agent_name, no
// label_source, no display_label. It must still decode and stay unmatchable.
func t443SessionLegacy(pane, workspace, label, status string) string {
	return fmt.Sprintf(`{"pane_id":%q,"workspace_id":%q,"label":%q,"status":%q,"revision":1,"state_change_seq":1}`, pane, workspace, label, status)
}

// t443Snapshot builds a synthetic session_snapshot object. Passing
// sessions="OMIT" drops the key, sessions="null" writes a JSON null, and any
// other value is embedded verbatim so malformed shapes stay expressible.
func t443Snapshot(status, sessions, receivedAt string, truncated, stale bool) string {
	var fields []string
	if sessions != "OMIT" {
		fields = append(fields, `"sessions":`+sessions)
	}
	if status != "OMIT" {
		fields = append(fields, fmt.Sprintf(`"snapshot_status":%q`, status))
	}
	fields = append(fields, fmt.Sprintf(`"truncated":%t`, truncated))
	if receivedAt != "OMIT" {
		fields = append(fields, fmt.Sprintf(`"received_at":%q`, receivedAt))
	}
	fields = append(fields, fmt.Sprintf(`"stale":%t`, stale))
	return "{" + strings.Join(fields, ",") + "}"
}

// t443Node builds one synthetic /v1/nodes row. snapshot="OMIT" drops the
// session_snapshot key entirely; snapshot="null" writes a JSON null.
func t443Node(machineID, state, snapshot string) string {
	if snapshot == "OMIT" {
		return fmt.Sprintf(`{"machine_id":%q,"state":%q,"alert_class":"presence-only","accepting":false,"remote_meta":{}}`, machineID, state)
	}
	return fmt.Sprintf(`{"machine_id":%q,"state":%q,"alert_class":"presence-only","accepting":false,"remote_meta":{},"session_snapshot":%s}`, machineID, state, snapshot)
}

func t443NodesBody(nodes ...string) string {
	return `{"nodes":[` + strings.Join(nodes, ",") + `]}`
}

type t443Request struct {
	method  string
	path    string
	headers http.Header
}

// t443Server serves one canned /v1/nodes body and records every request so
// tests can assert the exactly-one-GET boundary.
func t443Server(t *testing.T, status int, body string, recorded chan<- t443Request) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if recorded != nil {
			recorded <- t443Request{method: request.Method, path: request.URL.Path, headers: request.Header.Clone()}
		}
		writer.Header().Set("Content-Type", "application/json")
		if status != http.StatusOK {
			writer.WriteHeader(status)
		}
		_, _ = writer.Write([]byte(body))
	}))
}

func t443Run(t *testing.T, server *httptest.Server, args ...string) (int, string, string) {
	t.Helper()
	tokenEnv, cfEnv := t441Envs(t)
	var stdout, stderr bytes.Buffer
	full := append([]string{"find"}, args...)
	full = append(full, "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv)
	code := runSessionsCLI(full, &stdout, &stderr, t443Deps(server))
	return code, stdout.String(), stderr.String()
}

func t443Result(t *testing.T, stdout string) sessionsFindResult {
	t.Helper()
	var result sessionsFindResult
	if json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result) != nil {
		t.Fatalf("stdout is not the JSON envelope: %q", stdout)
	}
	return result
}

func t443FreshSnapshot(sessions string) string {
	return t443Snapshot("ok", sessions, t443Fresh, false, false)
}

// TestSessionsFindDispatchUsage proves the new dispatch entry exists and that
// every malformed invocation fails at a stable nonzero usage boundary before
// any hub request is possible.
func TestSessionsFindDispatchUsage(t *testing.T) {
	if code := RunCLI([]string{"sessions"}, CLIConfig{}); code != ExitUsage {
		t.Fatalf("dispatch for bare sessions returned %d", code)
	}
	server := t443Server(t, http.StatusOK, t443NodesBody(), nil)
	defer server.Close()
	tokenEnv, cfEnv := t441Envs(t)
	cases := []struct {
		name string
		args []string
	}{
		{"empty", nil},
		{"unknown subcommand", []string{"list"}},
		{"find without label", []string{"find", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
		{"empty label", []string{"find", "", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
		{"extra positional", []string{"find", "label-a", "extra", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
		{"missing hub url", []string{"find", "label-a", "--hub-token-env", tokenEnv}},
		{"missing token env", []string{"find", "label-a", "--hub-url", server.URL}},
		{"duplicate machine flag", []string{"find", "label-a", "--machine", "node-a", "--machine", "node-b", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
		{"invalid machine value", []string{"find", "label-a", "--machine", "Not_A_Machine!", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
		{"unknown flag", []string{"find", "label-a", "--watch", "--hub-url", server.URL, "--hub-token-env", tokenEnv}},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := fixture.args
			if len(args) > 0 && args[0] == "find" && cfEnv != "" {
				args = append(args, "--hub-cf-env", cfEnv)
			}
			code := runSessionsCLI(args, &stdout, &stderr, t443Deps(server))
			if code == ExitOK {
				t.Fatalf("%s succeeded: stdout=%q", fixture.name, stdout.String())
			}
		})
	}
}

// TestSessionsFindExactDefaultAndContains proves label matching is exact by
// default: a label that only substring-matches must not appear until
// --contains opts in, and every duplicate label is preserved in a
// deterministic order.
func TestSessionsFindExactDefaultAndContains(t *testing.T) {
	body := t443NodesBody(
		t443Node("node-a", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-a", "label-hit", "working")+"]")),
		t443Node("node-b", "connected", t443FreshSnapshot("["+t443Session("w1:p2", "ws-b", "label-hit-extra", "idle")+"]")),
		t443Node("node-c", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-c", "label-hit", "done")+"]")),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()

	code, stdout, stderr := t443Run(t, server, "label-hit", "--json")
	if code != ExitOK {
		t.Fatalf("exact find failed: code=%d stderr=%q", code, stderr)
	}
	result := t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomeFound || len(result.Matches) != 2 {
		t.Fatalf("exact find outcome=%s matches=%d want FOUND/2", result.Outcome, len(result.Matches))
	}
	if result.Matches[0].Machine != "node-a" || result.Matches[1].Machine != "node-c" {
		t.Fatalf("exact find kept a substring-only label or lost a duplicate: %+v", result.Matches)
	}
	if result.Matches[0].AgentName != "label-hit" || result.Matches[0].LabelSource != "agent_name" {
		t.Fatalf("match lost canonical identity/provenance: %+v", result.Matches[0])
	}
	if result.Query.Match != "exact" {
		t.Fatalf("query.match=%q want exact", result.Query.Match)
	}
	t441AssertNoCredentialLeak(t, stdout, stderr, "")

	code, stdout, stderr = t443Run(t, server, "label-hit", "--contains", "--json")
	if code != ExitOK {
		t.Fatalf("contains find failed: code=%d stderr=%q", code, stderr)
	}
	result = t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomeFound || len(result.Matches) != 3 || result.Query.Match != "contains" {
		t.Fatalf("contains find outcome=%s matches=%d match=%q", result.Outcome, len(result.Matches), result.Query.Match)
	}
	t441AssertNoCredentialLeak(t, stdout, stderr, "")
}

// TestSessionsFindMachineFilter narrows the required scope to one returned
// node and fails closed when the hub response does not contain it.
func TestSessionsFindMachineFilter(t *testing.T) {
	body := t443NodesBody(
		t443Node("node-a", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-a", "label-hit", "working")+"]")),
		t443Node("node-b", "connected", "null"),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()

	code, stdout, stderr := t443Run(t, server, "label-hit", "--machine", "node-a", "--json")
	if code != ExitOK {
		t.Fatalf("machine-scoped find failed: code=%d stderr=%q", code, stderr)
	}
	result := t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomeFound || result.Coverage.Expected != 1 || result.Coverage.Observed != 1 {
		t.Fatalf("machine scope outcome=%s coverage=%+v", result.Outcome, result.Coverage)
	}
	if result.Scope.Machine != "node-a" || result.Scope.CoverageScope != sessionsFindCoverageScope {
		t.Fatalf("scope=%+v", result.Scope)
	}

	code, stdout, stderr = t443Run(t, server, "label-hit", "--machine", "node-missing", "--json")
	if code == ExitOK {
		t.Fatalf("unknown machine succeeded: stdout=%q", stdout)
	}
	result = t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomeError {
		t.Fatalf("unknown machine outcome=%s want ERROR", result.Outcome)
	}
	t441AssertNoCredentialLeak(t, stdout, stderr, "")
}

// TestSessionsFindJSONEnvelope pins the exact top-level key set, the required
// coverage counters, and the explicit hub-returned-nodes scope limitation.
func TestSessionsFindJSONEnvelope(t *testing.T) {
	server := t443Server(t, http.StatusOK, t443NodesBody(t443Node("node-a", "connected", t443FreshSnapshot("[]"))), nil)
	defer server.Close()
	code, stdout, _ := t443Run(t, server, "label-x", "--json")
	if code != ExitOK {
		t.Fatalf("json find failed: code=%d", code)
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(strings.TrimSpace(stdout)), &envelope) != nil {
		t.Fatalf("stdout is not JSON: %q", stdout)
	}
	want := []string{"query", "scope", "fetched_at", "matches", "coverage", "outcome"}
	if len(envelope) != len(want) {
		t.Fatalf("envelope keys=%v want exactly %v", envelope, want)
	}
	for _, key := range want {
		if _, exists := envelope[key]; !exists {
			t.Fatalf("envelope missing key %q: %s", key, stdout)
		}
	}
	var coverage map[string]json.RawMessage
	if json.Unmarshal(envelope["coverage"], &coverage) != nil {
		t.Fatalf("coverage is not an object: %s", envelope["coverage"])
	}
	for _, key := range []string{"expected", "observed", "missing", "stale", "truncated", "invalid"} {
		if _, exists := coverage[key]; !exists {
			t.Fatalf("coverage missing required key %q", key)
		}
	}
	var scope struct {
		CoverageScope string `json:"coverage_scope"`
	}
	if json.Unmarshal(envelope["scope"], &scope) != nil || scope.CoverageScope != sessionsFindCoverageScope {
		t.Fatalf("scope does not name hub_returned_nodes: %s", envelope["scope"])
	}
	if strings.Contains(stdout, "cwd") || strings.Contains(stdout, "client_kind") {
		t.Fatalf("envelope carried a banned field: %s", stdout)
	}
}

// TestSessionsFindCoverageStates locks the closed six-state vocabulary and the
// precedence between conditions: absent/null first, then collector and shape
// contract errors, then staleness, then truncation, then a healthy empty list.
func TestSessionsFindCoverageStates(t *testing.T) {
	okEmpty := t443Snapshot("ok", "[]", t443Fresh, false, false)
	okOne := t443Snapshot("ok", "["+t443Session("w1:p1", "ws-a", "label-a", "idle")+"]", t443Fresh, false, false)
	cases := []struct {
		name       string
		node       sessionsFindNodeWire
		wantState  string
		wantReason string
		covered    bool
	}{
		{"absent", sessionsFindNodeWire{MachineID: "node-a", State: "connected"}, sessionsFindStateUnobserved, "unknown", false},
		{"null", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage("null")}, sessionsFindStateUnobserved, "unknown", false},
		{"absent on stale node stays unobserved", sessionsFindNodeWire{MachineID: "node-a", State: "stale"}, sessionsFindStateUnobserved, "unknown", false},
		{"unavailable", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("unavailable", "null", t443Fresh, false, false))}, sessionsFindStateCollectorUnavailable, "unavailable", false},
		{"unknown status", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("weird", "[]", t443Fresh, false, false))}, sessionsFindStateInvalidSnapshot, "snapshot_status_unknown", false},
		{"ok sessions null", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "null", t443Fresh, false, false))}, sessionsFindStateInvalidSnapshot, "sessions_null", false},
		{"ok sessions missing", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "OMIT", t443Fresh, false, false))}, sessionsFindStateInvalidSnapshot, "sessions_null", false},
		{"ok sessions wrong type", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", `"sessions"`, t443Fresh, false, false))}, sessionsFindStateInvalidSnapshot, "sessions_invalid", false},
		{"ok sessions non-object item", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[1]", t443Fresh, false, false))}, sessionsFindStateInvalidSnapshot, "sessions_invalid", false},
		{"snapshot not an object", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(`"snapshot"`)}, sessionsFindStateInvalidSnapshot, "snapshot_invalid", false},
		{"received_at missing", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", "OMIT", false, false))}, sessionsFindStateInvalidSnapshot, "received_at_missing", false},
		{"node stale", sessionsFindNodeWire{MachineID: "node-a", State: "stale", SessionSnapshot: json.RawMessage(okEmpty)}, sessionsFindStateStale, "node_stale", false},
		{"node disconnected", sessionsFindNodeWire{MachineID: "node-a", State: "disconnected", SessionSnapshot: json.RawMessage(okEmpty)}, sessionsFindStateStale, "node_disconnected", false},
		{"node state unknown", sessionsFindNodeWire{MachineID: "node-a", State: "bogus", SessionSnapshot: json.RawMessage(okEmpty)}, sessionsFindStateStale, "node_not_connected", false},
		{"snapshot stale flag", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", t443Fresh, false, true))}, sessionsFindStateStale, "snapshot_stale", false},
		{"age exceeded", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", t443Aged, false, false))}, sessionsFindStateStale, "snapshot_age_exceeded", false},
		{"future timestamp never fresh", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", t443Future, false, false))}, sessionsFindStateStale, "received_at_future", false},
		{"truncated", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", t443Fresh, true, false))}, sessionsFindStatePartial, "truncated", false},
		{"stale beats truncated", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "[]", t443Aged, true, false))}, sessionsFindStateStale, "snapshot_age_exceeded", false},
		{"invalid beats stale", sessionsFindNodeWire{MachineID: "node-a", State: "stale", SessionSnapshot: json.RawMessage(t443Snapshot("ok", "null", t443Aged, false, false))}, sessionsFindStateInvalidSnapshot, "sessions_null", false},
		{"observed empty", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(okEmpty)}, sessionsFindStateObservedEmpty, "", true},
		{"observed with sessions", sessionsFindNodeWire{MachineID: "node-a", State: "connected", SessionSnapshot: json.RawMessage(okOne)}, "", "", true},
	}
	vocabulary := map[string]bool{
		sessionsFindStateUnobserved: true, sessionsFindStateCollectorUnavailable: true,
		sessionsFindStateObservedEmpty: true, sessionsFindStateInvalidSnapshot: true,
		sessionsFindStateStale: true, sessionsFindStatePartial: true,
	}
	seen := map[string]bool{}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			got := classifySessionsFindNode(fixture.node, t443Now)
			if got.state != fixture.wantState || got.covered != fixture.covered {
				t.Fatalf("state=%q covered=%t want %q/%t", got.state, got.covered, fixture.wantState, fixture.covered)
			}
			if got.reason != fixture.wantReason {
				t.Fatalf("reason=%q want %q", got.reason, fixture.wantReason)
			}
			if got.state != "" && !vocabulary[got.state] {
				t.Fatalf("state %q outside the closed six-value vocabulary", got.state)
			}
			seen[got.state] = true
		})
	}
	for state := range vocabulary {
		if !seen[state] {
			t.Fatalf("vocabulary state %q never produced", state)
		}
	}
}

// TestSessionsFindOutcomeRules pins the fail-closed outcome/exit contract:
// NO_MATCH exists only for complete coverage with zero matches; every
// uncertainty forbids it and forces PARTIAL at a stable nonzero exit even when
// matches stay visible.
func TestSessionsFindOutcomeRules(t *testing.T) {
	sessionA := "[" + t443Session("w1:p1", "ws-a", "label-hit", "working") + "]"
	fresh := func(sessions string) string { return t443FreshSnapshot(sessions) }
	cases := []struct {
		name        string
		nodes       []string
		label       string
		wantOutcome string
		wantCode    int
		wantMatches int
	}{
		{"all fresh zero match", []string{
			t443Node("node-a", "connected", fresh("[]")),
			t443Node("node-b", "connected", fresh(sessionA)),
		}, "label-absent", sessionsFindOutcomeNoMatch, ExitOK, 0},
		{"all fresh match", []string{
			t443Node("node-a", "connected", fresh("[]")),
			t443Node("node-b", "connected", fresh(sessionA)),
		}, "label-hit", sessionsFindOutcomeFound, ExitOK, 1},
		{"unobserved forbids no_match", []string{
			t443Node("node-a", "connected", "null"),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"unavailable forbids no_match", []string{
			t443Node("node-a", "connected", t443Snapshot("unavailable", "null", t443Fresh, false, false)),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"invalid forbids no_match", []string{
			t443Node("node-a", "connected", t443Snapshot("ok", "null", t443Fresh, false, false)),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"stale forbids no_match", []string{
			t443Node("node-a", "stale", fresh("[]")),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"future forbids no_match", []string{
			t443Node("node-a", "connected", t443Snapshot("ok", "[]", t443Future, false, false)),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"truncated forbids no_match", []string{
			t443Node("node-a", "connected", t443Snapshot("ok", "[]", t443Fresh, true, false)),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-absent", sessionsFindOutcomePartial, ExitPartial, 0},
		{"visible match under partial", []string{
			t443Node("node-a", "connected", "null"),
			t443Node("node-b", "connected", fresh(sessionA)),
		}, "label-hit", sessionsFindOutcomePartial, ExitPartial, 1},
		{"stale match kept but not fresh", []string{
			t443Node("node-a", "stale", fresh(sessionA)),
			t443Node("node-b", "connected", fresh("[]")),
		}, "label-hit", sessionsFindOutcomePartial, ExitPartial, 1},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			server := t443Server(t, http.StatusOK, t443NodesBody(fixture.nodes...), nil)
			defer server.Close()
			code, stdout, stderr := t443Run(t, server, fixture.label, "--json")
			if code != fixture.wantCode {
				t.Fatalf("code=%d want=%d stdout=%q stderr=%q", code, fixture.wantCode, stdout, stderr)
			}
			result := t443Result(t, stdout)
			if result.Outcome != fixture.wantOutcome {
				t.Fatalf("outcome=%s want=%s", result.Outcome, fixture.wantOutcome)
			}
			if len(result.Matches) != fixture.wantMatches {
				t.Fatalf("matches=%d want=%d", len(result.Matches), fixture.wantMatches)
			}
			if fixture.wantOutcome == sessionsFindOutcomePartial && len(result.Coverage.Reasons) == 0 {
				t.Fatal("PARTIAL envelope lost its reason; stderr-only warnings disappear")
			}
			t441AssertNoCredentialLeak(t, stdout, stderr, "")
		})
	}
}

// TestSessionsFindStaleMatchMarked proves an old match stays visible with a
// last-seen timestamp but never counts as fresh or complete.
func TestSessionsFindStaleMatchMarked(t *testing.T) {
	sessionA := "[" + t443Session("w1:p1", "ws-a", "label-hit", "working") + "]"
	body := t443NodesBody(
		t443Node("node-a", "connected", t443Snapshot("ok", sessionA, t443Aged, false, false)),
		t443Node("node-b", "connected", t443FreshSnapshot("[]")),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()
	code, stdout, _ := t443Run(t, server, "label-hit", "--json")
	result := t443Result(t, stdout)
	if code != ExitPartial || result.Outcome != sessionsFindOutcomePartial || len(result.Matches) != 1 {
		t.Fatalf("code=%d outcome=%s matches=%d", code, result.Outcome, len(result.Matches))
	}
	match := result.Matches[0]
	if match.Fresh || match.LastSeen == "" || match.SnapshotState != sessionsFindStateStale {
		t.Fatalf("stale match misrepresented: %+v", match)
	}
}

// TestSessionsFindSixNodeBeforeAfter rebuilds the documented six-node shape
// synthetically: before has two null snapshots and must produce
// missing=2/PARTIAL/nonzero; after has all six fresh ok and missing=0. No live
// fleet or real NULL state is consulted.
func TestSessionsFindSixNodeBeforeAfter(t *testing.T) {
	matched := "[" + t443Session("w1:p1", "ws-a", "watcher", "working") + "]"
	before := t443NodesBody(
		t443Node("node-a", "connected", "null"),
		t443Node("node-b", "connected", "null"),
		t443Node("node-c", "connected", t443FreshSnapshot(matched)),
		t443Node("node-d", "connected", t443FreshSnapshot("[]")),
		t443Node("node-e", "connected", t443FreshSnapshot("[]")),
		t443Node("node-f", "connected", t443FreshSnapshot("[]")),
	)
	server := t443Server(t, http.StatusOK, before, nil)
	defer server.Close()
	code, stdout, stderr := t443Run(t, server, "label-absent", "--json")
	if code == ExitOK {
		t.Fatalf("before fixture returned success: stdout=%q", stdout)
	}
	result := t443Result(t, stdout)
	if result.Coverage.Missing != 2 || result.Coverage.Expected != 6 || result.Outcome != sessionsFindOutcomePartial || code != ExitPartial {
		t.Fatalf("before outcome=%s missing=%d expected=%d code=%d", result.Outcome, result.Coverage.Missing, result.Coverage.Expected, code)
	}
	var unobserved int
	for _, node := range result.Coverage.Nodes {
		if node.State == sessionsFindStateUnobserved {
			unobserved++
		}
	}
	if unobserved != 2 {
		t.Fatalf("unobserved nodes=%d want 2 (expected_snapshot_missing_count)", unobserved)
	}
	t441AssertNoCredentialLeak(t, stdout, stderr, "")

	after := t443NodesBody(
		t443Node("node-a", "connected", t443FreshSnapshot(matched)),
		t443Node("node-b", "connected", t443FreshSnapshot("[]")),
		t443Node("node-c", "connected", t443FreshSnapshot("[]")),
		t443Node("node-d", "connected", t443FreshSnapshot("[]")),
		t443Node("node-e", "connected", t443FreshSnapshot("[]")),
		t443Node("node-f", "connected", t443FreshSnapshot("[]")),
	)
	afterServer := t443Server(t, http.StatusOK, after, nil)
	defer afterServer.Close()
	code, stdout, _ = t443Run(t, afterServer, "label-absent", "--json")
	result = t443Result(t, stdout)
	if code != ExitOK || result.Outcome != sessionsFindOutcomeNoMatch || result.Coverage.Missing != 0 {
		t.Fatalf("after outcome=%s missing=%d code=%d", result.Outcome, result.Coverage.Missing, code)
	}
	code, stdout, _ = t443Run(t, afterServer, "watcher", "--json")
	result = t443Result(t, stdout)
	if code != ExitOK || result.Outcome != sessionsFindOutcomeFound || len(result.Matches) != 1 {
		t.Fatalf("after find outcome=%s matches=%d code=%d", result.Outcome, len(result.Matches), code)
	}
}

// TestSessionsFindExactlyOneRequest counts wire activity: one valid call is
// exactly one GET /v1/nodes through the common credentialled client, and a
// failure response still triggers zero retry, watch, or second request.
func TestSessionsFindExactlyOneRequest(t *testing.T) {
	tokenEnv, cfEnv := t441Envs(t)
	t.Run("success", func(t *testing.T) {
		recorded := make(chan t443Request, 8)
		server := t443Server(t, http.StatusOK, t443NodesBody(t443Node("node-a", "connected", t443FreshSnapshot("[]"))), recorded)
		defer server.Close()
		code, stdout, stderr := t443Run(t, server, "label-x", "--json")
		if code != ExitOK {
			t.Fatalf("code=%d stderr=%q", code, stderr)
		}
		if len(recorded) != 1 {
			t.Fatalf("requests=%d want exactly 1", len(recorded))
		}
		got := <-recorded
		if got.method != http.MethodGet || got.path != "/v1/nodes" || got.headers.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken || got.headers.Get("CF-Access-Client-Id") != t441CFClientID {
			t.Fatalf("request=%s %s headers=%v", got.method, got.path, got.headers)
		}
		t441AssertNoCredentialLeak(t, stdout, stderr, "")
	})
	for _, status := range []int{http.StatusInternalServerError, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var hits int64
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				atomic.AddInt64(&hits, 1)
				writer.WriteHeader(status)
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := runSessionsCLI([]string{"find", "label-x", "--json", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t443Deps(server))
			if code == ExitOK || atomic.LoadInt64(&hits) != 1 {
				t.Fatalf("status=%d code=%d requests=%d: a retry or second GET happened", status, code, hits)
			}
			result := t443Result(t, stdout.String())
			if result.Outcome != sessionsFindOutcomeError {
				t.Fatalf("status=%d outcome=%s want ERROR", status, result.Outcome)
			}
			t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
		})
	}
	t.Run("redirect refused", func(t *testing.T) {
		var targetHits int64
		target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			atomic.AddInt64(&targetHits, 1)
		}))
		defer target.Close()
		var redirectorHits int64
		redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			atomic.AddInt64(&redirectorHits, 1)
			http.Redirect(writer, request, target.URL+request.URL.Path, http.StatusFound)
		}))
		defer redirector.Close()
		var stdout, stderr bytes.Buffer
		code := runSessionsCLI([]string{"find", "label-x", "--hub-url", redirector.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t443Deps(redirector))
		if code == ExitOK || atomic.LoadInt64(&targetHits) != 0 || atomic.LoadInt64(&redirectorHits) != 1 {
			t.Fatalf("redirect followed: code=%d redirector=%d target=%d", code, redirectorHits, targetHits)
		}
		t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	})
	t.Run("malformed body", func(t *testing.T) {
		var hits int64
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			atomic.AddInt64(&hits, 1)
			_, _ = writer.Write([]byte("{not json"))
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		code := runSessionsCLI([]string{"find", "label-x", "--hub-url", server.URL, "--hub-token-env", tokenEnv}, &stdout, &stderr, t443Deps(server))
		if code == ExitOK || atomic.LoadInt64(&hits) != 1 {
			t.Fatalf("malformed body: code=%d requests=%d", code, hits)
		}
	})
}

// TestSessionsFindRendererParity proves the human and JSON renderers describe
// the same typed result: identical matches, coverage counters, and outcome.
func TestSessionsFindRendererParity(t *testing.T) {
	body := t443NodesBody(
		t443Node("node-a", "connected", "null"),
		t443Node("node-b", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-a", "label-hit", "idle")+"]")),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()
	code, human, _ := t443Run(t, server, "label-hit")
	if code != ExitPartial {
		t.Fatalf("human code=%d want PARTIAL", code)
	}
	code, jsonOut, _ := t443Run(t, server, "label-hit", "--json")
	if code != ExitPartial {
		t.Fatalf("json code=%d want PARTIAL", code)
	}
	result := t443Result(t, jsonOut)
	for _, want := range []string{
		"outcome\tPARTIAL", "expected=2", "observed=1", "missing=1",
		"match\tnode-b\tw1:p1\tws-a\tagent_name=label-hit\tlabel=label-hit\tlabel_source=agent_name\tdisplay_label=-\tstatus=idle",
		"node\tnode-a\tcovered=false\tstate=UNOBSERVED",
	} {
		if !strings.Contains(human, want) {
			t.Fatalf("human output missing %q:\n%s", want, human)
		}
	}
	if result.Outcome != sessionsFindOutcomePartial || result.Coverage.Missing != 1 || len(result.Matches) != 1 || result.Matches[0].Machine != "node-b" {
		t.Fatalf("renderers disagree: %+v", result)
	}
}

// TestSessionsFindDuplicateLabelsDeterministic proves two nodes carrying the
// same label both surface, sorted deterministically by machine and pane.
func TestSessionsFindDuplicateLabelsDeterministic(t *testing.T) {
	body := t443NodesBody(
		t443Node("node-b", "connected", t443FreshSnapshot("["+t443Session("w1:p2", "ws-b", "dup", "idle")+"]")),
		t443Node("node-a", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-a", "dup", "working")+"]")),
		t443Node("node-c", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-c", "dup", "done")+"]")),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()
	var last string
	for run := 0; run < 3; run++ {
		code, stdout, _ := t443Run(t, server, "dup", "--json")
		if code != ExitOK {
			t.Fatalf("code=%d", code)
		}
		result := t443Result(t, stdout)
		if len(result.Matches) != 3 || result.Matches[0].Machine != "node-a" || result.Matches[1].Machine != "node-b" || result.Matches[2].Machine != "node-c" {
			t.Fatalf("duplicates dropped or misordered: %+v", result.Matches)
		}
		if last != "" && stdout != last {
			t.Fatal("output not deterministic across runs")
		}
		last = stdout
	}
}

// TestSessionsFindSourceAndOutputBoundaries is the countable safety
// inventory: the command crosses exactly the #441 common client, adds no new
// credential host/loader, never names client_kind or a raw cwd field, and its
// output carries neither fixture secrets nor sensitive paths.
func TestSessionsFindSourceAndOutputBoundaries(t *testing.T) {
	data, err := os.ReadFile("sessions_find.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{"newHubOperatorClient(", ".do(ctx, http.MethodGet, \"/v1/nodes\""} {
		if !strings.Contains(source, required) {
			t.Fatalf("sessions find bypasses the common credentialled client: missing %q", required)
		}
	}
	for _, banned := range []string{
		"http.DefaultClient", "http.Get(", "http.Post(", "http.Head(", "http.Do(",
		"client_kind", `"cwd"`, "loadMode0600Env(",
	} {
		if strings.Contains(source, banned) {
			t.Fatalf("sessions find introduces %q outside the shared boundary", banned)
		}
	}
	body := t443NodesBody(t443Node("node-a", "connected", t443FreshSnapshot("["+t443Session("w1:p1", "ws-a", "label-hit", "idle")+"]")))
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()
	var stdout, stderr bytes.Buffer
	tokenEnv, cfEnv := t441Envs(t)
	code := runSessionsCLI([]string{"find", "label-hit", "--json", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t443Deps(server))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	for _, banned := range []string{tokenEnv, cfEnv, "client_kind", `"cwd"`, os.TempDir()} {
		if strings.Contains(stdout.String()+stderr.String(), banned) {
			t.Fatalf("output leaked %q", banned)
		}
	}
}
