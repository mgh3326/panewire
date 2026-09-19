package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// t443HerdrSocket fakes the herdr RPC socket for fill-path tests: agent.list
// returns the supplied agent maps and tab.list returns the supplied tab maps.
// Every identity string is synthetic.
func t443HerdrSocket(t *testing.T, agents, tabs []map[string]any) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "pw-t443-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				for scanner.Scan() {
					var request map[string]any
					if json.Unmarshal(scanner.Bytes(), &request) != nil {
						continue
					}
					result := any(map[string]any{})
					switch request["method"] {
					case "agent.list":
						result = map[string]any{"agents": agents}
					case "tab.list":
						result = map[string]any{"tabs": tabs}
					}
					response, err := json.Marshal(map[string]any{"id": request["id"], "result": result})
					if err != nil {
						return
					}
					_, _ = fmt.Fprintf(connection, "%s\n", response)
				}
			}(connection)
		}
	}()
	return listener.Addr().String()
}

func t443AgentStates(t *testing.T, agents, tabs []map[string]any) []HerdrAgentState {
	t.Helper()
	herdr, err := NewHerdrClient(t443HerdrSocket(t, agents, tabs))
	if err != nil {
		t.Fatal(err)
	}
	defer herdr.Close()
	states, err := herdr.AgentStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return states
}

// TestT443FillNamedAgentDistinctTabLabel pins the fill boundary: agent_name is
// a byte-for-byte copy of agent.list.name, display_label comes only from the
// tab.list join, the legacy label survives, and label_source names the class.
// Mixed-case names prove no normalization touches the canonical bytes.
func TestT443FillNamedAgentDistinctTabLabel(t *testing.T) {
	states := t443AgentStates(t,
		[]map[string]any{
			{"pane_id": "ws-a:p1", "workspace_id": "ws-a", "tab_id": "t1", "name": "Agent-CaMeL", "label": "Agent-CaMeL", "agent_status": "working", "revision": 3, "state_change_seq": 4},
			{"pane_id": "ws-a:p2", "workspace_id": "ws-a", "tab_id": "t2", "name": "agent-camel", "label": "agent-camel", "agent_status": "idle", "revision": 5, "state_change_seq": 6},
		},
		[]map[string]any{
			{"tab_id": "t1", "label": "Tab Display Alpha"},
			{"tab_id": "t2", "label": "Tab Display Beta"},
		},
	)
	if len(states) != 2 {
		t.Fatalf("states=%+v", states)
	}
	if states[0].AgentName != "Agent-CaMeL" || states[1].AgentName != "agent-camel" {
		t.Fatalf("agent names normalized or swapped: %q %q", states[0].AgentName, states[1].AgentName)
	}
	if states[0].DisplayLabel != "Tab Display Alpha" || states[1].DisplayLabel != "Tab Display Beta" {
		t.Fatalf("display labels wrong: %q %q", states[0].DisplayLabel, states[1].DisplayLabel)
	}
	sessions := hubSessionSnapshotsFromStates(states)
	if len(sessions) != 2 {
		t.Fatalf("sessions=%+v", sessions)
	}
	first := sessions[0]
	if first.AgentName != "Agent-CaMeL" || first.Label != "Agent-CaMeL" || first.LabelSource != hubLabelSourceAgentName || first.DisplayLabel != "Tab Display Alpha" {
		t.Fatalf("named agent projection lost provenance: %+v", first)
	}
	second := sessions[1]
	if second.AgentName != "agent-camel" || second.LabelSource != hubLabelSourceAgentName {
		t.Fatalf("case-variant agent name was normalized: %+v", second)
	}
}

// TestT443FillUnnamedAndStaleDisplay pins the negative cases: a tab label
// never becomes identity, stale agent.list display metadata never becomes
// identity, and a session with neither stays provenance-missing.
func TestT443FillUnnamedAndStaleDisplay(t *testing.T) {
	states := t443AgentStates(t,
		[]map[string]any{
			{"pane_id": "ws-b:p1", "workspace_id": "ws-b", "tab_id": "t1", "agent_status": "idle", "revision": 1, "state_change_seq": 1},
			{"pane_id": "ws-b:p2", "workspace_id": "ws-b", "display_agent": "stale-display", "agent_status": "working", "revision": 2, "state_change_seq": 2},
			{"pane_id": "ws-b:p3", "workspace_id": "ws-b", "name": "agent-real", "display_agent": "stale-other", "agent_status": "done", "revision": 3, "state_change_seq": 3},
			{"pane_id": "ws-b:p4", "workspace_id": "ws-b", "agent_status": "unknown", "revision": 4, "state_change_seq": 4},
		},
		[]map[string]any{{"tab_id": "t1", "label": "Tab Beta"}},
	)
	if len(states) != 4 {
		t.Fatalf("states=%+v", states)
	}
	sessions := hubSessionSnapshotsFromStates(states)
	if len(sessions) != 4 {
		t.Fatalf("sessions=%+v", sessions)
	}
	unnamedTab := sessions[0]
	if unnamedTab.AgentName != "" || unnamedTab.DisplayLabel != "Tab Beta" || unnamedTab.Label != "Tab Beta" || unnamedTab.LabelSource != hubLabelSourceTabLabel {
		t.Fatalf("tab label leaked into identity or provenance mislabeled: %+v", unnamedTab)
	}
	stale := sessions[1]
	if stale.AgentName != "" || stale.Label != "stale-display" || stale.LabelSource != hubLabelSourceTabLabel {
		t.Fatalf("stale display metadata became agent name: %+v", stale)
	}
	named := sessions[2]
	if named.AgentName != "agent-real" || named.LabelSource != hubLabelSourceTabLabel || named.Label != "stale-other" {
		t.Fatalf("named agent borrowed display metadata or mislabeled provenance: %+v", named)
	}
	missing := sessions[3]
	if missing.AgentName != "" || missing.Label != "" || missing.DisplayLabel != "" || missing.LabelSource != hubLabelSourceMissing {
		t.Fatalf("empty identity/display was not provenance-missing: %+v", missing)
	}
}

// TestT443WireAdditiveRoundTrip proves the new producer shape encodes and the
// strict hub decoder round-trips every additive field with closed provenance.
func TestT443WireAdditiveRoundTrip(t *testing.T) {
	ready := true
	sessions := []HubSession{
		{PaneID: "ws-a:p1", WorkspaceID: "ws-a", Label: "agent-hit", AgentName: "agent-hit", LabelSource: hubLabelSourceAgentName, DisplayLabel: "Tab Alpha", Status: "working", InteractiveReady: &ready, Revision: 1, StateChangeSeq: 2},
		{PaneID: "ws-a:p2", WorkspaceID: "ws-a", Label: "tab-beta", AgentName: "", LabelSource: hubLabelSourceTabLabel, DisplayLabel: "tab-beta", Status: "idle", Revision: 3, StateChangeSeq: 4},
		{PaneID: "ws-a:p3", WorkspaceID: "ws-a", Label: "", AgentName: "", LabelSource: hubLabelSourceMissing, DisplayLabel: "", Status: "done", Revision: 5, StateChangeSeq: 6},
	}
	raw, err := json.Marshal(sessions)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := decodeHubSessionSnapshots(raw)
	if !ok || len(decoded) != 3 {
		t.Fatalf("strict decoder rejected new producer shape: ok=%t sessions=%s", ok, raw)
	}
	if decoded[0].AgentName != "agent-hit" || decoded[0].LabelSource != hubLabelSourceAgentName || decoded[0].DisplayLabel != "Tab Alpha" || decoded[0].InteractiveReady == nil || !*decoded[0].InteractiveReady {
		t.Fatalf("additive fields did not round-trip: %+v", decoded[0])
	}
	if decoded[1].LabelSource != hubLabelSourceTabLabel || decoded[2].LabelSource != hubLabelSourceMissing {
		t.Fatalf("closed provenance did not round-trip: %+v", decoded)
	}
	heartbeat := hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Sessions: &sessions, SnapshotStatus: hubSnapshotStatusOK}
	rawHeartbeat, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubHeartbeatPayload(rawHeartbeat); !ok {
		t.Fatalf("hub heartbeat decoder rejected additive session fields: %s", rawHeartbeat)
	}
}

// TestT443WireOldPayloadCompat proves a pre-additive session row still
// decodes, stays unnamed and unmatchable, and is represented with absent
// provenance rather than an inferred agent_name.
func TestT443WireOldPayloadCompat(t *testing.T) {
	old := "[" + t443SessionLegacy("ws-a:p1", "ws-a", "legacy-only", "working") + "]"
	decoded, ok := decodeHubSessionSnapshots(json.RawMessage(old))
	if !ok || len(decoded) != 1 {
		t.Fatalf("strict decoder rejected pre-additive payload: ok=%t", ok)
	}
	session := decoded[0]
	if session.AgentName != "" || session.LabelSource != "" || session.DisplayLabel != "" {
		t.Fatalf("old payload gained inferred identity/provenance: %+v", session)
	}
	if session.Label != "legacy-only" {
		t.Fatalf("old payload lost its legacy label: %+v", session)
	}
	for _, match := range []string{"exact", "contains"} {
		if sessionsFindLabelMatches(session.AgentName, sessionsFindQuery{Label: "legacy-only", Match: match}) {
			t.Fatalf("old payload matched %s query by inferred identity", match)
		}
	}
}

// TestT443WireLabelSourceClosedVocabulary rejects non-empty provenance values
// outside the closed set and provenance/label combinations that contradict
// each other.
func TestT443WireLabelSourceClosedVocabulary(t *testing.T) {
	session := func(extra string) string {
		return `[{"pane_id":"ws-a:p1","workspace_id":"ws-a","label":"x","status":"idle","revision":1,"state_change_seq":1` + extra + `}]`
	}
	if decoded, ok := decodeHubSessionSnapshots(json.RawMessage(session(`,"label_source":"bogus"`))); ok {
		t.Fatalf("unknown label_source accepted: %+v", decoded)
	}
	if decoded, ok := decodeHubSessionSnapshots(json.RawMessage(session(`,"label_source":"agent_name","agent_name":"different"`))); ok {
		t.Fatalf("label_source=agent_name with mismatched agent_name accepted: %+v", decoded)
	}
	if decoded, ok := decodeHubSessionSnapshots(json.RawMessage(session(`,"label_source":"missing"`))); ok {
		t.Fatalf("label_source=missing with a populated label accepted: %+v", decoded)
	}
	if decoded, ok := decodeHubSessionSnapshots(json.RawMessage(session(`,"label_source":"tab_label","agent_name":"x"`))); ok {
		t.Fatalf("label_source=tab_label equal to agent_name accepted: %+v", decoded)
	}
	if decoded, ok := decodeHubSessionSnapshots(json.RawMessage(session(`,"label_source":"agent_name","agent_name":"x"`))); !ok || decoded[0].LabelSource != hubLabelSourceAgentName {
		t.Fatalf("consistent agent_name provenance rejected: ok=%t %+v", ok, decoded)
	}
}

// TestSessionsFindMixedProvenance is the blocking-contract fixture: named
// agent with a different tab label, unnamed pane with a tab label, a stale
// display label, a duplicate agent_name on a distinct machine/pane, and an
// old payload row. Only agent_name can match — under exact and --contains —
// and every duplicate survives.
func TestSessionsFindMixedProvenance(t *testing.T) {
	namedDistinctTab := t443SessionProvenance("ws-a:p1", "ws-a", "agent-hit", "agent-hit", "agent_name", "Tab Alpha", "working")
	unnamedTab := t443SessionProvenance("ws-b:p1", "ws-b", "", "tab-beta", "tab_label", "tab-beta", "idle")
	staleDisplay := t443SessionProvenance("ws-c:p1", "ws-c", "agent-c", "old-display", "tab_label", "old-display", "done")
	duplicate := t443SessionProvenance("ws-d:p1", "ws-d", "agent-hit", "agent-hit", "agent_name", "", "idle")
	legacy := t443SessionLegacy("ws-e:p1", "ws-e", "legacy-only", "working")
	body := t443NodesBody(
		t443Node("node-a", "connected", t443FreshSnapshot("["+namedDistinctTab+"]")),
		t443Node("node-b", "connected", t443FreshSnapshot("["+unnamedTab+"]")),
		t443Node("node-c", "connected", t443FreshSnapshot("["+staleDisplay+"]")),
		t443Node("node-d", "connected", t443FreshSnapshot("["+duplicate+"]")),
		t443Node("node-e", "connected", t443FreshSnapshot("["+legacy+"]")),
	)
	server := t443Server(t, http.StatusOK, body, nil)
	defer server.Close()

	run := func(args ...string) (int, sessionsFindResult) {
		code, stdout, stderr := t443Run(t, server, args...)
		if code != ExitOK {
			t.Fatalf("query %v failed: code=%d stderr=%q", args, code, stderr)
		}
		return code, t443Result(t, stdout)
	}

	_, result := run("agent-hit", "--json")
	if result.Outcome != sessionsFindOutcomeFound || len(result.Matches) != 2 {
		t.Fatalf("exact agent_name outcome=%s matches=%d want FOUND/2: %+v", result.Outcome, len(result.Matches), result.Matches)
	}
	if result.Matches[0].Machine != "node-a" || result.Matches[1].Machine != "node-d" {
		t.Fatalf("duplicate agent_name not preserved across machines: %+v", result.Matches)
	}
	for _, match := range result.Matches {
		if match.AgentName != "agent-hit" || match.LabelSource != hubLabelSourceAgentName {
			t.Fatalf("match lost canonical identity/provenance: %+v", match)
		}
	}
	if result.Matches[0].DisplayLabel != "Tab Alpha" {
		t.Fatalf("display context missing from match row: %+v", result.Matches[0])
	}

	// No display/tab/legacy value may match, exact or contains.
	for _, query := range []string{"Tab Alpha", "tab-beta", "old-display", "legacy-only", "stale-display"} {
		_, result = run(query, "--json")
		if result.Outcome != sessionsFindOutcomeNoMatch || len(result.Matches) != 0 {
			t.Fatalf("display/legacy value %q produced a match under exact: %+v", query, result)
		}
		_, result = run(query, "--contains", "--json")
		if result.Outcome != sessionsFindOutcomeNoMatch || len(result.Matches) != 0 {
			t.Fatalf("display/legacy value %q produced a match under --contains: %+v", query, result)
		}
	}

	// --contains still searches agent_name only: "agent" hits the two
	// agent-hit rows plus agent-c; it cannot hit tab/display/legacy values.
	_, result = run("agent", "--contains", "--json")
	if result.Outcome != sessionsFindOutcomeFound || len(result.Matches) != 3 {
		t.Fatalf("contains agent_name outcome=%s matches=%d want FOUND/3: %+v", result.Outcome, len(result.Matches), result.Matches)
	}

	// Unnamed rows stay visible: node-b and node-e each carry one unnamed
	// session in coverage even though they can never match.
	var unnamedNodes int
	for _, node := range result.Coverage.Nodes {
		if node.Unnamed > 0 {
			unnamedNodes++
		}
	}
	if unnamedNodes != 2 {
		t.Fatalf("unnamed session counts lost from coverage: %+v", result.Coverage.Nodes)
	}
}

// TestT443HeartbeatBudgetWireFeasible carries the director's replacement for
// the discarded decoder-maxima wording (hk:doc 2172): 64 sessions with all
// ten accepted wire fields populated by wire-feasible values must encode into
// a full heartbeat envelope below 32 KiB, keep every row, and stay
// untruncated. This is not a worst-case or decoder-maxima claim.
func TestT443HeartbeatBudgetWireFeasible(t *testing.T) {
	ready := true
	states := make([]HerdrAgentState, hubMaxHeartbeatSessions)
	for index := range states {
		agent := fmt.Sprintf("agent-%03d-%s", index, strings.Repeat("n", 52))
		states[index] = HerdrAgentState{
			PaneID:               fmt.Sprintf("workspace-%03d:pane-%s", index, strings.Repeat("p", 40)),
			WorkspaceID:          fmt.Sprintf("workspace-%03d-%s", index, strings.Repeat("w", 50)),
			Label:                agent,
			AgentName:            agent,
			DisplayLabel:         fmt.Sprintf("tab-display-%03d-%s", index, strings.Repeat("d", 43)),
			Status:               "working",
			Revision:             int64(index + 1),
			SourceStateChangeSeq: int64(index + 1),
			InteractiveReady:     &ready,
		}
	}
	payload := marshalHubHeartbeatWithSessions(hubHeartbeatPayload{
		Status:         "alive",
		Checks:         map[string]HubCheckStatus{},
		SnapshotStatus: hubSnapshotStatusOK,
	}, states)
	envelope, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) >= hubMaxMessageBytes || len(envelope) >= hubMaxMessageBytes {
		t.Fatalf("wire-feasible 64-session heartbeat exceeded budget: payload=%d envelope=%d limit=%d", len(payload), len(envelope), hubMaxMessageBytes)
	}
	t.Logf("wire-feasible 64-session heartbeat: payload=%d envelope=%d", len(payload), len(envelope))
	var decoded hubHeartbeatPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Sessions == nil || len(*decoded.Sessions) != hubMaxHeartbeatSessions {
		t.Fatalf("wire-feasible fixture dropped sessions: %d", len(*decoded.Sessions))
	}
	if decoded.Truncated {
		t.Fatal("wire-feasible 64-session fixture was marked truncated")
	}
	for index, session := range *decoded.Sessions {
		if session.AgentName == "" || session.LabelSource != hubLabelSourceAgentName || session.DisplayLabel == "" || session.InteractiveReady == nil {
			t.Fatalf("session %d lost an additive field: %+v", index, session)
		}
	}
}

// TestT443HeartbeatBudgetDecoderMaxBoundary proves the count cap and the byte
// cap work together at the other edge: sessions carrying decoder-maximum
// field sizes cannot all fit, so the encoder must deterministically keep a
// prefix, mark truncated=true, and keep both payload and envelope under
// 32 KiB — regardless of collector input order.
func TestT443HeartbeatBudgetDecoderMaxBoundary(t *testing.T) {
	states := make([]HerdrAgentState, hubMaxHeartbeatSessions)
	for index := range states {
		agent := strings.Repeat(fmt.Sprintf("%03d", index), 170)[:510] + "an"
		states[index] = HerdrAgentState{
			PaneID:               fmt.Sprintf("workspace-%03d:%s", index, strings.Repeat("p", 100)),
			WorkspaceID:          strings.Repeat("w", 500),
			Label:                agent,
			AgentName:            agent,
			DisplayLabel:         strings.Repeat("d", 500),
			Status:               "unknown",
			Revision:             int64(index + 1),
			SourceStateChangeSeq: int64(index + 1),
		}
	}
	encode := func(input []HerdrAgentState) ([]byte, hubHeartbeatPayload) {
		payload := marshalHubHeartbeatWithSessions(hubHeartbeatPayload{
			Status:         "alive",
			Checks:         map[string]HubCheckStatus{},
			SnapshotStatus: hubSnapshotStatusOK,
		}, input)
		var decoded hubHeartbeatPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatal(err)
		}
		return payload, decoded
	}
	payload, decoded := encode(states)
	envelope, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Truncated {
		t.Fatal("decoder-maximum fixture was not marked truncated")
	}
	if decoded.Sessions == nil || len(*decoded.Sessions) == 0 || len(*decoded.Sessions) >= hubMaxHeartbeatSessions {
		t.Fatalf("boundary selection kept %d sessions, want a nonempty strict prefix", len(*decoded.Sessions))
	}
	if len(payload) >= hubMaxMessageBytes || len(envelope) >= hubMaxMessageBytes {
		t.Fatalf("boundary heartbeat exceeded budget: payload=%d envelope=%d limit=%d", len(payload), len(envelope), hubMaxMessageBytes)
	}
	t.Logf("decoder-maximum boundary heartbeat: kept=%d payload=%d envelope=%d", len(*decoded.Sessions), len(payload), len(envelope))
	sorted := hubSessionSnapshotsFromStates(states)
	for index, session := range *decoded.Sessions {
		if session.PaneID != sorted[index].PaneID {
			t.Fatalf("boundary selection is not a deterministic sorted prefix: index=%d pane=%q want %q", index, session.PaneID, sorted[index].PaneID)
		}
	}
	reversed := append([]HerdrAgentState(nil), states...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	secondPayload, second := encode(reversed)
	if !bytes.Equal(payload, secondPayload) || len(*second.Sessions) != len(*decoded.Sessions) {
		t.Fatal("boundary truncation was input-order dependent")
	}
}

// TestSessionsFindUnnamedCoverageOutput pins deterministic unnamed context in
// both renderers and that unnamed/display rows cannot create false
// completion: a complete fresh snapshot with only unnamed sessions is a legal
// NO_MATCH, while any incomplete sibling stays PARTIAL.
func TestSessionsFindUnnamedCoverageOutput(t *testing.T) {
	unnamedTab := t443SessionProvenance("ws-b:p1", "ws-b", "", "tab-beta", "tab_label", "tab-beta", "idle")
	namedMiss := t443SessionProvenance("ws-a:p1", "ws-a", "agent-other", "agent-other", "agent_name", "", "working")

	complete := t443NodesBody(
		t443Node("node-a", "connected", t443FreshSnapshot("["+namedMiss+"]")),
		t443Node("node-b", "connected", t443FreshSnapshot("["+unnamedTab+"]")),
	)
	server := t443Server(t, http.StatusOK, complete, nil)
	defer server.Close()
	code, stdout, stderr := t443Run(t, server, "agent-absent", "--json")
	if code != ExitOK {
		t.Fatalf("complete unnamed scope failed: code=%d stderr=%q", code, stderr)
	}
	result := t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomeNoMatch {
		t.Fatalf("complete fresh scope with zero agent-name matches outcome=%s want NO_MATCH", result.Outcome)
	}
	if result.Coverage.Nodes[0].Unnamed != 0 || result.Coverage.Nodes[1].Unnamed != 1 {
		t.Fatalf("unnamed counts not deterministic per node: %+v", result.Coverage.Nodes)
	}

	code, human, _ := t443Run(t, server, "agent-absent")
	if code != ExitOK || !strings.Contains(human, "unnamed=1") {
		t.Fatalf("human output lost unnamed context: code=%d\n%s", code, human)
	}

	// An unobserved sibling forbids NO_MATCH regardless of unnamed rows.
	partial := t443NodesBody(
		t443Node("node-a", "connected", "null"),
		t443Node("node-b", "connected", t443FreshSnapshot("["+unnamedTab+"]")),
	)
	partialServer := t443Server(t, http.StatusOK, partial, nil)
	defer partialServer.Close()
	code, stdout, stderr = t443Run(t, partialServer, "agent-absent", "--json")
	if code != ExitPartial {
		t.Fatalf("incomplete scope exited %d want PARTIAL: stderr=%q", code, stderr)
	}
	result = t443Result(t, stdout)
	if result.Outcome != sessionsFindOutcomePartial {
		t.Fatalf("unnamed row collapsed incomplete coverage to NO_MATCH: %+v", result)
	}
}
