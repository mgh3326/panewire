package panewire

import (
	"encoding/json"
	"fmt"
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

// TestLanesAuditNodesRejectedBuilder covers the degraded renderer directly:
// sink lanes stay skipped and a malformed lane projection keeps its own
// more specific reason.
func TestLanesAuditNodesRejectedBuilder(t *testing.T) {
	lanes := []hubLaneProjection{
		{Lane: "sink-a", Sink: true},
		t509Lane("lane-a", "machine-a", "w1:p1"),
		t509Lane("lane-inv", "Machine-A", "w1:p1"),
	}
	result := buildLanesAuditNodesRejected(lanes, lanesAuditReasonNodesResponseBad, t509Now)
	if result.Outcome != lanesAuditOutcomePartial || result.Summary.SinkSkipped != 1 || result.Summary.Indeterminate != 2 {
		t.Fatalf("result=%+v", result)
	}
	rows := t509Verdicts(result)
	if rows["lane-a"].Reason != lanesAuditReasonNodesResponseBad {
		t.Fatalf("row=%+v", rows["lane-a"])
	}
	if rows["lane-inv"].Reason != lanesAuditReasonLaneInvalid {
		t.Fatalf("row=%+v", rows["lane-inv"])
	}
}

// t959SiblingPairs adapts --sibling A=B strings to the builder's variadic
// pair form so table rows read like the CLI flag.
func t959SiblingPairs(pairs ...string) [][2]string {
	parsed := make([][2]string, 0, len(pairs))
	for _, pair := range pairs {
		left, right, _ := strings.Cut(pair, "=")
		parsed = append(parsed, [2]string{left, right})
	}
	return parsed
}

// TestLanesAuditSiblingSessionMismatch pins the --sibling check: a dead lane
// whose pane provably lives on a sibling daemon of the same host stays dead
// but is annotated reason=session_mismatch sibling=<B>. The check never
// rescues a verdict — dead stays dead, alive and indeterminate never gain a
// sibling — and an incomplete sibling observation is skipped rather than
// guessed.
func TestLanesAuditSiblingSessionMismatch(t *testing.T) {
	present := t509Session("w1:p1")
	other := t509Session("w2:p7")
	cases := []struct {
		name        string
		lane        hubLaneProjection
		nodes       []lanesAuditNodeWire
		siblings    [][2]string
		wantVerdict string
		wantReason  string
		wantSibling string
	}{
		{name: "pane on sibling reports session_mismatch",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead, wantReason: lanesAuditReasonSessionMismatch, wantSibling: "machine-b"},
		{name: "no sibling flag leaves bare dead verdict",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
			},
			wantVerdict: lanesAuditVerdictDead},
		{name: "pair order is irrelevant",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-b=machine-a"),
			wantVerdict: lanesAuditVerdictDead, wantReason: lanesAuditReasonSessionMismatch, wantSibling: "machine-b"},
		{name: "sibling group is transitive",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+other+"]")),
				t509Node("machine-c", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b", "machine-b=machine-c"),
			wantVerdict: lanesAuditVerdictDead, wantReason: lanesAuditReasonSessionMismatch, wantSibling: "machine-c"},
		{name: "sorted sibling order wins when several match",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
				t509Node("machine-c", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-c=machine-a", "machine-b=machine-a"),
			wantVerdict: lanesAuditVerdictDead, wantReason: lanesAuditReasonSessionMismatch, wantSibling: "machine-b"},
		{name: "incomplete sibling observation never counts",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "stale", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead},
		{name: "truncated sibling observation never counts",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509Snapshot(hubSnapshotStatusOK, true, false, t509Now.Add(-10*time.Second), "["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead},
		{name: "sibling absent from nodes never counts",
			lane:        t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes:       []lanesAuditNodeWire{t509Node("machine-a", "connected", t509FreshSnapshot("[]"))},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead},
		{name: "pane absent from sibling too",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+other+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead},
		{name: "alive on own machine never gains sibling",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509FreshSnapshot("["+present+"]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictAlive},
		{name: "indeterminate observation never gains sibling",
			lane: t509Lane("lane-a", "machine-a", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-a", "connected", t509Snapshot(hubSnapshotStatusOK, false, true, t509Now.Add(-10*time.Second), "[]")),
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictIndeterminate, wantReason: lanesAuditReasonSnapshotStale},
		{name: "machine outside every sibling group is unchecked",
			lane: t509Lane("lane-c", "machine-c", "w1:p1"),
			nodes: []lanesAuditNodeWire{
				t509Node("machine-b", "connected", t509FreshSnapshot("["+present+"]")),
				t509Node("machine-c", "connected", t509FreshSnapshot("[]")),
			},
			siblings:    t959SiblingPairs("machine-a=machine-b"),
			wantVerdict: lanesAuditVerdictDead},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			result := buildLanesAuditResult([]hubLaneProjection{fixture.lane}, fixture.nodes, t509Now, fixture.siblings...)
			row, ok := t509Verdicts(result)[fixture.lane.Lane]
			if !ok {
				t.Fatalf("lane %q missing from result", fixture.lane.Lane)
			}
			if row.Verdict != fixture.wantVerdict {
				t.Fatalf("verdict=%q want=%q row=%+v", row.Verdict, fixture.wantVerdict, row)
			}
			if row.Reason != fixture.wantReason {
				t.Fatalf("reason=%q want=%q row=%+v", row.Reason, fixture.wantReason, row)
			}
			if row.Sibling != fixture.wantSibling {
				t.Fatalf("sibling=%q want=%q row=%+v", row.Sibling, fixture.wantSibling, row)
			}
		})
	}
}
