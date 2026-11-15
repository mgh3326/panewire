package panewire

import (
	"testing"
	"time"
)

func TestNodeJobReapArgsAcceptHistoricalDottedLane(t *testing.T) {
	if !validNodeJobReapArgs(nodeJobReapArgs{Job: "job-a", Lane: "legacy.parent"}) {
		t.Fatal("node rejected a historical lane reference")
	}
	if validNodeJobReapArgs(nodeJobReapArgs{Job: "job-a", Lane: "bad/name"}) {
		t.Fatal("node accepted an invalid lane reference")
	}
}

// nodeJobReapClaim and nodeJobReapSpawn are the ledger records a spawned job
// leaves on its node: the claim names the owner lane, the receipt the pane and
// label the pane must still carry.
func nodeJobReapClaim(lane string) map[string]any {
	return map[string]any{"kind": "job.claim", "payload": map[string]any{"owner_lane": lane, "agent_label": "job-a", "task_id": 3968}}
}

func nodeJobReapSpawn(lane string, keep bool) map[string]any {
	return map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "owner_lane": lane, "cwd": "/work/a", "keep": keep}}
}

// TestNodeJobReapSafety is acceptance (c): the node closes a finished job's
// pane and refuses every pane reap would not close — a job that is still
// working or waiting at a prompt without force, a job spawned with --keep, a
// pane that is not the job's, and a job another lane owns.
func TestNodeJobReapSafety(t *testing.T) {
	cases := []struct {
		name     string
		events   []map[string]any
		listed   string
		pane     string
		label    string
		force    bool
		composer string
		outcome  string
	}{
		{name: "finished job closes", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "idle", outcome: nodeJobReapClosed},
		{name: "done job closes", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "done", outcome: nodeJobReapClosed},
		{name: "working job needs force", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "working", outcome: nodeJobReapJobWorking},
		{name: "working job with force closes", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "working", force: true, outcome: nodeJobReapClosed},
		{name: "blocked pane needs force", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "blocked", outcome: nodeJobReapPaneBlocked},
		{name: "blocked pane with force closes", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "blocked", force: true, outcome: nodeJobReapClosed},
		{name: "protected job is never closed", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", true)}, listed: "idle", force: true, outcome: nodeJobReapProtected},
		{name: "another lane's job", events: []map[string]any{nodeJobReapClaim("lane-b"), nodeJobReapSpawn("lane-b", false)}, listed: "idle", outcome: nodeJobReapOwnerMismatch},
		{name: "a pane that is not the job's", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "idle", label: "job-other", outcome: nodeJobReapPaneChanged},
		{name: "pane gone", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "idle", pane: "w9:p9", outcome: nodeJobReapPaneGone},
		{name: "draft in the composer", events: []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}, listed: "idle", composer: "❯ half a thought", outcome: nodeJobReapPaneChanged},
		{name: "unknown job", outcome: nodeJobReapNotAJob},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			if test.events != nil {
				markLostJobEvents(t, inbox, "job-a", test.events...)
			}
			pane, label := test.pane, test.label
			if pane == "" {
				pane = "w1:p1"
			}
			if label == "" {
				label = "job-a"
			}
			composer := test.composer
			if composer == "" {
				composer = "❯ "
			}
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON(pane, label, test.listed, "/work/a"))}, read: nodeReapScreen(composer)}
			client := &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}
			result, code := nodeRun(t, client, nodeCommandJobReap, nodeJobReapArgs{Job: "job-a", Lane: "lane-a", Force: test.force})
			if code != "" || result["outcome"] != test.outcome {
				t.Fatalf("result=%v code=%q", result, code)
			}
			if closes := fake.called("pane", "close"); (test.outcome == nodeJobReapClosed) != (closes == 1) {
				t.Fatalf("outcome=%s but closes=%d", test.outcome, closes)
			}
			if test.outcome == nodeJobReapClosed && result["pane"] != "w1:p1" {
				t.Fatalf("closed pane=%v", result["pane"])
			}
		})
	}
}

// TestNodeJobReapRefusesAPaneThatGoesBackToWork is the settle rule: a pane
// that read idle and then reports working one interval later survives.
func TestNodeJobReapRefusesAPaneThatGoesBackToWork(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false))
	fake := &nodeFakeHerdr{
		agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a")), nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))},
		read:   nodeReapScreen("❯ "),
	}
	client := &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}
	result, code := nodeRun(t, client, nodeCommandJobReap, nodeJobReapArgs{Job: "job-a", Lane: "lane-a"})
	if code != "" || result["outcome"] != nodeJobReapPaneChanged || fake.called("pane", "close") != 0 {
		t.Fatalf("result=%v code=%q calls=%v", result, code, fake.calls)
	}
}

// TestNodeJobReapUnsetJobsRoot: a node that cannot see the job's ledger proves
// nothing, so it answers the code the other job ops answer.
func TestNodeJobReapUnsetJobsRoot(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, read: nodeReapScreen("❯ ")}
	if _, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandJobReap, nodeJobReapArgs{Job: "job-a", Lane: "lane-a"}); code != "jobs_root_unset" {
		t.Fatalf("code=%q", code)
	}
}

// TestNodeJobReapIsNotAPublicCommand: job.reap is issued by the hub after its
// own ownership checks, so no credential reaches it through the public
// node-command endpoint, and its arguments are a closed shape.
func TestNodeJobReapIsNotAPublicCommand(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	args := map[string]any{"job": "job-a", "lane": "lane-a", "force": false}
	if status, _, contents := t14HTTP(t, "POST", nodeCommandURL(server, "machine-a"), t14OperatorToken, map[string]any{"op": "job.reap", "args": args}); status != 403 {
		t.Fatalf("operator status=%d body=%s", status, contents)
	}
	for _, body := range []any{
		map[string]any{"op": "job.reap", "args": map[string]any{"job": "job-a", "lane": "lane-a"}},
		map[string]any{"op": "job.reap", "args": map[string]any{"job": "job-a", "lane": "lane-a", "force": false, "extra": 1}},
		map[string]any{"op": "job.reap", "args": map[string]any{"job": "../job-a", "lane": "lane-a", "force": false}},
		map[string]any{"op": "job.reap", "args": map[string]any{"job": "job-a", "lane": "bad/name", "force": false}},
	} {
		if status, _, contents := t14HTTP(t, "POST", nodeCommandURL(server, "machine-a"), t14OperatorToken, body); status != 400 {
			t.Fatalf("body=%v status=%d response=%s", body, status, contents)
		}
	}
	// Nothing reached the node: the next frame it reads is the command below.
	results := t14HTTPAsync(t, "POST", nodeCommandURL(server, "machine-a"), t14OperatorToken, map[string]any{"op": "agents.list"})
	command := nodeCommandRead(t, connection)
	if string(command["op"]) != `"agents.list"` {
		t.Fatalf("first command the node saw: %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"agents": []any{}, "panes": []any{}, "panes_ok": true}})
	if result := t14Result(t, results); result.status != 200 {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
}

// TestNodeJobReapResultShape keeps the result vocabulary closed: the hub
// re-encodes a node's answer through it.
func TestNodeJobReapResultShape(t *testing.T) {
	for _, test := range []struct {
		result nodeJobReapResult
		valid  bool
	}{
		{nodeJobReapResult{Outcome: nodeJobReapClosed, Pane: "w1:p1"}, true},
		{nodeJobReapResult{Outcome: nodeJobReapClosed}, false},
		{nodeJobReapResult{Outcome: nodeJobReapPaneGone, Pane: "w1:p1"}, true},
		{nodeJobReapResult{Outcome: nodeJobReapJobWorking, Pane: "w1:p1"}, true},
		{nodeJobReapResult{Outcome: nodeJobReapProtected}, true},
		{nodeJobReapResult{Outcome: nodeJobReapNotAJob}, true},
		{nodeJobReapResult{Outcome: nodeJobReapClosed, Pane: "bad pane"}, false},
		{nodeJobReapResult{Outcome: "deleted"}, false},
	} {
		if got := validNodeJobReapResult(test.result); got != test.valid {
			t.Errorf("%+v: %t", test.result, got)
		}
	}
}
