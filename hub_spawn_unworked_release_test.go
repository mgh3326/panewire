package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// unworkedNode answers every node command on connection from replies, keyed by
// op, and records the op and raw args of each command it saw. An op with no
// reply is answered with herdr_failed, so a command the hub should not have
// sent shows up in ops instead of hanging the hub.
type unworkedNode struct {
	mu      sync.Mutex
	ops     []string
	args    map[string]json.RawMessage
	replies map[string]map[string]any
}

func startUnworkedNode(t *testing.T, connection *websocket.Conn, replies map[string]map[string]any) *unworkedNode {
	t.Helper()
	node := &unworkedNode{args: map[string]json.RawMessage{}, replies: replies}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		for {
			_, payload, err := connection.Read(ctx)
			if err != nil {
				return
			}
			var command struct {
				Type      string          `json:"type"`
				RequestID string          `json:"request_id"`
				Op        string          `json:"op"`
				Args      json.RawMessage `json:"args"`
			}
			if json.Unmarshal(payload, &command) != nil || command.Type != "node.command" {
				continue
			}
			node.mu.Lock()
			node.ops = append(node.ops, command.Op)
			node.args[command.Op] = command.Args
			reply, ok := node.replies[command.Op]
			node.mu.Unlock()
			message := map[string]any{"type": "node.command.result", "request_id": command.RequestID, "op": command.Op}
			if !ok {
				message["error"] = "herdr_failed"
			}
			for key, value := range reply {
				message[key] = value
			}
			if wsjson.Write(ctx, connection, message) != nil {
				return
			}
		}
	}()
	return node
}

func (n *unworkedNode) seen() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.ops...)
}

// unworkedRow is the pane row of an idle agent in the held pane's checkout.
func unworkedRow(status, cwd string) nodeAgentRow {
	return nodeAgentRow{PaneID: roleHeldPane, Name: "test-job", AgentStatus: status, Agent: "claude", Cwd: cwd}
}

func unworkedReplies(row nodeAgentRow, ledger map[string]any) map[string]map[string]any {
	return map[string]map[string]any{
		"agents.list":   {"result": map[string]any{"agents": []nodeAgentRow{row}, "panes": []nodeAgentRow{row}, "panes_ok": true}},
		"job.result":    {"result": ledger},
		"pane.capture":  {"result": map[string]any{"outcome": "screen", "saved": "/inbox/jobs/test-job/pane-screen.txt"}},
		"pane.close":    {"result": map[string]any{"outcome": "closed"}},
		"job.mark_lost": {"result": map[string]any{"outcome": "lost"}},
	}
}

// unworkedLedger is a job.result body: a found job with no report, no
// completion and the given count of events after the claim.
func unworkedLedger(activity any) map[string]any {
	ledger := map[string]any{"found": true, "findings": nil, "findings_error": "missing", "report": map[string]any{"present": false, "bytes": 0, "sha256": "", "last_line": ""}, "completed": false}
	if activity != nil {
		ledger["activity_events"] = activity
	}
	return ledger
}

// TestRoleReleaseUnworkedLanding: a failed-before-brief holder whose pane is
// still there is released only when every proof of "never worked" holds
// (hk 3179). Each case breaks exactly one proof.
func TestRoleReleaseUnworkedLanding(t *testing.T) {
	const checkoutCwd = "/work/project.test-job"
	allOps := []string{"agents.list", "job.result", "pane.capture", "pane.close"}
	releasedOps := append(append([]string(nil), allOps...), "job.mark_lost")
	reportLedger := unworkedLedger(0)
	reportLedger["report"] = map[string]any{"present": true, "bytes": 12, "sha256": strings.Repeat("a", 64), "last_line": "done"}
	completedLedger := unworkedLedger(1)
	completedLedger["completed"] = true
	cases := []struct {
		name string
		// outcome overrides the holder's recorded failure.
		outcome *hubSpawnOutcome
		// since is how long after the holder's deadline the new submission arrives.
		since   time.Duration
		replies func() map[string]map[string]any
		// disconnected leaves the node unconnected.
		disconnected bool
		wantOps      []string
		released     bool
	}{
		{name: "idle past the grace with an empty ledger releases", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
		}, wantOps: releasedOps, released: true},
		{name: "done past the grace releases", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("done", checkoutCwd), unworkedLedger(0))
		}, wantOps: releasedOps, released: true},
		{name: "a lane event in the ledger keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(1))
		}, wantOps: allOps[:2]},
		{name: "a report keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), reportLedger)
		}, wantOps: allOps[:2]},
		{name: "job.completed keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), completedLedger)
		}, wantOps: allOps[:2]},
		{name: "a node that predates the ledger count keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(nil))
		}, wantOps: allOps[:2]},
		{name: "an unknown job keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			replies := unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
			replies["job.result"] = map[string]any{"result": map[string]any{"found": false, "findings": nil, "findings_error": "", "report": map[string]any{"present": false, "bytes": 0, "sha256": "", "last_line": ""}, "completed": false}}
			return replies
		}, wantOps: allOps[:2]},
		{name: "a job.result error keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			replies := unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
			replies["job.result"] = map[string]any{"error": "result_failed"}
			return replies
		}, wantOps: allOps[:2]},
		{name: "a working pane keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("working", checkoutCwd), unworkedLedger(0))
		}, wantOps: allOps[:1]},
		{name: "inside the grace keeps the reservation", since: 9 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
		}, wantOps: allOps[:1]},
		{name: "a pane in another checkout keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", "/work/other"), unworkedLedger(0))
		}, wantOps: allOps[:1]},
		{name: "a launch failure with unworked proof releases", since: 11 * time.Minute, outcome: &hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}, replies: func() map[string]map[string]any {
			return unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
		}, wantOps: releasedOps, released: true},
		{name: "an unreachable node keeps the reservation", since: 11 * time.Minute, disconnected: true, replies: func() map[string]map[string]any { return nil }},
		{name: "a capture that is not the screen keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			replies := unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
			replies["pane.capture"] = map[string]any{"result": map[string]any{"outcome": "pane-changed"}}
			return replies
		}, wantOps: allOps[:3]},
		{name: "a pane that changed before the close keeps the reservation", since: 11 * time.Minute, replies: func() map[string]map[string]any {
			replies := unworkedReplies(unworkedRow("idle", checkoutCwd), unworkedLedger(0))
			replies["pane.close"] = map[string]any{"result": map[string]any{"outcome": "pane-changed"}}
			return replies
		}, wantOps: allOps},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}
			if tc.outcome != nil {
				outcome = *tc.outcome
			}
			hub, server, jobs, held := roleReleaseHubFor(t, outcome)
			jobs.putErr = errors.New("job_host_forbidden")
			// The clock is fixed relative to the holder's deadline so the
			// default 10 minute grace is crossed exactly when the case says.
			now := held.Deadline.Add(tc.since)
			hub.now = func() time.Time { return now }
			var node *unworkedNode
			if !tc.disconnected {
				node = startUnworkedNode(t, t14Node(t, hub, server, true), tc.replies())
			}
			result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
			if node != nil && !reflect.DeepEqual(node.seen(), tc.wantOps) {
				t.Fatalf("node commands %v, want %v", node.seen(), tc.wantOps)
			}
			if tc.released {
				if result.status != http.StatusAccepted {
					t.Fatalf("status=%d body=%s", result.status, result.body)
				}
				// The holder's node wrote the record; the hub's client made no write.
				if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
					t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
				}
				owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:worker")
				if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
					t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
				}
				var mark, capture, closeArgs map[string]any
				_ = json.Unmarshal(node.args["job.mark_lost"], &mark)
				if mark["job"] != "test-job" || mark["pane_id"] != roleHeldPane || mark["label"] != "test-job" {
					t.Fatalf("mark lost args %v", mark)
				}
				_ = json.Unmarshal(node.args["pane.capture"], &capture)
				_ = json.Unmarshal(node.args["pane.close"], &closeArgs)
				if capture["pane_id"] != roleHeldPane || capture["name"] != "test-job" || capture["cwd"] != checkoutCwd || capture["write"] != true || capture["merged_after"] != float64(0) {
					t.Fatalf("capture args %v", capture)
				}
				if closeArgs["pane_id"] != roleHeldPane || closeArgs["name"] != "test-job" || closeArgs["cwd"] != checkoutCwd || closeArgs["agent_status"] != node.replyStatus() {
					t.Fatalf("close args %v", closeArgs)
				}
				return
			}
			var body map[string]string
			_ = json.Unmarshal(result.body, &body)
			if result.status != http.StatusConflict || body["pane"] != roleHeldPane || !strings.Contains(body["fix"], "close pane "+roleHeldPane) {
				t.Fatalf("status=%d body=%s", result.status, result.body)
			}
			if jobs.state("test-job") != "claimed" {
				t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
			}
			assertRoleHeldBy(t, hub, held.Request.RequestID)
		})
	}
}

func TestRoleReleaseFailedLaunchWithUnnamedPane(t *testing.T) {
	const checkoutCwd = "/work/project.test-job"
	hub, server, jobs, held := roleHolderHub(t,
		hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane},
		"claimed", "failed")
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := t14Node(t, hub, server, true)
	unworked := startUnworkedNode(t, node, unworkedReplies(
		nodeAgentRow{PaneID: roleHeldPane, AgentStatus: "idle", Cwd: checkoutCwd},
		unworkedLedger(0),
	))
	response := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
	if response.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.status, response.body)
	}
	if got, want := unworked.seen(), []string{"agents.list", "job.result", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node commands=%v, want %v", got, want)
	}
	if got := unworked.args["pane.capture"]; !strings.Contains(string(got), `"allow_unnamed":true`) {
		t.Fatalf("unnamed pane capture args=%s", got)
	}
	if got := unworked.args["pane.close"]; !strings.Contains(string(got), `"allow_unnamed":true`) {
		t.Fatalf("unnamed pane close args=%s", got)
	}
	if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
		t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
	}
	if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:worker"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
		t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
	}
}

// replyStatus is the agent_status the node's agents.list reply carried.
func (n *unworkedNode) replyStatus() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var list struct {
		Agents []nodeAgentRow `json:"agents"`
	}
	raw, _ := json.Marshal(n.replies["agents.list"]["result"])
	_ = json.Unmarshal(raw, &list)
	if len(list.Agents) == 0 {
		return ""
	}
	return list.Agents[0].AgentStatus
}

// A configured grace replaces the default: with a one minute grace the same
// pane that the default keeps held is released.
func TestRoleReleaseUnworkedLandingConfiguredGrace(t *testing.T) {
	hub, server, jobs, held := roleReleaseHub(t)
	hub.unworkedLandingGrace = time.Minute
	jobs.putErr = errors.New("job_host_forbidden")
	now := held.Deadline.Add(2 * time.Minute)
	hub.now = func() time.Time { return now }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), unworkedLedger(0)))
	if result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())); result.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s ops=%v", result.status, result.body, node.seen())
	}
	if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
		t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
	}
}
