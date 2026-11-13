package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reviewerReleaseRequest is the holder's spawn for task 42 under role (and
// review pass 2 for a reviewer); next is the same key under a new job.
func reviewerReleaseRequest(role string, next bool) fleet.Submission {
	request := durableSpawnRequest()
	request.Args = append([]string(nil), request.Args...)
	for i, arg := range request.Args {
		switch {
		case arg == "implementer":
			request.Args[i] = role
		case next && arg == "test-job":
			request.Args[i] = "test-job-r2"
		}
	}
	if role == "reviewer" {
		request.Args = append(request.Args, "--review-pass", "2")
	}
	if next {
		request.RequestID = "d676220b-e763-4af1-aefd-000000000002"
	}
	return request
}

func reviewerReleaseKey(role string) string {
	if role == "reviewer" {
		return "42:reviewer:p2"
	}
	return "42:" + role
}

// reviewerReleaseHub holds task 42 under role with an operation that ended in
// wantStatus with outcome and an hk job in state spawned (a failed holder is
// claimed).
func reviewerReleaseHub(t *testing.T, role, wantStatus string, outcome hubSpawnOutcome) (*HubServer, *httptest.Server, hubSpawnTask) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	state := "spawned"
	if wantStatus == "failed" {
		state = "claimed"
	}
	jobs := &roleJobs{records: map[string]map[string]any{"test-job": {"state": state}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), reviewerReleaseRequest(role, false), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, outcome, time.Now()); err != nil {
		t.Fatal(err)
	}
	held, err = hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID)
	if err != nil || held.Status != wantStatus {
		t.Fatalf("holder %+v %v", held, err)
	}
	return hub, server, held
}

// A reviewer pane that finished, or sits at a startup prompt, past the grace
// is closed and its job marked lost, so a new review of the task is not held
// (hk 3315). Each keep case breaks exactly one condition.
func TestReviewerRelease(t *testing.T) {
	const cwd = "/work/project.test-job"
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	failed := hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}
	failedLaunch := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}
	row := func(status string) nodeAgentRow { return unworkedRow(status, cwd) }
	closeOps := []string{"agents.list", "pane.capture", "pane.close", "job.mark_lost"}
	cases := []struct {
		name    string
		role    string
		status  string
		outcome hubSpawnOutcome
		pane    nodeAgentRow
		// since is how long after the holder's deadline the new submission arrives.
		since time.Duration
		// disconnected leaves the node unconnected.
		disconnected bool
		wantOps      []string
		wantReason   string
	}{
		{name: "succeeded reviewer with a done pane past the grace", role: "reviewer", status: "succeeded", outcome: succeeded, pane: row("done"), since: 11 * time.Minute, wantOps: closeOps, wantReason: "reviewer_pane_finished"},
		{name: "succeeded reviewer with an idle pane past the grace", role: "reviewer", status: "succeeded", outcome: succeeded, pane: row("idle"), since: 11 * time.Minute, wantOps: closeOps, wantReason: "reviewer_pane_finished"},
		{name: "failed reviewer blocked at a startup prompt past the grace", role: "reviewer", status: "failed", outcome: failed, pane: row("blocked"), since: 11 * time.Minute, wantOps: closeOps, wantReason: "reviewer_pane_stuck"},
		{name: "failed reviewer blocked after a launch failure past the grace", role: "reviewer", status: "failed", outcome: failedLaunch, pane: row("blocked"), since: 11 * time.Minute, wantOps: closeOps, wantReason: "reviewer_pane_stuck"},
		{name: "a working pane keeps the hold", role: "reviewer", status: "succeeded", outcome: succeeded, pane: row("working"), since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "a working pane of a failed holder keeps the hold", role: "reviewer", status: "failed", outcome: failedLaunch, pane: row("working"), since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "a pane inside the grace keeps the hold", role: "reviewer", status: "succeeded", outcome: succeeded, pane: row("done"), since: 9 * time.Minute, wantOps: closeOps[:1]},
		{name: "a blocked pane inside the grace keeps the hold", role: "reviewer", status: "failed", outcome: failedLaunch, pane: row("blocked"), since: 9 * time.Minute, wantOps: closeOps[:1]},
		{name: "an implementer keeps the hold", role: "implementer", status: "succeeded", outcome: succeeded, pane: row("done"), since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "a failed implementer blocked at a prompt keeps the hold", role: "implementer", status: "failed", outcome: failedLaunch, pane: row("blocked"), since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "a pane under another label keeps the hold", role: "reviewer", status: "succeeded", outcome: succeeded, pane: nodeAgentRow{PaneID: roleHeldPane, Name: "other-label", AgentStatus: "done", Agent: "claude", Cwd: cwd}, since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "a plain shell pane with no status keeps the hold", role: "reviewer", status: "succeeded", outcome: succeeded, pane: nodeAgentRow{PaneID: roleHeldPane, Name: "test-job", Cwd: cwd}, since: 11 * time.Minute, wantOps: closeOps[:1]},
		{name: "an unreachable node keeps the hold", role: "reviewer", status: "succeeded", outcome: succeeded, pane: row("done"), since: 11 * time.Minute, disconnected: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, held := reviewerReleaseHub(t, tc.role, tc.status, tc.outcome)
			sink := captureHubLog(hub)
			now := held.Deadline.Add(tc.since)
			hub.now = func() time.Time { return now }
			var node *unworkedNode
			if !tc.disconnected {
				node = startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(tc.pane, unworkedLedger(0)))
			}
			result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, reviewerReleaseRequest(tc.role, true)))
			if node != nil && !reflect.DeepEqual(node.seen(), tc.wantOps) {
				t.Fatalf("node commands %v, want %v", node.seen(), tc.wantOps)
			}
			key := reviewerReleaseKey(tc.role)
			owner, job, _, _ := hub.inbox.store.spawnRoleOwner(t.Context(), key)
			if tc.wantReason == "" {
				if result.status != http.StatusConflict || owner != held.Request.RequestID {
					t.Fatalf("hold lost: status=%d body=%s owner=%s", result.status, result.body, owner)
				}
				if strings.Contains(sink.String(), "closed a finished reviewer pane") {
					t.Fatalf("release logged for a kept hold: %s", sink.String())
				}
				return
			}
			if result.status != http.StatusAccepted || job != "test-job-r2" || owner == held.Request.RequestID {
				t.Fatalf("not released: status=%d body=%s owner=%s job=%s", result.status, result.body, owner, job)
			}
			var capture, closeArgs, mark map[string]any
			_ = json.Unmarshal(node.args["pane.capture"], &capture)
			_ = json.Unmarshal(node.args["pane.close"], &closeArgs)
			_ = json.Unmarshal(node.args["job.mark_lost"], &mark)
			if capture["pane_id"] != roleHeldPane || capture["name"] != "test-job" || capture["write"] != true {
				t.Fatalf("capture args %v", capture)
			}
			if closeArgs["pane_id"] != roleHeldPane || closeArgs["name"] != "test-job" || closeArgs["cwd"] != cwd || closeArgs["agent_status"] != tc.pane.AgentStatus {
				t.Fatalf("close args %v", closeArgs)
			}
			if mark["job"] != "test-job" || mark["pane_id"] != roleHeldPane || mark["label"] != "test-job" {
				t.Fatalf("mark lost args %v", mark)
			}
			if logged := sink.String(); strings.Count(logged, "closed a finished reviewer pane") != 1 || !strings.Contains(logged, `"branch":"`+tc.wantReason+`"`) {
				t.Fatalf("log %q lacks one release line with branch %s", logged, tc.wantReason)
			}
		})
	}
}

// pane.close accepts blocked, but a pane that changes between the node's two
// reads is refused: the hub keeps the hold and writes nothing to hk.
func TestReviewerReleaseKeepsHoldWhenCloseIsRefused(t *testing.T) {
	hub, server, held := reviewerReleaseHub(t, "reviewer", "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	now := held.Deadline.Add(11 * time.Minute)
	hub.now = func() time.Time { return now }
	replies := unworkedReplies(unworkedRow("done", "/work/project.test-job"), unworkedLedger(0))
	replies["pane.close"] = map[string]any{"result": map[string]any{"outcome": "pane-changed"}}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), replies)
	result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, reviewerReleaseRequest("reviewer", true)))
	if want := []string{"agents.list", "pane.capture", "pane.close"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p2"); result.status != http.StatusConflict || owner != held.Request.RequestID {
		t.Fatalf("hold lost: status=%d owner=%s", result.status, owner)
	}
}
