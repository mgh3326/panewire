package panewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A holder that ran on another host is marked lost by its own node: the hub's
// hk client holds no grant there, so a direct write would be refused (403).
func TestRoleReleaseMarksLostThroughHolderNode(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	jobs.putErr = errors.New("job_host_forbidden")
	node := t14Node(t, hub, server, true)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	mark := roleReplyMarkLost(t, node, nodeJobMarkLostLost)
	args := roleMarkLostArgs(t, mark)
	if args["job"] != "test-job" || args["pane_id"] != roleHeldPane || args["label"] != "test-job" {
		t.Fatalf("mark lost args=%v", args)
	}
	result := t14Result(t, response)
	if result.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if len(jobs.writes()) != 0 || jobs.state("test-job") != "spawned" {
		t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
	}
	owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
		t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
	}
}

// Every refusal the holder's node can answer keeps the reservation, and the
// log line names the node's reason.
func TestRoleReleaseHoldsWithHolderNodeReason(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply func(t *testing.T, node *nodeConn)
		why   string
	}{
		{"pane still listed", func(t *testing.T, node *nodeConn) {
			roleReplyMarkLost(t, node.conn, nodeJobMarkLostPaneListed)
		}, nodeJobMarkLostPaneListed},
		{"label still listed", func(t *testing.T, node *nodeConn) {
			roleReplyMarkLost(t, node.conn, nodeJobMarkLostLabelListed)
		}, nodeJobMarkLostLabelListed},
		{"completion in the ledger", func(t *testing.T, node *nodeConn) {
			roleReplyMarkLost(t, node.conn, nodeJobMarkLostCompleted)
		}, nodeJobMarkLostCompleted},
		{"newer claim in the ledger", func(t *testing.T, node *nodeConn) {
			roleReplyMarkLost(t, node.conn, nodeJobMarkLostNewerClaim)
		}, nodeJobMarkLostNewerClaim},
		{"pane list unreadable", func(t *testing.T, node *nodeConn) {
			roleReplyMarkLost(t, node.conn, nodeJobMarkLostPanesUnknown)
		}, nodeJobMarkLostPanesUnknown},
		{"node error", func(t *testing.T, node *nodeConn) {
			nodeCommandReplyTo(t, node.conn, nodeCommandRead(t, node.conn), map[string]any{"error": "mark_lost_failed"})
		}, "mark_lost_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
			sink := captureHubLog(hub)
			node := &nodeConn{conn: t14Node(t, hub, server, true)}
			response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
			roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
			test.reply(t, node)
			result := t14Result(t, response)
			if result.status != http.StatusConflict || jobs.state("test-job") != "spawned" || len(jobs.writes()) != 0 {
				t.Fatalf("status=%d body=%s hk=%v writes=%v", result.status, result.body, jobs.state("test-job"), jobs.writes())
			}
			assertRoleHeldBy(t, hub, held.Request.RequestID)
			assertHeldLine(t, sink, "release_write_failed", "spawned", "absent", test.why)
		})
	}
}

// A node that never answers the mark command keeps the reservation, and the
// hold is logged with the timeout.
func TestRoleReleaseHoldsWhenHolderNodeDoesNotAnswer(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	hub.nodeCommandWait = 100 * time.Millisecond
	sink := captureHubLog(hub)
	node := t14Node(t, hub, server, true)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	result := t14Result(t, response)
	if result.status != http.StatusConflict || jobs.state("test-job") != "spawned" || len(jobs.writes()) != 0 {
		t.Fatalf("status=%d body=%s hk=%v writes=%v", result.status, result.body, jobs.state("test-job"), jobs.writes())
	}
	assertRoleHeldBy(t, hub, held.Request.RequestID)
	assertHeldLine(t, sink, "release_write_failed", "spawned", "absent", "mark_lost_timeout")
}

// The hub writes the record itself only for a holder on its own host, whose
// host grant it holds.
func TestRoleReleaseWritesHKLostOnHubOwnHost(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	env := filepath.Join(t.TempDir(), "hub-node.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=machine-a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_NODE_ENV", env)
	node := startUnworkedNode(t, t14Node(t, hub, server, true), map[string]map[string]any{
		"agents.list": {"result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}},
	})
	result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
	if result.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if jobs.state("test-job") != "lost" || len(jobs.writes()) != 1 || jobs.writes()[0] != "test-job lost" {
		t.Fatalf("hub state=%v writes=%v", jobs.state("test-job"), jobs.writes())
	}
	if seen := node.seen(); len(seen) != 1 || seen[0] != nodeCommandAgentsList {
		t.Fatalf("node saw %v", seen)
	}
	owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
		t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
	}
}

// job.mark_lost is issued by the hub over the holder's connection; it is never
// a public node command, whichever credential presents it.
func TestJobMarkLostIsHubIssuedOnly(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, true)
	body := map[string]any{"op": nodeCommandJobMarkLost, "args": map[string]any{"job": "job-a", "pane_id": "w1:p1", "label": "job-a"}}

	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, nodeCommandURL(server, "machine-a"), bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer node-token")
	request.Header.Set(hubMachineIDHeader, "machine-a")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	contents, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("node credential status=%d body=%s", response.StatusCode, contents)
	}

	// The operator credential is refused too: no scope reaches this op.
	if status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, body); status != http.StatusForbidden {
		t.Fatalf("operator status=%d body=%s", status, contents)
	}

	// Nothing reached the node: the next frame it reads is the command below.
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, map[string]any{"op": "agents.list"})
	command := nodeCommandRead(t, connection)
	if string(command["op"]) != `"agents.list"` {
		t.Fatalf("first command the node saw: %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}})
	if result := t14Result(t, results); result.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
}
