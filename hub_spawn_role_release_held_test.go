package panewire

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

const heldRoleLogMessage = "hub held a spawn role reservation"

// captureHubLog swaps hub's logger for a JSON handler on a buffer.
func captureHubLog(hub *HubServer) *lockedBuffer {
	sink := &lockedBuffer{}
	hub.logger = slog.New(slog.NewJSONHandler(sink, nil))
	return sink
}

// heldRoleLines returns the decoded held-reservation lines in sink.
func heldRoleLines(t *testing.T, sink *lockedBuffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		if line["msg"] == heldRoleLogMessage {
			lines = append(lines, line)
		}
	}
	return lines
}

// An escalated hk job is terminal (hk never rewrites it), so its holder frees
// the role at once, with no node probe.
func TestRoleReleaseOfEscalatedHolder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome hubSpawnOutcome
		status  string
	}{
		{"succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "succeeded"},
		{"failed", hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, tc.outcome, "escalated", tc.status)
			sink := captureHubLog(hub)
			// A connected node is never asked: terminal state alone releases.
			t14Node(t, hub, server, true)
			result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
			if result.status != http.StatusAccepted || len(jobs.writes()) != 0 {
				t.Fatalf("status=%d body=%s writes=%v", result.status, result.body, jobs.writes())
			}
			if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
				t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
			}
			if lines := heldRoleLines(t, sink); len(lines) != 0 {
				t.Fatalf("a released role logged %v", lines)
			}
		})
	}
}

// A succeeded holder whose hk job has no record at all releases when its node
// proves the pane absent, and holds on every weaker answer.
func TestRoleReleaseOfRecordlessHolder(t *testing.T) {
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	setup := func(t *testing.T, outcome hubSpawnOutcome) (*HubServer, *httptest.Server, *roleJobs, hubSpawnTask) {
		hub, server, jobs, held := roleHolderHub(t, outcome, "spawned", "succeeded")
		delete(jobs.records, "test-job")
		return hub, server, jobs, held
	}
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("pane absent releases without an hk write", func(t *testing.T) {
		hub, server, jobs, held := setup(t, succeeded)
		sink := captureHubLog(hub)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		result := t14Result(t, response)
		if result.status != http.StatusAccepted || len(jobs.writes()) != 0 {
			t.Fatalf("status=%d body=%s writes=%v", result.status, result.body, jobs.writes())
		}
		if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
		}
		if lines := heldRoleLines(t, sink); len(lines) != 0 {
			t.Fatalf("a released role logged %v", lines)
		}
	})
	holds := []struct {
		name         string
		outcome      hubSpawnOutcome
		reply        func(t *testing.T, node nodeConn)
		probe, why   string
		connectNode  bool
		wantProbeCmd bool
	}{
		{name: "pane present", outcome: succeeded, connectNode: true, wantProbeCmd: true, probe: "present", why: "pane_listed",
			reply: func(t *testing.T, node nodeConn) {
				roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), []nodeAgentRow{{PaneID: roleHeldPane, Cwd: "/work/project.test-job"}}, true)
			}},
		{name: "agent label under another pane id", outcome: succeeded, connectNode: true, wantProbeCmd: true, probe: "present", why: "agent_label_listed",
			reply: func(t *testing.T, node nodeConn) {
				roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), []nodeAgentRow{{PaneID: "w3:p1", Name: "test-job", Cwd: "/work/project.test-job"}}, true)
			}},
		{name: "panes_ok false", outcome: succeeded, connectNode: true, wantProbeCmd: true, probe: "unknown", why: "panes_ok_false",
			reply: func(t *testing.T, node nodeConn) {
				roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), nil, false)
			}},
		{name: "node error", outcome: succeeded, connectNode: true, wantProbeCmd: true, probe: "unknown", why: "node_error",
			reply: func(t *testing.T, node nodeConn) {
				nodeCommandReplyTo(t, node.conn, nodeCommandRead(t, node.conn), map[string]any{"error": "herdr_failed"})
			}},
		{name: "node unreachable", outcome: succeeded, probe: "unknown", why: "node_unreachable"},
		{name: "no recorded pane", outcome: hubSpawnOutcome{RC: 0, Host: "machine-a"}, connectNode: true, probe: "unknown", why: "no_pane_id"},
	}
	for _, tc := range holds {
		t.Run(tc.name+" holds", func(t *testing.T) {
			hub, server, jobs, held := setup(t, tc.outcome)
			sink := captureHubLog(hub)
			var node nodeConn
			if tc.connectNode {
				node.conn = t14Node(t, hub, server, true)
			}
			response := submit(t, server)
			if tc.reply != nil {
				tc.reply(t, node)
			}
			result := t14Result(t, response)
			if result.status != http.StatusConflict || len(jobs.writes()) != 0 {
				t.Fatalf("status=%d body=%s writes=%v", result.status, result.body, jobs.writes())
			}
			assertRoleHeldBy(t, hub, held.Request.RequestID)
			assertHeldLine(t, sink, "recordless_pane_held", "missing", tc.probe, tc.why)
		})
	}
}

// An hk read error is not "missing": the holder may be live, so it holds
// without asking the node.
func TestRoleHeldOnHKReadError(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	sink := captureHubLog(hub)
	jobs.getErr = map[string]error{"test-job": errors.New("hk unavailable")}
	t14Node(t, hub, server, true)
	result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
	if result.status != http.StatusConflict || len(jobs.writes()) != 0 {
		t.Fatalf("status=%d body=%s writes=%v", result.status, result.body, jobs.writes())
	}
	assertRoleHeldBy(t, hub, held.Request.RequestID)
	assertHeldLine(t, sink, "hk_read_error", "error", "unknown", "not_probed")
}

// Every refused reclaim leaves exactly one INFO line naming the branch that
// held, for the holders the hub kept before this change too.
func TestRoleHeldLogsOneLinePerBranch(t *testing.T) {
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("succeeded holder with a live hk record and its pane present", func(t *testing.T) {
		hub, server, _, _ := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
		sink := captureHubLog(hub)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: roleHeldPane}}, true)
		if result := t14Result(t, response); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		assertHeldLine(t, sink, "succeeded_pane_held", "spawned", "present", "pane_listed")
	})
	t.Run("failed holder whose node is unreachable", func(t *testing.T) {
		hub, server, _, _ := roleReleaseHub(t)
		sink := captureHubLog(hub)
		if result := t14Result(t, submit(t, server)); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		assertHeldLine(t, sink, "failed_pane_held", "claimed", "unknown", "node_unreachable")
	})
	t.Run("failed holder with no hk record but a recorded pane", func(t *testing.T) {
		hub, server, jobs, _ := roleReleaseHub(t)
		sink := captureHubLog(hub)
		delete(jobs.records, "test-job")
		if result := t14Result(t, submit(t, server)); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		assertHeldLine(t, sink, "failed_record_missing", "missing", "unknown", "not_probed")
	})
}

// nodeConn lets a table row leave the node unconnected.
type nodeConn struct{ conn *websocket.Conn }

// assertHeldLine asserts sink holds exactly one held-reservation line with the
// given branch, hk read and probe, naming the holder, and that nothing the hub
// logged carries a token or brief text.
func assertHeldLine(t *testing.T, sink *lockedBuffer, branch, hk, probe, reason string) {
	t.Helper()
	lines := heldRoleLines(t, sink)
	if len(lines) != 1 {
		t.Fatalf("held lines=%d log=%s", len(lines), sink.String())
	}
	line := lines[0]
	want := map[string]string{"level": "INFO", "branch": branch, "hk_read": hk, "probe": probe, "probe_reason": reason, "role_key": "42:implementer", "holder_job": "test-job", "holder_request": durableSpawnRequest().RequestID, "job": "test-job-r2", "request": roleSecondRequest().RequestID}
	for key, value := range want {
		if line[key] != value {
			t.Fatalf("%s=%v, want %q; line=%v", key, line[key], value, line)
		}
	}
	for _, secret := range []string{"Implement the task", t14OperatorToken, "node-token", "/work/brief.md"} {
		if strings.Contains(sink.String(), secret) {
			t.Fatalf("log carries %q: %s", secret, sink.String())
		}
	}
}
