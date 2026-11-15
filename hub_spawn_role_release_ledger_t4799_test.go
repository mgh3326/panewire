package panewire

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
)

// hk 4799: a failed holder that recorded no pane, whose pane is absent and
// whose node has no ledger left for the job, frees its reservation only when
// the node proves from its own spawn journal and active-job list that no
// attempt of the job is live under any label. hk keeps its claimed record on
// every path, because the node never writes lost over a ledger it cannot check
// (hk 3685).
func TestRoleReleaseOfFailedHolderWithoutPaneOrLedger(t *testing.T) {
	probeAbsent := func(t *testing.T, node *websocket.Conn) {
		t.Helper()
		command := nodeCommandRead(t, node)
		if string(command["op"]) != `"agents.list"` {
			t.Fatalf("probe=%s", command["op"])
		}
		roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	}
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("node proves no live attempt releases", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
		sink := captureHubLog(hub)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		probeAbsent(t, node)
		roleReplyMarkLost(t, node, nodeJobMarkLostNoLiveAttempt)
		result := t14Result(t, response)
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s hk=%v writes=%v", result.status, result.body, jobs.state("test-job"), jobs.writes())
		}
		if jobs.state("test-job") != "claimed" || len(jobs.writes()) != 0 {
			t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
		}
		if lines := heldRoleLines(t, sink); len(lines) != 0 {
			t.Fatalf("a released role logged %v", lines)
		}
	})
	for _, tc := range []struct {
		name  string
		reply func(t *testing.T, node *websocket.Conn)
		why   string
	}{
		{"a newer attempt the node cannot rule out holds", func(t *testing.T, node *websocket.Conn) {
			roleReplyMarkLost(t, node, nodeJobMarkLostLedgerMissing)
		}, nodeJobMarkLostLedgerMissing},
		{"an unreadable proof holds", func(t *testing.T, node *websocket.Conn) {
			command := nodeCommandRead(t, node)
			if string(command["op"]) != `"job.mark_lost"` {
				t.Fatalf("mark lost op=%s", command["op"])
			}
			nodeCommandReplyTo(t, node, command, map[string]any{"error": "mark_lost_failed"})
		}, "mark_lost_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
			sink := captureHubLog(hub)
			node := t14Node(t, hub, server, true)
			response := submit(t, server)
			probeAbsent(t, node)
			tc.reply(t, node)
			result := t14Result(t, response)
			if result.status != http.StatusConflict || len(jobs.writes()) != 0 {
				t.Fatalf("status=%d body=%s writes=%v", result.status, result.body, jobs.writes())
			}
			assertRoleHeldBy(t, hub, held.Request.RequestID)
			assertHeldLine(t, sink, "mark_lost_refused", "claimed", "absent", tc.why)
		})
	}
}
