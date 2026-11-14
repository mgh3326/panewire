package panewire

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// roleUnpanedOutcome is a failed operation that recorded no pane id: the shape
// a refusal before the pane id is written leaves. Its herdr agent name (the -l
// label) is still known, so the holder can be probed by that label (hk 4620).
func roleUnpanedOutcome() hubSpawnOutcome {
	return hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a"}
}

// A failed holder with no pane id is probed by its job label. A node whose
// complete pane list lacks that label proves the holder gone and releases the
// role; a listed label, an incomplete list or an unreachable node keeps it.
func TestRoleReleaseOfFailedHolderWithoutPane(t *testing.T) {
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("label not listed releases through a label-only mark lost", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
		sink := captureHubLog(hub)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		command := nodeCommandRead(t, node)
		if string(command["op"]) != `"agents.list"` {
			t.Fatalf("probe=%s", command["op"])
		}
		roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		mark := roleReplyMarkLost(t, node, nodeJobMarkLostLost)
		args := roleMarkLostArgs(t, mark)
		if args["job"] != "test-job" || args["pane_id"] != "" || args["label"] != "test-job" {
			t.Fatalf("mark lost args=%v", args)
		}
		result := t14Result(t, response)
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
			t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
		}
		if lines := heldRoleLines(t, sink); len(lines) != 0 {
			t.Fatalf("a released role logged %v", lines)
		}
	})
	holds := []struct {
		name       string
		connect    bool
		reply      func(t *testing.T, node nodeConn)
		probe, why string
	}{
		{name: "label listed under another pane id", connect: true, probe: "present", why: "agent_label_listed",
			reply: func(t *testing.T, node nodeConn) {
				roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), []nodeAgentRow{{PaneID: "w3:p1", Name: "test-job", Cwd: "/work/project.test-job"}}, true)
			}},
		{name: "incomplete pane list", connect: true, probe: "unknown", why: "panes_ok_false",
			reply: func(t *testing.T, node nodeConn) {
				roleReplyPanes(t, node.conn, nodeCommandRead(t, node.conn), nil, false)
			}},
		{name: "node unreachable", probe: "unknown", why: "node_unreachable"},
	}
	for _, tc := range holds {
		t.Run(tc.name+" holds", func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
			sink := captureHubLog(hub)
			var node nodeConn
			if tc.connect {
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
			assertHeldLine(t, sink, "failed_pane_held", "claimed", tc.probe, tc.why)
		})
	}
	// A node that answers nothing holds on the probe deadline. t14Result waits
	// only 2s, so this calls releaseFailedSpawnRole with a 200ms parent deadline,
	// which the probe's own 5s deadline inherits: the wait is the code under test.
	t.Run("node timeout holds", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
		node := t14Node(t, hub, server, true)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		app := &fleet.App{JobStore: jobs, Out: io.Discard, Err: io.Discard, Now: time.Now}
		released, probe := hub.releaseFailedSpawnRole(ctx, app, held, map[string]any{"state": "claimed"}, "42:implementer")
		if command := nodeCommandRead(t, node); string(command["op"]) != `"agents.list"` {
			t.Fatalf("probe=%s", command["op"])
		}
		if released || probe.state != heldPaneUnknown || probe.reason != "probe_timeout" {
			t.Fatalf("released=%v probe=%+v", released, probe)
		}
		if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
			t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
}
