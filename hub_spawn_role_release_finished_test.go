package panewire

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A holder whose spawn succeeded but whose hk job never reached a terminal
// state (revoked and reaped by an older binary, or ended without writing its
// record) keeps the (task, role) reservation until its node proves the pane
// gone.
func TestRoleReleaseAfterSucceededSpawn(t *testing.T) {
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	submit := func(server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	keepsHold := func(t *testing.T, hub *HubServer, jobs *roleJobs, held hubSpawnTask, result t14HTTPResult, hkState string) {
		t.Helper()
		var body map[string]string
		if err := json.Unmarshal(result.body, &body); err != nil {
			t.Fatal(err)
		}
		// Today's 409 body for a succeeded holder: the generic fix plus the holder's identity.
		if result.status != http.StatusConflict || body["error"] != "task_role_claimed" || body["request_id"] != held.Request.RequestID || body["job"] != "test-job" || body["host"] != "machine-a" || body["pane"] != roleHeldPane || body["fix"] != "inspect the existing operation; do not submit a new request while ownership is live" {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != hkState || len(jobs.writes()) != 0 {
			t.Fatalf("hk job state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	}
	for _, hkState := range []string{"spawned", "claimed"} {
		t.Run("pane absent releases and the holder node marks the "+hkState+" job lost", func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, succeeded, hkState, "succeeded")
			node := t14Node(t, hub, server, true)
			response := submit(server)
			command := nodeCommandRead(t, node)
			if string(command["op"]) != `"agents.list"` {
				t.Fatalf("probe=%s", command["op"])
			}
			roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
			roleReplyMarkLost(t, node, nodeJobMarkLostLost)
			result := t14Result(t, response)
			if result.status != http.StatusAccepted {
				t.Fatalf("status=%d body=%s", result.status, result.body)
			}
			// The hub's client holds no grant for machine-a: it made no write.
			if len(jobs.writes()) != 0 || jobs.state("test-job") != hkState {
				t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
			}
			owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
			if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
				t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
			}
		})
	}
	t.Run("a holder that completes during the probe keeps its completed record and releases", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		jobs.completeAfterGet = true
		node := t14Node(t, hub, server, true)
		response := submit(server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		roleReplyMarkLost(t, node, nodeJobMarkLostCompleted)
		result := t14Result(t, response)
		if result.status != http.StatusAccepted || jobs.state("test-job") != "completed" {
			t.Fatalf("status=%d body=%s hk=%v", result.status, result.body, jobs.state("test-job"))
		}
		if len(jobs.writes()) != 0 {
			t.Fatalf("hub wrote hk: %v", jobs.writes())
		}
		if owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %v", owner, err)
		}
	})
	t.Run("a refused lost write on a record still not terminal keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		sink := captureHubLog(hub)
		node := t14Node(t, hub, server, true)
		response := submit(server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		nodeCommandReplyTo(t, node, nodeCommandRead(t, node), map[string]any{"error": "mark_lost_failed"})
		result := t14Result(t, response)
		if result.status != http.StatusConflict || jobs.state("test-job") != "spawned" {
			t.Fatalf("status=%d body=%s hk=%v", result.status, result.body, jobs.state("test-job"))
		}
		if len(jobs.writes()) != 0 {
			t.Fatalf("hub wrote hk: %v", jobs.writes())
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
		assertHeldLine(t, sink, "release_write_failed", "spawned", "absent", "mark_lost_failed")
	})
	t.Run("pane present keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		node := t14Node(t, hub, server, true)
		response := submit(server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: roleHeldPane, Cwd: "/work/project.test-job"}}, true)
		keepsHold(t, hub, jobs, held, t14Result(t, response), "spawned")
	})
	t.Run("the holder's agent under another pane id keeps the hold", func(t *testing.T) {
		// herdr renumbers a pane moved across workspaces; the live job must not be released.
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		node := t14Node(t, hub, server, true)
		response := submit(server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w3:p1", Name: "test-job", Cwd: "/work/project.test-job"}}, true)
		keepsHold(t, hub, jobs, held, t14Result(t, response), "spawned")
	})
	t.Run("panes_ok false keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		node := t14Node(t, hub, server, true)
		response := submit(server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), nil, false)
		keepsHold(t, hub, jobs, held, t14Result(t, response), "spawned")
	})
	t.Run("node error keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		node := t14Node(t, hub, server, true)
		response := submit(server)
		nodeCommandReplyTo(t, node, nodeCommandRead(t, node), map[string]any{"error": "herdr_failed"})
		keepsHold(t, hub, jobs, held, t14Result(t, response), "spawned")
	})
	t.Run("node unreachable keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		keepsHold(t, hub, jobs, held, t14Result(t, submit(server)), "spawned")
	})
	t.Run("no recorded pane keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a"}, "spawned", "succeeded")
		// A connected node that would answer "absent" must not be asked.
		t14Node(t, hub, server, true)
		result := t14Result(t, submit(server))
		if result.status != http.StatusConflict || jobs.state("test-job") != "spawned" || len(jobs.writes()) != 0 {
			t.Fatalf("status=%d body=%s hk=%v", result.status, result.body, jobs.state("test-job"))
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("hk read error keeps the hold", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, succeeded, "spawned", "succeeded")
		jobs.getErr = map[string]error{"test-job": errors.New("hk unavailable")}
		// A connected node that would answer "absent" must not be asked.
		t14Node(t, hub, server, true)
		keepsHold(t, hub, jobs, held, t14Result(t, submit(server)), "spawned")
	})
}

// A retryable operation (verified pre-landing cleanup) holds no hub
// reservation: completeSpawn deletes it. Its hk job stays `claimed`, and a new
// submission for the same (task, role) under another job id closes that record.
func TestRoleReleaseSupersedesRetryableHolder(t *testing.T) {
	retryable := hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}
	t.Run("another job id has the holder node mark the old job lost and claims", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, retryable, "claimed", "retryable")
		if _, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
			t.Fatal("a retryable operation still holds the reservation")
		}
		node := t14Node(t, hub, server, true)
		response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
		roleReplyMarkLost(t, node, nodeJobMarkLostLost)
		result := t14Result(t, response)
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		// No recorded pane: the node checks the request's label and the ledger.
		if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
			t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
		if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("new request did not claim: %s %s %v", owner, job, err)
		}
	})
	t.Run("a replay of the holder's own request writes nothing", func(t *testing.T) {
		hub, server, jobs, held := roleHolderHub(t, retryable, "claimed", "retryable")
		result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, durableSpawnRequest()))
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "claimed" || len(jobs.writes()) != 0 {
			t.Fatalf("replay wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
		if err != nil || owner != held.Request.RequestID {
			t.Fatalf("replay did not resume its own reservation: %s %v", owner, err)
		}
	})
	t.Run("a replay of the superseded request is refused while the new request holds the role", func(t *testing.T) {
		hub, server, _, _ := roleHolderHub(t, retryable, "claimed", "retryable")
		node := t14Node(t, hub, server, true)
		response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
		roleReplyMarkLost(t, node, nodeJobMarkLostLost)
		if result := t14Result(t, response); result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, durableSpawnRequest()))
		if result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if _, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" {
			t.Fatalf("role moved off the new request: %s %v", job, err)
		}
	})
	t.Run("a record that is no longer claimed is not rewritten", func(t *testing.T) {
		_, server, jobs, _ := roleHolderHub(t, retryable, "completed", "retryable")
		result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
		if result.status != http.StatusAccepted || len(jobs.writes()) != 0 {
			t.Fatalf("status=%d writes=%v", result.status, jobs.writes())
		}
	})
}
