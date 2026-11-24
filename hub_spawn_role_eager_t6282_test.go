package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

// eagerPass runs one eager release pass on its own goroutine, the way
// runEagerRoleRelease does, and returns a channel closed when it ends.
func eagerPass(t *testing.T, hub *HubServer) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.releaseProvenOverRoles(t.Context(), "")
	}()
	return done
}

func awaitPass(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("eager pass did not end")
	}
}

func assertRoleFree(t *testing.T, hub *HubServer) {
	t.Helper()
	if owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatalf("role still held by %s", owner)
	}
}

// a, e: a job hk records ended, whose node lists no pane of it, has its role
// released by the background pass, with no new spawn. The row stays.
// RED on c9769eeb: nothing releases a completed job's role without a spawn.
func TestEagerRoleReleaseProvenOver_t6282(t *testing.T) {
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	for _, state := range []string{"completed", "lost", "escalated"} {
		t.Run(state, func(t *testing.T) {
			hub, server, jobs, held := roleHolderHub(t, succeeded, state, "succeeded")
			node := t14Node(t, hub, server, true)
			done := eagerPass(t, hub)
			command := nodeCommandRead(t, node)
			if string(command["op"]) != `"agents.list"` {
				t.Fatalf("probe=%s", command["op"])
			}
			roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
			awaitPass(t, done)
			assertRoleFree(t, hub)
			if len(jobs.writes()) != 0 {
				t.Fatalf("hk written: %v", jobs.writes())
			}
			if _, err := hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID); err != nil {
				t.Fatalf("operation gone: %v", err)
			}
			if held, err := hub.inbox.store.spawnJobReserved(t.Context(), "test-job"); err != nil || !held {
				t.Fatalf("reservation row deleted: %v %v", held, err)
			}
		})
	}
	t.Run("failed holder", func(t *testing.T) {
		hub, server, _, _ := roleHolderHub(t, hubSpawnOutcome{RC: 1, Reason: "landing_failed", Host: "machine-a", Pane: roleHeldPane}, "completed", "failed")
		node := t14Node(t, hub, server, true)
		done := eagerPass(t, hub)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
		awaitPass(t, done)
		assertRoleFree(t, hub)
	})
}

// b, c, e: a live or unknown pane is never released, and an hk record that has
// not ended is not proof. Mutant killed: release on hk terminal state without
// the probe verdict (drop `probe.state != heldPaneAbsent`).
func TestEagerRoleReleaseKeepsLiveOrUnknownPane_t6282(t *testing.T) {
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	for _, state := range []string{"completed", "lost", "escalated"} {
		t.Run(state+" pane present", func(t *testing.T) {
			hub, server, _, held := roleHolderHub(t, succeeded, state, "succeeded")
			node := t14Node(t, hub, server, true)
			done := eagerPass(t, hub)
			roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: roleHeldPane, Cwd: "/work/project.test-job"}}, true)
			awaitPass(t, done)
			assertRoleHeldBy(t, hub, held.Request.RequestID)
		})
	}
	t.Run("agent label under another pane id", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, succeeded, "completed", "succeeded")
		node := t14Node(t, hub, server, true)
		done := eagerPass(t, hub)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w3:p1", Name: "test-job"}}, true)
		awaitPass(t, done)
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("panes_ok false", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, succeeded, "completed", "succeeded")
		node := t14Node(t, hub, server, true)
		done := eagerPass(t, hub)
		roleReplyPanes(t, node, nodeCommandRead(t, node), nil, false)
		awaitPass(t, done)
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("node error", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, succeeded, "completed", "succeeded")
		node := t14Node(t, hub, server, true)
		done := eagerPass(t, hub)
		nodeCommandReplyTo(t, node, nodeCommandRead(t, node), map[string]any{"error": "herdr_failed"})
		awaitPass(t, done)
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("no recorded pane", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a"}, "completed", "succeeded")
		t14Node(t, hub, server, true)
		awaitPass(t, eagerPass(t, hub))
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	for _, state := range []string{"claimed", "spawned"} {
		// Mutant killed: drop the hkTerminalState check.
		t.Run("hk "+state+" pane absent", func(t *testing.T) {
			hub, server, _, held := roleHolderHub(t, succeeded, state, "succeeded")
			node := t14Node(t, hub, server, true)
			var asked atomic.Bool
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
				defer cancel()
				if _, _, err := node.Read(ctx); err == nil {
					asked.Store(true)
				}
			}()
			awaitPass(t, eagerPass(t, hub))
			// Fixture pacing: the window proves no probe is sent.
			time.Sleep(350 * time.Millisecond)
			if asked.Load() {
				t.Fatal("a job hk has not ended was probed")
			}
			assertRoleHeldBy(t, hub, held.Request.RequestID)
		})
	}
}

// c: an unreachable host is not proof of absence; the role is kept and a later
// pass, once the node answers absent, releases it. Covers a disconnected node
// and a probe timeout. Mutant killed: treat heldPaneUnknown as absent.
func TestEagerRoleReleaseUnreachableHostRetries_t6282(t *testing.T) {
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	t.Run("disconnected then absent", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, succeeded, "completed", "succeeded")
		awaitPass(t, eagerPass(t, hub))
		assertRoleHeldBy(t, hub, held.Request.RequestID)
		node := t14Node(t, hub, server, true)
		done := eagerPass(t, hub)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
		awaitPass(t, done)
		assertRoleFree(t, hub)
	})
	t.Run("timeout keeps", func(t *testing.T) {
		hub, server, _, held := roleHolderHub(t, succeeded, "completed", "succeeded")
		node := t14Node(t, hub, server, true)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		go func() { _, _, _ = node.Read(ctx) }() // read the probe, never answer it
		hub.releaseProvenOverRoles(ctx, "")
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
}

// d: a pool action that owns the job keeps the role, even with the pane
// absent. Mutant killed: drop the lockWithoutPoolAction fence.
func TestEagerRoleReleaseKeepsPoolActionJob_t6282(t *testing.T) {
	hub, server, _, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "completed", "succeeded")
	action := poolAction{ID: "action-6282", Job: "test-job", Node: "machine-a", Epoch: 1, Phase: poolActionPending,
		Data: poolActionData{Owner: "parent", Text: "The job ended.", SpawnRequest: held.Request.RequestID, Pane: roleHeldPane, Label: "test-job"}}
	if err := hub.inbox.store.insertPoolAction(t.Context(), action, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	node := t14Node(t, hub, server, true)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, payload, err := node.Read(ctx)
		var command map[string]json.RawMessage
		if err == nil && json.Unmarshal(payload, &command) == nil && string(command["op"]) == `"agents.list"` {
			// Answer absent if the fence were missing, so the mutant releases.
			_ = wsjson.Write(ctx, node, map[string]any{"type": "node.command.result", "request_id": command["request_id"], "op": "agents.list", "result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}})
		}
	}()
	awaitPass(t, eagerPass(t, hub))
	assertRoleHeldBy(t, hub, held.Request.RequestID)
}

// f: two live workers cannot hold one task role. A new spawn of the same role
// is admitted (through admission's own lazy release) while the eager pass waits
// on its probe; when the probe then answers absent, the eager write names the
// old request id and clears nothing of the new holder. A sampler checks that at
// no point two spawn_reservations rows carry the key, and the unique task_role
// index (hub_mysql_migrations/0006_spawn_reservations.sql) refuses a second
// holder outright. Mutant killed: an eager write keyed on task_role alone
// (`WHERE task_role=?`), which frees the new live holder.
func TestEagerRoleReleaseNeverFreesALiveSuccessor_t6282(t *testing.T) {
	hub, server, _, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "completed", "succeeded")
	node := t14Node(t, hub, server, true)
	var two atomic.Bool
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			var n int
			if hub.inbox.store.db.QueryRow(hub.inbox.store.q(`SELECT COUNT(*) FROM spawn_reservations WHERE task_role=?`), "42:implementer").Scan(&n) == nil && n > 1 {
				two.Store(true)
			}
		}
	}()
	done := eagerPass(t, hub)
	probe := nodeCommandRead(t, node)
	// While the eager pass waits for this answer, admit the successor.
	started := time.Now()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if status != http.StatusAccepted {
		t.Fatalf("successor status=%d body=%s", status, body)
	}
	// Nothing may slow the fleet: admission does not wait on the eager probe.
	if waited := time.Since(started); waited >= spawnHeldPaneProbeWait {
		t.Fatalf("admission waited %v on the eager pass", waited)
	}
	successor, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || successor == held.Request.RequestID {
		t.Fatalf("successor not holding: %s %v", successor, err)
	}
	roleReplyPanes(t, node, probe, []nodeAgentRow{}, true)
	awaitPass(t, done)
	close(stop)
	<-sampled
	assertRoleHeldBy(t, hub, successor)
	if two.Load() {
		t.Fatal("two reservations held one task role")
	}
	// The backstop: the unique task_role index refuses a second holder.
	_, err = hub.inbox.store.db.Exec(hub.inbox.store.q(`UPDATE spawn_reservations SET task_role=? WHERE request_id=?`), "42:implementer", held.Request.RequestID)
	if err == nil {
		t.Fatal("unique task_role index accepted a second holder")
	}
	// And the eager write itself never clears the successor's role.
	if released, err := hub.inbox.store.releaseEndedSpawnRole(t.Context(), held.Request.RequestID, "42:implementer"); err != nil || released {
		t.Fatalf("eager write on the old id released=%v err=%v", released, err)
	}
	assertRoleHeldBy(t, hub, successor)
}

// The pass's read and write are plain SQL that the MySQL dialect sends as
// written (hub spawn tests run MySQL through the recorded-statement store).
func TestEagerRoleReleaseMySQLStatements_t6282(t *testing.T) {
	s, log := recordedStore(t, dialectMySQL)
	_, _, _ = s.heldEndedSpawnRoles(t.Context(), "", 16)
	_, _ = s.releaseEndedSpawnRole(t.Context(), "id", "42:implementer")
	text := strings.Join(log.snapshot(), "\n")
	for _, want := range []string{
		`SELECT r.request_id, r.task_role FROM spawn_reservations r JOIN spawn_operations o ON o.request_id=r.request_id WHERE r.task_role IS NOT NULL AND o.status IN ('succeeded','failed') AND r.request_id>? ORDER BY r.request_id LIMIT ?`,
		`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=? AND task_role=? AND EXISTS(SELECT 1 FROM spawn_operations WHERE request_id=? AND status IN ('succeeded','failed','cancelled'))`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
}
