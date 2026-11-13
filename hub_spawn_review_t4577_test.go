package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestBoundDispatchDoesNotBlockHeartbeat_t4577(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token-b"}, InboxStorePath: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14Node(t, h, server, true)
	h.mu.Lock()
	agent := h.nodes["machine-a"].agent
	h.mu.Unlock()
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 457719, "machine-a")
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node); err != nil || task.Status != "bound" {
		t.Fatalf("unsent binding status=%q err=%v", task.Status, err)
	}
	other, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `UPDATE spawn_operations SET last_run_ms=last_run_ms WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(ctx, node)
	}()
	eventually(t, 2*time.Second, "dispatch to wait on the store", func() bool {
		return h.inbox.store.db.Stats().InUse == 1
	})
	// Window: the held SQLite writer simulates a store write that cannot finish.
	select {
	case <-done:
		t.Fatal("dispatch finished while the store writer was held")
	case <-time.After(200 * time.Millisecond):
	}
	heartbeat := make(chan bool, 1)
	go func() { heartbeat <- h.touch("machine-a", agent) }()
	responsive := false
	select {
	case responsive = <-heartbeat:
	case <-time.After(500 * time.Millisecond):
	}
	otherOverride := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/nodes/machine-b/accepting", t14OperatorToken, map[string]string{"mode": "on"})
	var override t14HTTPResult
	select {
	case override = <-otherOverride:
	case <-time.After(500 * time.Millisecond):
	}
	_ = tx.Rollback()
	cancel()
	<-done
	if !responsive {
		<-heartbeat
		t.Fatal("node heartbeat blocked behind the spawn store write")
	}
	if override.status == 0 {
		<-otherOverride
		t.Fatal("another node's accepting override blocked behind the spawn store write")
	}
	if override.status != http.StatusOK {
		t.Fatalf("other node accepting override: status=%d body=%s", override.status, override.body)
	}
}

func TestBoundAbandonMarksClaimedJobLost_t4577(t *testing.T) {
	t.Setenv("PANEWIRE_NODE_ENV", filepath.Join(t.TempDir(), "missing-node-config"))
	h := asyncSpawnHub(t)
	jobs := &roleJobs{records: map[string]map[string]any{"test-job": {"state": "claimed"}}}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	first := boundaryTestNode(task, 457720, "machine-a")
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, first); err != nil {
		t.Fatal(err)
	}
	// Model an earlier sent attempt that the node cleaned up.
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=? AND node_id=?`, request.RequestID, first.RequestID); err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Retryable: true, NodeResult: true}, time.Now()); err != nil || !changed {
		t.Fatalf("settle cleaned attempt: changed=%v err=%v", changed, err)
	}
	if task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now()); err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundaryTestNode(task, 457721, "machine-a")); err != nil {
		t.Fatal(err)
	}
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns/"+request.RequestID+"/abandon", t14OperatorToken, map[string]string{"reason": "retry cancelled"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	commands := make(chan map[string]json.RawMessage, 1)
	go func() {
		_, payload, err := node.Read(ctx)
		if err != nil {
			commands <- nil
			return
		}
		var command map[string]json.RawMessage
		_ = json.Unmarshal(payload, &command)
		commands <- command
	}()
	select {
	case result := <-response:
		t.Fatalf("abandoned a claimed retry without marking its job lost: status=%d body=%s", result.status, result.body)
	case command := <-commands:
		if string(command["op"]) != `"job.mark_lost"` {
			t.Fatalf("abandon command=%s, want job.mark_lost", command["op"])
		}
		nodeCommandReplyTo(t, node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
	}
	if result := t14Result(t, response); result.status != http.StatusOK {
		t.Fatalf("abandon claimed retry: status=%d body=%s", result.status, result.body)
	}
}
