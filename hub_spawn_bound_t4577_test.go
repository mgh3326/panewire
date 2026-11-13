package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

func boundTestNode(task hubSpawnTask) hubSpawnRequest {
	return hubSpawnRequest{
		RequestID: t14RequestID(457700), Machine: "machine-a", CWDKey: "project",
		Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol"},
		WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli(),
	}
}

func TestBoundSpawnSurvivesStoreRestart_t4577(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	store := openSpawnStore(t, path)
	request := durableSpawnRequest()
	task, _, err := store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundTestNode(task)
	if _, err := store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	store.close()
	recovered := openSpawnStore(t, path)
	saved, err := recovered.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "bound" || saved.Node == nil || saved.Node.RequestID != node.RequestID {
		t.Fatalf("recovered status=%q node=%+v err=%v; want durable unsent binding", saved.Status, saved.Node, err)
	}
}

func TestBoundSpawnExpiresWithoutNodeProof_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now().Add(-11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), task)
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "retryable" || !saved.Outcome.Retryable {
		t.Fatalf("expired unsent status=%q outcome=%+v err=%v; want retryable", saved.Status, saved.Outcome, err)
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now()); err != nil {
		t.Fatalf("expired unsent request retained task/role reservation: %v", err)
	}
	var count int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE event_id=?`, request.RequestID+":1:retryable").Scan(&count); err != nil || count != 1 {
		t.Fatalf("retryable outbox rows=%d err=%v", count, err)
	}
}

func TestAbandonBoundSpawnWithoutNode_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns/"+request.RequestID+"/abandon", t14OperatorToken, map[string]string{"reason": "never sent"})
	if status != http.StatusOK {
		t.Fatalf("abandon unsent request: status=%d body=%s; want 200", status, body)
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now()); err != nil {
		t.Fatalf("abandoned unsent request retained task/role reservation: %v", err)
	}
}

func TestTerminalSpawnSnapshotCannotSend_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	nodeRequest := boundTestNode(task)
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, nodeRequest, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "refused"}, time.Now()); err != nil || !changed {
		t.Fatalf("finish request: changed=%v err=%v", changed, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := (&directSpawnTransport{h: h, task: task}).dispatch(ctx, nodeRequest)
		done <- err
	}()
	// Window: prove the terminal request does not send another node frame.
	readCtx, stopRead := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_, payload, readErr := node.Read(readCtx)
	stopRead()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not stop")
	}
	if readErr == nil {
		t.Fatalf("terminal request sent node frame: %s", payload)
	}
}

func TestAbandonCannotRaceLiveSend_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	nodeRequest := boundTestNode(task)
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, nodeRequest, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(ctx, nodeRequest)
	}()
	t14ReadNodeSpawn(t, node)
	probeCtx, stopProbe := context.WithCancel(t.Context())
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		for {
			_, payload, err := node.Read(probeCtx)
			if err != nil {
				return
			}
			var command struct {
				Type      string `json:"type"`
				RequestID string `json:"request_id"`
				Op        string `json:"op"`
			}
			if json.Unmarshal(payload, &command) != nil || command.Type != "node.command" || command.Op != "agents.list" {
				continue
			}
			_ = wsjson.Write(probeCtx, node, map[string]any{"type": "node.command.result", "request_id": command.RequestID, "op": command.Op, "result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}})
		}
	}()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns/"+request.RequestID+"/abandon", t14OperatorToken, map[string]string{"reason": "concurrent send"})
	stopProbe()
	cancel()
	<-probeDone
	<-dispatchDone
	if status != http.StatusConflict {
		t.Fatalf("abandon while send is live: status=%d body=%s; want 409", status, body)
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now()); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("live send lost its task/role reservation: %v", err)
	}
}
