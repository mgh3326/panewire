package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func boundaryTestNode(task hubSpawnTask, id int, machine string) hubSpawnRequest {
	return hubSpawnRequest{RequestID: t14RequestID(id), Machine: machine, CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
}

func TestContainedBoundSpawnIsNotSent_t4577(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	first, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	request := durableSpawnRequest()
	task, _, err := first.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	nodeRequest := boundaryTestNode(task, 457710, "machine-a")
	if _, err = first.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, nodeRequest, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	if task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	h.relayControl.acceptingOverride = map[string]acceptingOverride{"machine-a": {Mode: "off"}}
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(ctx, nodeRequest)
	}()
	// Window: prove containment prevents any fresh spawn frame.
	readCtx, stopRead := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_, payload, readErr := node.Read(readCtx)
	stopRead()
	cancel()
	<-done
	if readErr == nil {
		t.Fatalf("contained node received fresh spawn: %s", payload)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("contained unsent request = %+v, %v; want unbound accepted", saved, err)
	}
}

func TestOldNodeResultCannotFinishReboundRequest_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	first := boundaryTestNode(task, 457711, "machine-a")
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, first, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Model a committed send and definitive refusal, followed by re-placement.
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	task.Status = "dispatching"
	if _, err = h.inbox.store.unbindRefusedSpawn(t.Context(), task, hubSpawnExclusion{Machine: first.Machine, Reason: "cwd_unmapped", RC: 2}, nil); err != nil {
		t.Fatal(err)
	}
	second := boundaryTestNode(task, 457712, "machine-b")
	if _, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, second, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	changed, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: first.Machine, NodeResult: true}, time.Now())
	if err != nil || changed {
		t.Fatalf("old node result settled rebound request: changed=%v err=%v", changed, err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Node == nil || saved.Node.RequestID != second.RequestID {
		t.Fatalf("new binding lost: %+v %v", saved, err)
	}
}

func TestReboundRequestRejectsOldDispatch_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	first := boundaryTestNode(task, 457715, "machine-a")
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, first, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	task.Status = "dispatching"
	if _, err = h.inbox.store.unbindRefusedSpawn(t.Context(), task, hubSpawnExclusion{Machine: first.Machine, Reason: "cwd_unmapped", RC: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundaryTestNode(task, 457716, "machine-b"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(ctx, first)
	}()
	// Window: prove the stale node cannot receive another spawn frame.
	readCtx, stopRead := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_, payload, readErr := node.Read(readCtx)
	stopRead()
	cancel()
	<-done
	if readErr == nil {
		t.Fatalf("old binding sent after re-placement: %s", payload)
	}
}

func TestUnbindFailureKeepsUnsentBinding_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 457713, "machine-a")
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_unsent_unbind BEFORE UPDATE ON spawn_operations WHEN NEW.status='accepted' BEGIN SELECT RAISE(FAIL, 'injected unbind failure'); END`); err != nil {
		t.Fatal(err)
	}
	_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(t.Context(), node)
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "bound" || saved.Node == nil || saved.Node.RequestID != node.RequestID {
		t.Fatalf("failed unbind lost unsent proof: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}

func TestStalePlacementCannotFinishBoundRequest_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	accepted, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(accepted, 457714, "machine-a")
	if _, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, accepted.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	changed, err := h.inbox.store.completeSpawn(t.Context(), accepted, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "refused"}, time.Now())
	if err != nil || changed {
		t.Fatalf("stale placement settled bound request: changed=%v err=%v", changed, err)
	}
}

func TestBoundAbandonRollsBackWithReservationFailure_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundaryTestNode(task, 457717, "machine-a"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// The pre-migration store accepts text statuses too; set up the new state
	// so this test also exercises the old binary's response to that row.
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='bound' WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.db.Exec(`CREATE TRIGGER fail_role_release BEFORE UPDATE ON spawn_reservations BEGIN SELECT RAISE(FAIL, 'injected reservation failure'); END`); err != nil {
		t.Fatal(err)
	}
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns/"+request.RequestID+"/abandon", t14OperatorToken, map[string]string{"reason": "never sent"})
	var response map[string]string
	_ = json.Unmarshal(body, &response)
	if status != http.StatusConflict || response["reason"] != "store_unavailable" {
		t.Fatalf("abandon with failed reservation write: status=%d body=%s", status, body)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "bound" {
		t.Fatalf("failed atomic abandon left status=%q err=%v", saved.Status, err)
	}
	var notifications int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications`).Scan(&notifications); err != nil || notifications != 0 {
		t.Fatalf("failed atomic abandon published %d notifications: %v", notifications, err)
	}
}

func TestSentTimeoutDoesNotClaimNoNodeRan_t4577(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now().Add(-11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundaryTestNode(task, 457718, "machine-a"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	if task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), task)
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "deadline_exceeded" || strings.Contains(saved.Outcome.Fix, "no node ran its landing") || !strings.Contains(saved.Outcome.Fix, "may") {
		t.Fatalf("possibly sent timeout status=%q fix=%q err=%v", saved.Status, saved.Outcome.Fix, err)
	}
}
