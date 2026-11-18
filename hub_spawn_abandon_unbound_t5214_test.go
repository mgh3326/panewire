package panewire

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withdrawSpawn posts an abandon for requestID with token. machine, when set,
// names the node that token belongs to (hk 5214).
func withdrawSpawn(t *testing.T, h *HubServer, requestID, token, machine string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"reason": "owner withdrew it"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/spawns/"+requestID+"/abandon", bytes.NewReader(body))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	if machine != "" {
		request.Header.Set(hubMachineIDHeader, machine)
	}
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, request)
	return recorder
}

func withdrawBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	return body
}

// unboundSpawn claims the durable test request owned by lane parent and leaves
// it accepted and unbound, the state a request waits in before any node takes it.
func unboundSpawn(t *testing.T, h *HubServer) (requestID string) {
	t.Helper()
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if task.Node != nil || task.Status != "accepted" {
		t.Fatalf("fixture is not an unbound pending request: %+v", task)
	}
	return request.RequestID
}

func reservationTaskRole(t *testing.T, h *HubServer, requestID string) sql.NullString {
	t.Helper()
	var role sql.NullString
	if err := h.inbox.store.db.QueryRow(`SELECT task_role FROM spawn_reservations WHERE request_id=?`, requestID).Scan(&role); err != nil {
		t.Fatal(err)
	}
	return role
}

func TestUnboundSpawnAbandonWithdrawsRequest_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := unboundSpawn(t, h)

	recorder := withdrawSpawn(t, h, id, t14OperatorToken, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("operator withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if body := withdrawBody(t, recorder); body["status"] != "failed" || !strings.HasPrefix(body["reason"], spawnAbandonedReason) {
		t.Fatalf("withdrawal body=%v", body)
	}
	saved, err := h.inbox.store.spawnTask(context.Background(), id)
	if err != nil || saved.Status != "failed" || saved.Node != nil || !strings.HasPrefix(saved.Outcome.Reason, spawnAbandonedReason) {
		t.Fatalf("withdrawn request is not failed with the abandoned reason: %+v %v", saved, err)
	}
	if role := reservationTaskRole(t, h, id); role.Valid {
		t.Fatalf("withdrawn request still holds its task role %q", role.String)
	}
	var notices int
	if err = h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_notifications WHERE request_id=? AND result LIKE ?`, id, "%"+spawnAbandonedReason+"%").Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("withdrawal notices=%d err=%v, want one SPAWN_FAILED naming the abandonment", notices, err)
	}

	// A second withdrawal finds the request no longer pending.
	if again := withdrawSpawn(t, h, id, t14OperatorToken, ""); again.Code != http.StatusConflict || !strings.Contains(again.Body.String(), "spawn_not_pending") {
		t.Fatalf("second withdrawal status=%d body=%s", again.Code, again.Body)
	}

	// The role the withdrawn request held is free for the next request.
	if _, claimed, err := h.inbox.store.claimSpawn(context.Background(), roleSecondRequest(), "test-job-r2", "parent", time.Now().UTC()); err != nil || !claimed {
		t.Fatalf("role of a withdrawn request is still held: claimed=%v err=%v", claimed, err)
	}
}

func TestUnboundSpawnAbandonByOwnerLane_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := unboundSpawn(t, h)

	// The lane parent is routed to machine-b, so machine-a may not withdraw for it.
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`)
	if recorder := withdrawSpawn(t, h, id, "node-token", "machine-a"); recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "lane_not_routed_to_caller") {
		t.Fatalf("node outside the owner lane status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "accepted" {
		t.Fatalf("refused withdrawal changed the request: %+v %v", saved, err)
	}

	// Routed to machine-a, the owner lane may withdraw its own unbound request.
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if recorder := withdrawSpawn(t, h, id, "node-token", "machine-a"); recorder.Code != http.StatusOK {
		t.Fatalf("owner lane withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "failed" {
		t.Fatalf("owner lane withdrawal did not finish the request: %+v %v", saved, err)
	}
}

func TestBoundSpawnAbandonStaysOperatorOnly_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-a","pane":"w1:p1"}}}`)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(4232), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if _, err = h.inbox.store.bindSpawn(context.Background(), request.RequestID, task.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	// A node, even the owner lane's, may not withdraw a bound request.
	if recorder := withdrawSpawn(t, h, request.RequestID, "node-token", "machine-a"); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("node withdrew a bound request: status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), request.RequestID); err != nil || saved.Status != "bound" {
		t.Fatalf("bound request changed: %+v %v", saved, err)
	}
}

func TestUnboundSpawnWithClaimStaysWithItsHost_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := unboundSpawn(t, h)
	// A harness retry on machine-a left an hk job claim on that host; the request
	// stays unbound with it.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET claimed_host=? WHERE request_id=?`, "machine-a", id); err != nil {
		t.Fatal(err)
	}
	recorder := withdrawSpawn(t, h, id, t14OperatorToken, "")
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "claimed_host") {
		t.Fatalf("claimed withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "accepted" {
		t.Fatalf("claimed request was finished: %+v %v", saved, err)
	}
}
