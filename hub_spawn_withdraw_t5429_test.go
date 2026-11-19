package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// postWithdraw posts a withdrawal of requestID by requester through the hub's
// handler, with token as the bearer (hk 5429).
func postWithdraw(t *testing.T, h *HubServer, requestID, token, requester string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"reason": "parked too long", "requester": requester})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/spawns/"+requestID+"/withdraw", bytes.NewReader(body))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, request)
	return recorder
}

// parkedRequest claims request for lane owner and parks it, the state a request
// waits in when no host can take it before its next check (hk 4661). It
// returns the request id.
func parkedRequest(t *testing.T, h *HubServer, request fleet.Submission, owner string) string {
	t.Helper()
	now := time.Now().UTC()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", owner, now)
	if err != nil {
		t.Fatal(err)
	}
	if task.Node != nil || task.Status != "accepted" {
		t.Fatalf("fixture is not an unbound pending request: %+v", task)
	}
	parked, err := h.inbox.store.parkSpawn(context.Background(), request.RequestID, task.Attempt, spawnParkLaunchBackoff, nil, now.Add(time.Hour), now)
	if err != nil || !parked {
		t.Fatalf("park fixture: parked=%v err=%v", parked, err)
	}
	return request.RequestID
}

func TestSpawnWithdrawEndsParkedRequestCancelled_t5429(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := parkedRequest(t, h, durableSpawnRequest(), "parent")

	// The submitting lane (the owner, when no --requested-by was given) withdraws.
	recorder := postWithdraw(t, h, id, t14OperatorToken, "parent")
	if recorder.Code != http.StatusOK {
		t.Fatalf("requester withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if body := withdrawBody(t, recorder); body["status"] != "cancelled" || !strings.HasPrefix(body["reason"], spawnWithdrawnReason) {
		t.Fatalf("withdrawal body=%v", body)
	}
	saved, err := h.inbox.store.spawnTask(context.Background(), id)
	if err != nil || saved.Status != "cancelled" || saved.Node != nil || !strings.HasPrefix(saved.Outcome.Reason, spawnWithdrawnReason) {
		t.Fatalf("withdrawn request is not cancelled with the withdrawn reason: %+v %v", saved, err)
	}
	if role := reservationTaskRole(t, h, id); role.Valid {
		t.Fatalf("withdrawn request still holds its task role %q", role.String)
	}
	var notices int
	if err = h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_notifications WHERE request_id=? AND result LIKE ?`, id, "%"+spawnWithdrawnReason+"%").Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("withdrawal notices=%d err=%v, want one notice naming the withdrawal", notices, err)
	}

	// The public status reads cancelled, so the submitter's status command exits 84.
	status := httptest.NewRecorder()
	read := httptest.NewRequest(http.MethodGet, "/v1/spawns/"+id, nil)
	read.Header.Set(hubAuthorizationHeader, "Bearer "+t14OperatorToken)
	h.Handler().ServeHTTP(status, read)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("status read after withdrawal status=%d body=%s", status.Code, status.Body)
	}

	// A second withdrawal finds the request no longer pending.
	if again := postWithdraw(t, h, id, t14OperatorToken, "parent"); again.Code != http.StatusConflict || !strings.Contains(again.Body.String(), "spawn_not_pending") {
		t.Fatalf("second withdrawal status=%d body=%s", again.Code, again.Body)
	}

	// The role the withdrawn request held is free for the next request.
	if _, claimed, err := h.inbox.store.claimSpawn(context.Background(), roleSecondRequest(), "test-job-r2", "parent", time.Now().UTC()); err != nil || !claimed {
		t.Fatalf("role of a withdrawn request is still held: claimed=%v err=%v", claimed, err)
	}
}

func TestSpawnWithdrawRefusesAnotherRequester_t5429(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := parkedRequest(t, h, durableSpawnRequest(), "parent")

	// Another lane may not withdraw the request, and the refusal reads no state.
	if recorder := postWithdraw(t, h, id, t14OperatorToken, "someone-else"); recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "not_requester") {
		t.Fatalf("another requester status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "accepted" || saved.Outcome.Reason != "" {
		t.Fatalf("refused withdrawal changed the request: %+v %v", saved, err)
	}
	// A missing requester is a malformed request, not an anonymous withdrawal.
	if recorder := postWithdraw(t, h, id, t14OperatorToken, ""); recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty requester status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "accepted" {
		t.Fatalf("malformed withdrawal changed the request: %+v %v", saved, err)
	}
}

func TestSpawnWithdrawNamesTheRequestedBySubmitter_t5429(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	// The submitter named itself with --requested-by; the owner lane is parent.
	request := durableSpawnRequest()
	request.Args = append(append([]string(nil), request.Args...), "--requested-by", "requester-lane")
	id := parkedRequest(t, h, request, "parent")

	// The owner is not the submitter here, so the owner lane may not withdraw.
	if recorder := postWithdraw(t, h, id, t14OperatorToken, "parent"); recorder.Code != http.StatusForbidden {
		t.Fatalf("owner withdrew a request --requested-by named another lane for: status=%d body=%s", recorder.Code, recorder.Body)
	}
	if recorder := postWithdraw(t, h, id, t14OperatorToken, "requester-lane"); recorder.Code != http.StatusOK {
		t.Fatalf("submitter withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
}

func TestSpawnWithdrawNeedsTheOperatorCredential_t5429(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := parkedRequest(t, h, durableSpawnRequest(), "parent")
	// A node credential, even one routed to the owner lane, may not withdraw.
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-a","pane":"w1:p1"}}}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/spawns/"+id+"/withdraw", bytes.NewReader([]byte(`{"reason":"x","requester":"parent"}`)))
	request.Header.Set(hubAuthorizationHeader, "Bearer node-token")
	request.Header.Set(hubMachineIDHeader, "machine-a")
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("node withdrew a request: status=%d body=%s", recorder.Code, recorder.Body)
	}
	if saved, err := h.inbox.store.spawnTask(context.Background(), id); err != nil || saved.Status != "accepted" {
		t.Fatalf("request changed: %+v %v", saved, err)
	}
}

func TestSpawnWithdrawLandedRequestConflicts_t5429(t *testing.T) {
	s := newStrandedSpawn(t)
	// The node landed the request: its success is final.
	if _, err := s.hub.inbox.store.completeSpawn(context.Background(), s.task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := s.status(t); got.Status != "succeeded" {
		t.Fatalf("landing fixture status=%s", got.Status)
	}
	recorder := postWithdraw(t, s.hub, s.task.Request.RequestID, t14OperatorToken, "parent")
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "spawn_not_pending") {
		t.Fatalf("withdrawal of a landed request status=%d body=%s", recorder.Code, recorder.Body)
	}
	if got := s.status(t); got.Status != "succeeded" {
		t.Fatalf("landed request changed to %s", got.Status)
	}
}

func TestSpawnWithdrawBoundLivePaneRefuses_t5429(t *testing.T) {
	s := newStrandedSpawnWith(t, 11*time.Minute, true)
	node := t14Node(t, s.hub, s.server, true)
	id := s.task.Request.RequestID
	// The withdrawal probes the node for a pane bearing the job label; a live
	// labelled pane keeps the request with its host (the operator abandon rule).
	response := t14HTTPAsync(t, http.MethodPost, s.server.URL+"/v1/spawns/"+id+"/withdraw", t14OperatorToken, map[string]string{"reason": "parked too long", "requester": "parent"})
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w9:p1", Name: "test-job"}}, true)
	result := t14Result(t, response)
	var body map[string]string
	_ = json.Unmarshal(result.body, &body)
	if result.status != http.StatusConflict || body["error"] != "spawn_abandon_refused" || body["reason"] != "pane_present:w9:p1" {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("status=%s", got.Status)
	}
	if err := s.claimNext(t); err == nil {
		t.Fatal("reservation released while a labelled pane is live")
	}
}

func TestSpawnWithdrawBoundNoLabelledPaneCancels_t5429(t *testing.T) {
	s := newStrandedSpawnWith(t, 11*time.Minute, true)
	node := t14Node(t, s.hub, s.server, true)
	id := s.task.Request.RequestID
	response := t14HTTPAsync(t, http.MethodPost, s.server.URL+"/v1/spawns/"+id+"/withdraw", t14OperatorToken, map[string]string{"reason": "parked too long", "requester": "parent"})
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
	args := roleMarkLostArgs(t, roleReplyMarkLost(t, node, nodeJobMarkLostLost))
	if args["end_reason"] != spawnWithdrawnReason {
		t.Fatalf("mark lost args=%v", args)
	}
	if result := t14Result(t, response); result.status != http.StatusOK || !strings.Contains(string(result.body), `"status":"cancelled"`) {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	got := s.status(t)
	if got.Status != "cancelled" || !strings.HasPrefix(got.Outcome.Reason, spawnWithdrawnReason) {
		t.Fatalf("withdrawn request %+v", got)
	}
	if err := s.claimNext(t); err != nil {
		t.Fatalf("reservation still held: %v", err)
	}
	// The cancel is safe because the attempt was fenced first: no grant can start it.
	var state string
	if err := s.hub.inbox.store.db.QueryRow(`SELECT state FROM spawn_attempt_grants WHERE node_id=?`, t14RequestID(4231)).Scan(&state); err != nil || state != "fenced" {
		t.Fatalf("attempt not fenced before the withdrawal cancelled it: state=%q err=%v", state, err)
	}
}

func TestSpawnWithdrawLegacySentAttemptRefused_t5429(t *testing.T) {
	// A legacy node (no grant) may launch with no hub check, and nothing fences
	// it, so a pane probe cannot prove nothing starts after it. Refused.
	s := newStrandedSpawn(t)
	recorder := postWithdraw(t, s.hub, s.task.Request.RequestID, t14OperatorToken, "parent")
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "unfenced_launch") {
		t.Fatalf("legacy sent withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("refused withdrawal changed the request to %s", got.Status)
	}
	if err := s.claimNext(t); err == nil {
		t.Fatal("reservation released for an unfenced sent attempt")
	}
}

func TestSpawnWithdrawGrantedAttemptRefused_t5429(t *testing.T) {
	s := newStrandedSpawnWith(t, 11*time.Minute, true)
	// The hub granted the start: the node may be launching it, so it is not withdrawn.
	granted, err := s.hub.inbox.store.decideSpawnGrant(context.Background(), t14RequestID(4231), "machine-a", time.Now().UTC())
	if err != nil || !granted {
		t.Fatalf("grant fixture: granted=%v err=%v", granted, err)
	}
	recorder := postWithdraw(t, s.hub, s.task.Request.RequestID, t14OperatorToken, "parent")
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"granted"`) {
		t.Fatalf("granted withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("granted attempt changed to %s", got.Status)
	}
}

func TestSpawnWithdrawReleasesRoleWithItsOutcome_t5429(t *testing.T) {
	// The cancelled outcome and the role release are one transaction: a bound
	// deadline_exceeded holder must be free as soon as completeSpawn returns, not
	// only after a second write that can fail (hk 5429 review).
	s := newStrandedSpawnWith(t, 11*time.Minute, true)
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOutcomeReason(spawnWithdrawnReason, "parked too long"), Host: "machine-a"}
	if changed, err := s.hub.inbox.store.completeSpawn(context.Background(), s.task, outcome, time.Now().UTC()); err != nil || !changed {
		t.Fatalf("cancel outcome: changed=%v err=%v", changed, err)
	}
	if role := reservationTaskRole(t, s.hub, s.task.Request.RequestID); role.Valid {
		t.Fatalf("cancelled holder still holds role %q after its outcome was written", role.String)
	}
}

func TestSpawnAbandonUnboundNodeEndNamesTheWithdrawal_t5429(t *testing.T) {
	// The owner lane's node credential withdraws its unbound request (hk 5214).
	// The SPAWN_FAILED notice must not say the operator abandoned it.
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	id := unboundSpawn(t, h)
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if recorder := withdrawSpawn(t, h, id, "node-token", "machine-a"); recorder.Code != http.StatusOK {
		t.Fatalf("owner lane withdrawal status=%d body=%s", recorder.Code, recorder.Body)
	}
	var notice string
	if err := h.inbox.store.db.QueryRow(`SELECT result FROM spawn_notifications WHERE request_id=? AND owner=?`, id, "parent").Scan(&notice); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(notice, "operator abandoned") || !strings.Contains(notice, "withdrawn before any node took it") {
		t.Fatalf("owner lane notice misattributes the end: %s", notice)
	}
}

func TestSpawnFeatureAdvertisesWithdraw_t5429(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := httptest.NewRequest(http.MethodGet, "/v1/spawns/features", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+t14OperatorToken)
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"`+spawnWithdrawFeature+`"`) {
		t.Fatalf("features status=%d body=%s", recorder.Code, recorder.Body)
	}
}
