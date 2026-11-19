package panewire

// Contract: docs/hub-spawn-operations.md, "Withdrawing a request" (hk 5429).
//
// A requester ends its own pending request, parked or unbound or sent, as
// cancelled. The release is the operator abandon's (endPendingSpawn): the same
// pane probe, lost write and role release. The only new rule is the scope
// check: the requester named in the body must be the lane that submitted the
// request (spawnSubmitter), so one lane cannot withdraw another lane's work.

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
)

const (
	// spawnWithdrawFeature is advertised in GET /v1/spawns/features. A CLI
	// checks it before it sends a withdrawal to a hub that would not know the
	// route.
	spawnWithdrawFeature = "spawn_withdraw_v1"
	// spawnWithdrawnReason is the reason code a requester's withdrawal carries.
	// spawnOutcomeStatus maps it to cancelled, not failed.
	spawnWithdrawnReason = "requester_withdrawn"
)

// spawnWithdrawEnd ends a pending request as cancelled for its requester.
var spawnWithdrawEnd = spawnEnd{code: spawnWithdrawnReason, status: "cancelled", fix: "the requester withdrew this request; submit the work again under a new job id if it is still wanted", unboundFix: "the requester withdrew this request before any node took it; submit the work again under a new job id if it is still wanted", warning: "hub spawn withdraw"}

// fenceSentWithdrawal makes a possibly sent attempt safe to cancel, or says why
// it is not (hk 5429 review). A bound attempt has not crossed the send boundary,
// so it needs no fence. A sent grant-required attempt is fenced first:
// fenceSpawnAttempt commits only when no start grant was recorded, and a fenced
// attempt can never start. A sent legacy attempt has no fence, so the hub cannot
// prove it will not launch after a pane probe; it is refused until it ends by
// its deadline or the operator. Returns "" when the attempt may be cancelled,
// else the refusal reason.
func (h *HubServer) fenceSentWithdrawal(ctx context.Context, task hubSpawnTask) (string, error) {
	if task.Node == nil || task.Status == "bound" {
		return "", nil
	}
	if !task.Node.GrantRequired {
		return "unfenced_launch", nil
	}
	fenced, err := h.inbox.store.fenceSpawnAttempt(ctx, task.Request.RequestID, task.Node.RequestID, h.now().UTC())
	if err != nil {
		return "", err
	}
	if !fenced {
		return "granted", nil
	}
	return "", nil
}

// spawnSubmitter is the lane that submitted task: the lane its submitter named
// with --requested-by, else --parent (spawnSubmitterLane), else the owner lane,
// which submitted its own request. Only this lane may withdraw the request.
func spawnSubmitter(task hubSpawnTask) string {
	if lane := spawnSubmitterLane(task.Request.Args); lane != "" {
		return lane
	}
	return task.Owner
}

// handleSpawnWithdraw is POST /v1/spawns/{request_id}/withdraw. The operator
// credential is the one a submitter holds (submission needs it), so the caller
// is authenticated as the operator and the requester is the lane it names in
// the body; the hub checks that lane against the submitter of the request. A
// lane that is not the submitter gets 403 not_requester before the request's
// state is read. A request that already ended answers 409 spawn_not_pending.
func (h *HubServer) handleSpawnWithdraw(w http.ResponseWriter, r *http.Request) {
	caller := h.authenticate(r)
	if caller.kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	if h.inbox == nil {
		writeJSON(w, 503, map[string]string{"error": "durable_store_required"})
		return
	}
	var body struct {
		Reason    string `json:"reason"`
		Requester string `json:"requester"`
	}
	id := r.PathValue("request_id")
	if !validHubSpawnRequestID(id) || !decodeHubJSON(w, r, 4096, &body) || !validSpawnAbandonReason(body.Reason) || !validLaneReference(body.Requester) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "fix": "name a request id, the requesting lane and a one-line --reason"})
		return
	}
	if !h.spawnActions.begin(id) {
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": "sending_or_abandoning", "request_id": id})
		return
	}
	defer h.spawnActions.end(id)
	task, err := h.inbox.store.spawnTask(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
		return
	}
	if body.Requester != spawnSubmitter(task) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "not_requester", "request_id": id, "fix": "withdraw with the lane that submitted the request: its --requested-by, else --parent, else its owner"})
		return
	}
	if !spawnPending(task.Status) {
		writeJSON(w, 409, map[string]string{"error": "spawn_not_pending", "status": task.Status})
		return
	}
	refusal, err := h.fenceSentWithdrawal(r.Context(), task)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
		return
	}
	if refusal != "" {
		h.logger.Info("hub kept a withdrawn spawn", "request_id", id, "job", task.Job, "host", task.Node.Machine, "reason", refusal)
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": refusal, "request_id": id, "job": task.Job, "host": task.Node.Machine})
		return
	}
	h.endPendingSpawn(w, r, caller, task, spawnWithdrawEnd, body.Reason)
}
