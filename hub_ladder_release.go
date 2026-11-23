package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// `POST /v1/ladder/stages/{request_id}/release` is the operator's release of a
// ladder stage the reconciler keeps (hk 6115; docs/escalate-author.md,
// "Releasing an unresolved stage"). It is operator-only and never runs on its
// own: nothing in the hub calls it.
//
// The release goes through releaseStagedRowNoting, the same store release a
// normal release takes, so the holder's role and the reservation come back as
// after any other release. The release record and its owner notice commit in
// the same transaction as the release, so a failed release leaves neither a
// record nor a notice nor a changed stage. The notice is an outbox row of the
// spawn notice outbox (hub_spawn_notice_route.go), relayed after the commit and
// retried until the owner's lane takes it (hk 6137).

// ladderReleaseEventPrefix keys the record and the owner notice of a release by
// the stage's request id: one release per stage, however often it is repeated.
const ladderReleaseEventPrefix = "ladder-release:"

// ladderReleaseNoticeTimeout bounds the owner notice filed after a release
// commits. It runs on a context that outlives the request, so a client that
// gave up cannot cut the notice off.
const ladderReleaseNoticeTimeout = 10 * time.Second

// hubLadderReleaseBody is the answer to a release. Status is "released" on
// success and the stage's own status when the stage is not staged. Error names
// the refusal code; State carries the verdict that refused a v1 stage.
type hubLadderReleaseBody struct {
	RequestID string `json:"request_id,omitempty"`
	Job       string `json:"job,omitempty"`
	Status    string `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	State     string `json:"state,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ladderReleaseRecord is the durable answer of a release that committed: the
// body a repeat returns, and the owner notice the outbox routes.
type ladderReleaseRecord struct {
	Body   hubLadderReleaseBody `json:"body"`
	Notice string               `json:"notice"`
}

func (h *HubServer) handleLadderRelease(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "durable_store_required"})
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	id := r.PathValue("request_id")
	if !validHubSpawnRequestID(id) || !decodeHubJSON(w, r, 4096, &body) || !validSpawnAbandonReason(body.Reason) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "fix": "name a stage request id and a one-line --reason"})
		return
	}
	result, status := h.releaseLadderStage(r.Context(), id, body.Reason)
	writeJSON(w, status, result)
}

// releaseLadderStage releases the staged successor id when its close verdict
// settles that no close can run for it, and reports what it did. It holds
// ladderMu, as every other ladder step does.
//
// The verdict is read before the holder. The fence wait can be long, and the
// holder's state must be the one at the release, not the one before the wait.
func (h *HubServer) releaseLadderStage(ctx context.Context, id, reason string) (hubLadderReleaseBody, int) {
	h.ladderMu.Lock()
	defer h.ladderMu.Unlock()
	stage, err := h.inbox.store.spawnTask(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return h.replayLadderRelease(ctx, id)
	}
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if stage.Status != "staged" {
		return hubLadderReleaseBody{RequestID: id, Job: stage.Job, Status: stage.Status, Error: "not_staged"}, http.StatusConflict
	}
	if stage.Request.LadderStep == nil {
		return hubLadderReleaseBody{RequestID: id, Job: stage.Job, Status: stage.Status, Error: "not_ladder_stage"}, http.StatusConflict
	}
	step := stage.Request.LadderStep
	// stagedCloseVerdict is the read the reconciler uses: for a v1 stage it is
	// the fence, which sends no close and settles the K. A legacy marker stage
	// has no K to fence and reads as unknown; a stage with no close binding reads
	// as refused, as the reconciler reads it.
	verdict, err := h.stagedCloseVerdict(ctx, stage)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	switch {
	case !verdict.answered, verdict.state == nodeLadderClosing:
		// The fence did not answer, or answered closing. A node settles a closing
		// record to unknown under its lock before it answers (nodeLadderFence), so
		// a fence never reports closing; an answer that says so settles nothing.
		// Nothing is released on it: a close may still run. The operator retries
		// once the node answers.
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "verdict_unreadable"}, http.StatusServiceUnavailable
	case verdict.state == nodeLadderClosed, verdict.state == nodeLadderGone:
		// The pane was closed or ended on its own: the reconciler activates or
		// releases these, not the operator.
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: stage.Status, State: verdict.state, Error: "verdict_resolved"}, http.StatusConflict
	}
	// unknown (answered), fenced and refused: nothing closed the predecessor
	// for this K, or the answer is the ambiguity the operator decides on.
	holder, err := h.inbox.store.latestSpawnForJob(ctx, step.Parent)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	// The role key and the restore target are read as the expiry pass reads
	// them: the holder's submission names the role, the record the task.
	spawn, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "job_unreadable"}, http.StatusConflict
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	record, exists, err := app.ExistingJob(ctx, step.Parent)
	if err != nil || !exists {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "record_unreadable"}, http.StatusServiceUnavailable
	}
	roleKey := fleet.SpawnRoleKey(firstNonempty(spawn.Task, hkRecordTaskID(record)), spawn.Role, spawn.ReviewPass)
	// hk's state of the holder is read here, before the lock. Whether the hub has
	// seen the holder complete is read under h.mu below, at the write.
	holderState := escalateAuthorStr(record, "state")
	body := hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: "released", Reason: reason}
	now := h.now().UTC()
	// The holder's role comes back only while it is live in both sources: hk
	// still says claimed or spawned, and the hub has not seen it complete (a
	// holder that ended is never restored to, the rule reconcileStagedLadderStage
	// applies). The decision and the write share h.mu. A completion sets the job
	// record's Completed flag under h.mu, so one that landed before this lock is
	// seen here, and one that starts now waits for the commit. A completed holder
	// is therefore never restored: its role is freed, the discard path.
	// A holder restored here that completes later is not freed by completion:
	// observeJobCompletion does not call releaseEndedJobRole, which only the
	// settled-lost paths (observeJobLost, lateRegisterJobEnd) run (hk 6137).
	h.mu.Lock()
	previousID := ""
	if job := h.jobs[holder.Job]; (holderState == "claimed" || holderState == "spawned") && (job == nil || !job.Completed) {
		previousID = holder.Request.RequestID
	}
	notice := ladderReleaseNotice(step, id, previousID != "", reason)
	payload, err := json.Marshal(ladderReleaseRecord{Body: body, Notice: notice})
	if err != nil {
		h.mu.Unlock()
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	err = h.releaseStagedRowNoting(ctx, id, previousID, roleKey, func(ctx context.Context, tx *sql.Tx) error {
		return h.inbox.store.noteLadderRelease(ctx, tx, id, stage.Job, stage.Owner, payload, now)
	})
	h.mu.Unlock()
	if err != nil {
		if errors.Is(err, errSpawnClaimed) {
			return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: "changed", Error: "not_staged"}, http.StatusConflict
		}
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	// The notice is in the outbox from the commit on: it is relayed now, and
	// the spawn notice router retries it until the owner's lane takes it.
	h.relayLadderReleaseNotice(ctx, spawnNotification{id: ladderReleaseEventPrefix + id, request: id, owner: stage.Owner, raw: string(payload)})
	return body, http.StatusOK
}

// ladderReleaseNotice is the owner's notice of a release: the stage, the step
// it belonged to, what happened to the holder's role and the operator's reason.
func ladderReleaseNotice(step *fleet.LadderStepIdentity, id string, restored bool, reason string) string {
	role := "freed"
	if restored {
		role = "restored to its holder"
	}
	return fmt.Sprintf("ladder step %s (round %d, %s): the operator released the unresolved staged successor %s; its role is %s. Reason: %s", step.Parent, step.Round, step.PR, id, role, reason)
}

// relayLadderReleaseNotice routes a release's owner notice through the spawn
// notice outbox, on a context that outlives the request. The row was committed
// with the release (noteLadderRelease), so a notice the hub did not relay before
// it stopped is routed by the next flush. A duplicate event id is dropped by the
// lane store.
func (h *HubServer) relayLadderReleaseNotice(ctx context.Context, n spawnNotification) {
	noticeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ladderReleaseNoticeTimeout)
	defer cancel()
	h.routeSpawnNotification(noticeCtx, n)
}

// replayLadderRelease answers a repeat of a release that committed: the stage
// row is gone, and its record holds the answer the first release gave. A notice
// still owed is routed again; one already delivered is not filed a second time.
func (h *HubServer) replayLadderRelease(ctx context.Context, id string) (hubLadderReleaseBody, int) {
	n, owed, found, err := h.inbox.store.ladderReleaseNotice(ctx, id)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if !found {
		return hubLadderReleaseBody{RequestID: id, Error: "not_found"}, http.StatusNotFound
	}
	var record ladderReleaseRecord
	if err := json.Unmarshal([]byte(n.raw), &record); err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if owed {
		h.relayLadderReleaseNotice(ctx, n)
	}
	return record.Body, http.StatusOK
}

// noteLadderRelease writes the release's record and its owner notice inside the
// transaction that deletes the stage, so both exist exactly when the release
// commits. The notice is owed (delivered=0) and queued in spawn_notice_outbox,
// the same outbox a spawn notice waits in. The row is marked telemetry-recorded,
// so the spawn telemetry loop skips it.
func (s *laneInboxStore) noteLadderRelease(ctx context.Context, tx *sql.Tx, id, job, owner string, payload []byte, now time.Time) error {
	eventID := ladderReleaseEventPrefix + id
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,0,1)`), eventID, id, job, owner, string(payload), now.UnixMilli()); err != nil {
		return err
	}
	return s.enqueueSpawnNotice(ctx, tx, eventID)
}

// ladderReleaseNotice returns the notice of a committed release as the outbox
// routes it, and whether it is still owed.
func (s *laneInboxStore) ladderReleaseNotice(parent context.Context, id string) (n spawnNotification, owed, found bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	var delivered int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT result,owner,delivered FROM spawn_notifications WHERE event_id=? AND request_id=?`), ladderReleaseEventPrefix+id, id).Scan(&n.raw, &n.owner, &delivered)
	if errors.Is(err, sql.ErrNoRows) {
		return spawnNotification{}, false, false, nil
	}
	if err != nil {
		return spawnNotification{}, false, false, err
	}
	n.id, n.request = ladderReleaseEventPrefix+id, id
	return n, delivered == spawnNoticePending, true, nil
}
