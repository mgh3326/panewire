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
// after any other release. The release record and the release commit in one
// transaction, so a failed release leaves neither a record nor a changed stage.

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
// body a repeat returns, and the owner notice a repeat files again.
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
	case !verdict.answered:
		// The fence did not answer: whether the K is settled is unknown, so
		// nothing is released. The operator retries once the node answers.
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "verdict_unreadable"}, http.StatusServiceUnavailable
	case verdict.state == nodeLadderClosing:
		// A close has started on the node and release does not stop it: the
		// holder's role must not come back while that close runs.
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: stage.Status, State: verdict.state, Error: "verdict_pending"}, http.StatusServiceUnavailable
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
	// A live holder gets its role back; a holder that ended is never restored
	// to, the same rule the reconciler applies (reconcileStagedLadderStage).
	previousID := ""
	if state := escalateAuthorStr(record, "state"); state == "claimed" || state == "spawned" {
		previousID = holder.Request.RequestID
	}
	body := hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: "released", Reason: reason}
	notice := ladderReleaseNotice(step, id, previousID != "", reason)
	payload, err := json.Marshal(ladderReleaseRecord{Body: body, Notice: notice})
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	now := h.now().UTC()
	// The holder's liveness is compared at the write. A completion sets the job
	// record's Completed flag under h.mu, and h.mu is held across the release
	// transaction below, so a completion that starts now waits for the commit
	// and then frees the role itself (releaseEndedJobRole). A completion that
	// landed after the record was read, before this lock, is seen here.
	note := func(ctx context.Context, tx *sql.Tx) error {
		if previousID != "" {
			if job := h.jobs[holder.Job]; job != nil && job.Completed {
				return errLadderHolderChanged
			}
		}
		return h.inbox.store.noteLadderRelease(ctx, tx, id, stage.Job, stage.Owner, payload, now)
	}
	h.mu.Lock()
	err = h.releaseStagedRowNoting(ctx, id, previousID, roleKey, note)
	h.mu.Unlock()
	if err != nil {
		if errors.Is(err, errLadderHolderChanged) {
			return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: holder.Status, Error: "holder_changed"}, http.StatusConflict
		}
		if errors.Is(err, errSpawnClaimed) {
			return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Status: "changed", Error: "not_staged"}, http.StatusConflict
		}
		return hubLadderReleaseBody{RequestID: id, Job: step.Parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	h.fileLadderReleaseNotice(ctx, stage.Owner, step.Parent, id, notice)
	return body, http.StatusOK
}

// errLadderHolderChanged is a release whose holder row changed between the read
// and the write: nothing is released, and the operator repeats the release.
var errLadderHolderChanged = errors.New("the holder changed during the release")

// ladderReleaseNotice is the owner's notice of a release: the stage, the step
// it belonged to, what happened to the holder's role and the operator's reason.
func ladderReleaseNotice(step *fleet.LadderStepIdentity, id string, restored bool, reason string) string {
	role := "freed"
	if restored {
		role = "restored to its holder"
	}
	return fmt.Sprintf("ladder step %s (round %d, %s): the operator released the unresolved staged successor %s; its role is %s. Reason: %s", step.Parent, step.Round, step.PR, id, role, reason)
}

// fileLadderReleaseNotice files the owner notice under the release's event id,
// on a context that outlives the request. The lane store drops a duplicate, so
// filing it again on a replay is safe.
func (h *HubServer) fileLadderReleaseNotice(ctx context.Context, owner, parent, id, notice string) {
	noticeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ladderReleaseNoticeTimeout)
	defer cancel()
	h.relayLaneEventContext(noticeCtx, hubJobEventPayload{EventID: ladderReleaseEventPrefix + id, JobID: parent, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(notice, laneEventTextLimit), Host: "hub", Epoch: 1, Notice: ownerNoticeFor(parent, "ladder_released", false)}, nil)
}

// replayLadderRelease answers a repeat of a release that committed: the stage
// row is gone, and its record holds the answer the first release gave. The
// owner notice is filed again, so one the hub did not get to deliver lands now.
func (h *HubServer) replayLadderRelease(ctx context.Context, id string) (hubLadderReleaseBody, int) {
	raw, owner, found, err := h.inbox.store.ladderReleaseRecord(ctx, id)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if !found {
		return hubLadderReleaseBody{RequestID: id, Error: "not_found"}, http.StatusNotFound
	}
	var record ladderReleaseRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	h.fileLadderReleaseNotice(ctx, owner, record.Body.Job, id, record.Notice)
	return record.Body, http.StatusOK
}

// noteLadderRelease writes the release's record inside the transaction that
// deletes the stage, so the record exists exactly when the release commits. The
// row is inert: delivered and telemetry-recorded, so no notice route sends it.
func (s *laneInboxStore) noteLadderRelease(ctx context.Context, tx *sql.Tx, id, job, owner string, payload []byte, now time.Time) error {
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,1,1)`), ladderReleaseEventPrefix+id, id, job, owner, string(payload), now.UnixMilli())
	return err
}

// ladderReleaseRecord returns the record of a committed release and the owner
// it was filed for.
func (s *laneInboxStore) ladderReleaseRecord(parent context.Context, id string) (raw, owner string, found bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	err = s.db.QueryRowContext(ctx, s.q(`SELECT result,owner FROM spawn_notifications WHERE event_id=? AND request_id=?`), ladderReleaseEventPrefix+id, id).Scan(&raw, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return raw, owner, err == nil, err
}
