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
// ladder stage the reconciler cannot settle (hk 6115; docs/escalate-author.md,
// "Releasing an unresolved stage"). It is the one way a legacy marker stage, or
// a v1 stage whose node answered unknown or closing, leaves the staged state
// without the node's verdict deciding it. It is operator-only and never runs
// on its own: nothing in the hub calls it.
//
// The release goes through releaseStagedRow, the same path a normal release
// takes, so the holder's role and the reservation come back exactly as they
// would after any other release.

// ladderReleaseEventPrefix keys the durable record and the owner notice of a
// release by the stage's request id: one release per stage, however often the
// operator repeats it.
const ladderReleaseEventPrefix = "ladder-release:"

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

// releaseLadderStage releases the staged successor id when its close verdict is
// unresolved, and reports what it did. It holds ladderMu, as every other ladder
// step does, so the reconciler and the expiry pass never read the stage midway.
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
	parent := stage.Request.LadderStep.Parent
	holder, err := h.inbox.store.latestSpawnForJob(ctx, parent)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	// The role key and the restore target are read as the expiry pass reads
	// them: the holder's submission names the role, the record the task.
	spawn, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "job_unreadable"}, http.StatusConflict
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	record, exists, err := app.ExistingJob(ctx, parent)
	if err != nil || !exists {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "record_unreadable"}, http.StatusServiceUnavailable
	}
	roleKey := fleet.SpawnRoleKey(firstNonempty(spawn.Task, hkRecordTaskID(record)), spawn.Role, spawn.ReviewPass)
	// A live holder gets its role back; a holder that ended is never restored
	// to, the same rule the reconciler applies (reconcileStagedLadderStage).
	previousID := ""
	if state := escalateAuthorStr(record, "state"); state == "claimed" || state == "spawned" {
		previousID = holder.Request.RequestID
	}
	// stagedCloseVerdict is the read the reconciler uses: for a v1 stage it is
	// the fence, which sends no close and fences the K so none runs afterwards.
	// A legacy marker stage has no K to fence and reads as unknown.
	verdict, err := h.stagedCloseVerdict(ctx, stage)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	if !verdict.unresolved() {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Status: stage.Status, State: verdict.state, Error: "verdict_resolved"}, http.StatusConflict
	}
	body := hubLadderReleaseBody{RequestID: id, Job: parent, Status: "released", Reason: reason}
	payload, err := json.Marshal(body)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "hub_error"}, http.StatusServiceUnavailable
	}
	// The record is written before the release, so a release that lands always
	// has one to answer its repeat from; a release that fails takes it back.
	if err := h.inbox.store.recordLadderRelease(ctx, id, stage.Job, stage.Owner, payload, h.now().UTC()); err != nil {
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if err := h.releaseStagedRow(ctx, id, previousID, roleKey); err != nil {
		_ = h.inbox.store.forgetLadderRelease(ctx, id)
		if errors.Is(err, errSpawnClaimed) {
			return hubLadderReleaseBody{RequestID: id, Job: parent, Status: "changed", Error: "not_staged"}, http.StatusConflict
		}
		return hubLadderReleaseBody{RequestID: id, Job: parent, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	restored := "freed"
	if previousID != "" {
		restored = "restored to its holder"
	}
	round, pr := stage.Request.LadderStep.Round, stage.Request.LadderStep.PR
	text := fmt.Sprintf("ladder step %s (round %d, %s): the operator released the unresolved staged successor %s; its role is %s. Reason: %s", parent, round, pr, id, restored, reason)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: ladderReleaseEventPrefix + id, JobID: parent, OwnerLane: stage.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1, Notice: ownerNoticeFor(parent, "ladder_released", false)}, nil)
	return body, http.StatusOK
}

// replayLadderRelease answers a repeat of a release that already landed: the
// stage row is gone, and its record holds the answer the first release gave.
func (h *HubServer) replayLadderRelease(ctx context.Context, id string) (hubLadderReleaseBody, int) {
	raw, found, err := h.inbox.store.ladderReleaseRecord(ctx, id)
	if err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	if !found {
		return hubLadderReleaseBody{RequestID: id, Error: "not_found"}, http.StatusNotFound
	}
	var body hubLadderReleaseBody
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		return hubLadderReleaseBody{RequestID: id, Error: "store_unavailable"}, http.StatusServiceUnavailable
	}
	return body, http.StatusOK
}

// recordLadderRelease writes the release's durable answer. The row is inert: it
// is delivered and its telemetry recorded, so no notice route or trace sends it.
// An existing row is kept, so the first release's answer stands.
func (s *laneInboxStore) recordLadderRelease(parent context.Context, id, job, owner string, payload []byte, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,1,1)`, "")), ladderReleaseEventPrefix+id, id, job, owner, string(payload), now.UnixMilli())
	return err
}

// ladderReleaseRecord returns the answer a release recorded, if one landed.
func (s *laneInboxStore) ladderReleaseRecord(parent context.Context, id string) (string, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var raw string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT result FROM spawn_notifications WHERE event_id=? AND request_id=?`), ladderReleaseEventPrefix+id, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return raw, err == nil, err
}

// forgetLadderRelease removes a record whose release did not land.
func (s *laneInboxStore) forgetLadderRelease(parent context.Context, id string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM spawn_notifications WHERE event_id=? AND request_id=?`), ladderReleaseEventPrefix+id, id)
	return err
}
