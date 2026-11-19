package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Start proof (docs/hub-spawn-operations.md, "Start proof"; design
// hub-owned-spawn-queue §3 rule 6, hk 4612). A grant-required attempt starts
// on its node only after the hub durably granted that node request id. The
// hub re-places an attempt on silence only after it durably fenced the same
// id. Grant and fence are one INSERT each on spawn_attempt_grants keyed by the
// node request id, so exactly one of them is ever recorded: a fenced attempt
// can never start, and a granted one is never released on silence.

const (
	// defaultSpawnStartBy is how long a node may wait for its grant after the
	// hub sent the frame (design §9a: 60 s, unmeasured).
	defaultSpawnStartBy = 60 * time.Second
	// defaultSpawnStartByMargin is how long past start_by the hub waits before
	// it fences an ungranted attempt (design §9a: 30 s, unmeasured).
	defaultSpawnStartByMargin = 30 * time.Second
)

// A bound or sent grant-required attempt is stored under its own status
// values, those statuses plus spawnGrantStatusSuffix. A hub older than the start proof
// lists none of them, so after a rollback it never replays such an attempt
// (it would send a frame without start_by, which a node starts with no grant),
// never sweeps it and never releases its reservation: it holds the row until a
// newer hub returns. Every read maps the stored value back, so code and every
// external surface see only the sent status it refines.
const spawnGrantStatusSuffix = "_grant"

// spawnStoredStatus is the stored form of status for an attempt bound as node.
func spawnStoredStatus(status string, node *hubSpawnRequest) string {
	if node != nil && node.GrantRequired {
		switch status {
		case "bound", "dispatching", "reconciling", "deadline_exceeded":
			return status + spawnGrantStatusSuffix
		}
	}
	return status
}

// spawnLogicalStatus maps a stored status back to the one it refines.
func spawnLogicalStatus(stored string) string {
	switch stored {
	case "bound_grant", "dispatching_grant", "reconciling_grant", "deadline_exceeded_grant":
		return strings.TrimSuffix(stored, spawnGrantStatusSuffix)
	}
	return stored
}

// spawnAttemptStartByTable keeps the latest start_by the hub has sent for
// each grant-required attempt, written before the frame leaves. Once that
// start_by plus the margin has passed, no frame of the attempt can still be
// granted by its node, so any replay (a short recovery reprobe included) may
// fence it before sending again.
const spawnAttemptStartByTable = `CREATE TABLE IF NOT EXISTS spawn_attempt_start_by (node_id TEXT PRIMARY KEY,start_by_ms BIGINT NOT NULL)`

const spawnAttemptGrantsTable = `CREATE TABLE IF NOT EXISTS spawn_attempt_grants (node_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,state TEXT NOT NULL,decided_ms BIGINT NOT NULL)`

// decideSpawnGrant answers a node's job.spawn.claim. Only the current
// grant-required attempt, bound to the asking machine and sent, can be
// granted; the grant is idempotent for that node request id and is refused
// once the attempt is fenced. Anything else is refused without a record.
func (s *laneInboxStore) decideSpawnGrant(parent context.Context, nodeID, machine string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var requestID, status, payload string
	err = tx.QueryRowContext(ctx, s.q(`SELECT request_id,status,node_payload FROM spawn_operations WHERE node_id=?`), nodeID).Scan(&requestID, &status, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Only a row stored under a sent grant status went out with start_by:
	// one an older hub moved to a plain status is never granted.
	var node hubSpawnRequest
	if json.Unmarshal([]byte(payload), &node) != nil || !node.GrantRequired || node.Machine != machine {
		return false, nil
	}
	switch status {
	case "dispatching_grant", "reconciling_grant", "deadline_exceeded_grant":
	default:
		return false, nil
	}
	if _, err = tx.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'granted',?)`, "")), nodeID, requestID, now.UnixMilli()); err != nil {
		return false, err
	}
	var state string
	if err = tx.QueryRowContext(ctx, s.q(`SELECT state FROM spawn_attempt_grants WHERE node_id=?`), nodeID).Scan(&state); err != nil {
		return false, err
	}
	return state == "granted", tx.Commit()
}

// fenceSpawnAttempt records that the attempt nodeID will never be granted.
// It reports true only when the attempt is fenced, i.e. no grant was
// recorded first; then the node cannot have started it.
func (s *laneInboxStore) fenceSpawnAttempt(parent context.Context, requestID, nodeID string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'fenced',?)`, "")), nodeID, requestID, now.UnixMilli()); err != nil {
		return false, err
	}
	var state string
	if err = tx.QueryRowContext(ctx, s.q(`SELECT state FROM spawn_attempt_grants WHERE node_id=?`), nodeID).Scan(&state); err != nil {
		return false, err
	}
	return state == "fenced", tx.Commit()
}

// recordSpawnStartBy raises the recorded start_by of nodeID to startByMS.
func (s *laneInboxStore) recordSpawnStartBy(parent context.Context, nodeID string, startByMS int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.upsertGreatest(`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,?)`, "node_id", "spawn_attempt_start_by", "start_by_ms")), nodeID, startByMS)
	return err
}

// spawnAttemptStartBy returns the latest start_by sent for nodeID, or 0.
func (s *laneInboxStore) spawnAttemptStartBy(parent context.Context, nodeID string) (int64, error) {
	ctx, done := s.op(parent)
	defer done()
	var startBy int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT start_by_ms FROM spawn_attempt_start_by WHERE node_id=?`), nodeID).Scan(&startBy)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return startBy, err
}

// spawnAttemptFenced reports whether the attempt nodeID has a committed fence.
func (s *laneInboxStore) spawnAttemptFenced(parent context.Context, nodeID string) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var state string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT state FROM spawn_attempt_grants WHERE node_id=?`), nodeID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return state == "fenced", err
}

// parseHubSpawnClaim reads a node's {"type":"job.spawn.claim","request_id"}.
func parseHubSpawnClaim(raw []byte) (string, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return "", false
	}
	var kind, requestID string
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "job.spawn.claim" || json.Unmarshal(fields["request_id"], &requestID) != nil || !validHubSpawnRequestID(requestID) {
		return "", false
	}
	return requestID, true
}

// answerSpawnClaim commits the grant decision before it answers. A store
// error sends nothing: the node then gives up at start_by and never starts.
func (h *HubServer) answerSpawnClaim(ctx context.Context, machine string, agent *hubAgent, nodeID string) {
	if h.inbox == nil || h.inbox.store == nil {
		// No durable store, so no sent attempt to grant.
		_ = agent.writeJSONContext(ctx, struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
			Granted   bool   `json:"granted"`
		}{"job.spawn.grant", nodeID, false})
		return
	}
	granted, err := h.inbox.store.decideSpawnGrant(ctx, nodeID, machine, h.now().UTC())
	if err != nil {
		h.logger.Warn("hub spawn grant not decided", "node_request_id", nodeID, "host", machine, "err", err)
		return
	}
	_ = agent.writeJSONContext(ctx, struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Granted   bool   `json:"granted"`
	}{"job.spawn.grant", nodeID, granted})
}

// spawnFencedResult is the node answer a won fence stands for: the attempt
// was never started, exactly as a node's own deadline_not_started.
func spawnFencedResult(node hubSpawnRequest, now time.Time) hubSpawnResult {
	return hubSpawnResult{RequestID: node.RequestID, Machine: node.Machine, JobID: hubSpawnArg(node.Args, "--job"), RC: 124, Stage: "claim", Error: "deadline_not_started", Retryable: true, CompletedAt: &now}
}

// spawnGrantRowCount counts the rows a hub older than the start proof could
// misread: every status with spawnGrantStatusSuffix.
func (s *laneInboxStore) spawnGrantRowCount(parent context.Context) (int, error) {
	ctx, done := s.op(parent)
	defer done()
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM spawn_operations WHERE status LIKE '%\_grant'`+s.dialect.escapeClause()).Scan(&n)
	return n, err
}

// logSpawnGrantRows prints the *_grant row count at startup, so a rollout
// script can gate a rollback on grant_rows=0.
func (h *HubServer) logSpawnGrantRows() {
	if h.inbox == nil || h.inbox.store == nil {
		return
	}
	n, err := h.inbox.store.spawnGrantRowCount(h.closeCtx)
	if err != nil {
		h.logger.Warn("hub spawn grant rows unknown", "err", err)
		return
	}
	h.logger.Info("hub spawn grant rows", "grant_rows", n, "spawn_grant_drain", h.spawnGrantDrain)
}
