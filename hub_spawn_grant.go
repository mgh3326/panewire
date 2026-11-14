package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

const spawnAttemptGrantsTable = `CREATE TABLE IF NOT EXISTS spawn_attempt_grants (node_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,state TEXT NOT NULL,decided_ms BIGINT NOT NULL)`

// decideSpawnGrant answers a node's job.spawn.claim. Only the current
// grant-required attempt, bound to the asking machine and sent, can be
// granted; the grant is idempotent for that node request id and is refused
// once the attempt is fenced. Anything else is refused without a record.
func (s *laneInboxStore) decideSpawnGrant(parent context.Context, nodeID, machine string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
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
	var node hubSpawnRequest
	if json.Unmarshal([]byte(payload), &node) != nil || !node.GrantRequired || node.Machine != machine {
		return false, nil
	}
	switch status {
	case "dispatching", "reconciling", "deadline_exceeded":
	default:
		return false, nil
	}
	if _, err = tx.ExecContext(ctx, s.q(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'granted',?) ON CONFLICT DO NOTHING`), nodeID, requestID, now.UnixMilli()); err != nil {
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, s.q(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'fenced',?) ON CONFLICT DO NOTHING`), nodeID, requestID, now.UnixMilli()); err != nil {
		return false, err
	}
	var state string
	if err = tx.QueryRowContext(ctx, s.q(`SELECT state FROM spawn_attempt_grants WHERE node_id=?`), nodeID).Scan(&state); err != nil {
		return false, err
	}
	return state == "fenced", tx.Commit()
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
