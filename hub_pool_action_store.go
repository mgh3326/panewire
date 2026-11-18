package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// poolActionSchema is also installed by Postgres migration 0021.
const poolActionSchema = `CREATE TABLE IF NOT EXISTS pool_actions (
 id TEXT PRIMARY KEY, job TEXT NOT NULL, node TEXT NOT NULL,
 epoch BIGINT NOT NULL CHECK(epoch>0),
 phase TEXT NOT NULL CHECK(phase IN ('pending','guarded','settled','done','refused')),
 data TEXT NOT NULL)`

const (
	poolActionPending = "pending"
	poolActionGuarded = "guarded"
	poolActionSettled = "settled"
	poolActionDone    = "done"
	poolActionRefused = "refused"
)

// poolAction is the durable ownership fence and replayable close request. Its
// A guarded action needs no more close commands but retains the ownership
// fence until the old job cannot write. A settled action still owes the
// terminal-record check and owner notice.
type poolAction struct {
	ID, Job, Node, Phase string
	Epoch                uint64
	Data                 poolActionData
}

type poolActionData struct {
	Owner, Text, SpawnRequest string
	Pane, Label, EndReason    string
	TargetAttempt             string
	Args                      *nodePaneCloseOpArgs
	RetryID                   string
	RetryAfter                int64
	RetryCount                uint8
	// Kind is "" for a close (reviewer, consult), poolActionKindHandover for
	// an implementer hand-over and poolActionKindWait for a waited stop.
	Kind     string        `json:",omitempty"`
	Handover *poolHandover `json:",omitempty"`
}

// retryPoolAction atomically keeps the uncertain request fenced and records
// one successor request. A restart cannot create another successor for it.
func (s *laneInboxStore) retryPoolAction(parent context.Context, action, retry poolAction, now time.Time) (bool, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var currentData string
	if err := tx.QueryRowContext(ctx, s.q(`SELECT data FROM pool_actions WHERE id=? AND phase=?`), action.ID, poolActionPending).Scan(&currentData); err != nil {
		return false, err
	}
	var current poolActionData
	if err := json.Unmarshal([]byte(currentData), &current); err != nil {
		return false, err
	}
	if now.UnixMilli() < current.RetryAfter {
		return false, nil
	}
	if current.RetryID != "" {
		var phase string
		if err := tx.QueryRowContext(ctx, s.q(`SELECT phase FROM pool_actions WHERE id=? AND job=?`), current.RetryID, action.Job).Scan(&phase); err != nil {
			return false, err
		}
		if phase != poolActionRefused {
			return false, nil
		}
	}
	current.RetryID = retry.ID
	current.RetryAfter = now.Add(poolRetryBackoff(current.RetryCount)).UnixMilli()
	if current.RetryCount < 7 {
		current.RetryCount++
	}
	// Carry the same budget to an unknown successor: a chain of uncertain
	// closes must not mint a fresh request on every reconciliation pass.
	retry.Data.RetryAfter, retry.Data.RetryCount = current.RetryAfter, current.RetryCount
	retryData, err := json.Marshal(retry.Data)
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(current)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE pool_actions SET data=? WHERE id=? AND phase=? AND data=?`), string(data), action.ID, poolActionPending, currentData)
	if err != nil {
		return false, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return false, errors.New("pool action changed before retry")
	}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data) VALUES(?,?,?,?,?,?)`), retry.ID, retry.Job, retry.Node, retry.Epoch, retry.Phase, string(retryData)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func poolRetryBackoff(count uint8) time.Duration {
	delay := 10 * time.Second
	for i := uint8(0); i < count && delay < 10*time.Minute; i++ {
		delay *= 2
	}
	return min(delay, 10*time.Minute)
}

func (s *laneInboxStore) poolRetryReady(parent context.Context, action poolAction, now time.Time) (bool, error) {
	if now.UnixMilli() < action.Data.RetryAfter {
		return false, nil
	}
	if action.Data.RetryID == "" {
		return true, nil
	}
	ctx, cancel := s.op(parent)
	defer cancel()
	var phase string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT phase FROM pool_actions WHERE id=? AND job=?`), action.Data.RetryID, action.Job).Scan(&phase)
	return phase == poolActionRefused, err
}

func (s *laneInboxStore) insertPoolAction(parent context.Context, action poolAction) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	data, err := json.Marshal(action.Data)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data) VALUES(?,?,?,?,?,?)`), action.ID, action.Job, action.Node, action.Epoch, action.Phase, string(data))
	return err
}

func (s *laneInboxStore) savePoolAction(parent context.Context, action poolAction) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	data, err := json.Marshal(action.Data)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.q(`UPDATE pool_actions SET phase=?,data=? WHERE id=?`), action.Phase, string(data), action.ID)
	return err
}

func (s *laneInboxStore) pendingPoolAction(parent context.Context, job string) (string, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var id string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM pool_actions WHERE job=? AND phase IN (?,?) LIMIT 1`), job, poolActionPending, poolActionGuarded).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

func (s *laneInboxStore) guardedPoolAction(parent context.Context, job string) (bool, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var id string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM pool_actions WHERE job=? AND phase=? LIMIT 1`), job, poolActionGuarded).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *laneInboxStore) hasPoolAction(parent context.Context, job string) (bool, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var id string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM pool_actions WHERE job=? LIMIT 1`), job).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// poolNoticeOwned checks whether the normal durable inbox or relay already
// owns this fixed-id notice. Once it does, restart recovery belongs to the
// inbox/relay rather than another publish by the action reconciler.
func (s *laneInboxStore) poolNoticeOwned(parent context.Context, owner, eventID string) (bool, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var class string
	var relayID, digestAt, holdUntil, supersededAt int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT class,relay_id,digest_at,hold_until,superseded_at FROM lane_inbox WHERE event_key=?`),
		s.storedKey(relayLaneEventOutboxKey(owner, eventID))).Scan(&class, &relayID, &digestAt, &holdUntil, &supersededAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return class == "inbox" || relayID != 0 || digestAt != 0 || holdUntil != 0 || supersededAt != 0, err
}

func (s *laneInboxStore) activePoolActions(parent context.Context) ([]poolAction, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id,job,node,epoch,phase,data FROM pool_actions WHERE phase IN (?,?,?) ORDER BY id`), poolActionPending, poolActionGuarded, poolActionSettled)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []poolAction
	for rows.Next() {
		var action poolAction
		var data string
		if err := rows.Scan(&action.ID, &action.Job, &action.Node, &action.Epoch, &action.Phase, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(data), &action.Data); err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}

// lockWithoutPoolAction excludes creation of an action throughout a generic
// role-release attempt. An unreadable store cannot prove that release is safe.
func (h *HubServer) lockWithoutPoolAction(ctx context.Context, job string) (func(), bool) {
	unlock, err := h.poolActionLocks.lock(ctx, job)
	if err != nil {
		return nil, false
	}
	if h.inbox == nil {
		unlock()
		return nil, false
	}
	id, err := h.inbox.store.pendingPoolAction(ctx, job)
	if err != nil || id != "" {
		unlock()
		return nil, false
	}
	return unlock, true
}

// activePoolActionFor reports a non-final row of any kind for job: one row
// per job (hk 4633).
func (s *laneInboxStore) activePoolActionFor(parent context.Context, job string) (bool, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var id string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM pool_actions WHERE job=? AND phase IN (?,?,?) LIMIT 1`), job, poolActionPending, poolActionGuarded, poolActionSettled).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// activeHandover reports a non-final handover row for job: the fence that
// keeps the quota-stop replacement off a job the hub is handing over.
func (s *laneInboxStore) activeHandover(parent context.Context, job string) (bool, error) {
	rows, err := s.poolActionsOf(parent, job)
	if err != nil {
		return false, err
	}
	for _, action := range rows {
		if action.Data.Kind == poolActionKindHandover && action.Phase != poolActionDone && action.Phase != poolActionRefused {
			return true, nil
		}
	}
	return false, nil
}

// poolWaitResets lists the resets of the stops already waited out for job.
func (s *laneInboxStore) poolWaitResets(parent context.Context, job string) ([]int64, error) {
	rows, err := s.poolActionsOf(parent, job)
	if err != nil {
		return nil, err
	}
	var resets []int64
	for _, action := range rows {
		if action.Data.Kind == poolActionKindWait && action.Data.Handover != nil {
			resets = append(resets, action.Data.Handover.Reset)
		}
	}
	return resets, nil
}

func (s *laneInboxStore) poolActionsOf(parent context.Context, job string) ([]poolAction, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id,job,node,epoch,phase,data FROM pool_actions WHERE job=? ORDER BY id`), job)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []poolAction
	for rows.Next() {
		var action poolAction
		var data string
		if err := rows.Scan(&action.ID, &action.Job, &action.Node, &action.Epoch, &action.Phase, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(data), &action.Data); err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}
