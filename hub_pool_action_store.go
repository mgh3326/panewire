package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// poolActionSchema is also installed by Postgres migration 0021.
const poolActionSchema = `CREATE TABLE IF NOT EXISTS pool_actions (
 id TEXT PRIMARY KEY, job TEXT NOT NULL, node TEXT NOT NULL,
 epoch BIGINT NOT NULL CHECK(epoch>0),
 phase TEXT NOT NULL CHECK(phase IN ('pending','settled','done','refused')),
 data TEXT NOT NULL)`

const (
	poolActionPending = "pending"
	poolActionSettled = "settled"
	poolActionDone    = "done"
	poolActionRefused = "refused"
)

// poolAction is the durable ownership fence and replayable close request. Its
// phase becomes final only after a node answer excludes another hk write.
// The settled phase still owes the terminal-record check and owner notice.
type poolAction struct {
	ID, Job, Node, Phase string
	Epoch                uint64
	Data                 poolActionData
}

type poolActionData struct {
	Owner, Text, SpawnRequest string
	Pane, Label, EndReason    string
	Args                      *nodePaneCloseOpArgs
	RetryIssued               bool
}

// retryPoolAction atomically keeps the uncertain request fenced and records
// one successor request. A restart cannot create another successor for it.
func (s *laneInboxStore) retryPoolAction(parent context.Context, action, retry poolAction) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	data, err := json.Marshal(action.Data)
	if err != nil {
		return err
	}
	retryData, err := json.Marshal(retry.Data)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentData string
	if err := tx.QueryRowContext(ctx, s.q(`SELECT data FROM pool_actions WHERE id=? AND phase=?`), action.ID, poolActionPending).Scan(&currentData); err != nil {
		return err
	}
	var current poolActionData
	if err := json.Unmarshal([]byte(currentData), &current); err != nil {
		return err
	}
	if current.RetryIssued {
		return nil
	}
	var other int
	if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM pool_actions WHERE job=? AND phase=? AND id<>?`), action.Job, poolActionPending, action.ID).Scan(&other); err != nil {
		return err
	}
	if other != 0 {
		return nil
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE pool_actions SET data=? WHERE id=? AND phase=?`), string(data), action.ID, poolActionPending)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.New("pool action changed before retry")
	}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data) VALUES(?,?,?,?,?,?)`), retry.ID, retry.Job, retry.Node, retry.Epoch, retry.Phase, string(retryData)); err != nil {
		return err
	}
	return tx.Commit()
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
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM pool_actions WHERE job=? AND phase=? LIMIT 1`), job, poolActionPending).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
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
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id,job,node,epoch,phase,data FROM pool_actions WHERE phase IN (?,?) ORDER BY id`), poolActionPending, poolActionSettled)
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
