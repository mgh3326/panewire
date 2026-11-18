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
	// Kind is "" for the quota-stop close and poolActionKindSupersedeClose
	// for a successor closing its parent (hub_spawn_supersede.go).
	Kind      string         `json:",omitempty"`
	Supersede *poolSupersede `json:",omitempty"`
	// Acked is set once the node confirmed the hub's ack of its
	// intent_cancelled answer (hk 5073); a guarded action then asks no more.
	Acked bool
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

// supersedeActionsOf lists the supersede_close rows of job, any phase: one row
// per parent, so a final row also stops a second one.
func (s *laneInboxStore) supersedeActionsOf(parent context.Context, job string) ([]poolAction, error) {
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
		if action.Data.Kind == poolActionKindSupersedeClose {
			actions = append(actions, action)
		}
	}
	return actions, rows.Err()
}

// replaceSupersedeAction ends a refused supersede row and stores its next
// request in one transaction, so a restart never sees two live rows for a
// parent or none.
func (s *laneInboxStore) replaceSupersedeAction(parent context.Context, old, next poolAction) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	oldData, err := json.Marshal(old.Data)
	if err != nil {
		return err
	}
	nextData, err := json.Marshal(next.Data)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE pool_actions SET phase=?,data=? WHERE id=? AND phase=?`), poolActionRefused, string(oldData), old.ID, poolActionPending)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.New("supersede action changed before retry")
	}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data) VALUES(?,?,?,?,?,?)`), next.ID, next.Job, next.Node, next.Epoch, next.Phase, string(nextData)); err != nil {
		return err
	}
	return tx.Commit()
}

// supersedeFenceSchema and supersedeLandingSchema are also installed by
// Postgres migration 0025.
const supersedeFenceSchema = `CREATE TABLE IF NOT EXISTS supersede_fence (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), generation BIGINT NOT NULL)`

const supersedeLandingSchema = `CREATE TABLE IF NOT EXISTS supersede_landings (
 request_id TEXT PRIMARY KEY, parent TEXT NOT NULL, successor TEXT NOT NULL, owner TEXT NOT NULL,
 created_ms BIGINT NOT NULL, tried_ms BIGINT NOT NULL DEFAULT 0, generation BIGINT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('armed','pending','done')))`

// A landing is armed when its successor request is accepted with the switch on,
// pending once the successor has landed under the same fence, done when decided.
const (
	supersedeLandingArmed   = "armed"
	supersedeLandingPending = "pending"
)

var errSupersedeFenced = errors.New("supersede generation fenced")

// supersedeLanding is the durable intent of a landed explicit implementer
// successor to close its parent (hub_spawn_supersede.go). It exists only for
// landings that happened while --supersede-close was on, and carries the
// fence generation current at that moment: a later generation cancels it.
type supersedeLanding struct {
	RequestID, Parent, Successor, Owner string
	Created, Generation                 int64
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// supersedeGeneration reads the fence a hub dispatches under.
func (s *laneInboxStore) supersedeGeneration(parent context.Context) (int64, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	var generation int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT generation FROM supersede_fence WHERE singleton=1`)).Scan(&generation)
	return generation, err
}

// advanceSupersedeFence revokes every landing and close row of the earlier
// generations. A hub started with --supersede-close=false calls it before it
// accepts any work.
func (s *laneInboxStore) advanceSupersedeFence(parent context.Context) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE supersede_fence SET generation=generation+1 WHERE singleton=1`))
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.New("supersede fence missing")
	}
	return nil
}

// insertSupersedeLanding stores the intent at the current generation; it
// stores nothing when the fence row is absent, which the caller reads as an
// error through the row count.
func (s *laneInboxStore) insertSupersedeLanding(ctx context.Context, db sqlExecer, l supersedeLanding, state string) error {
	result, err := db.ExecContext(ctx, s.q(`INSERT INTO supersede_landings(request_id,parent,successor,owner,created_ms,tried_ms,generation,state) SELECT ?,?,?,?,?,0,generation,? FROM supersede_fence WHERE singleton=1 ON CONFLICT(request_id) DO NOTHING`), l.RequestID, l.Parent, l.Successor, l.Owner, l.Created, state)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		var existing string
		if err := db.QueryRowContext(ctx, s.q(`SELECT request_id FROM supersede_landings WHERE request_id=?`), l.RequestID).Scan(&existing); err != nil {
			return errors.New("supersede fence missing")
		}
	}
	return nil
}

func (s *laneInboxStore) recordSupersedeLanding(parent context.Context, l supersedeLanding) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	return s.insertSupersedeLanding(ctx, s.db, l, supersedeLandingPending)
}

// activateSupersedeLanding turns an armed landing pending, only while the
// fence is still the generation it was armed under. Anything else (accepted
// while the switch was off, or a hub started off since) leaves it armed, and
// the next end pass retires it: it never becomes a close.
func (s *laneInboxStore) activateSupersedeLanding(ctx context.Context, db sqlExecer, id string) error {
	_, err := db.ExecContext(ctx, s.q(`UPDATE supersede_landings SET state='pending' WHERE request_id=? AND state='armed' AND generation=(SELECT generation FROM supersede_fence WHERE singleton=1)`), id)
	return err
}

// pendingSupersedeLandings lists up to limit undecided landings, least
// recently tried first so a few stuck ones never starve the rest.
func (s *laneInboxStore) pendingSupersedeLandings(parent context.Context, limit int) ([]supersedeLanding, error) {
	ctx, cancel := s.op(parent)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id,parent,successor,owner,created_ms,generation FROM supersede_landings WHERE state='pending' ORDER BY tried_ms, created_ms, request_id LIMIT ?`), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []supersedeLanding
	for rows.Next() {
		var l supersedeLanding
		if err := rows.Scan(&l.RequestID, &l.Parent, &l.Successor, &l.Owner, &l.Created, &l.Generation); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// settleSupersedeLanding records a try: done ends the intent, otherwise the
// landing goes to the back of the line. The sweep calls it with done false
// before it probes anything, so a probe that uses up the pass cannot keep the
// same landing at the head.
func (s *laneInboxStore) settleSupersedeLanding(parent context.Context, id string, done bool, now time.Time) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	state := "pending"
	if done {
		state = "done"
	}
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE supersede_landings SET state=?,tried_ms=? WHERE request_id=? AND state='pending'`), state, now.UnixMilli(), id)
	return err
}

// endSupersedeLandings ends every pending landing created before cutoff (all
// of them when cutoff is zero): the retry window passed, or the switch is off.
func (s *laneInboxStore) endSupersedeLandings(parent context.Context, before time.Time) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	cutoff := before.UnixMilli()
	if before.IsZero() {
		cutoff = 1<<62 - 1
	}
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE supersede_landings SET state='done' WHERE state IN ('armed','pending') AND created_ms<?`), cutoff)
	return err
}

// insertSupersedeAction stores a supersede_close row only while the fence is
// still at generation, in one statement.
func (s *laneInboxStore) insertSupersedeAction(parent context.Context, action poolAction, generation int64) error {
	ctx, cancel := s.op(parent)
	defer cancel()
	data, err := json.Marshal(action.Data)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data) SELECT ?,?,?,?,?,? FROM supersede_fence WHERE singleton=1 AND generation=?`), action.ID, action.Job, action.Node, action.Epoch, action.Phase, string(data), generation)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errSupersedeFenced
	}
	return nil
}

// refuseUnsentSupersedeActions ends every pending supersede_close row that has
// stored no close request, so nothing was ever sent for it. The off start runs
// it after advancing the fence: the revocation of such a row is then a plain
// final phase, which any binary reads, with no node I/O.
func (s *laneInboxStore) refuseUnsentSupersedeActions(parent context.Context) error {
	actions, err := s.activePoolActions(parent)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if action.Data.Kind != poolActionKindSupersedeClose || action.Phase != poolActionPending || action.Data.Args != nil {
			continue
		}
		action.Phase = poolActionRefused
		if err := s.savePoolAction(parent, action); err != nil {
			return err
		}
	}
	return nil
}
