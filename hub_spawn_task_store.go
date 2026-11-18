package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

var errSpawnClaimed = errors.New("job_already_claimed")

type hubSpawnOutcome struct {
	RC      int    `json:"rc"`
	Stage   string `json:"stage"`
	Fix     string `json:"fix,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Host    string `json:"host,omitempty"`
	Pane    string `json:"pane,omitempty"`
	Profile string `json:"profile,omitempty"`
	Model   string `json:"model,omitempty"`
	Role    string `json:"role,omitempty"`
	// Pool and AccountKey are the placement record of the job: what the
	// executing host launched it on. Empty on an operation placed before they
	// were recorded, which the exhausted-pool sweep never touches.
	Pool       string `json:"pool,omitempty"`
	AccountKey string `json:"account_key,omitempty"`
	// E6Arm is the experiment marker the submission carried, kept on the
	// operation record so a run stays visible after the fact.
	E6Arm string `json:"e6_arm,omitempty"`
	// LaneUnregistered: the pane exists but hub lane registration failed.
	LaneUnregistered bool `json:"lane_unregistered,omitempty"`
	Retryable        bool `json:"retryable,omitempty"`
	NodeResult       bool `json:"node_result,omitempty"`
	// CheckoutMode is the node's answer for a checkout spec, "sparse",
	// "full" or "branch" (hk 5098, hk 5118): the stored outcome keeps it for a
	// status read after the immediate spawn answer is gone.
	CheckoutMode string `json:"checkout_mode,omitempty"`
	// QuotaRefresh names the shared quota refresh a timeout waited on and its
	// last recorded failure (hub_quota_refresh.go). It is set only when the
	// deadline expired before any node binding.
	QuotaRefresh *hubSpawnRefreshOutcome `json:"quota_refresh,omitempty"`
	StageMS      map[string]int64        `json:"stage_ms,omitempty"`
}

// hubSpawnRefreshWait is one shared quota refresh a request is parked on: the
// machine that measures the pool. It is stored with the operation
// (spawn_operations.refresh_waits) so the link survives a hub restart.
type hubSpawnRefreshWait struct {
	Machine string `json:"machine"`
	Pool    string `json:"pool"`
}

// hubSpawnRefreshOutcome is the last finished attempt for the refresh a timed
// out request waited on. FailedAgoS is set with Reason when it failed;
// SucceededAgoS is set when it finished without a failure, so the timeout can
// tell a failed measurement from one that ran and from one that never ran.
type hubSpawnRefreshOutcome struct {
	Machine       string `json:"machine"`
	Pool          string `json:"pool"`
	Reason        string `json:"reason,omitempty"`
	FailedAgoS    int64  `json:"failed_ago_s,omitempty"`
	SucceededAgoS int64  `json:"succeeded_ago_s,omitempty"`
}

type hubSpawnTask struct {
	Request            fleet.Submission
	Job, Owner, Status string
	Created, Deadline  time.Time
	// Expires is the request's time limit (hk 4660): past it, a request whose
	// start is not unproven ends given_up. Zero on a row from before the
	// column: such a row is never given up.
	Expires time.Time
	// ConditionsMet is when the queue last saw every start condition hold
	// (hk 4736). Zero means the row never passed the queue; a conditioned row
	// with zero here is never launched, whatever status it reached.
	ConditionsMet time.Time
	// ConditionResult is the last start-condition evaluation of a waiting row,
	// so GET /v1/spawns shows why it still waits.
	ConditionResult fleet.StartConditionResult
	// LastRun is when a coordinator worker last took the request (zero: never).
	LastRun time.Time
	// Placed is when the request was bound to its node, the decision the
	// placement count rests on (zero: never bound).
	Placed  time.Time
	Attempt int
	// PlacedAccountKey and PlacedAttempt are the in-flight occupancy of the
	// attempt: the account (and the attempt number) the bind chose, cleared
	// by every status change that leaves the attempt (hk 5124). Empty/zero
	// when the attempt holds no seat.
	PlacedAccountKey string
	PlacedAttempt    int
	Node             *hubSpawnRequest
	Outcome          hubSpawnOutcome
	// ClaimedHost survives attempts: handoffkeep binds a claimed job to a host.
	ClaimedHost string
	// RefreshWaits are the shared quota refreshes an unbound attempt parked
	// on. A timeout before binding names them (and their last failure) instead
	// of the queue-starved cause; binding clears them.
	RefreshWaits []hubSpawnRefreshWait
	// Exclusions are definitive host or harness refusals for this request.
	// They carry across its attempts (hk 4661); only a client's replay of an
	// ended request starts them over.
	Exclusions []hubSpawnExclusion
	// ParkReason and NextCheck park an accepted row no candidate can take now
	// (hk 4661, hub_spawn_park.go): placement skips it until NextCheck or an
	// event that wakes its reason. FailedAttempts counts its failed launches
	// for the re-placement back-off.
	ParkReason     string
	NextCheck      time.Time
	FailedAttempts int
	// ParkSince is when the request's current wait began, kept across its
	// re-parks and binds and restarted by a new attempt; ParkPriority is its
	// bound task's hk priority read for that wait (-1: not read yet). The
	// durable owner-notice marker (hk 4662) is ParkJudgedReason — the park
	// reason set the pass last judged — and ParkNoticeAt, when the owner was
	// told, so a restart never repeats a notice and never loses one.
	ParkSince        time.Time
	ParkPriority     int
	ParkNoticeAt     time.Time
	ParkJudgedReason string
	// ParkPriorityFailures counts the passes that could not read the task for
	// this wait, so a row hk will not answer for leaves the candidate page
	// instead of blocking it forever.
	ParkPriorityFailures int
	// ClaimResult is the node refusal that proved an hk job claim on
	// ClaimedHost (hk 4661): written once, never by a later pre-claim
	// refusal or not-started attestation. A request with one ends with it.
	ClaimResult *hubSpawnOutcome
}

type hubSpawnExclusion struct {
	Machine string `json:"machine"`
	Profile string `json:"profile,omitempty"`
	Reason  string `json:"reason"`
	RC      int    `json:"rc"`
}

func (s *laneInboxStore) ensureSpawnTasks() error {
	if s.dialect == laneInboxPostgres {
		return nil
	}
	ctx, done := s.op(s.life)
	defer done()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS spawn_operations (request_id TEXT PRIMARY KEY,job TEXT NOT NULL,owner TEXT NOT NULL,fingerprint TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,created_ms BIGINT NOT NULL,deadline_ms BIGINT NOT NULL,node_id TEXT UNIQUE,node_payload TEXT NOT NULL DEFAULT '',result TEXT NOT NULL DEFAULT '{}',trace_recorded INTEGER NOT NULL DEFAULT 0,last_run_ms BIGINT NOT NULL DEFAULT 0,attempt INTEGER NOT NULL DEFAULT 1,conditions_met_ms BIGINT NOT NULL DEFAULT 0,condition_result TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS spawn_reservations (job TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,task_role TEXT UNIQUE)`,
		`CREATE TABLE IF NOT EXISTS spawn_notifications (event_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,job TEXT NOT NULL,owner TEXT NOT NULL,result TEXT NOT NULL,created_ms BIGINT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0,telemetry_recorded INTEGER NOT NULL DEFAULT 0)`,
		spawnAttemptGrantsTable,
		spawnAttemptStartByTable,
		`CREATE TABLE IF NOT EXISTS spawn_placement_snapshots (request_id TEXT NOT NULL,attempt INTEGER NOT NULL,payload TEXT NOT NULL,recorded INTEGER NOT NULL DEFAULT 0,delivery TEXT NOT NULL DEFAULT '',PRIMARY KEY(request_id,attempt))`,
		`CREATE TABLE IF NOT EXISTS quota_refresh_attempts (machine TEXT NOT NULL,pool TEXT NOT NULL,outcome TEXT NOT NULL,seen_ms BIGINT NOT NULL,PRIMARY KEY(machine,pool))`,
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// Stores from before hk 4460 lack the placement columns (postgres: the
	// 0008 migration). A restart restores counts from these two columns, and
	// refresh_waits arrived with hk 4600 (postgres: 0011). The four parked
	// notice columns are hk 4662's (postgres: 0019); the placed seat columns
	// are hk 5124's (postgres: 0022), task_fail_notice_ms is hk 5137's
	// (postgres: 0023), and task_fail_retry_ms is hk 5137's (postgres: 0024).
	for _, column := range []string{`placed_ms BIGINT NOT NULL DEFAULT 0`, `claimed_ms BIGINT NOT NULL DEFAULT 0`, `exclusions TEXT NOT NULL DEFAULT '[]'`, `claimed_host TEXT NOT NULL DEFAULT ''`, `refresh_waits TEXT NOT NULL DEFAULT '[]'`, `expires_ms BIGINT NOT NULL DEFAULT 0`, `conditions_met_ms BIGINT NOT NULL DEFAULT 0`, `condition_result TEXT NOT NULL DEFAULT ''`, `park_reason TEXT NOT NULL DEFAULT ''`, `next_check_ms BIGINT NOT NULL DEFAULT 0`, `failed_attempts INTEGER NOT NULL DEFAULT 0`, `claim_result TEXT NOT NULL DEFAULT ''`, `park_since_ms BIGINT NOT NULL DEFAULT 0`, `park_priority INTEGER NOT NULL DEFAULT -1`, `park_notice_ms BIGINT NOT NULL DEFAULT 0`, `park_judged_reason TEXT NOT NULL DEFAULT ''`, `park_priority_failures INTEGER NOT NULL DEFAULT 0`, `placed_account_key TEXT NOT NULL DEFAULT ''`, `placed_attempt INTEGER NOT NULL DEFAULT 0`, `task_fail_notice_ms BIGINT NOT NULL DEFAULT 0`, `task_fail_retry_ms BIGINT NOT NULL DEFAULT 0`} {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE spawn_operations ADD COLUMN `+column); err != nil {
			if err := ignoreDuplicateColumn(err); err != nil {
				return err
			}
		}
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE spawn_placement_snapshots ADD COLUMN delivery TEXT NOT NULL DEFAULT ''`); err != nil {
		if err := ignoreDuplicateColumn(err); err != nil {
			return err
		}
	}
	// The coordinator's periodic spawn scans, indexed the way the Postgres
	// twin is (hk 4889, migration 0020): each statement reads one small key
	// set, and without an index it read the whole table every tick.
	for _, index := range []string{
		`CREATE INDEX IF NOT EXISTS spawn_notifications_delivered ON spawn_notifications(delivered,created_ms)`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_status ON spawn_operations(status)`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_queue ON spawn_operations(last_run_ms,created_ms,request_id) WHERE status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant')`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_parked_wake ON spawn_operations(next_check_ms,request_id) WHERE status='accepted' AND node_id IS NULL`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_park_notice ON spawn_operations(park_since_ms,request_id) WHERE status='accepted' AND node_id IS NULL AND park_reason<>''`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_bound_unclaimed ON spawn_operations(placed_ms) WHERE node_id IS NOT NULL AND claimed_ms=0`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_succeeded ON spawn_operations(created_ms DESC,request_id DESC) WHERE status='succeeded'`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_succeeded_run ON spawn_operations(last_run_ms) WHERE status='succeeded'`,
		`CREATE INDEX IF NOT EXISTS spawn_operations_job ON spawn_operations(job)`,
	} {
		if _, err := s.db.ExecContext(ctx, index); err != nil {
			return err
		}
	}
	// A row an older hub left executable with a start condition belongs to
	// the queue now (hk 4736). This is only a fast path for rows written
	// before the executeSpawnTask guard: it never takes a row the ladder owns
	// (staged) or one whose reservation completeSpawn deleted (retryable — the
	// replay path re-creates the reservation, and the guard then waits it).
	// node_id IS NULL keeps a bound legacy row on its own guard; a bound
	// deadline_exceeded row may still be running on its node.
	if _, err := s.db.ExecContext(ctx, `UPDATE spawn_operations SET status='waiting' WHERE status IN ('accepted','deadline_exceeded','deadline_exceeded_grant') AND node_id IS NULL AND conditions_met_ms=0 AND payload LIKE '%"start_conditions":%'`); err != nil {
		return err
	}
	// The spawn-failure task comment mark is hk 5137's (postgres: 0023): when
	// the sweep judged the row's final failure; the retry column (postgres:
	// 0024) holds a row hk did not take's next attempt. Failed and refused
	// rows that predate the columns are stamped judged once, so the first
	// sweep after deploy does not repost history; rows that turn final later
	// keep mark 0 and post normally. The mark suppresses a repost once its
	// write lands; a hub that stops between the hk post and the mark reposts
	// on the next sweep, so the guarantee is at-least-once across that window.
	if err := s.backfillSpawnTaskFailNotices(ctx, time.Now().UTC()); err != nil {
		return err
	}
	return nil
}

func (s *laneInboxStore) claimSpawn(parent context.Context, request fleet.Submission, job, owner string, now time.Time) (hubSpawnTask, bool, error) {
	// A request with a start condition waits in the queue (hk 4736): the
	// coordinator releases it to accepted once every predicate holds. This is
	// the only place a conditioned row is created; a later reader must not
	// assume accepted implies the condition was met, because an ungated writer
	// (unbind, replay resume, staged activation) may leave it for the
	// executeSpawnTask guard.
	status := "accepted"
	if len(request.StartConditions) != 0 {
		status = "waiting"
	}
	return s.claimSpawnReplacing(parent, request, job, owner, now, "", "", status)
}

// claimSpawnReplacing transfers the task role in the same transaction as the
// successor claim, so another worker cannot claim the gap between them.
func (s *laneInboxStore) claimSpawnReplacing(parent context.Context, request fleet.Submission, job, owner string, now time.Time, previousID, roleKey, status string) (hubSpawnTask, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	spawn, err := fleet.ValidateSubmission(request)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	defer tx.Rollback()
	var oldHash, oldOwner string
	err = tx.QueryRowContext(ctx, s.q(`SELECT fingerprint,owner FROM spawn_operations WHERE request_id=?`), request.RequestID).Scan(&oldHash, &oldOwner)
	if err == nil {
		if oldHash != fingerprint || oldOwner != owner {
			return hubSpawnTask{}, false, errSpawnClaimed
		}
		if err = tx.Commit(); err != nil {
			return hubSpawnTask{}, false, err
		}
		task, err := s.spawnTask(parent, request.RequestID)
		return task, false, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return hubSpawnTask{}, false, err
	}
	// Every spawn producer creates its row here: under a rollback drain none
	// may (hk 4661). A replay of an existing row (above) still answers it, so
	// a staged ladder step or a queued request can settle.
	if s.spawnFrozen {
		return hubSpawnTask{}, false, errSpawnRollbackDrain
	}

	var taskRole any
	if spawn.HoldsTaskRole() {
		taskRole = spawn.RoleKey()
	}
	if previousID != "" {
		result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=? AND task_role=?`), previousID, roleKey)
		if err != nil {
			return hubSpawnTask{}, false, err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return hubSpawnTask{}, false, errSpawnClaimed
		}
		taskRole = roleKey
	}
	claimed, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_reservations(job,request_id,task_role) VALUES(?,?,?) ON CONFLICT DO NOTHING`), job, request.RequestID, taskRole)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	n, _ := claimed.RowsAffected()
	if n != 1 {
		tx.Rollback()
		prior, lookupErr := s.spawnTask(parent, request.RequestID)
		if lookupErr == nil && prior.Owner == owner {
			stored, _ := json.Marshal(prior.Request)
			if bytes.Equal(stored, raw) {
				return prior, false, nil
			}
		}
		return hubSpawnTask{}, false, errSpawnClaimed
	}
	expires := now.Add(fleet.SpawnExpiresFor(spawn.Role, request.ExpiresSeconds))
	deadline := spawnAttemptDeadline(now, request.DeadlineSeconds, expires)
	_, err = tx.ExecContext(ctx, s.q(`INSERT INTO spawn_operations(request_id,job,owner,fingerprint,payload,status,created_ms,deadline_ms,expires_ms) VALUES(?,?,?,?,?,?,?,?,?)`), request.RequestID, job, owner, fingerprint, string(raw), status, now.UnixMilli(), deadline.UnixMilli(), expires.UnixMilli())
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return hubSpawnTask{}, false, err
	}
	return hubSpawnTask{Request: request, Job: job, Owner: owner, Status: status, Created: now, Deadline: deadline, Expires: expires, Attempt: 1}, true, nil
}

func (s *laneInboxStore) activateStagedSpawn(parent context.Context, id string, now time.Time, deadlineSeconds int) error {
	ctx, done := s.op(parent)
	defer done()
	// The landing wait never outlasts the request's limit: a successor
	// activated at or past it has a deadline already passed, and the first
	// pass gives it up unsent instead of placing it (hk 4660).
	deadline := now.Add(time.Duration(deadlineSeconds) * time.Second).UnixMilli()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted', deadline_ms=CASE WHEN expires_ms>0 AND expires_ms<? THEN expires_ms ELSE ? END WHERE request_id=? AND status='staged'`), deadline, deadline, id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	return nil
}

// cancelStagedSpawn restores the old holder only while the staged step has
// not been activated. Both changes commit together.
func (s *laneInboxStore) cancelStagedSpawn(parent context.Context, id, previousID, roleKey string) error {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`DELETE FROM spawn_operations WHERE request_id=? AND status='staged'`), id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	result, err = tx.ExecContext(ctx, s.q(`DELETE FROM spawn_reservations WHERE request_id=? AND task_role=?`), id, roleKey)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	result, err = tx.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=? WHERE request_id=? AND task_role IS NULL`), roleKey, previousID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	return tx.Commit()
}

// discardStagedSpawn releases a staged role when its predecessor ended for a
// different reason. It never restores a reservation to a terminal job.
func (s *laneInboxStore) discardStagedSpawn(parent context.Context, id, roleKey string) error {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`DELETE FROM spawn_operations WHERE request_id=? AND status='staged'`), id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	result, err = tx.ExecContext(ctx, s.q(`DELETE FROM spawn_reservations WHERE request_id=? AND task_role=?`), id, roleKey)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return errSpawnClaimed
	}
	return tx.Commit()
}

// Bind the immutable destination without claiming it was sent. Only the
// bound-to-dispatching transition permits a first send.
func (s *laneInboxStore) bindSpawn(parent context.Context, id string, attempt int, node hubSpawnRequest, now time.Time) (hubSpawnTask, error) {
	return s.bindSpawnPlaced(parent, id, attempt, node, "", now)
}

// bindSpawnPlaced binds attempt N to its host and account in one write: the
// placed columns name the seat the occupancy gate counted, so a second
// admission on the same account sees this bind (hk 5124). A bind with no
// account leaves the columns empty.
func (s *laneInboxStore) bindSpawnPlaced(parent context.Context, id string, attempt int, node hubSpawnRequest, accountKey string, now time.Time) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(node)
	if err != nil {
		return hubSpawnTask{}, err
	}
	// The placement stamp is written with the binding, so no dispatch can run
	// on a bound row that a restart would not count (restorePlacementCounts).
	// A new bound attempt is unclaimed: a claim an earlier attempt left on the
	// row does not describe this placement.
	placedAttempt := 0
	if accountKey != "" {
		placedAttempt = attempt
	}
	_, err = s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET node_id=?,node_payload=?,status=?,placed_ms=?,claimed_ms=0,refresh_waits='[]',park_reason='',placed_account_key=?,placed_attempt=? WHERE request_id=? AND attempt=? AND node_id IS NULL AND status='accepted' AND (claimed_host='' OR claimed_host=?)`), node.RequestID, string(raw), spawnStoredStatus("bound", &node), now.UnixMilli(), accountKey, placedAttempt, id, attempt, node.Machine)
	if err != nil {
		return hubSpawnTask{}, err
	}
	task, err := s.spawnTask(parent, id)
	if err == nil && (task.Attempt != attempt || task.Node == nil) {
		return task, errSpawnClaimed
	}
	if err == nil {
		bound, _ := json.Marshal(task.Node)
		if !bytes.Equal(raw, bound) {
			return task, errSpawnClaimed
		}
	}
	return task, err
}

// beginSpawnSend commits the send boundary, or revalidates a replay, using the
// complete durable identity and the state the caller actually read. Old
// dispatching rows remain uncertain; a restart never makes them bound again.
func (s *laneInboxStore) beginSpawnSend(parent context.Context, task hubSpawnTask) (bool, error) {
	if task.Node == nil {
		return false, nil
	}
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status=CASE WHEN status IN ('bound','bound_grant') THEN ? ELSE status END WHERE request_id=? AND attempt=? AND node_id=? AND status=? AND status IN ('bound','dispatching','reconciling','deadline_exceeded','bound_grant','dispatching_grant','reconciling_grant','deadline_exceeded_grant')`), spawnStoredStatus("dispatching", task.Node), task.Request.RequestID, task.Attempt, task.Node.RequestID, spawnStoredStatus(task.Status, task.Node))
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// Unknown observations retain the reservation. Only a definitive result may
// release it, and only when the executing node has attested complete cleanup.
func (s *laneInboxStore) completeSpawn(parent context.Context, task hubSpawnTask, outcome hubSpawnOutcome, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	if outcome.Host == "" {
		outcome.Host = spawnPinnedHost(task)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if outcome.Reason == "timeout" {
		var node sql.NullString
		var currentStatus string
		if err = tx.QueryRowContext(ctx, s.q(`SELECT node_id,status FROM spawn_operations WHERE request_id=? AND attempt=?`), task.Request.RequestID, task.Attempt).Scan(&node, &currentStatus); err != nil {
			return false, err
		}
		outcome.Retryable = !node.Valid || spawnLogicalStatus(currentStatus) == "bound" || outcome.NodeResult && outcome.Retryable
	}
	outcome.Retryable = outcome.Retryable && spawnRefusalReplayable(outcome.Reason)
	status := spawnOutcomeStatus(outcome)
	if status == "deadline_exceeded" || status == "reconciling" {
		outcome.Retryable = false
	}
	raw, err := json.Marshal(outcome)
	if err != nil {
		return false, err
	}
	nodeID := ""
	if task.Node != nil {
		nodeID = task.Node.RequestID
	}
	stored := spawnStoredStatus(status, task.Node)
	// A terminal post-claim node result is the claim proof a replay keeps; a
	// claim inferred from an older hub's saved result is made durable before
	// this write replaces it.
	legacyHost, legacyProof := spawnLegacyClaim(task)
	proof := spawnClaimProof(outcome, string(raw))
	if proof == "" {
		proof = legacyProof
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status=?,result=?,claim_result=CASE WHEN claim_result='' THEN ? ELSE claim_result END,claimed_host=CASE WHEN claimed_host='' THEN ? ELSE claimed_host END,placed_account_key=CASE WHEN ? IN ('reconciling','reconciling_grant') THEN placed_account_key ELSE '' END,placed_attempt=CASE WHEN ? IN ('reconciling','reconciling_grant') THEN placed_attempt ELSE 0 END WHERE request_id=? AND attempt=? AND status IN ('accepted','bound','dispatching','reconciling','deadline_exceeded','bound_grant','dispatching_grant','reconciling_grant','deadline_exceeded_grant') AND status<>? AND NOT (status IN ('deadline_exceeded','deadline_exceeded_grant') AND ?='reconciling') AND ((?='' AND node_id IS NULL) OR node_id=?) AND (? NOT IN ('accepted','bound') OR status=?) AND (node_id IS NULL OR status IN ('bound','bound_grant') OR ?<>'timeout' OR ?<>'retryable' OR ?)`), stored, string(raw), proof, legacyHost, stored, stored, task.Request.RequestID, task.Attempt, stored, status, nodeID, nodeID, task.Status, spawnStoredStatus(task.Status, task.Node), outcome.Reason, status, outcome.NodeResult)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if status == "retryable" && spawnAttemptEndedUnstarted(task, outcome, now) {
		// An attempt that ended with the start provably absent no longer ends
		// the request: it goes back to pending, or is given up at its limit.
		return s.endUnstartedAttempt(ctx, tx, task, outcome, now)
	}
	if status == "retryable" {
		if _, err = tx.ExecContext(ctx, s.q(`DELETE FROM spawn_reservations WHERE job=? AND request_id=?`), task.Job, task.Request.RequestID); err != nil {
			return false, err
		}
	} else if code, _, _ := strings.Cut(outcome.Reason, ": "); status == "failed" && task.Status == "bound" && code == spawnAbandonedReason {
		// Release the unsent role in the same transaction as its outcome and
		// outbox notification. Any prior hk claim was handled before completion.
		if _, err = tx.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=?`), task.Request.RequestID); err != nil {
			return false, err
		}
	}
	// reconciling is not an outcome: ownership stays and neither lane is told.
	// A later verified landing notifies SPAWNED, a terminal failure SPAWN_FAILED.
	if status == "reconciling" {
		return true, tx.Commit()
	}
	if err = queueSpawnNotifications(ctx, s, tx, task, status, string(raw), now); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// queueSpawnNotifications writes the outbox rows an outcome owes: one for the
// owner lane, and for a refusal or an unstarted end one for the submitting
// lane too.
func queueSpawnNotifications(ctx context.Context, s *laneInboxStore, tx *sql.Tx, task hubSpawnTask, status, raw string, now time.Time) error {
	eventID := fmt.Sprintf("%s:%d:%s", task.Request.RequestID, task.Attempt, status)
	// The owner keeps the bare event id. A failed or retryable spawn also tells
	// the submitting lane, whose only other sign of it was ACCEPTED.
	lanes := []struct{ id, lane string }{{eventID, task.Owner}}
	if status == "failed" || status == "retryable" || status == "given_up" || status == "refused" || status == "cancelled" {
		if lane := spawnSubmitterLane(task.Request.Args); lane != "" && lane != task.Owner {
			lanes = append(lanes, struct{ id, lane string }{eventID + ":" + lane, lane})
		}
	}
	for _, n := range lanes {
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING`), n.id, task.Request.RequestID, task.Job, n.lane, raw, now.UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

func scanSpawnTask(row interface{ Scan(...any) error }) (hubSpawnTask, error) {
	var task hubSpawnTask
	var payload, result, node, exclusions, refreshWaits, conditionResult, claimResult, parkJudgedReason, placedAccountKey string
	var created, deadline, lastRun, placed, expires, conditionsMet, nextCheck, parkSince, parkNoticeAt, placedAttempt int64
	err := row.Scan(&task.Job, &task.Owner, &payload, &task.Status, &created, &deadline, &node, &result, &task.Attempt, &lastRun, &placed, &exclusions, &task.ClaimedHost, &refreshWaits, &expires, &conditionsMet, &conditionResult, &task.ParkReason, &nextCheck, &task.FailedAttempts, &claimResult, &parkSince, &task.ParkPriority, &parkNoticeAt, &parkJudgedReason, &task.ParkPriorityFailures, &placedAccountKey, &placedAttempt)
	if err != nil {
		return task, err
	}
	task.PlacedAccountKey = placedAccountKey
	if placedAttempt > 0 {
		task.PlacedAttempt = int(placedAttempt)
	}
	task.ParkJudgedReason = parkJudgedReason
	if parkSince > 0 {
		task.ParkSince = time.UnixMilli(parkSince)
	}
	if parkNoticeAt > 0 {
		task.ParkNoticeAt = time.UnixMilli(parkNoticeAt)
	}
	stored := task.Status
	task.Status = spawnLogicalStatus(stored)
	task.Created = time.UnixMilli(created)
	task.Deadline = time.UnixMilli(deadline)
	if lastRun > 0 {
		task.LastRun = time.UnixMilli(lastRun)
	}
	if placed > 0 {
		task.Placed = time.UnixMilli(placed)
	}
	if expires > 0 {
		task.Expires = time.UnixMilli(expires)
	}
	if conditionsMet > 0 {
		task.ConditionsMet = time.UnixMilli(conditionsMet)
	}
	if nextCheck > 0 {
		task.NextCheck = time.UnixMilli(nextCheck)
	}
	if claimResult != "" {
		task.ClaimResult = &hubSpawnOutcome{}
		if err = json.Unmarshal([]byte(claimResult), task.ClaimResult); err != nil {
			return task, err
		}
	}
	if conditionResult != "" {
		if err = json.Unmarshal([]byte(conditionResult), &task.ConditionResult); err != nil {
			return task, err
		}
	}
	if err = json.Unmarshal([]byte(payload), &task.Request); err != nil {
		return task, err
	}
	if node != "" {
		task.Node = &hubSpawnRequest{}
		if err = json.Unmarshal([]byte(node), task.Node); err != nil {
			return task, err
		}
		// Provenance is the stored status, not the payload flag: a row an older
		// hub moved to a plain status may have gone out with no start_by, so
		// it is never treated as grant-required again.
		if spawnLogicalStatus(stored) == stored {
			task.Node.GrantRequired = false
		}
	}
	if err = json.Unmarshal([]byte(result), &task.Outcome); err != nil {
		return task, err
	}
	err = json.Unmarshal([]byte(exclusions), &task.Exclusions)
	if err != nil {
		return task, err
	}
	return task, json.Unmarshal([]byte(refreshWaits), &task.RefreshWaits)
}

// unbindRefusedSpawn records the refusal in the same CAS that releases the
// destination. A claimed refusal also pins the host and saves its node result.
// The refusal is a failed attempt: the request is parked launch_backoff until
// its back-off passes (hk 4661).
func (s *laneInboxStore) unbindRefusedSpawn(parent context.Context, task hubSpawnTask, exclusion hubSpawnExclusion, claimed *hubSpawnOutcome, now time.Time) (hubSpawnTask, error) {
	if task.Node == nil || (task.Status != "dispatching" && task.Status != "reconciling") {
		return hubSpawnTask{}, errSpawnClaimed
	}
	exclusions := append(append([]hubSpawnExclusion(nil), task.Exclusions...), exclusion)
	raw, err := json.Marshal(exclusions)
	if err != nil {
		return hubSpawnTask{}, err
	}
	claimedHost, claimedResult, claimProof := "", "", ""
	if claimed != nil {
		claimedHost = task.Node.Machine
		result, err := json.Marshal(claimed)
		if err != nil {
			return hubSpawnTask{}, err
		}
		claimedResult = string(result)
		// Only a refusal after the job claim proves one; the first such
		// proof stays (claim_result is set once).
		claimProof = spawnClaimProof(*claimed, claimedResult)
	}
	legacyHost, legacyProof := spawnLegacyClaim(task)
	if claimProof == "" {
		claimProof = legacyProof
	}
	if claimedHost == "" {
		claimedHost = legacyHost
	}
	// A claimed-harness retry is not backed off: hk holds the job claim on
	// that host, and a park that ran into expires_at would give the request
	// up with the claim still held. It retries at once as before.
	parkReason, retryAt := "", int64(0)
	if claimed == nil {
		parkReason, retryAt = spawnBackoffPark(s.spawnRetryAt(task, task.FailedAttempts+1, now))
	}
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET node_id=NULL,node_payload='',status='accepted',exclusions=?,failed_attempts=failed_attempts+1,park_reason=?,next_check_ms=?,park_since_ms=CASE WHEN park_since_ms=0 AND ?<>'' THEN CAST(? AS BIGINT) ELSE park_since_ms END,park_priority=CASE WHEN park_since_ms=0 THEN -1 ELSE park_priority END,park_priority_failures=CASE WHEN park_since_ms=0 THEN 0 ELSE park_priority_failures END,claimed_host=CASE WHEN ?='' THEN claimed_host ELSE ? END,result=CASE WHEN ?='' THEN result ELSE ? END,claim_result=CASE WHEN claim_result='' THEN ? ELSE claim_result END,placed_account_key='',placed_attempt=0 WHERE request_id=? AND attempt=? AND node_id=? AND status=? AND (claimed_host='' OR ?='' OR claimed_host=?)`), string(raw), parkReason, retryAt, parkReason, now.UnixMilli(), claimedHost, claimedHost, claimedResult, claimedResult, claimProof, task.Request.RequestID, task.Attempt, task.Node.RequestID, spawnStoredStatus(task.Status, task.Node), claimedHost, claimedHost)
	if err != nil {
		return hubSpawnTask{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return hubSpawnTask{}, errSpawnClaimed
	}
	return s.spawnTask(parent, task.Request.RequestID)
}

// An unsent binding can be returned to placement without a node result.
func (s *laneInboxStore) unbindUnsentSpawn(parent context.Context, id string, attempt int, nodeID string) (hubSpawnTask, error) {
	return s.unbindSpawn(parent, id, attempt, nodeID, "bound")
}

func (s *laneInboxStore) unbindSpawn(parent context.Context, id string, attempt int, nodeID, status string) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET node_id=NULL,node_payload='',status='accepted',placed_account_key='',placed_attempt=0 WHERE request_id=? AND attempt=? AND node_id=? AND status IN (?,?)`), id, attempt, nodeID, status, status+spawnGrantStatusSuffix)
	if err != nil {
		return hubSpawnTask{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return hubSpawnTask{}, errSpawnClaimed
	}
	return s.spawnTask(parent, id)
}

const spawnTaskColumns = `job,owner,payload,status,created_ms,deadline_ms,node_payload,result,attempt,last_run_ms,placed_ms,exclusions,claimed_host,refresh_waits,expires_ms,conditions_met_ms,condition_result,park_reason,next_check_ms,failed_attempts,claim_result,park_since_ms,park_priority,park_notice_ms,park_judged_reason,park_priority_failures,placed_account_key,placed_attempt`

func (s *laneInboxStore) spawnTask(parent context.Context, id string) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	return scanSpawnTask(s.db.QueryRowContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE request_id=?`), id))
}
func (s *laneInboxStore) latestSpawnForJob(parent context.Context, job string) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	return scanSpawnTask(s.db.QueryRowContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE job=? ORDER BY created_ms DESC,request_id DESC LIMIT 1`), job))
}

// quotaSuccessorSpawns lists recent operations whose job id is a quota-stop
// successor ("<old>-q<n>"), newest first.
func (s *laneInboxStore) quotaSuccessorSpawns(parent context.Context) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE job LIKE ? ORDER BY created_ms DESC LIMIT 200`), "%-q%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
func (s *laneInboxStore) spawnTaskByNode(parent context.Context, id string) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	return scanSpawnTask(s.db.QueryRowContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE node_id=?`), id))
}

// markSpawnClaimed records that a job's claim reached the hub from the node it
// was placed on. A restart must not count that job again, even if it finished
// before the restart and no heartbeat will show it.
func (s *laneInboxStore) markSpawnClaimed(parent context.Context, job string, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET claimed_ms=? WHERE job=? AND node_id IS NOT NULL AND claimed_ms=0`), now.UnixMilli(), job)
	return err
}

// boundSpawnsSince lists the operations placed at or after since that are bound
// to a node, not yet claimed, and may still hold an unclaimed launch: in flight,
// reconciling, timed out while bound, or succeeded. Refused, failed and
// retryable ones released their count and are left out. Oldest placement first,
// so a job's newest placement is read last (restorePlacementCounts,
// placement_count.go).
func (s *laneInboxStore) boundSpawnsSince(parent context.Context, since time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE node_id IS NOT NULL AND placed_ms>=? AND claimed_ms=0 AND (status IN ('bound','dispatching','reconciling','succeeded','bound_grant','dispatching_grant','reconciling_grant','deadline_exceeded_grant') OR status='deadline_exceeded') ORDER BY placed_ms`), since.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// pendingSpawns returns the fresh runnable page and a bounded number of due
// stale rows. Conditioned rows wait as status waiting, so they hold no slot
// here until the start-condition check releases them.
func (s *laneInboxStore) pendingSpawns(parent context.Context, now time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	tasks, err := s.queryPendingSpawns(ctx, fmt.Sprintf(`status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant') AND (status<>'accepted' OR next_check_ms<=%d) ORDER BY last_run_ms,created_ms LIMIT 32`, now.UnixMilli()))
	if err != nil {
		return nil, err
	}
	stale, err := s.queryPendingSpawns(ctx, `status IN ('deadline_exceeded','deadline_exceeded_grant') ORDER BY last_run_ms,created_ms LIMIT 256`)
	if err != nil {
		return nil, err
	}
	due := 0
	for _, task := range stale {
		if due < spawnStaleBatch && spawnStaleProbeDue(task, now) {
			tasks = append(tasks, task)
			due++
		}
	}
	return tasks, nil
}

func (s *laneInboxStore) queryPendingSpawns(ctx context.Context, where string) ([]hubSpawnTask, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE `+where)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

type spawnScanPosition struct {
	lastRun, created int64
	id               string
	valid            bool
}

// pendingSpawnPage continues through the durable queue even when earlier rows
// are active or waiting for a busy node. No query loads an unbounded backlog.
// A parked row (hk 4661) is left out until its next check.
func (s *laneInboxStore) pendingSpawnPage(parent context.Context, after spawnScanPosition, now time.Time) ([]hubSpawnTask, spawnScanPosition, error) {
	ctx, done := s.op(parent)
	defer done()
	query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant') AND (status<>'accepted' OR next_check_ms<=?)`
	args := []any{now.UnixMilli()}
	if after.valid {
		query += ` AND (last_run_ms>? OR (last_run_ms=? AND (created_ms>? OR (created_ms=? AND request_id>?))))`
		args = append(args, after.lastRun, after.lastRun, after.created, after.created, after.id)
	}
	query += ` ORDER BY last_run_ms,created_ms,request_id LIMIT 32`
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, after, err
		}
		tasks = append(tasks, task)
		lastRun := int64(0)
		if !task.LastRun.IsZero() {
			lastRun = task.LastRun.UnixMilli()
		}
		after = spawnScanPosition{lastRun: lastRun, created: task.Created.UnixMilli(), id: task.Request.RequestID, valid: true}
	}
	return tasks, after, rows.Err()
}

// A deliberate repeat of an identical, cleanup-confirmed submission starts a
// new attempt while preserving the logical idempotency key and old outbox rows.
func (s *laneInboxStore) resumeCleanedSpawn(parent context.Context, task hubSpawnTask, now time.Time) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return task, err
	}
	defer tx.Rollback()
	if err = s.freezeSpawnPlacementForRetry(ctx, tx, task); err != nil {
		return task, err
	}
	spawn, err := fleet.ValidateSubmission(task.Request)
	if err != nil {
		return task, err
	}
	var taskRole any
	if spawn.HoldsTaskRole() {
		taskRole = spawn.RoleKey()
	}
	result, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_reservations(job,request_id,task_role) VALUES(?,?,?) ON CONFLICT DO NOTHING`), task.Job, task.Request.RequestID, taskRole)
	if err != nil {
		return task, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return task, errSpawnClaimed
	}
	// A deliberate repeat asks for the request again, so its time limit
	// starts over with the new attempt. The new attempt holds no seat yet:
	// the placed columns clear in this same write (hk 5124). The outcome
	// stays the attempt's terminal record.
	expires := now.Add(fleet.SpawnExpiresFor(spawn.Role, task.Request.ExpiresSeconds))
	result, err = tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',deadline_ms=?,expires_ms=?,node_id=NULL,node_payload='',attempt=attempt+1,trace_recorded=0,last_run_ms=0,exclusions='[]',claimed_host=?,refresh_waits='[]',park_since_ms=0,park_priority=-1,park_priority_failures=0,placed_account_key='',placed_attempt=0 WHERE request_id=? AND status='retryable' AND attempt=?`), spawnAttemptDeadline(now, task.Request.DeadlineSeconds, expires).UnixMilli(), expires.UnixMilli(), spawnPinnedHost(task), task.Request.RequestID, task.Attempt)
	if err != nil {
		return task, err
	}
	n, _ = result.RowsAffected()
	if n != 1 {
		return task, errSpawnClaimed
	}
	if err = tx.Commit(); err != nil {
		return task, err
	}
	return s.spawnTask(parent, task.Request.RequestID)
}
func (s *laneInboxStore) touchSpawn(parent context.Context, id string, now time.Time) {
	ctx, done := s.op(parent)
	defer done()
	_, _ = s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET last_run_ms=? WHERE request_id=?`), now.UnixMilli(), id)
}

func (s *laneInboxStore) spawnRoleOwner(parent context.Context, roleKey string) (id, job, status string, err error) {
	ctx, done := s.op(parent)
	defer done()
	err = s.db.QueryRowContext(ctx, s.q(`SELECT o.request_id,o.job,o.status FROM spawn_reservations r JOIN spawn_operations o ON r.request_id=o.request_id WHERE r.task_role=?`), roleKey).Scan(&id, &job, &status)
	status = spawnLogicalStatus(status)
	return
}

// spawnJobReserved reports whether a reservation already holds the job id, the
// collision a duplicate job id makes before any role is considered.
func (s *laneInboxStore) spawnJobReserved(parent context.Context, job string) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var one int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT 1 FROM spawn_reservations WHERE job=?`), job).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *laneInboxStore) expiredStagedSpawns(parent context.Context, now time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='staged' AND deadline_ms<?`), now.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// retryableSpawns lists the operations that ended with verified cleanup and
// wait for a replay of their own request.
func (s *laneInboxStore) retryableSpawns(parent context.Context) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='retryable'`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
func (s *laneInboxStore) releaseCompletedSpawnRole(parent context.Context, id, roleKey string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=? AND task_role=? AND EXISTS(SELECT 1 FROM spawn_operations WHERE request_id=? AND status IN ('succeeded','failed'))`), id, roleKey, id)
	return err
}

// releaseCompletedSpawnRoleOf frees whatever slot the finished request id
// holds, whatever its key says.
func (s *laneInboxStore) releaseCompletedSpawnRoleOf(parent context.Context, id string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=? AND task_role IS NOT NULL AND EXISTS(SELECT 1 FROM spawn_operations WHERE request_id=? AND status IN ('succeeded','failed'))`), id, id)
	return err
}

func spawnOutcomeStatus(outcome hubSpawnOutcome) string {
	if outcome.RC == 0 {
		return "succeeded"
	}
	switch outcome.Reason {
	case "timeout":
		if outcome.Retryable {
			return "retryable"
		}
		return "deadline_exceeded"
	case "disconnected", "unknown":
		return "reconciling"
	}
	if outcome.Retryable {
		return "retryable"
	}
	return "failed"
}

// With no persisted node binding this attempt cannot have dispatched a spawn.
// Recover placement under the existing reservation, never under a fresh key.
func (s *laneInboxStore) resumeUndispatchedSpawn(parent context.Context, task hubSpawnTask) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',placed_account_key='',placed_attempt=0 WHERE request_id=? AND attempt=? AND node_id IS NULL AND status IN ('reconciling','reconciling_grant')`), task.Request.RequestID, task.Attempt)
	if err != nil {
		return task, err
	}
	return s.spawnTask(parent, task.Request.RequestID)
}

// noteSpawnRefreshWaits records the shared quota refreshes an unbound attempt
// is parked on. A timeout that later settles the attempt names them, and the
// last failure stored for each, instead of the queue-starved cause; binding
// the attempt clears them. It is a link, not a decision: a failed write leaves
// placement pending on its own terms.
func (s *laneInboxStore) noteSpawnRefreshWaits(parent context.Context, id string, attempt int, waits []hubSpawnRefreshWait) error {
	raw, err := json.Marshal(waits)
	if err != nil {
		return err
	}
	ctx, done := s.op(parent)
	defer done()
	_, err = s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET refresh_waits=? WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`), string(raw), id, attempt)
	return err
}

// recordQuotaRefreshAttempt persists the latest finished attempt for one
// machine/pool, success included (outcome ""), so a request that times out on
// that key can tell a failed measurement from one that finished and from one
// that never ran. The scheduler's in-memory copy only suppresses repeated logs.
func (s *laneInboxStore) recordQuotaRefreshAttempt(parent context.Context, machine, pool, outcome string, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO quota_refresh_attempts(machine,pool,outcome,seen_ms) VALUES(?,?,?,?) ON CONFLICT(machine,pool) DO UPDATE SET outcome=excluded.outcome,seen_ms=excluded.seen_ms`), machine, pool, outcome, now.UnixMilli())
	return err
}

// quotaRefreshAttempt reads the last finished attempt for one machine/pool.
func (s *laneInboxStore) quotaRefreshAttempt(parent context.Context, machine, pool string) (string, time.Time, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var outcome string
	var seen int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT outcome,seen_ms FROM quota_refresh_attempts WHERE machine=? AND pool=?`), machine, pool).Scan(&outcome, &seen)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	return outcome, time.UnixMilli(seen), true, nil
}

// clearSpawnRefreshWaits drops the link a previous placement pass recorded, so
// a pass that ends for another reason cannot leave a stale one behind.
func (s *laneInboxStore) clearSpawnRefreshWaits(parent context.Context, id string, attempt int) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET refresh_waits='[]' WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`), id, attempt)
	return err
}

// spawnClaimProof is the outcome to keep as claim_result: a node result from
// a stage after the job claim that is not a not-started attestation, or "".
func spawnClaimProof(outcome hubSpawnOutcome, raw string) string {
	// A timeout is never proof: uncertain when not cleaned up, and a cleaned-up
	// one is the not-started evidence the time limit renews on (hk 4660).
	code, _, _ := strings.Cut(outcome.Reason, ": ")
	if outcome.NodeResult && !spawnStageBeforeJobClaim(outcome.Stage) && !spawnOutcomeNotStarted(outcome) && code != "timeout" {
		return raw
	}
	return ""
}

// spawnLegacyClaim is a claim held only by an older hub's saved result (no
// claimed_host, no claim_result): its host and the result, to persist before
// a later write replaces that result. Empty when there is none.
func spawnLegacyClaim(task hubSpawnTask) (string, string) {
	if task.ClaimedHost != "" || task.ClaimResult != nil || !spawnReplayPinsHost(task.Outcome) {
		return "", ""
	}
	raw, err := json.Marshal(task.Outcome)
	if err != nil {
		return "", ""
	}
	return task.Outcome.Host, string(raw)
}
