package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// The stall-detect tables are the node's own journal for worker-stall
// observations. They sit beside idle_wake_* and share the same rule:
// observation records are durable, and nothing in them may turn into a write
// toward a worker, a batch decision, or a quota number.

// stallCauseVocabulary is the closed set of observation names. Each names
// only what was observed; none asserts that a worker is stopped.
const (
	stallCauseLimitRefused     = "limit_refused"
	stallCauseAuthRefused      = "auth_refused"
	stallCauseInputUnsubmitted = "input_unsubmitted"
	stallCauseOverdue          = "overdue"
	stallCauseUnobserved       = "unobserved"
	stallCauseUnowned          = "unowned"
	stallCauseConflict         = "conflict"
	stallCauseStartupSuspect   = "startup_block_suspect"
)

const (
	stallDeadlineIssuer     = "issuer"
	stallDeadlineReps       = "reps"
	stallDeadlineUnmeasured = "unmeasured"
)

const (
	stallNotifyShadow    = "shadow"   // recorded, never sent — rollout step 1
	stallNotifyPending   = "pending"  // owed to the lane, not yet attempted
	stallNotifySent      = "sent"     // emit accepted locally; receipt unproven
	stallNotifyAcked     = "acked"    // relay persisted by the hub
	stallNotifyNoLane    = "no_lane"  // no valid lane existed to notify
	stallNotifyExhausted = "lapsed"   // grace expired while still unacknowledged
)

// stallMS and stallTime keep zero times zero: UnixMilli on the zero time is a
// large negative that would otherwise read back as a 1970 deadline.
func stallMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func stallTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// stallJobRow is one (job, attempt) scope. A new job.spawned event opens a new
// attempt; an explicit round on the spawn record overrides the attempt-derived
// round so later rounds of one job never share an incident key.
type stallJobRow struct {
	JobID          string
	Attempt        int64
	Round          int64
	OwnerLane      string
	ParentLane     string
	AgentLabel     string
	Harness        string
	Profile        string
	PaneID         string
	WorkspaceID    string
	CWD            string
	ClaimedAt      time.Time
	SpawnedAt      time.Time
	DeadlineAt     time.Time // zero = unmeasured
	DeadlineSource string
	Terminal       bool
	TerminalKind   string
	LastEventAt    time.Time
	ReportPath     string
}

type stallIncidentRow struct {
	JobID        string
	Attempt      int64
	Round        int64
	Cause        string
	Occurrence   int64
	PaneID       string
	Observable   bool
	FirstSeenAt  time.Time
	LastSeenAt   time.Time
	RecoveredAt  time.Time
	Evidence     json.RawMessage
	NotifyState  string
}

// incidentKey is the durable five-field identity of one incident row.
func (row stallIncidentRow) incidentKey() string {
	return fmt.Sprintf("%s\x00%d\x00%d\x00%s\x00%d", row.JobID, row.Attempt, row.Round, row.Cause, row.Occurrence)
}

type stallNotificationRow struct {
	IncidentKey      string
	Lane             string
	Level            int64
	EventID          string
	NotifiedAt       time.Time
	AckedAt          time.Time
	Attempts         int64
	NextRetryAt      time.Time
	GraceDeadlineAt  time.Time
	SuppressedReason string
}

type stallPaneRow struct {
	PaneID        string
	JobID         string
	Attempt       int64
	WorkspaceID   string
	CWD           string
	Harness       string
	LastRevision  int64
	LastReadAt    time.Time
	LastReadOK    bool
	BaselineDone  bool
	Fingerprints  map[string]int64
	FirstSeenAt   time.Time
	SpawnedRecent bool
	Subscribed    bool
}

type stallReportRow struct {
	JobID       string
	Attempt     int64
	ReportPath  string
	LocalPath   string
	SHA256      string
	RemoteKey   string
	RemoteState string
	RemoteAt    time.Time
	RemoteError string
	FinalizedAt time.Time
}

// stallUnownedRow tracks a process that may lack an owning job. Recording is
// deliberately two-scan: a single sighting during a spawn/resubscribe window
// proves nothing about ownership.
type stallUnownedRow struct {
	ProcKey        string
	PID            int64
	PPID           int64
	CWD            string
	StartedAt      time.Time
	FirstSeenAt    time.Time
	Confirmations  int64
	Recorded       bool
}

func stallDetectMigrate(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS stall_jobs (
		 job_id TEXT NOT NULL, attempt INTEGER NOT NULL,
		 owner_lane TEXT NOT NULL DEFAULT '', parent_lane TEXT NOT NULL DEFAULT '',
		 agent_label TEXT NOT NULL DEFAULT '', harness TEXT NOT NULL DEFAULT '', profile TEXT NOT NULL DEFAULT '',
		 pane_id TEXT NOT NULL DEFAULT '', workspace_id TEXT NOT NULL DEFAULT '', cwd TEXT NOT NULL DEFAULT '',
		 round INTEGER NOT NULL DEFAULT 1,
		 claimed_at INTEGER NOT NULL DEFAULT 0, spawned_at INTEGER NOT NULL DEFAULT 0,
		 deadline_at INTEGER NOT NULL DEFAULT 0, deadline_source TEXT NOT NULL DEFAULT '',
		 terminal INTEGER NOT NULL DEFAULT 0, terminal_kind TEXT NOT NULL DEFAULT '',
		 last_event_at INTEGER NOT NULL DEFAULT 0, report_path TEXT NOT NULL DEFAULT '',
		 PRIMARY KEY(job_id, attempt))`,
		`CREATE TABLE IF NOT EXISTS stall_deadline_ext (
		 job_id TEXT NOT NULL, seq INTEGER NOT NULL, issuer_lane TEXT NOT NULL,
		 reason TEXT NOT NULL DEFAULT '', new_deadline_at INTEGER NOT NULL,
		 recorded_at INTEGER NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
		 PRIMARY KEY(job_id, seq))`,
		`CREATE TABLE IF NOT EXISTS stall_incidents (
		 job_id TEXT NOT NULL, attempt INTEGER NOT NULL, round INTEGER NOT NULL,
		 cause TEXT NOT NULL, occurrence INTEGER NOT NULL,
		 pane_id TEXT NOT NULL DEFAULT '', observable INTEGER NOT NULL DEFAULT 1,
		 first_seen_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
		 recovered_at INTEGER NOT NULL DEFAULT 0,
		 evidence_json TEXT NOT NULL DEFAULT '{}',
		 notify_state TEXT NOT NULL DEFAULT '',
		 PRIMARY KEY(job_id, attempt, round, cause, occurrence))`,
		`CREATE INDEX IF NOT EXISTS stall_incidents_job ON stall_incidents(job_id, attempt, round)`,
		`CREATE INDEX IF NOT EXISTS stall_incidents_pane ON stall_incidents(pane_id)`,
		`CREATE TABLE IF NOT EXISTS stall_notifications (
		 incident_key TEXT NOT NULL, lane TEXT NOT NULL, level INTEGER NOT NULL,
		 event_id TEXT NOT NULL DEFAULT '', notified_at INTEGER NOT NULL DEFAULT 0,
		 acked_at INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
		 next_retry_at INTEGER NOT NULL DEFAULT 0, grace_deadline_at INTEGER NOT NULL DEFAULT 0,
		 suppressed_reason TEXT NOT NULL DEFAULT '',
		 PRIMARY KEY(incident_key, lane, level))`,
		`CREATE TABLE IF NOT EXISTS stall_panes (
		 pane_id TEXT PRIMARY KEY, job_id TEXT NOT NULL DEFAULT '', attempt INTEGER NOT NULL DEFAULT 0,
		 workspace_id TEXT NOT NULL DEFAULT '', cwd TEXT NOT NULL DEFAULT '', harness TEXT NOT NULL DEFAULT '',
		 last_revision INTEGER NOT NULL DEFAULT 0, last_read_at INTEGER NOT NULL DEFAULT 0,
		 last_read_ok INTEGER NOT NULL DEFAULT 0, baseline_done INTEGER NOT NULL DEFAULT 0,
		 fingerprints_json TEXT NOT NULL DEFAULT '{}',
		 first_seen_at INTEGER NOT NULL DEFAULT 0, spawn_recent INTEGER NOT NULL DEFAULT 0,
		 subscribed INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS stall_reports (
		 job_id TEXT NOT NULL, attempt INTEGER NOT NULL, report_path TEXT NOT NULL DEFAULT '',
		 local_path TEXT NOT NULL DEFAULT '', sha256 TEXT NOT NULL DEFAULT '',
		 remote_key TEXT NOT NULL DEFAULT '', remote_state TEXT NOT NULL DEFAULT 'pending',
		 remote_at INTEGER NOT NULL DEFAULT 0, remote_error TEXT NOT NULL DEFAULT '',
		 finalized_at INTEGER NOT NULL DEFAULT 0,
		 PRIMARY KEY(job_id, attempt))`,
		`CREATE TABLE IF NOT EXISTS stall_unowned (
		 proc_key TEXT PRIMARY KEY, pid INTEGER NOT NULL, ppid INTEGER NOT NULL,
		 cwd TEXT NOT NULL, started_at INTEGER NOT NULL,
		 first_seen_at INTEGER NOT NULL, confirmations INTEGER NOT NULL DEFAULT 0,
		 recorded INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS stall_beat (
		 id INTEGER PRIMARY KEY CHECK(id=1),
		 beat_ms INTEGER NOT NULL DEFAULT 0, interval_ms INTEGER NOT NULL DEFAULT 0,
		 panes INTEGER NOT NULL DEFAULT 0)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

// upsertStallJob inserts or refreshes the (job, attempt) row. The deadline is
// recorded at assignment time: once a row has one, only an issuer-sourced
// value may replace it. A rescanned reps proposal must not drift a deadline
// that was already recorded.
func (s *Store) upsertStallJob(ctx context.Context, job stallJobRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var deadlineAt int64
	var deadlineSource string
	err = tx.QueryRowContext(ctx, `SELECT deadline_at,deadline_source FROM stall_jobs WHERE job_id=? AND attempt=?`, job.JobID, job.Attempt).Scan(&deadlineAt, &deadlineSource)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows || job.DeadlineSource == stallDeadlineIssuer {
		deadlineAt = stallMS(job.DeadlineAt)
		deadlineSource = job.DeadlineSource
	}
	spawned := stallMS(job.SpawnedAt)
	_, err = tx.ExecContext(ctx, `INSERT INTO stall_jobs(job_id,attempt,owner_lane,parent_lane,agent_label,harness,profile,pane_id,workspace_id,cwd,round,claimed_at,spawned_at,deadline_at,deadline_source,terminal,terminal_kind,last_event_at,report_path)
	 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	 ON CONFLICT(job_id,attempt) DO UPDATE SET
	  owner_lane=CASE WHEN excluded.owner_lane<>'' THEN excluded.owner_lane ELSE owner_lane END,
	  parent_lane=CASE WHEN excluded.parent_lane<>'' THEN excluded.parent_lane ELSE parent_lane END,
	  agent_label=CASE WHEN excluded.agent_label<>'' THEN excluded.agent_label ELSE agent_label END,
	  harness=CASE WHEN excluded.harness<>'' THEN excluded.harness ELSE harness END,
	  profile=CASE WHEN excluded.profile<>'' THEN excluded.profile ELSE profile END,
	  pane_id=CASE WHEN excluded.pane_id<>'' THEN excluded.pane_id ELSE pane_id END,
	  workspace_id=CASE WHEN excluded.workspace_id<>'' THEN excluded.workspace_id ELSE workspace_id END,
	  cwd=CASE WHEN excluded.cwd<>'' THEN excluded.cwd ELSE cwd END,
	  round=excluded.round,
	  claimed_at=CASE WHEN excluded.claimed_at<>0 THEN excluded.claimed_at ELSE claimed_at END,
	  spawned_at=CASE WHEN excluded.spawned_at<>0 THEN excluded.spawned_at ELSE spawned_at END,
	  deadline_at=excluded.deadline_at, deadline_source=excluded.deadline_source,
	  terminal=MAX(terminal,excluded.terminal), terminal_kind=CASE WHEN excluded.terminal_kind<>'' THEN excluded.terminal_kind ELSE terminal_kind END,
	  last_event_at=MAX(last_event_at,excluded.last_event_at),
	  report_path=CASE WHEN excluded.report_path<>'' THEN excluded.report_path ELSE report_path END`,
		job.JobID, job.Attempt, job.OwnerLane, job.ParentLane, job.AgentLabel, job.Harness, job.Profile,
		job.PaneID, job.WorkspaceID, job.CWD, job.Round, stallMS(job.ClaimedAt), spawned,
		deadlineAt, deadlineSource, boolInt(job.Terminal), job.TerminalKind, stallMS(job.LastEventAt), job.ReportPath)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) stallJobs(ctx context.Context, includeTerminal bool) ([]stallJobRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := `SELECT job_id,attempt,owner_lane,parent_lane,agent_label,harness,profile,pane_id,workspace_id,cwd,round,claimed_at,spawned_at,deadline_at,deadline_source,terminal,terminal_kind,last_event_at,report_path FROM stall_jobs`
	if !includeTerminal {
		query += ` WHERE terminal=0`
	}
	query += ` ORDER BY job_id,attempt`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []stallJobRow
	for rows.Next() {
		var job stallJobRow
		var claimedAt, spawnedAt, deadlineAt, lastEventAt int64
		var terminal int
		if err := rows.Scan(&job.JobID, &job.Attempt, &job.OwnerLane, &job.ParentLane, &job.AgentLabel, &job.Harness, &job.Profile, &job.PaneID, &job.WorkspaceID, &job.CWD, &job.Round, &claimedAt, &spawnedAt, &deadlineAt, &job.DeadlineSource, &terminal, &job.TerminalKind, &lastEventAt, &job.ReportPath); err != nil {
			return nil, err
		}
		job.ClaimedAt = stallTime(claimedAt)
		job.SpawnedAt = stallTime(spawnedAt)
		job.DeadlineAt = stallTime(deadlineAt)
		job.Terminal = terminal != 0
		job.LastEventAt = stallTime(lastEventAt)
		out = append(out, job)
	}
	return out, rows.Err()
}

// applyStallDeadline moves the deadline forward on issuer authority only.
// Past overdue incidents are rows of their own and are never touched here.
func (s *Store) applyStallDeadline(ctx context.Context, jobID string, attempt int64, deadline time.Time, source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE stall_jobs SET deadline_at=?,deadline_source=? WHERE job_id=? AND attempt=?`, stallMS(deadline), source, jobID, attempt)
	return err
}

func (s *Store) insertStallDeadlineExt(ctx context.Context, jobID string, seq int64, issuer, reason string, newDeadline, at time.Time) (applied bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO stall_deadline_ext(job_id,seq,issuer_lane,reason,new_deadline_at,recorded_at,applied) VALUES(?,?,?,?,?,?,0)`, jobID, seq, issuer, reason, stallMS(newDeadline), stallMS(at))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, tx.Commit()
	}
	return true, tx.Commit()
}

func (s *Store) stallDeadlineExts(ctx context.Context, jobID string) ([]stallDeadlineExt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT seq,issuer_lane,reason,new_deadline_at,applied FROM stall_deadline_ext WHERE job_id=? ORDER BY seq`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []stallDeadlineExt
	for rows.Next() {
		var ext stallDeadlineExt
		var applied int
		var deadline int64
		if err := rows.Scan(&ext.Seq, &ext.IssuerLane, &ext.Reason, &deadline, &applied); err != nil {
			return nil, err
		}
		ext.NewDeadlineAt = stallTime(deadline)
		ext.Applied = applied != 0
		out = append(out, ext)
	}
	return out, rows.Err()
}

func (s *Store) markStallDeadlineExtApplied(ctx context.Context, jobID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE stall_deadline_ext SET applied=1 WHERE job_id=? AND seq=?`, jobID, seq)
	return err
}

// recordStallIncident inserts one incident keyed by (job, attempt, round,
// cause, occurrence). It never updates an existing row — a second sighting of
// the same cause is a new occurrence — and returns false when the row already
// exists so restarts stay idempotent.
func (s *Store) recordStallIncident(ctx context.Context, row stallIncidentRow) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evidence := row.Evidence
	if len(evidence) == 0 {
		evidence = json.RawMessage(`{}`)
	}
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO stall_incidents(job_id,attempt,round,cause,occurrence,pane_id,observable,first_seen_at,last_seen_at,recovered_at,evidence_json,notify_state) VALUES(?,?,?,?,?,?,?,?,?,0,?,?)`,
		row.JobID, row.Attempt, row.Round, row.Cause, row.Occurrence, row.PaneID, boolInt(row.Observable), stallMS(row.FirstSeenAt), stallMS(row.LastSeenAt), string(evidence), row.NotifyState)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// touchStallIncident refreshes last_seen/evidence on an open incident. A
// recovered incident is never resurrected: recovery is itself an observation.
func (s *Store) touchStallIncident(ctx context.Context, row stallIncidentRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE stall_incidents SET last_seen_at=?,evidence_json=? WHERE job_id=? AND attempt=? AND round=? AND cause=? AND occurrence=? AND recovered_at=0`,
		stallMS(row.LastSeenAt), string(row.Evidence), row.JobID, row.Attempt, row.Round, row.Cause, row.Occurrence)
	return err
}

func (s *Store) recoverStallIncidents(ctx context.Context, jobID string, attempt, round int64, causes []string, at time.Time) error {
	if len(causes) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cause := range causes {
		if _, err := s.db.ExecContext(ctx, `UPDATE stall_incidents SET recovered_at=? WHERE job_id=? AND attempt=? AND round=? AND cause=? AND recovered_at=0`, stallMS(at), jobID, attempt, round, cause); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) stallIncidents(ctx context.Context, jobID string, attempt, round int64, openOnly bool) ([]stallIncidentRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := `SELECT job_id,attempt,round,cause,occurrence,pane_id,observable,first_seen_at,last_seen_at,recovered_at,evidence_json,notify_state FROM stall_incidents WHERE job_id=? AND attempt=? AND round=?`
	if openOnly {
		query += ` AND recovered_at=0`
	}
	query += ` ORDER BY cause,occurrence`
	rows, err := s.db.QueryContext(ctx, query, jobID, attempt, round)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStallIncidents(rows)
}

func (s *Store) stallIncidentsAll(ctx context.Context, openOnly bool) ([]stallIncidentRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := `SELECT job_id,attempt,round,cause,occurrence,pane_id,observable,first_seen_at,last_seen_at,recovered_at,evidence_json,notify_state FROM stall_incidents`
	if openOnly {
		query += ` WHERE recovered_at=0`
	}
	query += ` ORDER BY job_id,attempt,round,cause,occurrence`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStallIncidents(rows)
}

func scanStallIncidents(rows *sql.Rows) ([]stallIncidentRow, error) {
	var out []stallIncidentRow
	for rows.Next() {
		var row stallIncidentRow
		var firstSeen, lastSeen, recovered int64
		var observable int
		var evidence string
		if err := rows.Scan(&row.JobID, &row.Attempt, &row.Round, &row.Cause, &row.Occurrence, &row.PaneID, &observable, &firstSeen, &lastSeen, &recovered, &evidence, &row.NotifyState); err != nil {
			return nil, err
		}
		row.FirstSeenAt = stallTime(firstSeen)
		row.LastSeenAt = stallTime(lastSeen)
		row.RecoveredAt = stallTime(recovered)
		row.Observable = observable != 0
		row.Evidence = json.RawMessage(evidence)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) maxStallOccurrence(ctx context.Context, jobID string, attempt, round int64, cause string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(occurrence) FROM stall_incidents WHERE job_id=? AND attempt=? AND round=? AND cause=?`, jobID, attempt, round, cause).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

func (s *Store) upsertStallNotification(ctx context.Context, row stallNotificationRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO stall_notifications(incident_key,lane,level,event_id,notified_at,acked_at,attempts,next_retry_at,grace_deadline_at,suppressed_reason) VALUES(?,?,?,?,?,?,?,?,?,?)
	 ON CONFLICT(incident_key,lane,level) DO UPDATE SET
	  event_id=CASE WHEN excluded.event_id<>'' THEN excluded.event_id ELSE event_id END,
	  notified_at=CASE WHEN excluded.notified_at<>0 THEN excluded.notified_at ELSE notified_at END,
	  acked_at=CASE WHEN excluded.acked_at<>0 THEN excluded.acked_at ELSE acked_at END,
	  attempts=MAX(attempts,excluded.attempts),
	  next_retry_at=excluded.next_retry_at,
	  grace_deadline_at=CASE WHEN excluded.grace_deadline_at<>0 THEN excluded.grace_deadline_at ELSE grace_deadline_at END,
	  suppressed_reason=CASE WHEN excluded.suppressed_reason<>'' THEN excluded.suppressed_reason ELSE suppressed_reason END`,
		row.IncidentKey, row.Lane, row.Level, row.EventID, stallMS(row.NotifiedAt), stallMS(row.AckedAt), row.Attempts, stallMS(row.NextRetryAt), stallMS(row.GraceDeadlineAt), row.SuppressedReason)
	return err
}

func (s *Store) stallNotifications(ctx context.Context, incidentKey string) ([]stallNotificationRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT incident_key,lane,level,event_id,notified_at,acked_at,attempts,next_retry_at,grace_deadline_at,suppressed_reason FROM stall_notifications WHERE incident_key=? ORDER BY level`, incidentKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStallNotifications(rows)
}

func (s *Store) stallNotificationsDue(ctx context.Context, now time.Time) ([]stallNotificationRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT incident_key,lane,level,event_id,notified_at,acked_at,attempts,next_retry_at,grace_deadline_at,suppressed_reason FROM stall_notifications WHERE suppressed_reason='' AND acked_at=0 AND (next_retry_at=0 OR next_retry_at<=?) ORDER BY incident_key,level`, stallMS(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStallNotifications(rows)
}

func scanStallNotifications(rows *sql.Rows) ([]stallNotificationRow, error) {
	var out []stallNotificationRow
	for rows.Next() {
		var row stallNotificationRow
		var notified, acked, nextRetry, grace int64
		if err := rows.Scan(&row.IncidentKey, &row.Lane, &row.Level, &row.EventID, &notified, &acked, &row.Attempts, &nextRetry, &grace, &row.SuppressedReason); err != nil {
			return nil, err
		}
		row.NotifiedAt = stallTime(notified)
		row.AckedAt = stallTime(acked)
		row.NextRetryAt = stallTime(nextRetry)
		row.GraceDeadlineAt = stallTime(grace)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) stallPane(ctx context.Context, paneID string) (stallPaneRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var row stallPaneRow
	var lastReadAt, firstSeenAt int64
	var lastReadOK, baselineDone, spawnRecent, subscribed int
	var fingerprints string
	err := s.db.QueryRowContext(ctx, `SELECT pane_id,job_id,attempt,workspace_id,cwd,harness,last_revision,last_read_at,last_read_ok,baseline_done,fingerprints_json,first_seen_at,spawn_recent,subscribed FROM stall_panes WHERE pane_id=?`, paneID).
		Scan(&row.PaneID, &row.JobID, &row.Attempt, &row.WorkspaceID, &row.CWD, &row.Harness, &row.LastRevision, &lastReadAt, &lastReadOK, &baselineDone, &fingerprints, &firstSeenAt, &spawnRecent, &subscribed)
	if err == sql.ErrNoRows {
		return stallPaneRow{}, false, nil
	}
	if err != nil {
		return stallPaneRow{}, false, err
	}
	row.LastReadAt = stallTime(lastReadAt)
	row.LastReadOK = lastReadOK != 0
	row.BaselineDone = baselineDone != 0
	row.FirstSeenAt = stallTime(firstSeenAt)
	row.SpawnedRecent = spawnRecent != 0
	row.Subscribed = subscribed != 0
	row.Fingerprints = map[string]int64{}
	if fingerprints != "" {
		if err := json.Unmarshal([]byte(fingerprints), &row.Fingerprints); err != nil {
			return stallPaneRow{}, false, fmt.Errorf("stall pane fingerprint journal is invalid")
		}
	}
	return row, true, nil
}

func (s *Store) upsertStallPane(ctx context.Context, row stallPaneRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprints, err := json.Marshal(row.Fingerprints)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO stall_panes(pane_id,job_id,attempt,workspace_id,cwd,harness,last_revision,last_read_at,last_read_ok,baseline_done,fingerprints_json,first_seen_at,spawn_recent,subscribed) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
	 ON CONFLICT(pane_id) DO UPDATE SET
	  job_id=excluded.job_id, attempt=excluded.attempt,
	  workspace_id=CASE WHEN excluded.workspace_id<>'' THEN excluded.workspace_id ELSE workspace_id END,
	  cwd=CASE WHEN excluded.cwd<>'' THEN excluded.cwd ELSE cwd END,
	  harness=CASE WHEN excluded.harness<>'' THEN excluded.harness ELSE harness END,
	  last_revision=excluded.last_revision, last_read_at=excluded.last_read_at, last_read_ok=excluded.last_read_ok,
	  baseline_done=excluded.baseline_done, fingerprints_json=excluded.fingerprints_json,
	  spawn_recent=excluded.spawn_recent, subscribed=excluded.subscribed`,
		row.PaneID, row.JobID, row.Attempt, row.WorkspaceID, row.CWD, row.Harness, row.LastRevision, stallMS(row.LastReadAt), boolInt(row.LastReadOK), boolInt(row.BaselineDone), string(fingerprints), stallMS(row.FirstSeenAt), boolInt(row.SpawnedRecent), boolInt(row.Subscribed))
	return err
}

func (s *Store) upsertStallReport(ctx context.Context, row stallReportRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO stall_reports(job_id,attempt,report_path,local_path,sha256,remote_key,remote_state,remote_at,remote_error,finalized_at) VALUES(?,?,?,?,?,?,?,?,?,?)
	 ON CONFLICT(job_id,attempt) DO UPDATE SET
	  report_path=CASE WHEN excluded.report_path<>'' THEN excluded.report_path ELSE report_path END,
	  local_path=CASE WHEN excluded.local_path<>'' THEN excluded.local_path ELSE local_path END,
	  sha256=CASE WHEN excluded.sha256<>'' THEN excluded.sha256 ELSE sha256 END,
	  remote_key=CASE WHEN excluded.remote_key<>'' THEN excluded.remote_key ELSE remote_key END,
	  remote_state=CASE WHEN excluded.remote_state<>'' THEN excluded.remote_state ELSE remote_state END,
	  remote_at=CASE WHEN excluded.remote_at<>0 THEN excluded.remote_at ELSE remote_at END,
	  remote_error=excluded.remote_error,
	  finalized_at=CASE WHEN excluded.finalized_at<>0 THEN excluded.finalized_at ELSE finalized_at END`,
		row.JobID, row.Attempt, row.ReportPath, row.LocalPath, row.SHA256, row.RemoteKey, row.RemoteState, stallMS(row.RemoteAt), row.RemoteError, stallMS(row.FinalizedAt))
	return err
}

func (s *Store) stallReportsPending(ctx context.Context) ([]stallReportRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT job_id,attempt,report_path,local_path,sha256,remote_key,remote_state,remote_at,remote_error,finalized_at FROM stall_reports WHERE remote_state IN ('pending','failed') ORDER BY job_id,attempt`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStallReports(rows)
}

func (s *Store) stallReport(ctx context.Context, jobID string, attempt int64) (stallReportRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT job_id,attempt,report_path,local_path,sha256,remote_key,remote_state,remote_at,remote_error,finalized_at FROM stall_reports WHERE job_id=? AND attempt=?`, jobID, attempt)
	if err != nil {
		return stallReportRow{}, false, err
	}
	defer rows.Close()
	out, err := scanStallReports(rows)
	if err != nil || len(out) == 0 {
		return stallReportRow{}, false, err
	}
	return out[0], true, nil
}

func scanStallReports(rows *sql.Rows) ([]stallReportRow, error) {
	var out []stallReportRow
	for rows.Next() {
		var row stallReportRow
		var remoteAt, finalizedAt int64
		if err := rows.Scan(&row.JobID, &row.Attempt, &row.ReportPath, &row.LocalPath, &row.SHA256, &row.RemoteKey, &row.RemoteState, &remoteAt, &row.RemoteError, &finalizedAt); err != nil {
			return nil, err
		}
		row.RemoteAt = stallTime(remoteAt)
		row.FinalizedAt = stallTime(finalizedAt)
		out = append(out, row)
	}
	return out, rows.Err()
}

// stallPaneCWDs lists every directory a tracked pane has been seen in. The
// unowned scan uses it to bound "known worktree" beyond what is live now.
func (s *Store) stallPaneCWDs(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT cwd FROM stall_panes WHERE cwd<>''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cwd string
		if err := rows.Scan(&cwd); err != nil {
			return nil, err
		}
		out = append(out, cwd)
	}
	return out, rows.Err()
}

func (s *Store) upsertStallUnowned(ctx context.Context, row stallUnownedRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO stall_unowned(proc_key,pid,ppid,cwd,started_at,first_seen_at,confirmations,recorded) VALUES(?,?,?,?,?,?,?,?)
	 ON CONFLICT(proc_key) DO UPDATE SET confirmations=excluded.confirmations, recorded=excluded.recorded`,
		row.ProcKey, row.PID, row.PPID, row.CWD, stallMS(row.StartedAt), stallMS(row.FirstSeenAt), row.Confirmations, boolInt(row.Recorded))
	return err
}

func (s *Store) stallUnownedCandidates(ctx context.Context) ([]stallUnownedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT proc_key,pid,ppid,cwd,started_at,first_seen_at,confirmations,recorded FROM stall_unowned`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []stallUnownedRow
	for rows.Next() {
		var row stallUnownedRow
		var startedAt, firstSeenAt int64
		var recorded int
		if err := rows.Scan(&row.ProcKey, &row.PID, &row.PPID, &row.CWD, &startedAt, &firstSeenAt, &row.Confirmations, &recorded); err != nil {
			return nil, err
		}
		row.StartedAt = stallTime(startedAt)
		row.FirstSeenAt = stallTime(firstSeenAt)
		row.Recorded = recorded != 0
		out = append(out, row)
	}
	return out, rows.Err()
}

// recordStallBeat is the node-side durability half of the hub no-data
// contract: the last completed scan survives a daemon restart.
func (s *Store) recordStallBeat(ctx context.Context, beat time.Time, interval time.Duration, panes int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO stall_beat(id,beat_ms,interval_ms,panes) VALUES(1,?,?,?)
	 ON CONFLICT(id) DO UPDATE SET beat_ms=excluded.beat_ms,interval_ms=excluded.interval_ms,panes=excluded.panes`, stallMS(beat), interval.Milliseconds(), panes)
	return err
}

func (s *Store) stallBeat(ctx context.Context) (beat time.Time, interval time.Duration, panes int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var beatMS, intervalMS int64
	err = s.db.QueryRowContext(ctx, `SELECT beat_ms,interval_ms,panes FROM stall_beat WHERE id=1`).Scan(&beatMS, &intervalMS, &panes)
	if err == sql.ErrNoRows {
		return time.Time{}, 0, 0, nil
	}
	if err != nil {
		return time.Time{}, 0, 0, err
	}
	return stallTime(beatMS), time.Duration(intervalMS) * time.Millisecond, panes, nil
}

// relayLanePersisted answers whether a lane.event we emitted is known to have
// reached the hub's durable store. emit rc=0 is not receipt; only the
// relay_sent persisted stamp is.
func (s *Store) relayLanePersisted(ctx context.Context, lane, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var persisted sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT persisted_at FROM relay_sent WHERE kind='lane.event' AND lane=? AND event_id=?`, lane, eventID).Scan(&persisted)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return persisted.Valid && persisted.Int64 > 0, nil
}
