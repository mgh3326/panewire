package panewire

import (
	"context"
	"strings"
	"time"
)

// Durable consult rows (hk 4552 phase 1, docs/consult-on-request.md). A
// consult survives a hub restart: the request, its seat, the spawn operation
// and the answer document are one row keyed by the idempotency key
// (task, repo#pr, head).

const (
	consultQueued    = "queued"
	consultRunning   = "running"
	consultAnswered  = "answered"
	consultRefused   = "refused"
	consultFailed    = "failed"
	consultCancelled = "cancelled"
)

type hubConsult struct {
	ID, IdemKey, Task, PR, Head, Requester, QuestionDoc, AuthorJob, AuthorFamily string
	Machine, CallerPane                                                          string
	State, SeatProfile, SeatFamily, SeatGrade, Job, RequestID, Reason, AnswerDoc string
	Created, Updated, Answered, Deadline                                         time.Time
	ClaimUntil                                                                   time.Time
	ExcludeProfiles                                                              []string
	Respawns                                                                     int
	Notified                                                                     bool
	// Owner is the lane the consult belongs to (operator decision 421): the
	// author job's owner lane, else the requesting lane, else operator-desk.
	// It is fixed when the row is planned, so a respawn and a recovery keep
	// the same owner. Empty on a row from before the column: ownerLane reads
	// the requester rule for it.
	Owner string
}

// ownerLane is the stored owner, or for a row from before the column the
// requester rule, which is what every row meant before decision 421.
func (c hubConsult) ownerLane() string {
	if c.Owner != "" {
		return c.Owner
	}
	return consultOwner(c.Requester)
}

func consultActive(state string) bool {
	return state == consultQueued || state == consultRunning
}

func (s *laneInboxStore) ensureConsults() error {
	if s.dialect == laneInboxPostgres {
		return nil
	}
	ctx, done := s.op(s.life)
	defer done()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS consult_requests (
			id TEXT PRIMARY KEY,
			idem_key TEXT NOT NULL UNIQUE,
			task TEXT NOT NULL,
			pr TEXT NOT NULL DEFAULT '',
			head TEXT NOT NULL DEFAULT '',
			requester TEXT NOT NULL,
			question_doc TEXT NOT NULL,
			author_job TEXT NOT NULL DEFAULT '',
			author_family TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL,
			seat_profile TEXT NOT NULL DEFAULT '',
			seat_family TEXT NOT NULL DEFAULT '',
			seat_grade TEXT NOT NULL DEFAULT '',
			job TEXT NOT NULL DEFAULT '',
			request_id TEXT NOT NULL DEFAULT '',
			reason TEXT NOT NULL DEFAULT '',
			answer_doc TEXT NOT NULL DEFAULT '',
			created_ms BIGINT NOT NULL,
			updated_ms BIGINT NOT NULL DEFAULT 0,
			claim_until_ms BIGINT NOT NULL DEFAULT 0,
			exclude_profiles TEXT NOT NULL DEFAULT '',
			answered_ms BIGINT NOT NULL DEFAULT 0,
			deadline_ms BIGINT NOT NULL,
			respawns INTEGER NOT NULL DEFAULT 0,
			notified INTEGER NOT NULL DEFAULT 0)`,
		`CREATE INDEX IF NOT EXISTS consult_requests_active ON consult_requests(state) WHERE state IN ('queued','running')`,
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	for _, column := range []string{
		`updated_ms BIGINT NOT NULL DEFAULT 0`,
		`claim_until_ms BIGINT NOT NULL DEFAULT 0`,
		`exclude_profiles TEXT NOT NULL DEFAULT ''`,
		`machine TEXT NOT NULL DEFAULT ''`,
		`caller_pane TEXT NOT NULL DEFAULT ''`,
		`owner TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE consult_requests ADD COLUMN `+column); err != nil {
			if err := ignoreDuplicateColumn(err); err != nil {
				return err
			}
		}
	}
	return nil
}

// insertConsult stores a new consult, or returns the row the same idempotency
// key already holds. fresh is false for the replay.
func (s *laneInboxStore) insertConsult(parent context.Context, c hubConsult) (hubConsult, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO consult_requests
		(id,idem_key,task,pr,head,requester,machine,caller_pane,question_doc,author_job,author_family,state,seat_profile,seat_family,seat_grade,job,request_id,reason,answer_doc,created_ms,updated_ms,claim_until_ms,exclude_profiles,answered_ms,deadline_ms,respawns,notified,owner)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(idem_key) DO NOTHING`),
		c.ID, c.IdemKey, c.Task, c.PR, c.Head, c.Requester, c.Machine, c.CallerPane, c.QuestionDoc, c.AuthorJob, c.AuthorFamily, c.State,
		c.SeatProfile, c.SeatFamily, c.SeatGrade, c.Job, c.RequestID, c.Reason, c.AnswerDoc,
		c.Created.UnixMilli(), consultUpdatedMillis(c), consultMs(c.ClaimUntil), strings.Join(c.ExcludeProfiles, ","), c.Answered.UnixMilli(), c.Deadline.UnixMilli(), c.Respawns, consultBoolInt(c.Notified), c.Owner)
	if err != nil {
		return hubConsult{}, false, err
	}
	n, _ := result.RowsAffected()
	if n == 1 {
		// A new consult is the consult loop's work (hk 5302).
		s.noteWork(readConsult)
		return c, true, nil
	}
	existing, err := s.consultByKey(parent, c.IdemKey)
	return existing, false, err
}

func (s *laneInboxStore) consultByKey(parent context.Context, key string) (hubConsult, error) {
	return s.scanConsult(parent, `SELECT `+consultColumns+` FROM consult_requests WHERE idem_key=?`, key)
}

func (s *laneInboxStore) consultByID(parent context.Context, id string) (hubConsult, error) {
	return s.scanConsult(parent, `SELECT `+consultColumns+` FROM consult_requests WHERE id=?`, id)
}

const consultColumns = `id,idem_key,task,pr,head,requester,machine,caller_pane,question_doc,author_job,author_family,state,seat_profile,seat_family,seat_grade,job,request_id,reason,answer_doc,created_ms,updated_ms,claim_until_ms,exclude_profiles,answered_ms,deadline_ms,respawns,notified,owner`

func (s *laneInboxStore) scanConsult(parent context.Context, query string, args ...any) (hubConsult, error) {
	ctx, done := s.op(parent)
	defer done()
	var c hubConsult
	var created, updated, claimUntil, answered, deadline int64
	var notified int
	var exclude string
	err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&c.ID, &c.IdemKey, &c.Task, &c.PR, &c.Head, &c.Requester, &c.Machine, &c.CallerPane, &c.QuestionDoc, &c.AuthorJob, &c.AuthorFamily, &c.State, &c.SeatProfile, &c.SeatFamily, &c.SeatGrade, &c.Job, &c.RequestID, &c.Reason, &c.AnswerDoc, &created, &updated, &claimUntil, &exclude, &answered, &deadline, &c.Respawns, &notified, &c.Owner)
	if err != nil {
		return hubConsult{}, err
	}
	c.Created = time.UnixMilli(created).UTC()
	if updated != 0 {
		c.Updated = time.UnixMilli(updated).UTC()
	}
	if claimUntil != 0 {
		c.ClaimUntil = time.UnixMilli(claimUntil).UTC()
	}
	c.ExcludeProfiles = consultProfiles(exclude)
	if answered != 0 {
		c.Answered = time.UnixMilli(answered).UTC()
	}
	c.Deadline = time.UnixMilli(deadline).UTC()
	c.Notified = notified != 0
	return c, nil
}

// activeConsults lists every consult a restart must keep working on.
func (s *laneInboxStore) activeConsults(parent context.Context) ([]hubConsult, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+consultColumns+` FROM consult_requests WHERE state IN ('queued','running') OR (state='answered' AND notified=0) ORDER BY created_ms`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hubConsult
	for rows.Next() {
		var c hubConsult
		var created, updated, claimUntil, answered, deadline int64
		var notified int
		var exclude string
		if err := rows.Scan(&c.ID, &c.IdemKey, &c.Task, &c.PR, &c.Head, &c.Requester, &c.Machine, &c.CallerPane, &c.QuestionDoc, &c.AuthorJob, &c.AuthorFamily, &c.State, &c.SeatProfile, &c.SeatFamily, &c.SeatGrade, &c.Job, &c.RequestID, &c.Reason, &c.AnswerDoc, &created, &updated, &claimUntil, &exclude, &answered, &deadline, &c.Respawns, &notified, &c.Owner); err != nil {
			return nil, err
		}
		c.Created = time.UnixMilli(created).UTC()
		if updated != 0 {
			c.Updated = time.UnixMilli(updated).UTC()
		}
		if claimUntil != 0 {
			c.ClaimUntil = time.UnixMilli(claimUntil).UTC()
		}
		c.ExcludeProfiles = consultProfiles(exclude)
		if answered != 0 {
			c.Answered = time.UnixMilli(answered).UTC()
		}
		c.Deadline = time.UnixMilli(deadline).UTC()
		c.Notified = notified != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

// saveConsult writes the mutable fields of one row. The coordinator owns one
// consult at a time; a retry replaces a terminal row under a compare-and-set.
func (s *laneInboxStore) saveConsult(parent context.Context, c hubConsult) error {
	_, err := s.updateConsult(parent, c, "")
	return err
}

// replaceConsult is the retry transition: it only replaces a refused or
// failed row, so two concurrent POSTs for one key cannot both claim a seat.
func (s *laneInboxStore) replaceConsult(parent context.Context, c hubConsult) (bool, error) {
	n, err := s.updateConsult(parent, c, " AND state IN ('refused','failed')")
	return n == 1, err
}

func (s *laneInboxStore) updateConsult(parent context.Context, c hubConsult, guard string) (int64, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE consult_requests SET requester=?,machine=?,caller_pane=?,question_doc=?,author_job=?,author_family=?,state=?,seat_profile=?,seat_family=?,seat_grade=?,job=?,request_id=?,reason=?,answer_doc=?,answered_ms=?,deadline_ms=?,updated_ms=?,claim_until_ms=?,exclude_profiles=?,respawns=?,notified=?,owner=? WHERE id=?`+guard),
		c.Requester, c.Machine, c.CallerPane, c.QuestionDoc, c.AuthorJob, c.AuthorFamily, c.State, c.SeatProfile, c.SeatFamily, c.SeatGrade, c.Job, c.RequestID, c.Reason, c.AnswerDoc, c.Answered.UnixMilli(), c.Deadline.UnixMilli(), consultUpdatedMillis(c), consultMs(c.ClaimUntil), strings.Join(c.ExcludeProfiles, ","), c.Respawns, consultBoolInt(c.Notified), c.Owner, c.ID)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if n > 0 {
		// A state change (queued, running, answered, a respawn intent, a
		// notified mark) is what the consult loop acts on.
		s.noteWork(readConsult)
	}
	return n, err
}

// consultUpdatedMillis is the row's last write; a row written before the
// column existed falls back to its created time.
func consultMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func consultProfiles(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

func consultUpdatedMillis(c hubConsult) int64 {
	if !c.Updated.IsZero() {
		return c.Updated.UnixMilli()
	}
	return c.Created.UnixMilli()
}

func consultBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
