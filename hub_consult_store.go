package panewire

import (
	"context"
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
	State, SeatProfile, SeatFamily, SeatGrade, Job, RequestID, Reason, AnswerDoc string
	Created, Answered, Deadline                                                  time.Time
	Respawns                                                                     int
	Notified                                                                     bool
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
	return nil
}

// insertConsult stores a new consult, or returns the row the same idempotency
// key already holds. fresh is false for the replay.
func (s *laneInboxStore) insertConsult(parent context.Context, c hubConsult) (hubConsult, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO consult_requests
		(id,idem_key,task,pr,head,requester,question_doc,author_job,author_family,state,seat_profile,seat_family,seat_grade,job,request_id,reason,answer_doc,created_ms,answered_ms,deadline_ms,respawns,notified)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(idem_key) DO NOTHING`),
		c.ID, c.IdemKey, c.Task, c.PR, c.Head, c.Requester, c.QuestionDoc, c.AuthorJob, c.AuthorFamily, c.State,
		c.SeatProfile, c.SeatFamily, c.SeatGrade, c.Job, c.RequestID, c.Reason, c.AnswerDoc,
		c.Created.UnixMilli(), c.Answered.UnixMilli(), c.Deadline.UnixMilli(), c.Respawns, consultBoolInt(c.Notified))
	if err != nil {
		return hubConsult{}, false, err
	}
	n, _ := result.RowsAffected()
	if n == 1 {
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

const consultColumns = `id,idem_key,task,pr,head,requester,question_doc,author_job,author_family,state,seat_profile,seat_family,seat_grade,job,request_id,reason,answer_doc,created_ms,answered_ms,deadline_ms,respawns,notified`

func (s *laneInboxStore) scanConsult(parent context.Context, query string, args ...any) (hubConsult, error) {
	ctx, done := s.op(parent)
	defer done()
	var c hubConsult
	var created, answered, deadline int64
	var notified int
	err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&c.ID, &c.IdemKey, &c.Task, &c.PR, &c.Head, &c.Requester, &c.QuestionDoc, &c.AuthorJob, &c.AuthorFamily, &c.State, &c.SeatProfile, &c.SeatFamily, &c.SeatGrade, &c.Job, &c.RequestID, &c.Reason, &c.AnswerDoc, &created, &answered, &deadline, &c.Respawns, &notified)
	if err != nil {
		return hubConsult{}, err
	}
	c.Created = time.UnixMilli(created).UTC()
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
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+consultColumns+` FROM consult_requests WHERE state IN ('queued','running') ORDER BY created_ms`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hubConsult
	for rows.Next() {
		var c hubConsult
		var created, answered, deadline int64
		var notified int
		if err := rows.Scan(&c.ID, &c.IdemKey, &c.Task, &c.PR, &c.Head, &c.Requester, &c.QuestionDoc, &c.AuthorJob, &c.AuthorFamily, &c.State, &c.SeatProfile, &c.SeatFamily, &c.SeatGrade, &c.Job, &c.RequestID, &c.Reason, &c.AnswerDoc, &created, &answered, &deadline, &c.Respawns, &notified); err != nil {
			return nil, err
		}
		c.Created = time.UnixMilli(created).UTC()
		if answered != 0 {
			c.Answered = time.UnixMilli(answered).UTC()
		}
		c.Deadline = time.UnixMilli(deadline).UTC()
		c.Notified = notified != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

// saveConsult writes the mutable fields of one row. One coordinator goroutine
// owns a consult at a time, so the row id is the whole concurrency guard.
func (s *laneInboxStore) saveConsult(parent context.Context, c hubConsult) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE consult_requests SET state=?,seat_profile=?,seat_family=?,seat_grade=?,job=?,request_id=?,reason=?,answer_doc=?,answered_ms=?,respawns=?,notified=? WHERE id=?`),
		c.State, c.SeatProfile, c.SeatFamily, c.SeatGrade, c.Job, c.RequestID, c.Reason, c.AnswerDoc, c.Answered.UnixMilli(), c.Respawns, consultBoolInt(c.Notified), c.ID)
	return err
}

func consultBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
