package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

var errSpawnClaimed = errors.New("job_already_claimed")

type hubSpawnOutcome struct {
	RC        int              `json:"rc"`
	Stage     string           `json:"stage"`
	Reason    string           `json:"reason,omitempty"`
	Host      string           `json:"host,omitempty"`
	Pane      string           `json:"pane,omitempty"`
	Profile   string           `json:"profile,omitempty"`
	Model     string           `json:"model,omitempty"`
	Role      string           `json:"role,omitempty"`
	Retryable bool             `json:"retryable,omitempty"`
	StageMS   map[string]int64 `json:"stage_ms,omitempty"`
}

type hubSpawnTask struct {
	Request            fleet.Submission
	Job, Owner, Status string
	Created            time.Time
	Outcome            hubSpawnOutcome
}

func (s *laneInboxStore) ensureSpawnTasks() error {
	ctx, done := s.op(s.life)
	defer done()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS spawn_tasks (job TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,owner TEXT NOT NULL,fingerprint TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,created_ms BIGINT NOT NULL,result TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE IF NOT EXISTS spawn_task_events (request_id TEXT PRIMARY KEY,job TEXT NOT NULL,owner TEXT NOT NULL,result TEXT NOT NULL,created_ms BIGINT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0)`,
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *laneInboxStore) claimSpawn(parent context.Context, request fleet.Submission, job, owner string, now time.Time) (hubSpawnTask, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(request)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO spawn_tasks(job,request_id,owner,fingerprint,payload,status,created_ms) VALUES(?,?,?,?,?,'accepted',?) ON CONFLICT DO NOTHING`), job, request.RequestID, owner, fingerprint, string(raw), now.UnixMilli())
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	inserted, _ := result.RowsAffected()
	if inserted == 1 {
		return hubSpawnTask{Request: request, Job: job, Owner: owner, Status: "accepted", Created: now}, true, nil
	}
	var oldID, oldOwner, oldHash, status, outcome string
	if err = s.db.QueryRowContext(ctx, s.q(`SELECT request_id,owner,fingerprint,status,result FROM spawn_tasks WHERE job=?`), job).Scan(&oldID, &oldOwner, &oldHash, &status, &outcome); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hubSpawnTask{}, false, errSpawnClaimed
		}
		return hubSpawnTask{}, false, err
	}
	if oldID == request.RequestID && oldHash == fingerprint {
		return hubSpawnTask{Request: request, Job: job, Owner: oldOwner, Status: status}, false, nil
	}
	if status != "retryable" || oldOwner != owner {
		return hubSpawnTask{}, false, errSpawnClaimed
	}
	var prior hubSpawnOutcome
	if json.Unmarshal([]byte(outcome), &prior) != nil {
		return hubSpawnTask{}, false, errSpawnClaimed
	}
	updated, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_tasks SET request_id=?,fingerprint=?,payload=?,status='accepted',created_ms=?,result='{}' WHERE job=? AND request_id=? AND status='retryable'`), request.RequestID, fingerprint, string(raw), now.UnixMilli(), job, oldID)
	if err != nil {
		return hubSpawnTask{}, false, err
	}
	count, _ := updated.RowsAffected()
	if count != 1 {
		return hubSpawnTask{}, false, errSpawnClaimed
	}
	return hubSpawnTask{Request: request, Job: job, Owner: owner, Status: "accepted", Created: now, Outcome: prior}, true, nil
}

// completeSpawn owns the terminal transition and its durable notification in
// one transaction. A timeout and a late result cannot both win.
func (s *laneInboxStore) completeSpawn(parent context.Context, task hubSpawnTask, outcome hubSpawnOutcome, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(outcome)
	if err != nil {
		return false, err
	}
	status := "failed"
	if outcome.RC == 0 {
		status = "succeeded"
	} else if outcome.Retryable {
		status = "retryable"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_tasks SET status=?,result=? WHERE job=? AND request_id=? AND status='accepted'`), status, string(raw), task.Job, task.Request.RequestID)
	if err != nil {
		return false, err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, s.q(`INSERT INTO spawn_task_events(request_id,job,owner,result,created_ms) VALUES(?,?,?,?,?)`), task.Request.RequestID, task.Job, task.Owner, string(raw), now.UnixMilli())
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *laneInboxStore) spawnTask(parent context.Context, id string) (hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	var task hubSpawnTask
	var payload, result string
	var created int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT job,owner,payload,status,created_ms,result FROM spawn_tasks WHERE request_id=?`), id).Scan(&task.Job, &task.Owner, &payload, &task.Status, &created, &result)
	if err != nil {
		return task, err
	}
	task.Created = time.UnixMilli(created)
	if err = json.Unmarshal([]byte(payload), &task.Request); err != nil {
		return task, err
	}
	err = json.Unmarshal([]byte(result), &task.Outcome)
	return task, err
}
