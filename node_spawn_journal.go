package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Entries are retained as deduplication tombstones. A restart with an executing
// entry never reruns it: there may already be a live pane without a result.
// Completion is fsynced before any response; replay also recovers a lost ACK.
type nodeSpawnJournal struct{ db *sql.DB }

func openNodeSpawnJournal(root string) (*nodeSpawnJournal, error) {
	if root == "" {
		return nil, errors.New("spawn journal directory required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(root, "spawn-inbox.sqlite3")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return nil, errors.New("unsafe spawn journal")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Chmod(0600)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, q := range []string{`PRAGMA busy_timeout=5000`, `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `CREATE TABLE IF NOT EXISTS spawn_inbox (request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,result TEXT NOT NULL DEFAULT '')`} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	// job_id and label name what an executing row launched, so a restart can
	// settle it from the pane (hub_spawn_settle.go). Rows written before the
	// columns existed keep an empty value and settle as lost.
	for _, column := range []string{"job_id", "label"} {
		var present int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name=?`, column).Scan(&present); err == nil && present == 0 {
			_, err = db.ExecContext(ctx, `ALTER TABLE spawn_inbox ADD COLUMN `+column+` TEXT NOT NULL DEFAULT ''`)
		}
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	return &nodeSpawnJournal{db}, nil
}
func (j *nodeSpawnJournal) claim(ctx context.Context, message hubOutboundMessage) (bool, *hubSpawnResult, error) {
	// Only execution fields contribute; transport/reconnect metadata does not.
	raw, _ := json.Marshal(struct {
		ID, CWD, Inline, Doc string
		Args                 []string
		Checkout             *hubSpawnCheckout
		Wait                 int
		Deadline             int64
	}{message.RequestID, message.CWDKey, message.BriefInline, message.BriefDoc, message.Args, message.Checkout, message.WaitSeconds, message.DeadlineMS})
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	result, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,job_id,label) VALUES(?,?,'executing',?,?) ON CONFLICT DO NOTHING`, message.RequestID, hash, hubSpawnArg(message.Args, "--job"), hubSpawnArg(message.Args, "-l"))
	if err != nil {
		return false, nil, err
	}
	n, _ := result.RowsAffected()
	if n == 1 {
		return true, nil, nil
	}
	var old, status, stored string
	if err = j.db.QueryRowContext(ctx, `SELECT fingerprint,status,result FROM spawn_inbox WHERE request_id=?`, message.RequestID).Scan(&old, &status, &stored); err != nil {
		return false, nil, err
	}
	if old != hash {
		return false, nil, errors.New("spawn request payload conflicts with durable ownership")
	}
	if status != "completed" {
		return false, nil, nil
	}
	var cached hubSpawnResult
	if err = json.Unmarshal([]byte(stored), &cached); err != nil {
		return false, nil, err
	}
	return false, &cached, nil
}
func (j *nodeSpawnJournal) complete(ctx context.Context, result hubSpawnResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, `UPDATE spawn_inbox SET status='completed',result=? WHERE request_id=? AND status='executing'`, string(raw), result.RequestID)
	return err
}

// executingSpawn is a journal row no completion has been written for.
type executingSpawn struct{ RequestID, Job, Label string }

func (j *nodeSpawnJournal) executing(ctx context.Context) ([]executingSpawn, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT request_id,job_id,label FROM spawn_inbox WHERE status='executing' ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executingSpawn
	for rows.Next() {
		var row executingSpawn
		if err = rows.Scan(&row.RequestID, &row.Job, &row.Label); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// resultOf returns the stored completion of a request, if it has one.
func (j *nodeSpawnJournal) resultOf(ctx context.Context, requestID string) (*hubSpawnResult, error) {
	var status, stored string
	if err := j.db.QueryRowContext(ctx, `SELECT status,result FROM spawn_inbox WHERE request_id=?`, requestID).Scan(&status, &stored); err != nil || status != "completed" {
		return nil, err
	}
	var result hubSpawnResult
	if err := json.Unmarshal([]byte(stored), &result); err != nil {
		return nil, err
	}
	return &result, nil
}
