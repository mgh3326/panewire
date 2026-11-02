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
	result, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status) VALUES(?,?,'executing') ON CONFLICT DO NOTHING`, message.RequestID, hash)
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
