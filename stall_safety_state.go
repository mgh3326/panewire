package panewire

import (
	"context"
	"database/sql"
	"time"
)

// stallSafetyState stores the start and examination status of the current
// uninterrupted inactive episode. A busy foreground command starts a fresh
// interval; a later return to work removes the episode altogether.
type stallSafetyState struct{ store *Store }

func newStallSafetyState(store *Store) (*stallSafetyState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, err := store.db.Exec(`CREATE TABLE IF NOT EXISTS stall_safety_episodes (job TEXT NOT NULL, pane TEXT NOT NULL, since_ms INTEGER NOT NULL, examined INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job,pane))`)
	if err != nil {
		return nil, err
	}
	_, err = store.db.Exec(`CREATE TABLE IF NOT EXISTS stall_safety_examinations (job TEXT NOT NULL, pane TEXT NOT NULL, since_ms INTEGER NOT NULL, screen TEXT NOT NULL, explanation TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', hk_done INTEGER NOT NULL DEFAULT 0, action_done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job,pane,since_ms))`)
	if err != nil {
		return nil, err
	}
	return &stallSafetyState{store: store}, nil
}

func (s *stallSafetyState) observe(ctx context.Context, job, pane string, working, busy bool, now time.Time, limit time.Duration) (bool, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if working {
		_, err := s.store.db.ExecContext(ctx, `DELETE FROM stall_safety_episodes WHERE job=? AND pane=?`, job, pane)
		return false, err
	}
	if busy {
		_, err := s.store.db.ExecContext(ctx, `INSERT INTO stall_safety_episodes(job,pane,since_ms,examined) VALUES(?,?,?,0) ON CONFLICT(job,pane) DO UPDATE SET since_ms=excluded.since_ms,examined=0`, job, pane, now.UnixMilli())
		return false, err
	}
	var since int64
	var examined int
	err := s.store.db.QueryRowContext(ctx, `SELECT since_ms,examined FROM stall_safety_episodes WHERE job=? AND pane=?`, job, pane).Scan(&since, &examined)
	if err == sql.ErrNoRows {
		_, err = s.store.db.ExecContext(ctx, `INSERT INTO stall_safety_episodes(job,pane,since_ms) VALUES(?,?,?)`, job, pane, now.UnixMilli())
		return false, err
	}
	if err != nil {
		return false, err
	}
	return examined == 0 && now.After(time.UnixMilli(since).Add(limit)), nil
}

// recordExamination saves the bounded, redacted evidence before publishing any
// external effects. The first writer owns the episode's evidence.
func (s *stallSafetyState) recordExamination(ctx context.Context, job, pane, screen, explanation, details string) (bool, error) {
	screen = utf8SuffixWithin(redactSecrets(screen), 4096)
	explanation = utf8SuffixWithin(redactSecrets(explanation), 4096)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO stall_safety_examinations(job,pane,since_ms,screen,explanation,details) SELECT job,pane,since_ms,?,?,? FROM stall_safety_episodes WHERE job=? AND pane=? AND examined=0`, screen, explanation, details, job, pane)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE stall_safety_episodes SET examined=1 WHERE job=? AND pane=?`, job, pane); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
