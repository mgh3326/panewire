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
	_, err := store.db.Exec(`CREATE TABLE IF NOT EXISTS stall_safety_episodes (job TEXT NOT NULL, pane TEXT NOT NULL, attempt TEXT NOT NULL, since_ms INTEGER NOT NULL, state_seq INTEGER NOT NULL, examined INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job,pane))`)
	if err != nil {
		return nil, err
	}
	_, err = store.db.Exec(`CREATE TABLE IF NOT EXISTS stall_safety_examinations (job TEXT NOT NULL, pane TEXT NOT NULL, attempt TEXT NOT NULL, since_ms INTEGER NOT NULL, screen TEXT NOT NULL, explanation TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', hk_done INTEGER NOT NULL DEFAULT 0, action_done INTEGER NOT NULL DEFAULT 0, action_result TEXT NOT NULL DEFAULT '', PRIMARY KEY(job,pane,attempt,since_ms))`)
	if err != nil {
		return nil, err
	}
	return &stallSafetyState{store: store}, nil
}

func (s *stallSafetyState) observe(ctx context.Context, job, pane, attempt string, working, busy bool, seq int64, now time.Time, limit time.Duration) (bool, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if working {
		_, err := s.store.db.ExecContext(ctx, `DELETE FROM stall_safety_episodes WHERE job=? AND pane=?`, job, pane)
		return false, err
	}
	if busy {
		_, err := s.store.db.ExecContext(ctx, `INSERT INTO stall_safety_episodes(job,pane,attempt,since_ms,state_seq,examined) VALUES(?,?,?,?,?,0) ON CONFLICT(job,pane) DO UPDATE SET attempt=excluded.attempt,since_ms=excluded.since_ms,state_seq=excluded.state_seq,examined=0`, job, pane, attempt, now.UnixMilli(), seq)
		return false, err
	}
	var since, previousSeq int64
	var previousAttempt string
	var examined int
	err := s.store.db.QueryRowContext(ctx, `SELECT since_ms,state_seq,examined,attempt FROM stall_safety_episodes WHERE job=? AND pane=?`, job, pane).Scan(&since, &previousSeq, &examined, &previousAttempt)
	if err == sql.ErrNoRows {
		_, err = s.store.db.ExecContext(ctx, `INSERT INTO stall_safety_episodes(job,pane,attempt,since_ms,state_seq) VALUES(?,?,?,?,?)`, job, pane, attempt, now.UnixMilli(), seq)
		return false, err
	}
	if err != nil {
		return false, err
	}
	if attempt != previousAttempt || (seq != previousSeq && examined == 0) {
		_, err := s.store.db.ExecContext(ctx, `UPDATE stall_safety_episodes SET attempt=?,since_ms=?,state_seq=?,examined=0 WHERE job=? AND pane=?`, attempt, now.UnixMilli(), seq, job, pane)
		return false, err
	}
	if seq != previousSeq {
		_, err := s.store.db.ExecContext(ctx, `UPDATE stall_safety_episodes SET state_seq=? WHERE job=? AND pane=?`, seq, job, pane)
		return false, err
	}
	return examined == 0 && now.After(time.UnixMilli(since).Add(limit)), nil
}

func (s *stallSafetyState) markWorking(ctx context.Context, pane string) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	_, err := s.store.db.ExecContext(ctx, `DELETE FROM stall_safety_episodes WHERE pane=?`, pane)
	return err
}

func (s *stallSafetyState) retainPanes(ctx context.Context, agents []HerdrAgentState) error {
	seen := make(map[string]bool, len(agents))
	for _, agent := range agents {
		seen[agent.PaneID] = true
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	rows, err := s.store.db.QueryContext(ctx, `SELECT DISTINCT pane FROM stall_safety_episodes`)
	if err != nil {
		return err
	}
	var missing []string
	for rows.Next() {
		var pane string
		if err := rows.Scan(&pane); err != nil {
			_ = rows.Close()
			return err
		}
		if !seen[pane] {
			missing = append(missing, pane)
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	for _, pane := range missing {
		if _, err := s.store.db.ExecContext(ctx, `DELETE FROM stall_safety_episodes WHERE pane=?`, pane); err != nil {
			return err
		}
	}
	return nil
}

func (s *stallSafetyState) episodeSequence(ctx context.Context, job, pane, attempt string, since int64) (int64, bool, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	var seq int64
	err := s.store.db.QueryRowContext(ctx, `SELECT state_seq FROM stall_safety_episodes WHERE job=? AND pane=? AND attempt=? AND since_ms=? AND examined=1`, job, pane, attempt, since).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return seq, err == nil, err
}

// recordExamination saves the bounded, redacted evidence before publishing any
// external effects. The first writer owns the episode's evidence.
func (s *stallSafetyState) recordExamination(ctx context.Context, job, pane, screen, explanation, details string) (bool, error) {
	screen = stallExaminationExcerpt(redactSecrets(screen))
	explanation = stallExaminationExcerpt(redactSecrets(explanation))
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO stall_safety_examinations(job,pane,attempt,since_ms,screen,explanation,details) SELECT job,pane,attempt,since_ms,?,?,? FROM stall_safety_episodes WHERE job=? AND pane=? AND examined=0`, screen, explanation, details, job, pane)
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
