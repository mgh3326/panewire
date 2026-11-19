package panewire

import (
	"context"
	"errors"
	"time"
)

// quotaResumeCount is how many quota-stop resumes this node recorded for job,
// read from the store the relay outbox shares (SetRelayOutbox). A node with no
// store attached cannot prove a count, so it is an error and job.result leaves
// the field out, which the hub reads as unknown.
func (client *HubClient) quotaResumeCount(job string) (int, error) {
	client.outboxMu.Lock()
	store := client.outbox
	client.outboxMu.Unlock()
	if store == nil {
		return 0, errors.New("quota-stop store not attached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return store.quotaResumesOf(ctx, job)
}

// quotaResumesOf counts the resumes the quota-stop manager recorded for job. A
// resume is the one decision row of a wait that the manager sent (attempted=1,
// quota_stop_runtime.go); the hub's own wait rows are counted on the hub. A
// node whose manager never created the table recorded no resume, so it is 0.
func (s *Store) quotaResumesOf(ctx context.Context, job string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tables int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='quota_stop_decisions'`).Scan(&tables); err != nil {
		return 0, err
	}
	if tables == 0 {
		return 0, nil
	}
	var resumes int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_stop_decisions WHERE job=? AND attempted=1`, job).Scan(&resumes)
	return resumes, err
}
