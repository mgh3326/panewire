package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// PoolActionFile is the marker a holder node keeps beside its job ledger while
// the hub owns an exhausted-pool action for the job's attempt. The hub registers
// it before it closes the pane; while it is unresolved the watcher must not
// write its own terminal record, because the hub's action writes the quota
// reason (and, for a hand-over, the successor pointer) and will not be able to
// rewrite an hk record the watcher made terminal first.
const PoolActionFile = "pool-action.json"

type poolActionMarker struct {
	ActionID  string    `json:"action_id"`
	EndReason string    `json:"end_reason"`
	At        time.Time `json:"at"`
	Done      bool      `json:"done"`
}

// poolActionOpen reports whether the hub registered an exhausted-pool action
// for this job's attempt that may still be running.
func (a *App) poolActionStatus(id string) (bool, bool) {
	if a.Jobs == "" || id == "" {
		return false, false
	}
	raw, err := os.ReadFile(filepath.Join(a.Jobs, id, PoolActionFile))
	if os.IsNotExist(err) {
		return false, false
	}
	if err != nil {
		return true, false
	}
	var marker poolActionMarker
	if json.Unmarshal(raw, &marker) != nil || marker.ActionID == "" || marker.At.IsZero() {
		return true, false
	}
	return true, marker.Done
}

func (a *App) poolActionOpen(id string) bool {
	open, _ := a.poolActionStatus(id)
	return open
}

// LockPoolAction serializes marker changes with a watcher's terminal decision.
func LockPoolAction(job *os.Root) (func(), error) {
	f, err := job.OpenFile(".panewire-pool-action.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}

// poolActionFallback holds the same lock as registration while deciding whether
// the watcher may write a terminal record. Unknown marker state fails closed.
func (a *App) poolActionFallback(id string, fallback func() error) (bool, error) {
	job, err := os.OpenRoot(filepath.Join(a.Jobs, id))
	if err != nil {
		return false, err
	}
	defer job.Close()
	unlock, err := LockPoolAction(job)
	if err != nil {
		return false, err
	}
	defer unlock()
	open, done := a.poolActionStatus(id)
	if done {
		return true, nil
	}
	if open {
		return false, nil
	}
	return true, fallback()
}

// reconcilePoolGuardedQuotaIntent serializes a watcher's quota ending with
// registration. A completed marker ends the watch; a pending marker defers it.
func (a *App) reconcilePoolGuardedQuotaIntent(ctx context.Context, j Job, in quotaIntent, herdrOK, present, reportPending bool, ag Agent) (quotaIntentOutcome, error) {
	out := quotaIntentRetry
	called := false
	finished, err := a.poolActionFallback(j.ID, func() error {
		called = true
		var err error
		out, err = a.reconcileQuotaIntent(ctx, j, in, herdrOK, present, reportPending, ag)
		return err
	})
	if !called && finished {
		return quotaIntentEnded, err
	}
	return out, err
}
