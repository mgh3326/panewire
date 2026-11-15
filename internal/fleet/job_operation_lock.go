package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// ErrJobOperationBusy means another process holds the job's operation lock.
var ErrJobOperationBusy = errors.New("job operation lock is held")

// jobOperationLockPoll is how often lockJobOperation retries a held lock.
const jobOperationLockPoll = 50 * time.Millisecond

// TryLockJobOperation takes the job's operation lock, the one `panewire done`
// and the job-dir TTL pass hold (lifecycle.go, job_dir_ttl.go), without waiting.
// jobDir is the job's directory. It returns ErrJobOperationBusy when the lock
// is held. The hub's reviewer_result_posted end holds it across its checks and
// the hk write, so a quota-stop record or a completion cannot land between the
// proof and the write (hk 4684).
func TryLockJobOperation(jobDir string) (unlock func(), err error) {
	f, err := secureOpen(filepath.Join(jobDir, jobDirLock), true, nil)
	if err != nil {
		return nil, err
	}
	if !tryLock(f) {
		f.Close()
		return nil, ErrJobOperationBusy
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// lockJobOperation is TryLockJobOperation for the job id under a.Jobs, retried
// until wait has passed. The watcher's quota-stop records take it, so the
// node's posted-result end either sees the record or is seen by it.
//
// A job with no directory under a.Jobs has no job-dir ledger for the node's end
// to read, and that end refuses such a job (ledger_missing), so there is nothing
// to order against and the lock is a no-op.
func (a *App) lockJobOperation(id string, wait time.Duration) (func(), error) {
	if _, err := os.Lstat(filepath.Join(a.Jobs, id)); errors.Is(err, os.ErrNotExist) {
		return func() {}, nil
	}
	deadline := time.Now().Add(wait)
	for {
		unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, id))
		if !errors.Is(err, ErrJobOperationBusy) || !time.Now().Before(deadline) {
			return unlock, err
		}
		time.Sleep(jobOperationLockPoll)
	}
}

// ReviewerResultPosted is the reason of the final job.lost, and the hk
// end_reason, of a reviewer job the hub ended because it posted its result.
const ReviewerResultPosted = "reviewer_result_posted"

// endedByResultPosted reports that the job's newest end is the hub's
// posted-result end (RecordJobEnded). Two records can show it: the ledger's
// final job.lost, and an hk record lost with this end_reason, which the node
// writes first, so a failure between the two leaves the hk record alone. A
// claim, spawn, re-prompt or another end after the ledger event clears it. The
// ledger is folded in sequence order, not timestamp order: a clock stepped
// backwards reorders timestamps (see ledgerOrder). A quota-stop record that
// waited for the operation lock checks this under the lock and writes nothing
// when the end won, so it cannot re-arm a hand-over for a job hk already holds
// lost.
func (a *App) endedByResultPosted(ctx context.Context, id string) bool {
	if state, reason, ok := a.hkJobState(ctx, id); ok && state == "lost" && reason == ReviewerResultPosted {
		return true
	}
	j, err := a.job(id)
	if err != nil {
		return false
	}
	ended := false
	for _, ev := range ledgerOrder(j.Events) {
		switch ev.Kind {
		case "job.lost":
			ended = str(ev.Payload, "reason") == ReviewerResultPosted
		case "job.claim", "job.claimed", "job.reclaim", "job.spawned", "job.reprompted",
			"job.completed", "job.joined", "job.revoked", "job.handed_off":
			ended = false
		}
	}
	return ended
}

// RecordJobEnded appends the final job.lost event of a job the hub ended
// because it posted its result (hk 4684). The node's ended-job pane sweep
// recognises a reviewer's end from its local ledger, not from hk, so without
// this event the pane would stay open after the hk record is written. jobs is
// the node's jobs root (<inbox>/jobs).
func RecordJobEnded(ctx context.Context, jobs, job, pane, reason string) error {
	home, _ := os.UserHomeDir()
	a := &App{Jobs: jobs, Home: home, Now: time.Now, Runner: ExecRunner{}}
	return a.event(ctx, job, "job.lost", map[string]any{"pane_id": pane, "reason": reason, "final": true})
}
