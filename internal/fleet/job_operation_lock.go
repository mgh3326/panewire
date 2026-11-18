package fleet

import (
	"context"
	"errors"
	"fmt"
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
//
// The wait also ends when ctx is done, with ctx's error, so a canceled watcher
// or a stopping daemon never waits out the whole bound behind a slow holder
// (hk 4761, from #593 round 6).
func (a *App) lockJobOperation(ctx context.Context, id string, wait time.Duration) (func(), error) {
	if _, err := os.Lstat(filepath.Join(a.Jobs, id)); errors.Is(err, os.ErrNotExist) {
		return func() {}, nil
	}
	deadline := time.Now().Add(wait)
	for {
		unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, id))
		if !errors.Is(err, ErrJobOperationBusy) || !time.Now().Before(deadline) {
			return unlock, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(jobOperationLockPoll):
		}
	}
}

// ReviewerResultPosted is the reason of the final job.lost, and the hk
// end_reason, of a reviewer job the hub ended because it posted its result.
const ReviewerResultPosted = "reviewer_result_posted"

// errQuotaStopSuperseded is what a quota-stop record returns, with nothing
// written, when the hub's posted-result end won while it waited for the
// operation lock. Its callers must not go on as if the record had been written:
// an activated hand-over would later re-prompt the pane and make the ended
// ledger live again.
var errQuotaStopSuperseded = errors.New("quota stop superseded by the posted-result end")

// endedByResultPostedAttempts and endedByResultPostedRetry bound how long a
// quota-stop record waits for hk to become readable before it gives up.
const (
	endedByResultPostedAttempts = 3
	endedByResultPostedRetry    = 2 * time.Second
)

// endedByResultPosted reports that the job's newest end is the hub's
// posted-result end (RecordJobEnded). Two records can show it: the ledger's
// final job.lost, and an hk record lost with this end_reason, which the node
// writes first, so a failure between the two leaves the hk record alone. An hk
// record that cannot be read is an error, never "not ended": the quota-stop
// record would otherwise be written over an end hk may already hold. A missing
// hk record, or an app with no hk store configured, holds no end. The ledger is
// folded in sequence order, not timestamp order: a clock stepped backwards
// reorders timestamps (see ledgerOrder). A quota-stop record that waited for the
// operation lock checks this under the lock and writes nothing when the end won,
// so it cannot re-arm a hand-over for a job hk already holds lost.
//
// strictHK says what an unreadable hk record means. The announcement is what the
// hub acts on, so it is strict: the read is retried and then fails the record.
// The close intent is not strict: it is recorded before any hk write so that it
// survives an hk outage, it has no effect the hub sees, and its reconciliation
// reads hk first and ends the watch without announcing when hk holds another end
// (reconcileQuotaIntent), so an unreadable record there is left to that step.
func (a *App) endedByResultPosted(ctx context.Context, id string, strictHK bool) (bool, error) {
	attempts := endedByResultPostedAttempts
	if !strictHK {
		attempts = 1
	}
	ended, err := a.hkHoldsPostedResultEnd(ctx, id, attempts)
	if err != nil && strictHK {
		return false, err
	}
	if ended {
		return true, nil
	}
	j, err := a.job(id)
	if err != nil {
		return false, nil
	}
	ledgerEnded := false
	for _, ev := range ledgerOrder(j.Events) {
		switch ev.Kind {
		case "job.lost":
			ledgerEnded = str(ev.Payload, "reason") == ReviewerResultPosted
		case "job.claim", "job.claimed", "job.reclaim", "job.spawned", "job.reprompted",
			"job.completed", "job.joined", "job.revoked", "job.handed_off":
			ledgerEnded = false
		}
	}
	return ledgerEnded, nil
}

// hkHoldsPostedResultEnd reads the job's hk record, retrying a read error a
// few times, and reports whether it is lost with ReviewerResultPosted.
func (a *App) hkHoldsPostedResultEnd(ctx context.Context, id string, attempts int) (bool, error) {
	store, err := a.jobStore()
	if err != nil {
		return false, nil
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if err := a.sleepOrWait(ctx, endedByResultPostedRetry); err != nil {
				return false, err
			}
		}
		rec, err := store.Get(ctx, id)
		if errors.Is(err, errJobNotFound) {
			return false, nil
		}
		if err != nil {
			last = err
			continue
		}
		state, _ := rec["state"].(string)
		reason, _ := rec["end_reason"].(string)
		return state == "lost" && reason == ReviewerResultPosted, nil
	}
	return false, fmt.Errorf("hk record of %s unreadable: %w", id, last)
}

// sleepOrWait waits d through the app's Sleep, or the real clock when the app
// has none (AnnounceQuotaStop builds one without).
func (a *App) sleepOrWait(ctx context.Context, d time.Duration) error {
	if a.Sleep != nil {
		return a.Sleep(ctx, d)
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
