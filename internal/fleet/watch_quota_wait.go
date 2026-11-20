package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// A reviewer on a short provider-limit reset may wait for the reset and be
// resumed once (hk 5355), but only after the live watcher of the current attempt
// has adopted the wait (hk 5749). The handshake is two durable ledger events:
//
//   - quotaWaitRequestKind, written by the quota-stop manager: the attempt it
//     observed the stop in, the stop, and until, how long the job is to be held;
//   - quotaWaitAdoptedKind, written by the watcher that holds the job's watch lock:
//     the same attempt and until, and deadline, the watcher's completion
//     deadline once it has extended it by the wait.
//
// A successful append of the request proves nothing about the watcher; the
// manager resumes only on the ack, and only while the watch lock is still held
// (QuotaWatcherLive). Without an ack it hands the stop over as it does for
// every other reviewer stop. See docs/quota-stop.md "A reviewer's short reset".
const (
	quotaWaitRequestKind = "job.quota_stop_wait_request"
	quotaWaitAdoptedKind = "job.quota_stop_wait_adopted"
)

// QuotaReviewerWait is the longest reset a reviewer's quota stop may have and
// still be waited out: the quota-stop manager's ceiling for the policy's
// reviewer_wait.
const QuotaReviewerWait = 10 * time.Minute

// QuotaReviewerWaitGrace is how long past its reset a waited reviewer stays
// held: the manager's resume lands at the reset, so the wait ends this long
// after it. The manager asks the watcher to hold the job until then.
const QuotaReviewerWaitGrace = 5 * time.Minute

// quotaWait is a wait the manager asked a watcher to adopt, and whether a
// watcher has.
type quotaWait struct {
	attempt string
	until   time.Time
	pool    string
	reset   string
	// acked is whether an ack names this attempt and this until.
	acked bool
	// ackDeadline is the completion deadline the acks named, the longest of them.
	ackDeadline time.Time
	// ackedAt is when the first ack of this wait was written (see ackWrittenAt).
	ackedAt time.Time
}

// ackWrittenAt is when an acknowledgement was written. The watcher records
// adopted_at to the millisecond: the ledger's created_at is whole seconds
// (arbiter Store.now), so a deadline inside a second cannot be judged from it.
// An ack without adopted_at predates that field, and only its whole second is
// known, so it counts as written at the end of that second: a late ack is never
// taken for an on-time one.
func ackWrittenAt(ev Event) time.Time {
	if at, err := time.Parse(time.RFC3339Nano, str(ev.Payload, "adopted_at")); err == nil {
		return at
	}
	return ev.when.Add(time.Second)
}

// currentQuotaWait is the newest wait request of attempt in evs, in ledger
// sequence order, with the ack that answers it.
func currentQuotaWait(evs []Event, attempt string) (quotaWait, bool) {
	var w quotaWait
	found := false
	for _, ev := range ledgerOrder(evs) {
		if str(ev.Payload, "attempt") != attempt {
			continue
		}
		until, err := time.Parse(time.RFC3339, str(ev.Payload, "until"))
		if err != nil {
			continue
		}
		switch ev.Kind {
		case quotaWaitRequestKind:
			// A repeated request for the same wait (the manager restarted before
			// it noted the first) is the same request: it keeps the ack.
			if found && until.Equal(w.until) {
				continue
			}
			w = quotaWait{attempt: attempt, until: until, pool: str(ev.Payload, "pool"), reset: str(ev.Payload, "reset_at")}
			found = true
		case quotaWaitAdoptedKind:
			if found && until.Equal(w.until) {
				if at := ackWrittenAt(ev); !w.acked || at.Before(w.ackedAt) {
					w.ackedAt = at
				}
				w.acked = true
				// The deadline an ack names is the longest any ack of this wait named.
				if deadline, err := time.Parse(time.RFC3339Nano, str(ev.Payload, "deadline")); err == nil && deadline.After(w.ackDeadline) {
					w.ackDeadline = deadline
				}
			}
		}
	}
	return w, found
}

// quotaWaitApp is the App the manager's handshake calls run as: with the user's
// home, so the ledger resolves to the same Arbiter database the watcher writes
// (App.ledger), as AnnounceQuotaStop's does.
func quotaWaitApp(jobs, session string) *App {
	home, _ := os.UserHomeDir()
	return &App{Jobs: jobs, Home: home, Now: time.Now, Runner: ExecRunner{}, HerdrSession: session}
}

// RequestQuotaWait asks the job's watcher to adopt a wait until the given time,
// for the attempt the manager observed the stop in. sent is false, with no
// error, when that attempt is over (ended, reaped, revoked, another attempt, or
// the hub's posted result ended the job): there is nothing to wait for. The
// request is not an acknowledgement; QuotaWaitAdopted is.
func RequestQuotaWait(ctx context.Context, jobs, job, pane, attempt string, stop quotastop.Stop, until time.Time) (sent bool, err error) {
	if attempt == "" {
		return false, nil
	}
	a := quotaWaitApp(jobs, "")
	payload := map[string]any{"pane_id": pane, "attempt": attempt, "pool": stop.Pool, "until": until.UTC().Format(time.RFC3339)}
	if !stop.Reset.IsZero() {
		payload["reset_at"] = stop.Reset.UTC().Format(time.RFC3339)
	}
	// Under the job's end and operation locks, as an announcement is, so the
	// request is ordered against the hub's posted-result end. An hk record that
	// cannot be read does not block it: the request has no effect the hub sees,
	// and the ack is refused once the attempt has ended.
	lockCtx, cancel := context.WithTimeout(ctx, quotaRecordLockWait)
	defer cancel()
	endUnlock, err := LockJobEndContext(lockCtx, filepath.Join(jobs, job))
	if err != nil {
		return false, err
	}
	defer endUnlock()
	unlock, err := a.lockJobOperation(ctx, job, quotaRecordLockWait)
	if err != nil {
		return false, err
	}
	defer unlock()
	if ended, err := a.endedByResultPosted(ctx, job, false); err != nil {
		return false, err
	} else if ended {
		return false, nil
	}
	if evs, err := a.ledgerEvents(job); err == nil {
		if w, ok := currentQuotaWait(evs, attempt); ok && w.until.Equal(until.UTC().Truncate(time.Second)) {
			// Already requested (the manager restarted before it noted it).
			return true, nil
		}
	}
	err = a.eventIf(ctx, job, quotaWaitRequestKind, payload, func(v ledgerView) error {
		if v.Superseded(pane, attempt) || v.Ended != "" {
			return errQuotaStopSuperseded
		}
		return nil
	})
	if errors.Is(err, errQuotaStopSuperseded) {
		return false, nil
	}
	return err == nil, err
}

// QuotaWaitAckedAt is when a watcher of the job acknowledged the wait the manager
// requested for attempt until the given time, zero when none did: an ack names
// this attempt, this until, and a completion deadline that reaches until, and the
// attempt is still the current, unended one. It says nothing about the watcher
// being alive now; QuotaWatcherLive does.
func QuotaWaitAckedAt(jobs, job, attempt string, until time.Time) (time.Time, error) {
	a := quotaWaitApp(jobs, "")
	evs, err := a.ledgerEvents(job)
	if err != nil {
		return time.Time{}, err
	}
	v := viewLedger(evs)
	if v.Superseded("", attempt) || v.Ended != "" {
		return time.Time{}, nil
	}
	w, ok := currentQuotaWait(evs, attempt)
	if !ok || !w.acked || !w.until.Equal(until.UTC().Truncate(time.Second)) || w.ackDeadline.Before(w.until) {
		return time.Time{}, nil
	}
	return w.ackedAt, nil
}

// QuotaWatcherLive reports whether a watcher that defers to hub-close intents
// holds the job now, on positive evidence: its own life-long lock
// (ProbeHubCloseWatcher answers aware), which a hub close fencing or borrowing
// the watch lock never takes, and no hub-close intent open in the ledger. The
// watch lock alone (watchActive) is a cleanup probe that also answers true when
// it cannot be opened, for a legacy sentinel, and while the hub holds it.
func QuotaWatcherLive(jobs, job string) bool {
	kind, err := ProbeHubCloseWatcher(filepath.Join(jobs, job))
	if err != nil || kind != HubCloseWatcherAware {
		return false
	}
	j, err := quotaWaitApp(jobs, "").job(job)
	if err != nil {
		return false
	}
	_, open := pendingHubCloseIntent(j)
	return !open
}

// KeepQuotaWatcher starts the job's re-armed watcher when none holds the watch
// lock, so a resumed or waiting reviewer is never left without one. session is
// the herdr session the pane lives in (hk 5777).
func KeepQuotaWatcher(jobs, job, session string) error {
	a := quotaWaitApp(jobs, session)
	if a.watchActive(job) {
		return nil
	}
	return announceStartWatch(a, job)
}

// adoptQuotaWait is the watcher's side of the handshake, one look per poll. It
// returns the completion deadline the watcher is to hold and the wait it has
// adopted (zero when none). A request of the current attempt that is still ahead
// extends the deadline by the wait, once per wait; the ack is written first, so
// the manager never sees a wait the watcher did not extend for. A request for
// another attempt, an attempt that ended, or a wait already over is ignored: the
// deadline moves only for an adopted wait.
func (a *App) adoptQuotaWait(ctx context.Context, j Job, view ledgerView, held, deadline time.Time) (time.Time, time.Time) {
	if view.Ended != "" || view.Reaped || view.Revoked || view.Attempt == "" {
		return deadline, held
	}
	w, ok := currentQuotaWait(j.Events, view.Attempt)
	now := a.Now()
	if !ok || !w.until.After(now) || w.until.Equal(held) {
		return deadline, held
	}
	extended := deadline.Add(w.until.Sub(now))
	if !w.acked {
		payload := map[string]any{"pane_id": j.Pane, "attempt": w.attempt, "pool": w.pool, "until": w.until.UTC().Format(time.RFC3339), "deadline": extended.UTC().Format(time.RFC3339Nano), "adopted_at": now.UTC().Format(time.RFC3339Nano)}
		if w.reset != "" {
			payload["reset_at"] = w.reset
		}
		err := a.eventIf(ctx, j.ID, quotaWaitAdoptedKind, payload, func(v ledgerView) error {
			if v.Superseded(j.Pane, w.attempt) || v.Ended != "" {
				return errQuotaStopSuperseded
			}
			return nil
		})
		if err != nil {
			if !errors.Is(err, errQuotaStopSuperseded) {
				a.warn("quota-stop wait not adopted, retrying: %v", err)
			}
			return deadline, held
		}
	}
	return extended, w.until
}
