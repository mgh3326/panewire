package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// A hub close is the hub ending a job by closing its pane through a node close
// operation (hk 4652; docs/node-commands.md, "Hub close protocol"). The node
// records a hub-close intent in the job's ledger, with the planned end_reason,
// before it registers the operation. While the intent is open for the job's
// current attempt, its watcher writes no terminal record of its own; once the
// operation is final it finishes the job with exactly the intended record.
//
// It is a sibling of the quota-stop intent (watch_quota_stop.go), not a reuse:
// that one cancels on a missing pane, and a hub close makes the pane missing on
// purpose.
const (
	hubCloseIntentKind = "job.hub_close_intent"
	hubCloseCancelKind = "job.hub_close_cancelled"
	// hubCloseLostReason is the job.lost reason the watcher records when it
	// finishes a hub close.
	hubCloseLostReason = "hub_close"
)

// HubCloseIntent is what the node records before it registers a hub close
// operation: the request, the attempt and receipt it is bound to, and the
// end_reason of the hk lost record that ends the job.
type HubCloseIntent struct {
	RequestID string
	Attempt   string
	Pane      string
	Label     string
	EndReason string
}

// RecordHubCloseIntent appends job.hub_close_intent to job's ledger under the
// jobs root.
func RecordHubCloseIntent(ctx context.Context, jobs, job string, in HubCloseIntent) error {
	home, _ := os.UserHomeDir()
	return (&App{Jobs: jobs, Home: home, Now: time.Now}).recordHubCloseIntent(ctx, job, in)
}

// recordHubCloseIntent appends the intent and then fsyncs the events
// directory itself: the ledger's own directory sync is best effort, and the
// watcher reads intents from the event files, so the close must not proceed on
// an intent a crash could drop.
func (a *App) recordHubCloseIntent(ctx context.Context, job string, in HubCloseIntent) error {
	if e := a.event(ctx, job, hubCloseIntentKind, map[string]any{
		"request_id": in.RequestID, "attempt": in.Attempt, "pane_id": in.Pane, "label": in.Label, "end_reason": in.EndReason,
	}); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Join(a.Jobs, job, "events"))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}

// Two per-job lock files coordinate a hub close with the job's watcher, which
// is a separate process:
//
//   - jobEndLock is held by the node while it binds and records a hub-close
//     intent, and by the watcher around every terminal or expiry decision,
//     which it re-checks against a freshly read ledger under the lock. So no
//     watcher end can interleave with an intent being recorded.
//   - hubCloseWatchLock is held for its whole life by a watcher that defers to
//     hub-close intents. A watcher of an older binary holds only the watch
//     lock; the node refuses to open a hub close while one runs.
const (
	jobEndLock        = ".panewire-end.lock"
	hubCloseWatchLock = ".panewire-watch-hubclose.lock"
)

// LockJobEnd takes the job's end lock, blocking, and returns its release.
func LockJobEnd(jobDir string) (func(), error) {
	f, e := secureOpen(filepath.Join(jobDir, jobEndLock), true, nil)
	if e != nil {
		return nil, e
	}
	if e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); e != nil {
		f.Close()
		return nil, e
	}
	return func() { f.Close() }, nil
}

// HubCloseWatcher is what runs as a job's watcher, for a hub close.
type HubCloseWatcher int

const (
	// HubCloseWatcherNone: no watcher runs; the caller starts one.
	HubCloseWatcherNone HubCloseWatcher = iota
	// HubCloseWatcherAware: a watcher that defers to hub-close intents runs.
	HubCloseWatcherAware
	// HubCloseWatcherIncompatible: a watcher that does not read hub-close
	// intents (an older binary that outlived a node upgrade) runs.
	HubCloseWatcherIncompatible
)

// ProbeHubCloseWatcher reports which watcher runs for the job directory.
func ProbeHubCloseWatcher(jobDir string) (HubCloseWatcher, error) {
	held := func(name string) (bool, error) {
		f, e := secureOpen(filepath.Join(jobDir, name), false, nil)
		if errors.Is(e, fs.ErrNotExist) {
			return false, nil
		} else if e != nil {
			return false, e
		}
		defer f.Close()
		return !tryLock(f), nil
	}
	if aware, e := held(hubCloseWatchLock); e != nil || aware {
		return HubCloseWatcherAware, e
	}
	// A watcher takes the watch lock before the hub-close one, so a watcher
	// caught in between reads as incompatible: the hub retries.
	if running, e := held(".panewire-watch.lock"); e != nil || running {
		return HubCloseWatcherIncompatible, e
	}
	return HubCloseWatcherNone, nil
}

// StartHubCloseWatcher starts the job's re-armed watcher, for a hub close that
// found none running: the watcher is what finishes the job.
func StartHubCloseWatcher(jobs, session, job string) error {
	a, e := New(os.Stderr, os.Stderr)
	if e != nil {
		return e
	}
	a.Jobs, a.HerdrSession = jobs, session
	return a.startWatchMode(job, true)
}

// OpenCloseOps lists the job's close operations for attempt that are not yet
// final, so the node can refuse a second, overlapping one.
func OpenCloseOps(job *os.Root, attempt string) ([]CloseOp, error) {
	dir, e := job.Open(CloseOpDir)
	if errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	} else if e != nil {
		return nil, e
	}
	names, e := dir.Readdirnames(-1)
	dir.Close()
	if e != nil {
		return nil, e
	}
	var open []CloseOp
	for _, name := range names {
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, e := job.ReadFile(filepath.Join(CloseOpDir, name))
		if e != nil {
			return nil, e
		}
		var op CloseOp
		if e := json.Unmarshal(raw, &op); e != nil {
			return nil, e
		}
		if op.Attempt == attempt && !op.Final() {
			open = append(open, op)
		}
	}
	return open, nil
}

// endUnlessHubClose runs end, a terminal or expiry decision of the watcher,
// under the job's end lock, unless a freshly read ledger shows an open hub
// close: deferred is then true and end does not run.
func (a *App) endUnlessHubClose(id string, end func(j Job) error) (deferred bool, err error) {
	unlock, e := LockJobEnd(filepath.Join(a.Jobs, id))
	if e != nil {
		return false, e
	}
	defer unlock()
	j, e := a.job(id)
	if e != nil {
		return false, e
	}
	if _, open := pendingHubCloseIntent(j); open && !j.Reaped {
		return true, nil
	}
	return false, end(j)
}

// pendingHubCloseIntent returns the job's open hub-close intent: the newest
// intent recorded for the current attempt and its receipt's pane and label,
// not followed by a cancellation, an end or a new attempt. An intent naming
// another attempt, pane or label is stale and ignored, as the node's binding
// ignores it (node_pane_close_op.go closeTargetBound).
func pendingHubCloseIntent(j Job) (HubCloseIntent, bool) {
	var cur HubCloseIntent
	var attempt, pane, label string
	open := false
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.claim", "job.claimed", "job.reclaim":
			attempt, pane, label, open = ev.name, "", "", false
		case "job.spawned":
			attempt, pane, label, open = ev.name, str(ev.Payload, "pane_id"), str(ev.Payload, "label"), false
			if label == "" {
				label = str(ev.Payload, "agent_label")
			}
		case "job.reprompted":
			attempt, open = ev.name, false
		case hubCloseIntentKind:
			cur = HubCloseIntent{RequestID: str(ev.Payload, "request_id"), Attempt: str(ev.Payload, "attempt"), Pane: str(ev.Payload, "pane_id"), Label: str(ev.Payload, "label"), EndReason: str(ev.Payload, "end_reason")}
			open = ValidCloseOpID(cur.RequestID) && cur.EndReason != "" && cur.Attempt == attempt && cur.Pane == pane && cur.Pane != "" && cur.Label == label
		case hubCloseCancelKind:
			if str(ev.Payload, "request_id") == cur.RequestID {
				open = false
			}
		case "job.completed", "job.completion", "job.record_settled", "job.joined", "job.revoked", "job.handed_off", "job.escalate", "job.reaped":
			open = false
		case "job.lost":
			// An announced quota stop is the hand-over the hub closes for; any
			// other loss ends the attempt.
			if str(ev.Payload, "reason") != "quota_stop" || boolean(ev.Payload, "final", false) {
				open = false
			}
		}
	}
	return cur, open
}

// hubCloseOutcome is what one reconciliation of a hub-close intent did.
type hubCloseOutcome int

const (
	// hubCloseWait: the close is still open; the watcher defers.
	hubCloseWait hubCloseOutcome = iota
	// hubCloseEnded: the job was finished, or another terminal record owns it.
	hubCloseEnded
	// hubCloseCancelled: the operation ended without closing the pane; the
	// watcher resumes its own duties.
	hubCloseCancelled
)

// readJobCloseOp reads the job's close operation id from its journal, made
// durable first: a state a failed fsync left visible is not acted on.
func (a *App) readJobCloseOp(job, id string) (CloseOp, bool, error) {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, job))
	if err != nil {
		return CloseOp{}, false, err
	}
	defer root.Close()
	op, found, err := ReadCloseOp(root, id)
	if err == nil && found {
		err = SyncCloseOps(root)
	}
	return op, found, err
}

// paneProvenGone reports whether a successful herdr read shows the job's pane
// absent. An unreadable herdr proves nothing.
func (a *App) paneProvenGone(ctx context.Context, pane string) bool {
	agents, e := a.agents(ctx)
	if e != nil {
		return false
	}
	for _, ag := range agents {
		if ag.Pane == pane {
			return false
		}
	}
	return true
}

// reconcileHubCloseIntent drives an open hub-close intent one step, under the
// job's end lock. The node's journal decides first: an operation not yet
// final (or not yet registered) keeps the watcher waiting. closed and gone
// prove the pane stopped; unknown does only once herdr shows the pane absent,
// and waits otherwise (the hub retries under a new request id). refused,
// cancelled and preempted closed nothing and cancel the intent. Then hk: a
// live record takes exactly the intended lost record; the intended record
// already there is accepted; another terminal record is kept, and the
// watcher still releases the lease and closes the intent, since the pane is
// proven gone.
func (a *App) reconcileHubCloseIntent(ctx context.Context, j Job, in HubCloseIntent) (hubCloseOutcome, error) {
	unlock, e := LockJobEnd(filepath.Join(a.Jobs, j.ID))
	if e != nil {
		return hubCloseWait, e
	}
	defer unlock()
	cancel := func(reason string) (hubCloseOutcome, error) {
		return hubCloseCancelled, a.event(ctx, j.ID, hubCloseCancelKind, map[string]any{"request_id": in.RequestID, "reason": reason})
	}
	op, found, err := a.readJobCloseOp(j.ID, in.RequestID)
	switch {
	case err != nil:
		a.warn("hub close of job %s waits: close operation unreadable: %v", j.ID, err)
		return hubCloseWait, nil
	case !found, !op.Final():
		// Registered after the intent, or still closing: the hub repeats the
		// request until it is final (docs/node-commands.md).
		return hubCloseWait, nil
	case op.Attempt != in.Attempt:
		return cancel("stale")
	}
	switch op.State {
	case CloseOpClosed, CloseOpGone:
	case CloseOpUnknown:
		if !a.paneProvenGone(ctx, j.Pane) {
			return hubCloseWait, nil
		}
	default:
		return cancel("not_closed")
	}
	state, reason, ok := a.hkJobState(ctx, j.ID)
	switch {
	case !ok:
		return hubCloseWait, nil
	case state == "lost" && reason == in.EndReason:
	case state == "completed" || state == "lost" || state == "escalated":
		a.warn("hub close of job %s: hk holds another terminal record (%s); kept", j.ID, state)
		return a.finishHubClose(ctx, j, in, true), nil
	default:
		if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": in.EndReason, "at": a.Now().UTC()}); e != nil {
			a.warn("hub close lost record not written, retrying: %v", e)
			return hubCloseWait, nil
		}
	}
	return a.finishHubClose(ctx, j, in, false), nil
}

// finishHubClose finishes a job whose pane the hub close stopped and whose hk
// record is terminal: the lease is released, then the final job.lost closes
// the intent. A failure between the two leaves the intent open, so the next
// poll or a re-armed watcher repeats both idempotent steps.
func (a *App) finishHubClose(ctx context.Context, j Job, in HubCloseIntent, superseded bool) hubCloseOutcome {
	if e := a.releaseQuota(ctx, j); e != nil {
		a.warn("hub close lease release failed, retrying: %v", e)
		return hubCloseWait
	}
	payload := map[string]any{"pane_id": j.Pane, "reason": hubCloseLostReason, "final": true, "request_id": in.RequestID, "end_reason": in.EndReason}
	if superseded {
		payload["superseded"] = true
	}
	if e := a.event(ctx, j.ID, "job.lost", payload); e != nil {
		a.warn("hub close job.lost not recorded, retrying: %v", e)
		return hubCloseWait
	}
	return hubCloseEnded
}
