package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func (a *App) recordHubCloseIntent(ctx context.Context, job string, in HubCloseIntent) error {
	return a.event(ctx, job, hubCloseIntentKind, map[string]any{
		"request_id": in.RequestID, "attempt": in.Attempt, "pane_id": in.Pane, "label": in.Label, "end_reason": in.EndReason,
	})
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

// readJobCloseOp reads the job's close operation id from its journal.
func (a *App) readJobCloseOp(job, id string) (CloseOp, bool, error) {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, job))
	if err != nil {
		return CloseOp{}, false, err
	}
	defer root.Close()
	return ReadCloseOp(root, id)
}

// reconcileHubCloseIntent drives an open hub-close intent one step. hk decides
// first: exactly the intended record finishes the job, another terminal record
// ends the watch without writing anything. Then the node's journal: an
// operation not yet final (or not yet registered) keeps the watcher waiting; a
// final closed, gone or unknown writes the intended lost record and finishes;
// refused, cancelled or preempted closed nothing and cancels the intent.
func (a *App) reconcileHubCloseIntent(ctx context.Context, j Job, in HubCloseIntent) (hubCloseOutcome, error) {
	cancel := func(reason string) (hubCloseOutcome, error) {
		return hubCloseCancelled, a.event(ctx, j.ID, hubCloseCancelKind, map[string]any{"request_id": in.RequestID, "reason": reason})
	}
	state, reason, ok := a.hkJobState(ctx, j.ID)
	if !ok {
		return hubCloseWait, nil
	}
	switch {
	case state == "lost" && reason == in.EndReason:
		return a.finishHubClose(ctx, j, in), nil
	case state == "completed" || state == "lost" || state == "escalated":
		a.warn("hub close of job %s: hk holds another terminal record (%s)", j.ID, state)
		if _, e := cancel("superseded"); e != nil {
			return hubCloseWait, e
		}
		return hubCloseEnded, nil
	}
	op, found, err := a.readJobCloseOp(j.ID, in.RequestID)
	switch {
	case err != nil && !errors.Is(err, os.ErrNotExist):
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
	case CloseOpClosed, CloseOpGone, CloseOpUnknown:
	default:
		return cancel("not_closed")
	}
	if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": in.EndReason, "at": a.Now().UTC()}); e != nil {
		a.warn("hub close lost record not written, retrying: %v", e)
		return hubCloseWait, nil
	}
	return a.finishHubClose(ctx, j, in), nil
}

// finishHubClose finishes a job whose intended lost record is in hk: the lease
// is released, then the final job.lost closes the intent. A failure between
// the two leaves the intent open, so the next poll or a re-armed watcher
// repeats both idempotent steps.
func (a *App) finishHubClose(ctx context.Context, j Job, in HubCloseIntent) hubCloseOutcome {
	if e := a.releaseQuota(ctx, j); e != nil {
		a.warn("hub close lease release failed, retrying: %v", e)
		return hubCloseWait
	}
	if e := a.event(ctx, j.ID, "job.lost", map[string]any{"pane_id": j.Pane, "reason": hubCloseLostReason, "final": true, "request_id": in.RequestID, "end_reason": in.EndReason}); e != nil {
		a.warn("hub close job.lost not recorded, retrying: %v", e)
		return hubCloseWait
	}
	return hubCloseEnded
}
