package fleet

import (
	"context"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// quotaStopRoles are the seats whose idle pane on a quota or rate-limit screen
// ends the job. Counsel panes are consults, not jobs a retry can replace.
var quotaStopRoles = []string{"worker", "tester", "reviewer"}

// quotaStopWatch remembers the pane state the watcher last read, so an idle
// pane is read once per state change and not on every poll.
type quotaStopWatch struct {
	read bool
	seq  int64
}

// quotaStopped reports the quota stop an idle pane of j shows. It only reads
// the visible screen; it never sends input to the pane, so no "send a message
// to retry" automation exists here. A failed read is not a stop.
func (a *App) quotaStopped(ctx context.Context, j Job, ag Agent, seen *quotaStopWatch) (quotastop.Stop, bool) {
	if !slices.Contains(quotaStopRoles, j.Role) || (ag.Status != "idle" && ag.Status != "done") {
		return quotastop.Stop{}, false
	}
	if ag.StateChangeSeq != nil {
		if seen.read && seen.seq == *ag.StateChangeSeq {
			return quotastop.Stop{}, false
		}
		seen.read, seen.seq = true, *ag.StateChangeSeq
	}
	r := a.call(ctx, "herdr", "agent", "read", ag.Pane, "--source", "visible", "--lines", "60")
	if e := check(r, "pane screen"); e != nil {
		if ag.StateChangeSeq != nil {
			a.warn("quota-stop screen read unavailable: %v", e)
		}
		return quotastop.Stop{}, false
	}
	return quotastop.Classify(r.Out, a.Now(), time.Local)
}

// quotaStopLost ends a job whose idle pane shows a quota stop and has no
// report. The contract mergewake reads (hk 3302): the hk record takes
// state=lost and end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"
// (reset_at left out when unknown), and job.lost carries reason="quota_stop",
// pool and reset_at. No result is written; hk takes one only with completed.
func (a *App) quotaStopLost(ctx context.Context, j Job, stop quotastop.Stop) error {
	endReason := "quota_stop pool=" + stop.Pool
	payload := map[string]any{"pane_id": j.Pane, "reason": "quota_stop", "pool": stop.Pool}
	if !stop.Reset.IsZero() {
		reset := stop.Reset.UTC().Format(time.RFC3339)
		endReason += " reset_at=" + reset
		payload["reset_at"] = reset
	}
	if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": endReason}); e != nil {
		a.jobRecordFailed(ctx, j.ID, "lost", e)
	}
	if e := a.event(ctx, j.ID, "job.lost", payload); e != nil {
		return e
	}
	return a.releaseQuota(ctx, j)
}
