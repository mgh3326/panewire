package fleet

import (
	"context"
	"os"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// quotaStopRoles are the seats whose idle pane on a quota or rate-limit screen
// the job watcher ends. Counsel panes are consults, not jobs a retry can
// replace. A worker or tester is not here: the quota-stop manager decides
// whether it waits for the reset or is handed over (hk 3334,
// docs/quota-stop.md), so the watcher leaves its job live.
var quotaStopRoles = []string{"reviewer"}

// QuotaStopHandOffGrace is how long a reviewer's watcher leaves the hub to
// replace a quota-stopped job before it ends the job itself, without a
// replacement. The hub places the replacement, closes the pane and writes the
// hk record with replaced_by (hk never rewrites a lost record, so the pointer
// can only be written once). Zero ends the job at once. It must outlast the
// hub's whole replacement (hub_quota_replace.go quotaReplaceBudget), or the
// watcher writes a terminal record without the successor pointer while the hub
// is still placing it.
const QuotaStopHandOffGrace = 6 * time.Minute

// QuotaStopEndReason is the hk end_reason of a quota-stopped job:
// "quota_stop pool=<pool> reset_at=<RFC3339 UTC>", reset_at left out when the
// reset is unknown. A successor, when there is one, is the LAST key=value
// token: "replaced_by=<job>" for a reviewer or "handed_over_to=<job>" for a
// worker or tester (mergewake hk 3335 follows replaced_by).
func QuotaStopEndReason(pool string, reset time.Time, successorKey, successor string) string {
	reason := "quota_stop pool=" + pool
	if !reset.IsZero() {
		reason += " reset_at=" + reset.UTC().Format(time.RFC3339)
	}
	if successor != "" {
		reason += " " + successorKey + "=" + successor
	}
	return reason
}

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

// quotaHandOff is a reviewer watcher's wait for the hub to replace a
// quota-stopped job: the stop it announced, when it ends the job itself, and
// the pane state the stop was read in.
type quotaHandOff struct {
	stop   quotastop.Stop
	until  time.Time
	seq    *int64
	active bool
}

// unchanged reports whether the pane is still in the state the stop was read
// in. A pane that went back to work is not stopped any more.
func (h quotaHandOff) unchanged(ag Agent) bool {
	if ag.Status != "idle" && ag.Status != "done" {
		return false
	}
	return h.seq == nil || ag.StateChangeSeq == nil || *h.seq == *ag.StateChangeSeq
}

func (h quotaHandOff) expired(now time.Time, ag Agent) bool {
	return !now.Before(h.until) && h.unchanged(ag)
}

func quotaStopPayload(j Job, stop quotastop.Stop) map[string]any {
	payload := map[string]any{"pane_id": j.Pane, "reason": "quota_stop", "pool": stop.Pool}
	if !stop.Reset.IsZero() {
		payload["reset_at"] = stop.Reset.UTC().Format(time.RFC3339)
	}
	return payload
}

// AnnounceQuotaStop records job.lost reason "quota_stop" for job on the node's
// jobs root without ending its hk record: the quota-stop manager's hand-over of
// a worker or tester (hk 3334). The hub replaces the job and closes the pane;
// the job's own watcher ends it without a replacement when the hub does not
// (QuotaStopHandOffGrace).
func AnnounceQuotaStop(ctx context.Context, jobs, job, pane string, stop quotastop.Stop) error {
	home, _ := os.UserHomeDir()
	a := &App{Jobs: jobs, Home: home, Now: time.Now}
	return a.quotaStopAnnounce(ctx, Job{ID: job, Pane: pane}, stop)
}

// announcedQuotaStop returns the quota stop a job.lost of this attempt
// announced, if the job's newest end is one.
func announcedQuotaStop(j Job) (quotastop.Stop, bool) {
	if !j.Terminal || j.Ended != "job.lost" {
		return quotastop.Stop{}, false
	}
	for i := len(j.Events) - 1; i >= 0; i-- {
		ev := j.Events[i]
		if ev.Kind != "job.lost" {
			continue
		}
		if str(ev.Payload, "reason") != "quota_stop" {
			return quotastop.Stop{}, false
		}
		stop := quotastop.Stop{Pool: str(ev.Payload, "pool")}
		if reset, err := time.Parse(time.RFC3339, str(ev.Payload, "reset_at")); err == nil {
			stop.Reset = reset
		}
		return stop, stop.Pool != ""
	}
	return quotastop.Stop{}, false
}

// quotaStopAnnounce records job.lost for a quota stop without ending the hk
// record, so the hub sees the stop and can replace the job first.
func (a *App) quotaStopAnnounce(ctx context.Context, j Job, stop quotastop.Stop) error {
	return a.event(ctx, j.ID, "job.lost", quotaStopPayload(j, stop))
}

// quotaStopLost ends a job whose idle pane shows a quota stop and has no
// report. The contract mergewake reads (hk 3302): the hk record takes
// state=lost and end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"
// (reset_at left out when unknown), and job.lost carries reason="quota_stop",
// pool and reset_at. No result is written; hk takes one only with completed.
// emit is false when quotaStopAnnounce already recorded job.lost.
func (a *App) quotaStopLost(ctx context.Context, j Job, stop quotastop.Stop, emit bool) error {
	endReason := QuotaStopEndReason(stop.Pool, stop.Reset, "", "")
	if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": endReason}); e != nil {
		a.jobRecordFailed(ctx, j.ID, "lost", e)
	}
	if emit {
		if e := a.quotaStopAnnounce(ctx, j, stop); e != nil {
			return e
		}
	}
	return a.releaseQuota(ctx, j)
}
