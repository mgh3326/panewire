package fleet

import (
	"context"
	"os"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// quotaStopRoles are the seats whose idle pane on a quota or rate-limit screen
// the job watcher ends. Counsel panes are consults, not jobs a retry can
// replace. A worker or tester is not here: the quota-stop manager decides
// whether it waits for the reset or is handed over (hk 3334,
// docs/quota-stop.md), so the watcher leaves its job live.
var quotaStopRoles = []string{"reviewer"}

// QuotaStopHandOffGrace is how long a watcher leaves the hub to replace a job
// whose quota stop was announced (a worker or tester the quota-stop manager
// handed over, or a credit-stopped job) before it ends the job itself, without
// a replacement. The hub places the replacement, closes the pane and writes the
// hk record with replaced_by or handed_over_to (hk never rewrites a lost record,
// so the pointer can only be written once). Zero ends the job at once. It must
// outlast the hub's whole replacement (hub_quota_replace.go quotaReplaceBudget,
// a hard deadline) plus the relay to the hub, or the watcher writes a terminal
// record without the successor pointer while the hub is still placing it.
//
// A reviewer on a visible quota screen has no grace: its watcher ends the job
// at once (hk 4507) and mergewake respawns the review.
const QuotaStopHandOffGrace = 10 * time.Minute

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

// pendingCreditRejection recovers a recorded rejection until publication is
// acknowledged. The event contains only the pool and the fixed timestamp.
func pendingCreditRejection(j Job) (quotastop.Stop, bool) {
	var pending quotastop.Stop
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.credit_rejection":
			at, err := time.Parse(time.RFC3339Nano, str(ev.Payload, "at"))
			if err == nil {
				pending = quotastop.Stop{Pool: str(ev.Payload, "pool"), CreditExhaustedAt: at}
			}
		case "job.credit_published":
			if str(ev.Payload, "pool") == pending.Pool && str(ev.Payload, "at") == pending.CreditExhaustedAt.UTC().Format(time.RFC3339Nano) {
				pending = quotastop.Stop{}
			}
		}
	}
	return pending, pending.Pool != "" && !pending.CreditExhaustedAt.IsZero()
}

func creditPublicationPaused(j Job, stop quotastop.Stop) bool {
	paused := false
	for _, ev := range j.Events {
		if str(ev.Payload, "pool") != stop.Pool || str(ev.Payload, "at") != stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano) {
			continue
		}
		switch ev.Kind {
		case "job.credit_publication_paused":
			paused = true
		case "job.credit_published":
			paused = false
		}
	}
	return paused
}

func creditPublicationAccountReady(a *App, j Job, stop quotastop.Stop) bool {
	key := str(j.Receipt, "account_key")
	if key != "" {
		return validAccountKey(key) && accountPool(key) == stop.Pool
	}
	_, err := manualAccountKey(a, stop.Pool)
	return err == nil
}

func creditRejectionStateChangeSeq(j Job, stop quotastop.Stop) (*int64, bool) {
	want := stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano)
	var seq *int64
	found := false
	for _, ev := range j.Events {
		if ev.Kind != "job.credit_rejection" || str(ev.Payload, "pool") != stop.Pool || str(ev.Payload, "at") != want {
			continue
		}
		found = true
		seq = nil
		if value, ok := ev.Payload["state_change_seq"].(float64); ok {
			parsed := int64(value)
			seq = &parsed
		}
	}
	return seq, found
}

func safeCreditStopHandoff(j Job, stop quotastop.Stop, agent Agent, present, herdrReachable bool) bool {
	if !herdrReachable || !present || agent.Status != "idle" && agent.Status != "done" || agent.StateChangeSeq == nil {
		return false
	}
	expected, found := creditRejectionStateChangeSeq(j, stop)
	return found && expected != nil && *agent.StateChangeSeq == *expected
}

func creditRejectionPublished(j Job, stop quotastop.Stop) bool {
	for i := len(j.Events) - 1; i >= 0; i-- {
		ev := j.Events[i]
		if ev.Kind == "job.credit_published" && str(ev.Payload, "pool") == stop.Pool && str(ev.Payload, "at") == stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano) {
			return true
		}
	}
	return false
}

// creditStopped uses the session's structured provider error, not pane text.
func (a *App) creditStopped(j Job, ag Agent) (quotastop.Stop, bool) {
	if ag.Status != "idle" && ag.Status != "done" {
		return quotastop.Stop{}, false
	}
	if live := FindHarnessSession([]Agent{ag}, j.Pane, "omp"); live.Source != "unavailable" {
		receipt := make(map[string]any, len(j.Receipt)+1)
		for key, value := range j.Receipt {
			receipt[key] = value
		}
		receipt["harness_session"] = map[string]any{"kind": live.Kind, "id": live.ID, "source": live.Source}
		j.Receipt = receipt
	}
	if slices.Contains([]string{"worker", "tester", "reviewer", "counsel"}, j.QuotaRole()) {
		if stop, ok := ompCreditStop(a.Home, j); ok {
			return stop, true
		}
	}
	return quotastop.Stop{}, false
}

// quotaStopped also recognizes the existing visible quota status lines. A
// failed read is not a stop; neither path sends input to the pane.
func (a *App) quotaStopped(ctx context.Context, j Job, ag Agent, seen *quotaStopWatch) (quotastop.Stop, bool) {
	if stop, ok := a.creditStopped(j, ag); ok {
		return stop, true
	}
	if ag.Status != "idle" && ag.Status != "done" {
		return quotastop.Stop{}, false
	}
	if !slices.Contains(quotaStopRoles, j.QuotaRole()) {
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

// QuotaRole is the role a quota stop is handled by: the spawn receipt's
// (reviewer, tester, worker), else the claim's. The claim always says worker
// for a spawned job, so reading only it would treat every reviewer as a worker.
func (j Job) QuotaRole() string {
	if j.SpawnRole != "" {
		return j.SpawnRole
	}
	return j.Role
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

// expired reports whether the grace has run out with the stop still standing:
// the pane unchanged, or gone (the hub closed it, or it died).
func (h quotaHandOff) expired(now time.Time, ag Agent, present bool) bool {
	return !now.Before(h.until) && (!present || h.unchanged(ag))
}

func quotaStopPayload(j Job, stop quotastop.Stop) map[string]any {
	payload := map[string]any{"pane_id": j.Pane, "reason": "quota_stop", "pool": stop.Pool}
	if !stop.Reset.IsZero() {
		payload["reset_at"] = stop.Reset.UTC().Format(time.RFC3339)
	}
	if !stop.CreditExhaustedAt.IsZero() {
		payload["credit_exhausted_at"] = stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano)
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
	a := &App{Jobs: jobs, Home: home, Now: time.Now, Runner: ExecRunner{}}
	if e := a.quotaStopAnnounce(ctx, Job{ID: job, Pane: pane}, stop); e != nil {
		return e
	}
	// The job's own watcher may have ended already (a worker that waited past
	// its completion deadline). Its fallback is what ends the job and frees its
	// quota lease when the hub cannot replace it, so make sure one is running.
	if !a.watchActive(job) {
		if e := announceStartWatch(a, job); e != nil {
			return e
		}
	}
	return nil
}

// announceStartWatch starts the job's re-armed watcher; tests replace it.
var announceStartWatch = func(a *App, job string) error { return a.startWatchMode(job, true) }

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
		stop.CreditExhaustedAt, _ = time.Parse(time.RFC3339Nano, str(ev.Payload, "credit_exhausted_at"))
		return stop, stop.Pool != ""
	}
	return quotastop.Stop{}, false
}

// quotaStopAnnounce records job.lost for a quota stop without ending the hk
// record, so the hub sees the stop and can replace the job first. A devin
// free-tier limit is also written to this host's free-limit record first, so
// the next devin measurement publishes it and later selections skip the free
// rung until its reset (RecordFreeLimit).
func (a *App) quotaStopAnnounce(ctx context.Context, j Job, stop quotastop.Stop) error {
	if stop.Pool == "devin-free" {
		if e := RecordFreeLimit(devin.Config{Home: a.Home, Now: a.Now}, j.ID, stop); e != nil {
			a.warn("free-limit observation failed: %v", e)
		}
	}
	return a.event(ctx, j.ID, "job.lost", quotaStopPayload(j, stop))
}

// RecordFreeLimit writes a devin free-tier limit a quota-stopped pane reported
// into cfg's host free-limit record (internal/quota/devin), the same record
// `panewire quota observe devin-free-limit` writes. The node merges it into the
// devin account's next published reading (shared.Flow.LocalFree), and selection
// drops the free rung as free_limit until the reset. A zero stop.Reset uses the
// record's one-hour unknown-reset hold. A stop of another pool is a no-op.
func RecordFreeLimit(cfg devin.Config, job string, stop quotastop.Stop) error {
	if stop.Pool != "devin-free" {
		return nil
	}
	reset := ""
	if !stop.Reset.IsZero() {
		reset = stop.Reset.UTC().Format(time.RFC3339Nano)
	}
	return devin.ObserveFreeLimit(cfg, reset, "", "", "job:"+job, "", stop.Reset.IsZero(), false)
}

// quotaStopLost ends a job whose idle pane shows a quota stop and has no
// report. The contract mergewake reads (hk 3302): the hk record takes
// state=lost and end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"
// (reset_at left out when unknown), and job.lost carries reason="quota_stop",
// pool and reset_at. No result is written; hk takes one only with completed.
// emit is false when quotaStopAnnounce already recorded job.lost.
func (a *App) quotaStopLost(ctx context.Context, j Job, stop quotastop.Stop, emit bool) error {
	endReason := QuotaStopEndReason(stop.Pool, stop.Reset, "", "")
	// After a hand-off grace the hub may already have written the record lost
	// with the successor pointer; hk would refuse a rewrite, so leave it.
	if emit || !a.hkJobEnded(ctx, j.ID) {
		if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": endReason}); e != nil {
			a.jobRecordFailed(ctx, j.ID, "lost", e)
		}
	}
	if emit {
		if e := a.quotaStopAnnounce(ctx, j, stop); e != nil {
			return e
		}
	}
	return a.releaseQuota(ctx, j)
}

// hkJobEnded reports whether hk already holds a terminal record for the job. An
// unreadable record is not an end.
func (a *App) hkJobEnded(ctx context.Context, id string) bool {
	store, e := a.jobStore()
	if e != nil {
		return false
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		return false
	}
	state, _ := rec["state"].(string)
	return state == "completed" || state == "lost" || state == "escalated"
}
