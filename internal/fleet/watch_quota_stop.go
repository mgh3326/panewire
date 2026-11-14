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
// replace. An implementer is not here: the quota-stop manager decides
// whether it waits for the reset or is handed over (hk 3334,
// docs/quota-stop.md), so the watcher leaves its job live.
var quotaStopRoles = []string{"reviewer"}

// QuotaStopHandOffGrace is how long a watcher leaves the hub to replace a job
// whose quota stop was announced (an implementer the quota-stop manager
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
// implementer (mergewake hk 3335 follows replaced_by).
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

// safeCreditStopHandoff reports whether a credit-rejected job may be handed off:
// herdr is reachable, the pane is still idle or done, and it is the state the
// rejection was read in. A missing state_change_seq on either side cannot prove
// the pane unchanged, so the idle or done check stands alone then, as
// quotaHandOff.unchanged does (hk 4505).
func safeCreditStopHandoff(j Job, stop quotastop.Stop, agent Agent, present, herdrReachable bool) bool {
	if !herdrReachable || !present || agent.Status != "idle" && agent.Status != "done" {
		return false
	}
	expected, found := creditRejectionStateChangeSeq(j, stop)
	if !found {
		return false
	}
	return expected == nil || agent.StateChangeSeq == nil || *agent.StateChangeSeq == *expected
}

// paneAfterPublication reads the pane's agent row now, for a decision taken
// after a credit rejection was published. present is false when the pane is
// gone; reachable is false when herdr could not be read.
func (a *App) paneAfterPublication(ctx context.Context, pane string) (agent Agent, present, reachable bool) {
	agents, err := a.agents(ctx)
	if err != nil {
		return Agent{}, false, false
	}
	for _, ag := range agents {
		if ag.Pane == pane {
			return ag, true, true
		}
	}
	return Agent{}, false, true
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
	if slices.Contains(spawnRoles, j.QuotaRole()) {
		if stop, ok := ompCreditStop(a.Home, j); ok {
			return stop, true
		}
	}
	return quotastop.Stop{}, false
}

// quotaSeen is what one look at a pane found.
type quotaSeen int

const (
	// quotaNotStopped: the look succeeded and the pane is not on a quota stop
	// (or is not a pane this path watches).
	quotaNotStopped quotaSeen = iota
	// quotaStopFound: the pane shows a quota stop.
	quotaStopFound
	// quotaScreenUnavailable: the screen could not be read. It proves nothing
	// either way, so a pending close keeps waiting instead of dropping.
	quotaScreenUnavailable
)

// quotaStopped also recognizes the existing visible quota status lines. A
// failed read is not a stop; neither path sends input to the pane.
func (a *App) quotaStopped(ctx context.Context, j Job, ag Agent, seen *quotaStopWatch) (quotastop.Stop, bool) {
	stop, found := a.quotaStopScreen(ctx, j, ag, seen)
	return stop, found == quotaStopFound
}

// quotaStopScreen looks at the pane once. seen, when not nil, skips a pane
// whose state_change_seq was already read successfully; a failed read is not
// remembered, so it is tried again.
func (a *App) quotaStopScreen(ctx context.Context, j Job, ag Agent, seen *quotaStopWatch) (quotastop.Stop, quotaSeen) {
	if stop, ok := a.creditStopped(j, ag); ok {
		return stop, quotaStopFound
	}
	if ag.Status != "idle" && ag.Status != "done" {
		return quotastop.Stop{}, quotaNotStopped
	}
	if !slices.Contains(quotaStopRoles, j.QuotaRole()) {
		return quotastop.Stop{}, quotaNotStopped
	}
	if seen != nil && ag.StateChangeSeq != nil && seen.read && seen.seq == *ag.StateChangeSeq {
		return quotastop.Stop{}, quotaNotStopped
	}
	r := a.call(ctx, "herdr", "agent", "read", ag.Pane, "--source", "visible", "--lines", "60")
	if e := check(r, "pane screen"); e != nil {
		a.warn("quota-stop screen read unavailable: %v", e)
		return quotastop.Stop{}, quotaScreenUnavailable
	}
	if seen != nil && ag.StateChangeSeq != nil {
		seen.read, seen.seq = true, *ag.StateChangeSeq
	}
	if stop, ok := quotastop.ClassifyAgent(r.Out, ag.Kind, a.Now(), time.Local); ok {
		return QuotaStopPool(stop, j.Profile), quotaStopFound
	}
	return quotastop.Stop{}, quotaNotStopped
}

// QuotaRole is the role a quota stop is handled by: the spawn receipt's
// (reviewer, implementer, consult), else the claim's. The claim always says implementer
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
// an implementer (hk 3334). The hub replaces the job and closes the pane;
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
		if str(ev.Payload, "reason") != "quota_stop" || boolean(ev.Payload, "final", false) {
			// A final stop is a reviewer whose hk record the watcher already
			// wrote lost: there is nothing left to hand over or to re-arm.
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
// free-tier limit is also written to this host's free-limit record and
// published at once, so later selections skip the free rung until its reset on
// every host (RecordFreeLimit, publishFreeLimit).
func (a *App) quotaStopAnnounce(ctx context.Context, j Job, stop quotastop.Stop) error {
	return a.quotaStopEvent(ctx, j, stop, false)
}

// quotaStopEvent records job.lost for a quota stop. final marks a stop whose hk
// lost record is already written (a reviewer ended by its watcher): unlike an
// announcement it hands nothing over, so it is never adopted as a hand-off
// grace by a watcher or a re-arm, which would reprompt a resumed pane.
func (a *App) quotaStopEvent(ctx context.Context, j Job, stop quotastop.Stop, final bool) error {
	if stop.Pool == "devin-free" {
		if e := RecordFreeLimit(devin.Config{Home: a.Home, Now: a.Now}, j.ID, stop); e != nil {
			a.warn("free-limit observation failed: %v", e)
		} else {
			a.publishFreeLimit(ctx, j.ID)
		}
	}
	payload := quotaStopPayload(j, stop)
	if final {
		payload["final"] = true
	}
	return a.event(ctx, j.ID, "job.lost", payload)
}

// freeLimitPublishTimeout bounds publishing a recorded free limit into the
// shared store. The refresh is a store read, a publication and a release
// unless the paid reading needs a measurement, so it normally ends in well
// under a second; a slow measurement must not hold the stop.
const freeLimitPublishTimeout = 15 * time.Second

// publishFreeLimit publishes the free limit this host just recorded into the
// devin account's shared reading, so every host's selection drops the free
// rungs from the stop itself. Waiting for the next devin measurement is not
// enough: a host with a fresh reading never runs one, and a host without the
// record never publishes it, so the account keeps admitting free rungs that
// stop on the same limit as soon as they start. A failure only logs: the
// record is durable, and the next measurement publishes it.
func (a *App) publishFreeLimit(ctx context.Context, job string) {
	if !a.nativeQuotaPool("devin") {
		return
	}
	if e := checkQuotaPlain(runQuotaCommand(ctx, a.Runner, refreshCommand("devin", "", true), "quota refresh", freeLimitPublishTimeout), "quota refresh"); e != nil {
		a.warn("free-limit publish for job %s: %v", job, e)
	}
}

// RecordFreeLimit writes a devin free-tier limit a quota-stopped pane reported
// into cfg's host free-limit record (internal/quota/devin), the same record
// `panewire quota observe devin-free-limit` writes. The node merges it into the
// devin account's published reading (shared.Flow.LocalFree); quotaStopEvent also
// publishes it at once (publishFreeLimit), and selection drops the free rungs
// as free_limit until the reset. A zero stop.Reset uses the record's one-hour
// unknown-reset hold. A stop of another pool is a no-op.
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
	// After a hand-off grace the hub may already have written the record lost
	// with the successor pointer; hk would refuse a rewrite, so leave it.
	if emit || !a.hkJobEnded(ctx, j.ID) {
		if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": QuotaStopEndReason(stop.Pool, stop.Reset, "", ""), "at": a.Now().UTC()}); e != nil {
			a.jobRecordFailed(ctx, j.ID, "lost", e)
		}
	}
	return a.quotaStopFinish(ctx, j, stop, emit)
}

// quotaStopFinish announces job.lost (when emit) and releases the quota lease.
func (a *App) quotaStopFinish(ctx context.Context, j Job, stop quotastop.Stop, emit bool) error {
	if emit {
		if e := a.quotaStopAnnounce(ctx, j, stop); e != nil {
			return e
		}
	}
	return a.releaseQuota(ctx, j)
}

// A reviewer's quota stop is ended through a durable close intent. The intent
// is recorded before the first hk write, with the stop and the exact end_reason
// of that attempt, and is reconciled until the job is finished: the end_reason
// is frozen because a fresh parse of a relative reset ("reset in 5 minutes")
// differs on every poll, and a restart between the hk write and the local
// finish finds the intent and finishes the job instead of losing the lease.
const (
	quotaIntentKind       = "job.quota_stop_intent"
	quotaIntentCancelKind = "job.quota_stop_cancelled"
)

// quotaIntent is a recorded close intent.
type quotaIntent struct {
	stop   quotastop.Stop
	reason string
	pane   string
}

// pendingQuotaIntent returns the job's open close intent: the newest intent not
// followed by a cancellation, an end, a re-prompt or a new claim or spawn.
func pendingQuotaIntent(j Job) (quotaIntent, bool) {
	var cur quotaIntent
	open := false
	for _, ev := range j.Events {
		switch ev.Kind {
		case quotaIntentKind:
			cur = quotaIntent{stop: quotastop.Stop{Pool: str(ev.Payload, "pool")}, reason: str(ev.Payload, "end_reason"), pane: str(ev.Payload, "pane_id")}
			if reset, err := time.Parse(time.RFC3339, str(ev.Payload, "reset_at")); err == nil {
				cur.stop.Reset = reset
			}
			open = cur.stop.Pool != "" && cur.reason != ""
		case quotaIntentCancelKind, "job.reprompted", "job.spawned", "job.claim", "job.claimed", "job.reclaim",
			"job.completed", "job.joined", "job.revoked", "job.handed_off", "job.lost":
			open = false
		}
	}
	return cur, open
}

func (a *App) recordQuotaIntent(ctx context.Context, j Job, stop quotastop.Stop) error {
	payload := map[string]any{"pane_id": j.Pane, "pool": stop.Pool, "end_reason": QuotaStopEndReason(stop.Pool, stop.Reset, "", "")}
	if !stop.Reset.IsZero() {
		payload["reset_at"] = stop.Reset.UTC().Format(time.RFC3339)
	}
	return a.event(ctx, j.ID, quotaIntentKind, payload)
}

// quotaIntentOutcome is what one reconciliation of a close intent did.
type quotaIntentOutcome int

const (
	// quotaIntentRetry: nothing was proven; the intent stays open.
	quotaIntentRetry quotaIntentOutcome = iota
	// quotaIntentEnded: the job was finished, or another terminal outcome owns it.
	quotaIntentEnded
	// quotaIntentCancelled: fresh evidence shows the pane moved on.
	quotaIntentCancelled
)

// finishQuotaIntent finishes a job whose hk lost record is known to be written:
// the lease is released first, then the final job.lost closes the intent. The
// order is the recovery: a crash or a failure between the two leaves the intent
// open, so the next poll (or a re-armed watcher) repeats both idempotent steps,
// and the closing event is only written once the lease is free.
func (a *App) finishQuotaIntent(ctx context.Context, j Job, stop quotastop.Stop) quotaIntentOutcome {
	if e := a.releaseQuota(ctx, j); e != nil {
		a.warn("quota-stop lease release failed, retrying: %v", e)
		return quotaIntentRetry
	}
	if e := a.quotaStopEvent(ctx, j, stop, true); e != nil {
		a.warn("quota-stop job.lost not recorded, retrying: %v", e)
		return quotaIntentRetry
	}
	return quotaIntentEnded
}

// reconcileQuotaIntent drives an open close intent one step. hk decides first:
// exactly the intended lost record finishes the job even if the screen can no
// longer be read (the write already happened); another terminal record ends the
// watch without announcing or releasing anything; an unreadable record keeps
// the intent. Only a live hk record needs another write, and that needs fresh
// positive evidence: a readable screen that still shows a stop. An unreadable
// screen or herdr never cancels the intent.
func (a *App) reconcileQuotaIntent(ctx context.Context, j Job, in quotaIntent, herdrOK, present, reportPending bool, ag Agent) (quotaIntentOutcome, error) {
	cancel := func(reason string) (quotaIntentOutcome, error) {
		return quotaIntentCancelled, a.event(ctx, j.ID, quotaIntentCancelKind, map[string]any{"reason": reason})
	}
	state, reason, ok := a.hkJobState(ctx, j.ID)
	if !ok {
		return quotaIntentRetry, nil
	}
	switch {
	case state == "lost" && reason == in.reason:
		return a.finishQuotaIntent(ctx, j, in.stop), nil
	case state == "completed" || state == "lost" || state == "escalated":
		a.warn("quota-stop lost record not written: hk holds another terminal record (%s)", state)
		_, e := cancel("superseded")
		return quotaIntentEnded, e
	}
	if reportPending {
		return cancel("report_written")
	}
	if !herdrOK {
		return quotaIntentRetry, nil
	}
	if !present {
		return cancel("pane_gone")
	}
	stop, seen := a.quotaStopScreen(ctx, j, ag, nil)
	switch {
	case seen == quotaScreenUnavailable:
		return quotaIntentRetry, nil
	case seen != quotaStopFound || !stop.CreditExhaustedAt.IsZero():
		return cancel("pane_moved_on")
	}
	if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": in.reason, "at": a.Now().UTC()}); e != nil {
		a.warn("quota-stop lost record not written, retrying: %v", e)
		return quotaIntentRetry, nil
	}
	return a.finishQuotaIntent(ctx, j, in.stop), nil
}

// hkJobState reads the job's hk record state and end_reason. ok is false when
// the record cannot be read.
func (a *App) hkJobState(ctx context.Context, id string) (state, endReason string, ok bool) {
	store, e := a.jobStore()
	if e != nil {
		return "", "", false
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		return "", "", false
	}
	state, _ = rec["state"].(string)
	endReason, _ = rec["end_reason"].(string)
	return state, endReason, true
}

// hkJobEnded reports whether hk already holds a terminal record for the job. An
// unreadable record is not an end.
func (a *App) hkJobEnded(ctx context.Context, id string) bool {
	state, _, ok := a.hkJobState(ctx, id)
	return ok && (state == "completed" || state == "lost" || state == "escalated")
}
