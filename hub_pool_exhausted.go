package panewire

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// An exhausted pool triggers an owner notice for each matching live job (hk 4550; docs/quota-stop.md, "An exhausted pool").

// poolExhaustion is one reading of a pool used up for an account: what ran out
// and until when. The free bucket pays only for the free-tier rungs
// (fleet.IsFreeRung, the devin-swe2* profiles), an account window only for the
// paid rungs, so each exhaustion reaches only the jobs its bucket pays for.
// A pool-level cause (retired, credit_exhausted) has no account and no reset:
// it covers every job on the pool until the pool comes back.
type poolExhaustion struct {
	Pool, Account string
	// Cause is "free_limit" for the free bucket, "window:<name>" for an account
	// window, "retired" for a pool whose catalog rows are all retired, and
	// "credit_exhausted" for a pane showing the provider's credit rejection.
	Cause string
	Reset time.Time
}

// poolCauseRetired: every catalog row of the pool is retired (hk 4625).
const poolCauseRetired = "retired"

// poolCauseCreditExhausted: a pane of a job on the pool shows the provider's
// credit-exhausted rejection (hk 4625).
const poolCauseCreditExhausted = "credit_exhausted"

// fromQuota reports a cause read from a node's published quota value, which a
// newer reading of the same bucket can supersede. The pool triggers are read
// by the hub itself and are cleared by the pool coming back.
func (e poolExhaustion) fromQuota() bool {
	return e.Cause == poolCauseFreeLimit || strings.HasPrefix(e.Cause, "window:")
}

// poolWide reports a cause that speaks for the whole pool rather than one
// account: every job on the pool draws on it. A credit rejection is pool-wide
// only when it names no account; a multi-account pool's rejection marks one
// account (hk 5138).
func (e poolExhaustion) poolWide() bool {
	return e.Cause == poolCauseRetired || e.Cause == poolCauseCreditExhausted && e.Account == ""
}

// accountCredit reports a commandcode credit rejection scoped to one account
// (hk 5138). It is the only cause a reading of that account lifts.
func (e poolExhaustion) accountCredit() bool {
	return e.Cause == poolCauseCreditExhausted && e.Account != ""
}

// key identifies the pool event: the same exhaustion seen again has the same key.
func (e poolExhaustion) key() string {
	return e.Pool + "|" + e.Account + "|" + e.Cause + "|" + e.Reset.UTC().Format(time.RFC3339)
}

// expired reports whether a reset has passed. A pool-level cause has none.
func (e poolExhaustion) expired(now time.Time) bool {
	return !e.Reset.IsZero() && !now.Before(e.Reset)
}

// covers reports whether a job placed on profile draws on what ran out. A job
// with no recorded profile cannot be told apart, so it is left alone. A
// pool-level cause covers every profile.
func (e poolExhaustion) covers(profile string) bool {
	if e.poolWide() || e.accountCredit() {
		return true
	}
	if profile == "" {
		return false
	}
	return fleet.IsFreeRung(profile) == (e.Cause == poolCauseFreeLimit)
}

// poolFreeLimitWaitableWithin mirrors policy/quota-stop max_wait (hk 4503,
// currently 1h): a free-limit reset this close is waited out on the pane
// (hk 569), so the hub sends no hand-over instruction for it. A reset further
// away still notifies, since the pane hands over instead of waiting.
const poolFreeLimitWaitableWithin = time.Hour

const poolCauseFreeLimit = "free_limit"

// poolExhaustionsOf reads a quota publish for what is used up and still holding
// at now: at most one free-limit exhaustion (the free bucket exhausted until a
// reset ahead) and at most one paid-window exhaustion (an account window, a
// bucket of scope account or none and no model, at 100 % with its reset ahead;
// with several, the one that clears last). A bucket of any other scope (a model
// group, a model) leaves the account's other models usable, and one with no
// readable reset cannot be said to hold.
func poolExhaustionsOf(quota *FleetStateQuota, now time.Time) []poolExhaustion {
	if quota == nil {
		return nil
	}
	var found []poolExhaustion
	if quota.Free != nil && quota.Free.State == shared.FreeExhausted && quota.Free.ResetsAt != nil && quota.Free.ResetsAt.After(now) {
		found = append(found, poolExhaustion{Pool: quota.Pool, Account: quota.AccountKey, Cause: poolCauseFreeLimit, Reset: quota.Free.ResetsAt.UTC()})
	}
	var window poolExhaustion
	for _, bucket := range quota.Buckets {
		if bucket.UsedPct == nil || *bucket.UsedPct < 100 || !accountWideBucket(bucket) || !fleet.WindowRead(quota.Pool, bucket.Window) {
			continue
		}
		if reset, err := time.Parse(time.RFC3339, bucket.ResetsAt); err == nil && reset.After(now) && reset.After(window.Reset) {
			window = poolExhaustion{Pool: quota.Pool, Account: quota.AccountKey, Cause: "window:" + bucket.Window, Reset: reset.UTC()}
		}
	}
	if window.Cause != "" {
		found = append(found, window)
	}
	return found
}

// accountWideBucket reports whether a window speaks for the whole account: scope
// account or none, and no model.
func accountWideBucket(bucket FleetStateBucket) bool {
	return bucket.Model == "" && (bucket.Scope == "" || bucket.Scope == "account")
}

// speaksTo reports whether a reading says anything about what e is about: a free
// bucket for a free limit, the account window of that name for a window. A
// free-only reading carries no paid bucket and a paid one no free bucket, so a
// reading that does not speak to the cause neither confirms nor clears it.
func (e poolExhaustion) speaksTo(quota *FleetStateQuota) bool {
	if e.Cause == poolCauseFreeLimit {
		return quota.Free != nil
	}
	for _, bucket := range quota.Buckets {
		if "window:"+bucket.Window == e.Cause && accountWideBucket(bucket) {
			return true
		}
	}
	return false
}

// exhaustionSuperseded reports whether the freshest reading of the account that
// speaks to e's cause, across every publishing host, no longer shows it. The
// state store keeps one value per host, so a delayed or cached exhausted value
// can be recorded after another host has published the account as available
// while its old reset is still ahead; that is not an exhaustion to act on. A
// tie in time goes to the exhausted reading.
func (h *HubServer) exhaustionSuperseded(e poolExhaustion, now time.Time) bool {
	store := h.stateStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	var freshest time.Time
	var holds, found bool
	for key, message := range store.latest {
		if key.kind != fleetStateKindQuota || key.key != e.Account || message.Quota == nil || !e.speaksTo(message.Quota) {
			continue
		}
		at := fleetStateObservedAt(message)
		still := slices.ContainsFunc(poolExhaustionsOf(message.Quota, now), func(current poolExhaustion) bool { return current.Cause == e.Cause })
		if !found || at.After(freshest) || at.Equal(freshest) && still {
			freshest, holds, found = at, still, true
		}
	}
	return found && !holds
}

// poolSweepState is what keeps two passes from working the same job at once.
// jobs holds each job a pass is acting on or has notified. The lane event's
// fixed producer identity gives notifications durable deduplication.
// events remembers the pool events seen, so a repeat is logged quietly.
type poolSweepState struct {
	mu     sync.Mutex
	events map[string]time.Time
	jobs   map[string]struct{}
}

// seen records the pool event and reports whether it is new. An event whose
// reset has passed is forgotten; a pool-level event has no reset and is held
// until forget clears it.
func (s *poolSweepState) seen(key string, reset, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for known, until := range s.events {
		if !until.IsZero() && !until.After(now) {
			delete(s.events, known)
		}
	}
	if s.events == nil {
		s.events = map[string]time.Time{}
	}
	_, known := s.events[key]
	s.events[key] = reset
	return !known
}

// held reports whether key is currently held.
func (s *poolSweepState) held(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.events[key]
	return ok
}

// forget drops a held pool event, so the next observation of it fires again.
func (s *poolSweepState) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.events, key)
}

// claim reserves job for one pass; false means a pass already holds or finished it.
func (s *poolSweepState) claim(job string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.jobs[job]; held {
		return false
	}
	if s.jobs == nil {
		s.jobs = map[string]struct{}{}
	}
	s.jobs[job] = struct{}{}
	return true
}

// release frees a job a pass did not act on, so the next pass tries it again.
func (s *poolSweepState) release(job string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, job)
}

const (
	poolJobNotified = "notified"
	poolJobQueued   = "queued"
	poolJobSkipped  = "skipped"
)

// poolJobResult is what a pass did for one job.
type poolJobResult struct {
	action string
	role   string
	why    string
}

func (r poolJobResult) acted() bool {
	return r.action == poolJobNotified || r.action == poolJobQueued
}

func poolJobSkip(role, why string) poolJobResult {
	return poolJobResult{action: poolJobSkipped, role: role, why: why}
}

// observePoolExhaustion is the trigger: a node published a quota value the hub
// recorded. It never blocks the publish; the pass runs on its own goroutine.
func (h *HubServer) observePoolExhaustion(quota *FleetStateQuota, now time.Time) {
	for _, exhaustion := range poolExhaustionsOf(quota, now) {
		h.startPoolSweep(exhaustion)
	}
}

// startPoolSweep runs one exhausted-pool pass on its own goroutine, so neither
// a publish nor the trigger tick waits for the sweep.
func (h *HubServer) startPoolSweep(exhaustion poolExhaustion) {
	if h.inbox == nil {
		return
	}
	h.closeMu.Lock()
	if h.closed {
		h.closeMu.Unlock()
		return
	}
	h.spawnWorkers.Add(1)
	h.closeMu.Unlock()
	go func() {
		defer h.spawnWorkers.Done()
		h.sweepExhaustedPool(h.closeCtx, exhaustion)
	}()
}

// poolEventGone reports whether an event has ended: its reset passed, a newer
// quota reading cleared it, or an account's credit mark lifted. A pool-level
// cause is cleared only by the pool coming back (poolSweep.forget).
func (h *HubServer) poolEventGone(exhaustion poolExhaustion, now time.Time) bool {
	if exhaustion.accountCredit() && !h.accountCreditMarkHeld(exhaustion.Account, now) {
		return true
	}
	return exhaustion.expired(now) || exhaustion.fromQuota() && h.exhaustionSuperseded(exhaustion, now)
}

// sweepExhaustedPool acts on every live job placed on the exhausted pool and
// account, all at once: each owner's notice is recorded independently. A
// pool-level cause (Account empty) acts on every live job on the pool.
func (h *HubServer) sweepExhaustedPool(ctx context.Context, exhaustion poolExhaustion) {
	h.loopTick(loopPoolExhaustedSweep)
	if h.poolEventGone(exhaustion, h.now()) {
		h.logger.Debug("pool exhausted sweep superseded by a newer reading", "pool", exhaustion.Pool, "account", exhaustion.Account, "cause", exhaustion.Cause)
		return
	}
	if exhaustion.Cause == poolCauseFreeLimit {
		// A free-limit stop holds the free band until its reset: cool it here so
		// the next selection skips the free rows even before the shared free
		// reading reaches every host (hk 4765). The sweep below still runs, so
		// a reset past the pane's wait horizon notifies its owners.
		h.observePoolCooling("quota_stop pool=devin-free reset_at="+exhaustion.Reset.Format(time.RFC3339Nano), h.now())
	}
	isNew := h.poolSweep.seen(exhaustion.key(), exhaustion.Reset, h.now())
	onPool, err := h.poolExhaustionJobs(ctx, exhaustion)
	if err != nil {
		h.loopNoteError(loopPoolExhaustedSweep)
		h.logger.Warn("pool exhausted sweep could not list its jobs", "pool", exhaustion.Pool, "account", exhaustion.Account, "error", err)
		return
	}
	for _, task := range onPool {
		h.loopNoteMade(loopPoolExhaustedSweep, task.Job)
	}
	results := make([]poolJobResult, len(onPool))
	var wg sync.WaitGroup
	for i, task := range onPool {
		if !h.poolSweep.claim(task.Job) {
			results[i] = poolJobSkip("", "in_progress_or_done")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = h.sweepPoolJob(ctx, exhaustion, task)
			if results[i].acted() {
				h.logger.Info("pool exhausted job", "job", task.Job, "role", results[i].role, "pool", exhaustion.Pool, "account", exhaustion.Account, "cause", exhaustion.Cause,
					"action", results[i].action, "reason", results[i].why)
				// A legacy node receives the existing hand-over notice. Revisit
				// its job when close operations are later enabled.
				// A waited stop is released too: its wait row answers a repeat,
				// and a later stop with another reset must reach the job (hk 4633).
				if poolClaimReleased(results[i]) {
					h.poolSweep.release(task.Job)
				}
			} else {
				h.poolSweep.release(task.Job)
			}
		}()
	}
	wg.Wait()

	notified, skipped := 0, map[string]int{}
	for i, result := range results {
		switch result.action {
		case poolJobNotified, poolJobQueued:
			notified++
			h.loopNoteDone(loopPoolExhaustedSweep, onPool[i].Job)
		default:
			skipped[result.why]++
		}
	}
	level := h.logger.Debug
	if isNew || notified > 0 {
		level = h.logger.Info
	}
	level("pool exhausted sweep", "pool", exhaustion.Pool, "account", exhaustion.Account, "cause", exhaustion.Cause, "reset", exhaustion.Reset.Format(time.RFC3339),
		"jobs", len(onPool), "notified", notified, "skipped", skippedSummary(skipped))
}

// poolClaimReleased reports a notified result whose job claim the sweep
// releases: a reviewer or consult notice, and a waited implementer stop (its
// wait row answers a repeat, and a later stop with another reset must reach
// the job).
func poolClaimReleased(r poolJobResult) bool {
	return r.action == poolJobNotified && (r.role == "reviewer" || r.role == "consult" || poolWaited(r.why))
}

// skippedSummary renders the skip reasons as "reason=count,..." in a stable order.
func skippedSummary(skipped map[string]int) string {
	parts := make([]string, 0, len(skipped))
	for why, count := range skipped {
		parts = append(parts, fmt.Sprintf("%s=%d", why, count))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// poolExhaustionText is the one owner notice for a job on an exhausted pool.
// Every notified cause asks the same act: hand the job over or end it and respawn it on
// another pool; the cause only says what ran out. A free-limit reset inside the
// pane's wait horizon never reaches this text (waitable_reset, above).
func poolExhaustionText(exhaustion poolExhaustion, job string) string {
	switch exhaustion.Cause {
	case poolCauseRetired:
		return fmt.Sprintf("Pool %s has no live catalog row left for job %s; hand it over or end it and respawn it on another pool.", exhaustion.Pool, job)
	case poolCauseCreditExhausted:
		if exhaustion.Account != "" {
			// The pool's other accounts still spend their credits (hk 5138).
			return fmt.Sprintf("Account %s has no provider credit left for job %s; hand it over or end it and respawn it on another account.", exhaustion.Account, job)
		}
		return fmt.Sprintf("Pool %s has no provider credit left for job %s; hand it over or end it and respawn it on another pool.", exhaustion.Pool, job)
	}
	return fmt.Sprintf("Pool %s is exhausted for job %s; hand it over or end it and respawn it on another pool.", exhaustion.Pool, job)
}

// sweepPoolJob records one notice asking the owner to act on a live job.
func (h *HubServer) sweepPoolJob(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask) poolJobResult {
	prior, err := fleet.ValidateSubmission(task.Request)
	if err != nil {
		return poolJobSkip("", "unreadable_request")
	}
	role := prior.Role
	h.mu.Lock()
	registered := h.jobs[task.Job]
	completed := registered != nil && registered.Completed
	h.mu.Unlock()
	if completed {
		return poolJobSkip(role, "completed")
	}
	if h.poolEventGone(exhaustion, h.now()) {
		return poolJobSkip(role, "superseded")
	}
	// A capable implementer is decided by the policy itself (its max_wait),
	// not by the legacy one-hour shortcut below (hk 4633).
	capableImplementer := role == "implementer" && h.nodeHasCapability(spawnHolderHost(task), hubCapabilityCloseOps) && h.nodeHasCapability(spawnHolderHost(task), hubCapabilityHandoverCheck)
	if !capableImplementer && exhaustion.Cause == poolCauseFreeLimit && !exhaustion.Reset.After(h.now().Add(poolFreeLimitWaitableWithin)) {
		// The pane waits out a reset inside max_wait and retries on it (hk 569):
		// no hand-over instruction leaves the hub (hk 4765).
		return poolJobSkip(role, "waitable_reset")
	}
	switch role {
	case "implementer", "reviewer", "consult":
		app, err := h.newSpawnApp(io.Discard, io.Discard)
		if err != nil {
			return poolJobSkip(role, "spawn_config_unavailable")
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, err := app.ExistingJob(readCtx, task.Job)
		cancel()
		state, _ := record["state"].(string)
		if err != nil || !exists || (state != "claimed" && state != "spawned") {
			return poolJobSkip(role, "job_record")
		}
		if pool, _ := record["pool"].(string); pool != "" && pool != exhaustion.Pool {
			return poolJobSkip(role, "pool_mismatch")
		}
		if h.poolEventGone(exhaustion, h.now()) {
			return poolJobSkip(role, "superseded")
		}
		if role == "implementer" && h.nodeHasCapability(spawnHolderHost(task), hubCapabilityCloseOps) && h.nodeHasCapability(spawnHolderHost(task), hubCapabilityHandoverCheck) {
			// The hub hands the implementer over itself (hk 4633).
			return h.sweepImplementer(ctx, exhaustion, task, prior, app, record)
		}
		if role == "reviewer" || role == "consult" {
			if h.nodeHasCapability(spawnHolderHost(task), hubCapabilityCloseOps) {
				if err := h.createPoolAction(ctx, exhaustion, task, app, nil); err != nil {
					return poolJobSkip(role, "close_not_queued")
				}
				return poolJobResult{action: poolJobQueued, role: role}
			}
			// Do not publish a hand-over notice in place of an existing action's
			// held notice if the node temporarily withdraws its capability.
			if exists, err := h.inbox.store.hasPoolAction(ctx, task.Job); err != nil || exists {
				return poolJobSkip(role, "close_action_exists")
			}
		}
		owner := task.Owner
		if strings.HasPrefix(task.Job, "mwr-") {
			owner = "mergewake"
		}
		text := poolExhaustionText(exhaustion, task.Job)
		eventID := "pool-exhausted:" + task.Job
		result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(owner, eventID), OwnerLane: owner, NotifyType: "info", Text: text, Host: "hub", Epoch: 1, Notice: ownerNoticeFor(task.Job, "pool_exhausted", false)}, nil)
		if result.ID == 0 || result.PersistFailed {
			return poolJobSkip(role, "owner_not_notified")
		}
		if role == "implementer" && exhaustion.accountCredit() {
			// A commandcode account out of credit is handed over exactly as a
			// quota stop is: the quota-stop flow's successor (hk 5138). When the
			// replacement does not place (pane busy, claim refused, …) the job
			// is released so the next pass retries while the mark holds, rather
			// than staying claimed on the spent account. The owner notice above
			// is not repeated: its event id is fixed per job.
			if !h.handOffAccountCreditStopped(ctx, task, exhaustion) {
				return poolJobSkip(role, "handover_failed")
			}
		}
		return poolJobResult{action: poolJobNotified, role: role}
	default:
		return poolJobSkip(role, "role")
	}
}

// poolExhaustionJobs filters the active operations by recorded placement. A
// pool-level cause matches every account on the pool; an account event only
// its own account.
func (h *HubServer) poolExhaustionJobs(ctx context.Context, exhaustion poolExhaustion) ([]hubSpawnTask, error) {
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		return nil, err
	}
	var onPool []hubSpawnTask
	listed := map[string]bool{}
	for _, task := range tasks {
		if task.Status != "succeeded" || task.Outcome.Pool != exhaustion.Pool || !exhaustion.covers(task.Outcome.Profile) || listed[task.Job] {
			continue
		}
		if exhaustion.Account != "" && task.Outcome.AccountKey != exhaustion.Account {
			continue
		}
		listed[task.Job] = true
		onPool = append(onPool, task)
	}
	return onPool, nil
}
