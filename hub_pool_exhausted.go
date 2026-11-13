package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// An exhausted pool ends or hands over every live job on it at once (hk 4550,
// docs/quota-stop.md, "An exhausted pool"). A pane's own quota screen is
// observed only after the pane goes idle, so the jobs on a pool that a
// published reading already shows used up would sit on dead panes for hours.
// The trigger is that reading: a node's quota publish (publishFleetStateFromNode)
// whose free bucket is exhausted, or one of whose account windows is used up,
// with its reset still ahead. The hub then lists the jobs whose placement
// record (hubSpawnOutcome.Pool and AccountKey) names that pool and account and
// acts on each, role by role, through the quota-stop mechanics:
//
//	worker, tester     handed over: the record ends with handed_over_to, the
//	                   successor takes the same worktree and branch with -m auto
//	                   and the pool excluded.
//	reviewer, counsel  ended: the pane is closed, the record ends with
//	                   end_reason "quota_stop pool=<p> reset_at=<t>", the role is
//	                   released, and the owner gets job.lost reason quota_stop
//	                   with the pool and reset, so it respawns the review.
//
// Each job's decision and its progress are a durable row (hub_pool_action.go),
// written before the pane is touched: an ending interrupted by a failed step, a
// lost heartbeat or a hub restart is finished from that row rather than from
// another quota publish.

// poolExhaustion is one reading of a pool used up for an account: what ran out
// and until when. The free bucket pays only for the free-tier rungs
// (fleet.IsFreeRung, the devin-swe2* profiles), an account window only for the
// paid rungs, so each exhaustion reaches only the jobs its bucket pays for.
type poolExhaustion struct {
	Pool, Account string
	// Cause is "free_limit" for the free bucket, "window:<name>" for an account window.
	Cause string
	Reset time.Time
}

// key identifies the pool event: the same exhaustion seen again has the same key.
func (e poolExhaustion) key() string {
	return e.Account + "|" + e.Cause + "|" + e.Reset.UTC().Format(time.RFC3339)
}

// covers reports whether a job placed on profile draws on what ran out. A job
// with no recorded profile cannot be told apart, so it is left alone.
func (e poolExhaustion) covers(profile string) bool {
	if profile == "" {
		return false
	}
	return fleet.IsFreeRung(profile) == (e.Cause == poolCauseFreeLimit)
}

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
		if bucket.UsedPct == nil || *bucket.UsedPct < 100 || !accountWideBucket(bucket) {
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
// jobs holds each job a pass is acting on, so a pass that starts while another
// still drives it leaves it alone; the action row is the durable half (a job a
// pass has finished has a done row, and an unfinished one is resumed from it).
// events remembers the pool events seen, so a repeat is logged quietly.
type poolSweepState struct {
	mu     sync.Mutex
	events map[string]time.Time
	jobs   map[string]struct{}
}

// seen records the pool event and reports whether it is new. An event whose
// reset has passed is forgotten.
func (s *poolSweepState) seen(key string, reset, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for known, until := range s.events {
		if !until.After(now) {
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
	poolJobHandedOver = "handed_over"
	poolJobEnded      = "ended"
	poolJobSkipped    = "skipped"
)

// poolJobResult is what a pass did for one job.
type poolJobResult struct {
	action    string
	role      string
	successor string
	why       string
}

func (r poolJobResult) acted() bool { return r.action == poolJobHandedOver || r.action == poolJobEnded }

func poolJobSkip(role, why string) poolJobResult {
	return poolJobResult{action: poolJobSkipped, role: role, why: why}
}

// observePoolExhaustion is the trigger: a node published a quota value the hub
// recorded. It never blocks the publish; the pass runs on its own goroutine.
func (h *HubServer) observePoolExhaustion(quota *FleetStateQuota, now time.Time) {
	if h.inbox == nil {
		return
	}
	for _, exhaustion := range poolExhaustionsOf(quota, now) {
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
}

// sweepExhaustedPool acts on every live job placed on the exhausted pool and
// account, all at once: each job's action runs on its own goroutine, because a
// hand-over waits for its successor to be placed.
func (h *HubServer) sweepExhaustedPool(ctx context.Context, exhaustion poolExhaustion) {
	if h.exhaustionSuperseded(exhaustion, h.now()) {
		h.logger.Debug("pool exhausted sweep superseded by a newer reading", "pool", exhaustion.Pool, "account", exhaustion.Account, "cause", exhaustion.Cause)
		return
	}
	isNew := h.poolSweep.seen(exhaustion.key(), exhaustion.Reset, h.now())
	onPool, err := h.poolExhaustionJobs(ctx, exhaustion)
	if err != nil {
		h.logger.Warn("pool exhausted sweep could not list its jobs", "pool", exhaustion.Pool, "account", exhaustion.Account, "error", err)
		return
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
					"action", results[i].action, "successor", results[i].successor, "reason", results[i].why)
			} else {
				h.poolSweep.release(task.Job)
			}
		}()
	}
	wg.Wait()

	handedOver, ended, skipped := 0, 0, map[string]int{}
	for _, result := range results {
		switch result.action {
		case poolJobHandedOver:
			handedOver++
		case poolJobEnded:
			ended++
		default:
			skipped[result.why]++
		}
	}
	level := h.logger.Debug
	if isNew || handedOver+ended > 0 {
		level = h.logger.Info
	}
	level("pool exhausted sweep", "pool", exhaustion.Pool, "account", exhaustion.Account, "cause", exhaustion.Cause, "reset", exhaustion.Reset.Format(time.RFC3339),
		"jobs", len(onPool), "handed_over", handedOver, "ended", ended, "skipped", skippedSummary(skipped))
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

// sweepPoolJob acts on one job of the exhausted pool by its role.
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
	release, held := h.jobGuard.acquire(ctx, task.Job)
	if !held {
		return poolJobSkip(role, "job_busy")
	}
	action, exists, err := h.inbox.store.poolAction(ctx, task.Request.RequestID, task.Attempt)
	if err != nil {
		release()
		return poolJobSkip(role, "action_unreadable")
	}
	if !exists {
		// Another operation may already own this role (for example a staged
		// author escalation). Do not start a competing ending in that gap.
		if prior.Task != "" && prior.ParentJob == "" {
			owner, _, _, err := h.inbox.store.spawnRoleOwner(ctx, prior.RoleKey())
			if err != nil || owner != task.Request.RequestID {
				release()
				return poolJobSkip(role, "role_not_owned")
			}
		}
		var why string
		action, why = h.newPoolAction(exhaustion, task, prior)
		if why != "" {
			release()
			return poolJobSkip(role, why)
		}
		if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
			release()
			h.logger.Warn("pool exhausted action: the intent was not saved", "job", task.Job, "error", err)
			return poolJobSkip(role, "action_not_saved")
		}
	}
	release()
	return h.drivePoolAction(ctx, action)
}

// poolExhaustionJobs lists the live jobs placed on the exhausted pool and
// account, from the durable operation records rather than from the node
// heartbeats: a heartbeat stops naming a job as soon as its pane is gone, and a
// half-finished ending must still be found. The lookback only bounds how far
// back a pass looks for work it has not acted on; an action that exists is
// durable and is finished whatever its age.
func (h *HubServer) poolExhaustionJobs(ctx context.Context, exhaustion poolExhaustion) ([]hubSpawnTask, error) {
	tasks, err := h.inbox.store.succeededSpawnsSince(ctx, h.now().Add(-poolActionLookback))
	if err != nil {
		return nil, err
	}
	// The lookback bounds history, not liveness: a job started longer ago is
	// still a candidate while a node reports it active.
	if live, liveErr := h.liveSpawnTasks(ctx); liveErr == nil {
		tasks = append(tasks, live...)
	} else {
		h.logger.Warn("pool exhausted sweep could not list active jobs", "pool", exhaustion.Pool, "account", exhaustion.Account, "error", liveErr)
	}
	var onPool []hubSpawnTask
	listed := map[string]bool{}
	for _, task := range tasks {
		if task.Outcome.Pool != exhaustion.Pool || task.Outcome.AccountKey != exhaustion.Account || !exhaustion.covers(task.Outcome.Profile) || listed[task.Job] {
			continue
		}
		listed[task.Job] = true
		onPool = append(onPool, task)
	}
	return onPool, nil
}

// newPoolAction builds the intent for one job, or names why the job is left
// alone. An implementer is handed over, which reuses the quota-stop
// hand-over's rules; a reviewer or consult is ended. Every input the ending
// needs later — the hk end reason, the owner event, and for a hand-over the
// successor job and its exact submission — is fixed here, so a retry replays
// the same work instead of generating a new request id.
func (h *HubServer) newPoolAction(exhaustion poolExhaustion, task hubSpawnTask, prior fleet.Spawn) (poolAction, string) {
	now := h.now().UTC()
	action := poolAction{
		Request: task.Request.RequestID, Attempt: task.Attempt, Job: task.Job, Owner: task.Owner, Role: prior.Role,
		Pool: exhaustion.Pool, Account: exhaustion.Account, Cause: exhaustion.Cause, Reset: exhaustion.Reset,
		EventID: "pool-exhausted:" + task.Job, Epoch: h.registeredEpoch(task.Job), Stage: poolActionIntent, Created: now, Updated: now,
	}
	switch prior.Role {
	case "implementer":
		// The hand-over's own contract: an explicit profile cannot change pool,
		// and a submission with neither a task nor a parent job has nothing to
		// continue.
		if prior.Task == "" && prior.ParentJob == "" || prior.Profile != "" && prior.Profile != "auto" {
			return poolAction{}, "not_replaceable"
		}
		successor, _, ok := quotaReplacementJob(task.Job)
		if !ok {
			return poolAction{}, "replacement_limit"
		}
		request, err := quotaReplacementRequest(task.Request, task.Job, successor, exhaustion.Pool, prior.Role)
		if err != nil {
			return poolAction{}, "request_id"
		}
		next, err := fleet.ValidateSubmission(request)
		if err != nil || !validLaneReference(next.Owner) {
			return poolAction{}, "invalid_replacement"
		}
		raw, err := json.Marshal(request)
		if err != nil {
			return poolAction{}, "successor_unreadable"
		}
		action.Kind, action.Successor, action.SuccessorRequest = poolActionHandOver, successor, string(raw)
		action.EndReason = fleet.QuotaStopEndReason(exhaustion.Pool, time.Time{}, "handed_over_to", successor)
	case "reviewer", "consult":
		action.Kind = poolActionEnd
		action.EndReason = fleet.QuotaStopEndReason(exhaustion.Pool, exhaustion.Reset, "", "")
	default:
		return poolAction{}, "role"
	}
	if !validNodeEndReason(action.EndReason) {
		return poolAction{}, "end_reason_too_long"
	}
	return action, ""
}

// registeredEpoch is the epoch the hub's job registry holds for job, 1 when the
// hub does not know it.
func (h *HubServer) registeredEpoch(job string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if record := h.jobs[job]; record != nil && record.Epoch > 0 {
		return record.Epoch
	}
	return 1
}
