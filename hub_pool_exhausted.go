package panewire

import (
	"context"
	"fmt"
	"io"
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
// with its reset still ahead. The hub then lists the live jobs whose placement
// record (hubSpawnOutcome.Pool and AccountKey) names that pool and account and
// acts on each, role by role, with the quota-stop mechanics that already exist:
//
//	worker, tester     handed over (replaceQuotaStoppedWhy): the record ends with
//	                   handed_over_to, the successor takes the same worktree and
//	                   branch with -m auto and the pool excluded.
//	reviewer, counsel  ended now: the pane is closed and the record ends with
//	                   end_reason "quota_stop pool=<p> reset_at=<t>", and the owner
//	                   gets job.lost reason quota_stop with the pool and reset, so
//	                   it respawns the review.

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
// with several, the one that clears last). A window of one model group leaves
// the account's other models usable, and one with no readable reset cannot be
// said to hold.
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
		if bucket.UsedPct == nil || *bucket.UsedPct < 100 || bucket.Model != "" || bucket.Scope == "group" {
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

// poolSweepState is what makes a pass idempotent. jobs holds each job a pass is
// acting on or has acted on, so a pass that starts while another is still
// waiting for a successor, or a later pass, leaves it alone; the job's own
// record is the durable half (a job already handed over or ended is lost in hk
// and skipped). events remembers the pool events seen, so a repeat is logged
// quietly.
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
	isNew := h.poolSweep.seen(exhaustion.key(), exhaustion.Reset, h.now())
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		h.logger.Warn("pool exhausted sweep could not list live jobs", "pool", exhaustion.Pool, "account", exhaustion.Account, "error", err)
		return
	}
	var onPool []hubSpawnTask
	listed := map[string]bool{}
	for _, task := range tasks {
		if task.Status == "succeeded" && task.Outcome.Pool == exhaustion.Pool && task.Outcome.AccountKey == exhaustion.Account && exhaustion.covers(task.Outcome.Profile) && !listed[task.Job] {
			listed[task.Job] = true
			onPool = append(onPool, task)
		}
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
	switch role {
	case "worker", "tester":
		successor, replaced, why := h.replaceQuotaStoppedWhy(ctx, spawnHolderHost(task), hubJobEventPayload{JobID: task.Job, Epoch: 1, Reason: "quota_stop", OwnerLane: task.Owner})
		switch {
		case replaced:
			return poolJobResult{action: poolJobHandedOver, role: role, successor: successor}
		case why == quotaHandOverNotPlaced:
			// The record already ends with handed_over_to; the owner is told by
			// the spawn failure notice. The job is over either way.
			successor, _, _ := quotaReplacementJob(task.Job)
			return poolJobResult{action: poolJobHandedOver, role: role, successor: successor, why: why}
		}
		return poolJobSkip(role, why)
	case "reviewer", "counsel":
		return h.endPoolJob(ctx, exhaustion, task, prior)
	}
	return poolJobSkip(role, "role")
}

// endPoolJob ends a reviewer or consult job on the exhausted pool: the quota-stop
// end of a job with no replacement (docs/quota-stop.md, "Job end on a quota
// stop"), done by the hub because the pane has not been observed. The pane must
// be idle (nothing is closed under a pane that is working), is saved and
// closed, the hk record is written lost with end_reason "quota_stop
// pool=<p> reset_at=<t>", the role is released so the owner can respawn it, and
// the owner is sent job.lost reason quota_stop with the pool and reset.
func (h *HubServer) endPoolJob(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, prior fleet.Spawn) poolJobResult {
	role := prior.Role
	skip := func(why string, kv ...any) poolJobResult {
		h.logger.Info("pool exhausted end skipped", append([]any{"job", task.Job, "pool", exhaustion.Pool, "reason", why}, kv...)...)
		return poolJobSkip(role, why)
	}
	ctx, cancel := context.WithTimeout(ctx, quotaReplaceBudget)
	defer cancel()
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return skip("spawn_config_unavailable")
	}
	readCtx, cancelRead := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, task.Job)
	cancelRead()
	state, _ := record["state"].(string)
	if err != nil || !exists || (state != "claimed" && state != "spawned") {
		return skip("job_record", "state", state)
	}
	if pool, _ := record["pool"].(string); pool != "" && pool != exhaustion.Pool {
		return skip("pool_mismatch", "record_pool", pool)
	}
	endReason := fleet.QuotaStopEndReason(exhaustion.Pool, exhaustion.Reset, "", "")
	if !validNodeEndReason(endReason) {
		return skip("end_reason_too_long", "length", len(endReason))
	}
	machine, name := spawnHolderHost(task), heldAgentName(task, record)
	probe := h.probeHeldPane(ctx, machine, task.Outcome.Pane, name)
	if probe.state == heldPaneUnknown || probe.state == heldPanePresent && (!nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd)) {
		return skip("pane_not_idle", "probe", probe.reason, "status", probe.row.AgentStatus)
	}
	if probe.state == heldPanePresent && !h.closeHeldPane(ctx, task, name, probe.row) {
		return skip("pane_not_closed", "pane", task.Outcome.Pane)
	}
	if lost, reason := h.markHolderLostWithRetry(ctx, app, task, name, endReason); !lost {
		h.logger.Warn("pool exhausted end: the job's record was not written", "job", task.Job, "reason", reason)
		return skip("record_not_written", "write", reason)
	}
	if prior.Task != "" && prior.ParentJob == "" {
		if priorID, _, _, err := h.inbox.store.spawnRoleOwner(ctx, prior.RoleKey()); err == nil && priorID == task.Request.RequestID {
			_ = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, prior.RoleKey())
		}
	}
	h.relayJobEventFromContext(ctx, "", "job.lost", hubJobEventPayload{
		JobID: task.Job, Epoch: h.registeredEpoch(task.Job), OwnerLane: task.Owner, AgentLabel: name, PaneID: task.Outcome.Pane,
		EventID: "pool-exhausted:" + task.Job, Reason: "quota_stop pool=" + exhaustion.Pool + " reset_at=" + exhaustion.Reset.Format(time.RFC3339),
	})
	return poolJobResult{action: poolJobEnded, role: role}
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
