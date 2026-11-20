package panewire

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// A quota-stopped job is handled by role (hk 3334, docs/quota-stop.md): a
// reviewer whose stop is announced on a live job (its watcher saw a limit screen
// or a credit rejection, hk 5690) is replaced on another pool at once; an
// implementer the quota-stop manager has handed over is replaced the same way.
// The node announces the stop with job.lost reason "quota_stop" and leaves the hk
// record alone, because hk never rewrites a lost record and the pointer to the
// successor can only be written with it. The hub places the successor, then
// closes the old pane with a screen capture and writes the old hk record lost
// with end_reason "quota_stop pool=<p> replaced_by=<job>" (an implementer:
// handed_over_to=<job>); a reviewer's requester is told nothing. When no other
// pool can take a reviewer the hub gives it up at once (giveUpQuotaStop): the
// record ends with the plain reason and the owner gets one notice. An implementer
// that cannot be placed is left to the watcher's grace and the spawn failure
// notice.

// isQuotaStopReason reports whether reason is a relayed quota stop: the bare
// "quota_stop" an older node sends, or the form the node's relay builds from the
// announcement (hub_jobs_client.go), "quota_stop pool=<p> reset_at=<t>". The hub
// compared the bare word only until hk 5690, so no announced stop was ever
// replaced: the relayed reason always carries the pool.
func isQuotaStopReason(reason string) bool {
	return reason == "quota_stop" || strings.HasPrefix(reason, "quota_stop ")
}

// quotaReplaceMax is the replacements one task role (one review pass) gets.
const quotaReplaceMax = 2

var quotaReplaceChain = regexp.MustCompile(`(-q[0-9]+)+$`)

// quotaReplacementJob names the successor of old: "<old>-q<n>", where n counts
// the replacements already in old's id. It refuses past quotaReplaceMax and
// an id outside what a job id may be.
func quotaReplacementJob(old string) (job string, n int, ok bool) {
	n = 1
	if chain := quotaReplaceChain.FindString(old); chain != "" {
		n += strings.Count(chain, "-q")
	}
	if n > quotaReplaceMax {
		return "", n, false
	}
	job = fmt.Sprintf("%s-q%d", old, n)
	return job, n, len(job) <= 128 && hubJobIDPattern.MatchString(job)
}

// quotaReplaceDeadline is the deadline the hub gives the successor's spawn, so
// the operation itself expires (with its own cleanup) before the hub stops
// waiting. quotaReplaceBudget is a HARD bound, a context deadline on the whole
// replacement (every hk read, node command and wait inside it), not a sum of
// phase constants: the node's watcher ends an unchanged stopped job after
// fleet.QuotaStopHandOffGrace, and a replacement still running then would lose
// the successor pointer (hk never rewrites a lost record). The grace leaves
// quotaReplaceSlack beyond the budget for the relay from the watcher's job.lost
// to the hub (TestQuotaReplaceBudgetFitsTheWatcherGrace).
const (
	quotaReplaceDeadline = 150 * time.Second
	quotaReplaceWait     = quotaReplaceDeadline + 30*time.Second
	quotaReplaceBudget   = 6 * time.Minute
	quotaReplaceSlack    = 2 * time.Minute
	// The old record is written with up to quotaMarkAttempts tries, quotaMarkBackoff apart.
	quotaMarkAttempts = 3
	quotaMarkBackoff  = 5 * time.Second
)

// quotaReplacePoll is how often the hub looks for the successor's outcome.
const quotaReplacePoll = 500 * time.Millisecond

func (h *HubServer) startQuotaReplacement(machine string, event hubJobEventPayload) {
	if h.inbox == nil || !isQuotaStopReason(event.Reason) {
		return
	}
	if h.spawnGrantDrain || h.spawnRollbackDrain {
		// A rollback drain creates no successor: an older hub could misread
		// its *_grant row (docs/hub-spawn-operations.md).
		h.logger.Warn("hub quota hand-off skipped while draining grant rows", "host", machine, "job", event.JobID)
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
		h.replaceQuotaStopped(h.closeCtx, machine, event)
	}()
}

// quotaReplacementRequest is the holder's submission for the successor: same
// task, role, review pass, owner and brief; a new job id, agent label and (for
// a reviewer) checkout directory; and --exclude-pool for exclude, the band the
// stop spent (the stop's own pool through fleet.BandKey, not always the record
// pool). pool names the stopped job in the implementer's continue line. An
// implementer also gets one line naming the earlier job's report and branch to
// continue from.
func quotaReplacementRequest(old fleet.Submission, oldJob, newJob, pool, exclude string, role string) (fleet.Submission, error) {
	id, err := fleet.NewRequestID()
	if err != nil {
		return fleet.Submission{}, err
	}
	next := old
	next.RequestID = id
	// A hub-built successor was never typed by a submitter: it must not
	// arm its parent's close, whatever the replaced request carried (hk 5195).
	next.ParentJobExplicit = false
	if next.DeadlineSeconds < 1 || next.DeadlineSeconds > int(quotaReplaceDeadline/time.Second) {
		next.DeadlineSeconds = int(quotaReplaceDeadline / time.Second)
	}
	next.Args = slices.Clone(old.Args)
	next.RetryHost = ""
	suffix := strings.TrimPrefix(newJob, oldJob)
	setArg := func(flag, value string) {
		for i := 0; i+1 < len(next.Args); i++ {
			if next.Args[i] == flag {
				next.Args[i+1] = value
				return
			}
		}
		next.Args = append(next.Args, flag, value)
	}
	setArg("--job", newJob)
	if label := hubSpawnArg(old.Args, "-l"); label != "" {
		setArg("-l", label+suffix)
	}
	// A report path under the stopped job's directory moves to the successor's;
	// the node resolves it inside the job that owns it. A path elsewhere (an
	// inherited parent's) stays.
	if report := hubSpawnArg(old.Args, "--report-relative"); strings.HasPrefix(report, oldJob+"/") {
		setArg("--report-relative", newJob+strings.TrimPrefix(report, oldJob))
	}
	pools := []string{}
	for _, p := range strings.Split(hubSpawnArg(old.Args, "--exclude-pool"), ",") {
		if p != "" && p != exclude {
			pools = append(pools, p)
		}
	}
	if exclude != "" {
		pools = append(pools, exclude)
	}
	if len(pools) > 0 {
		setArg("--exclude-pool", strings.Join(pools, ","))
	} else if hubSpawnArg(next.Args, "--exclude-pool") != "" {
		for i := 0; i+1 < len(next.Args); i++ {
			if next.Args[i] == "--exclude-pool" {
				next.Args = slices.Delete(next.Args, i, i+2)
				break
			}
		}
	}
	if next.Repository.Checkout != nil {
		checkout := *next.Repository.Checkout
		next.Repository.Checkout = &checkout
	}
	if role == "reviewer" {
		// A reviewer's checkout is named by its job; the stopped pane still
		// holds the old directory until the hub closes it.
		if old.Repository.Worktree == oldJob+"-checkout" && next.Repository.Checkout != nil {
			next.Repository.Worktree = newJob + "-checkout"
			next.Repository.Checkout.Dir = next.Repository.Worktree
		}
	} else {
		line := "\n\nContinue the earlier job " + oldJob + " (stopped on a " + pool + " quota limit)"
		if report := hubSpawnArg(old.Args, "--report-relative"); report != "" {
			line += ": its report is " + report
		}
		if old.Repository.Checkout != nil && old.Repository.Checkout.Branch != "" {
			line += ", its branch is " + old.Repository.Checkout.Branch
		}
		next.Brief = strings.TrimRight(old.Brief, "\n") + line + ".\n"
	}
	return next, nil
}

// replaceQuotaStopped answers one job.lost reason "quota_stop" from machine.
// It reports the successor's job id when one was placed and the old job closed.
func (h *HubServer) replaceQuotaStopped(ctx context.Context, machine string, event hubJobEventPayload) (string, bool) {
	skip := func(reason string, kv ...any) (string, bool) {
		h.logger.Info("quota-stop replacement skipped", append([]any{"job", event.JobID, "node", machine, "reason", reason}, kv...)...)
		return "", false
	}
	budget := h.quotaReplaceBudget
	if budget <= 0 {
		budget = quotaReplaceBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// The existing quota replacement also closes and writes this job. Order
	// it with pool-action admission so neither can overwrite the other's end.
	unlock, free := h.lockWithoutPoolAction(ctx, event.JobID)
	if !free {
		return skip("pool_action_pending")
	}
	defer unlock()
	store := h.inbox.store
	holder, err := store.latestSpawnForJob(ctx, event.JobID)
	if err != nil {
		return skip("unknown_job")
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return skip("unreadable_request")
	}
	role := prior.Role
	// An announced reviewer stop is replaced at once. An implementer only
	// announces job.lost when the quota-stop manager handed it over.
	if role != "reviewer" && role != "implementer" {
		return skip("role", "role", role)
	}
	// A task-bound job, or a reviewer that inherited its builder's
	// task (--parent-job), is replaced with the same arguments.
	if prior.Task == "" && prior.ParentJob == "" || (prior.Profile != "" && prior.Profile != "auto") {
		return skip("not_replaceable")
	}
	if holder.Status != "succeeded" || spawnHolderHost(holder) != machine {
		return skip("holder_state", "status", holder.Status)
	}
	// A reviewer the hub cannot carry on is given up: its record ends lost now
	// (not after the watcher's grace) and the owner is told once (hk 5690).
	// An implementer's record is already written before its successor is
	// claimed, and a spawn that fails tells its owner itself.
	giveUp := func(reason string, kv ...any) (string, bool) {
		if role == "reviewer" {
			h.giveUpQuotaStop(ctx, holder, prior.Owner, event, reason)
		}
		return skip(reason, kv...)
	}
	newJob, n, ok := quotaReplacementJob(event.JobID)
	if !ok {
		return giveUp("replacement_limit", "replacements", n-1)
	}
	if _, err := store.latestSpawnForJob(ctx, newJob); err == nil {
		return skip("already_replaced", "replacement", newJob)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return skip("spawn_config_unavailable")
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if err != nil || !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
		return skip("job_record", "state", state)
	}
	// The band that stopped comes from the stop's own pool, not the record:
	// the record pool is the launch pool, which is the catalog pool devin for
	// both Devin bands, so it cannot tell them apart. Until this change the
	// successor excluded the whole catalog pool, free rungs included (hk 4582).
	// A reason that names no readable pool keeps the record pool, which
	// excludes every rung of it.
	exclude := pool
	if stopPool, _ := quotaStopReasonFields(event.Reason); stopPool != "" && spawnargs.ValuePattern.MatchString(stopPool) {
		exclude = fleet.BandKey(stopPool)
	}
	if quotaStopReasonAccount(event.Reason) != "" {
		// An account-scoped stop says one account ran out, not the pool: the
		// successor must be able to take the pool's other accounts, and the
		// durable account mark keeps it off the spent one (hk 5138).
		exclude = ""
	}
	// The pointer to the successor is written once, with state=lost, so its
	// whole value must fit hk's limit before anything is closed or placed.
	key, verb := "replaced_by", "replaced"
	if role != "reviewer" {
		key, verb = "handed_over_to", "handed over"
	}
	// The carried-on record keeps the pool the job record names (the catalog
	// pool, devin for both Devin bands, hk 4582) as every hand-over writer does;
	// the band that stopped is in the stop's job.lost and the carry-over log line.
	stopBand, _ := quotaStopReasonFields(event.Reason)
	if stopBand == "" || !spawnargs.ValuePattern.MatchString(stopBand) {
		stopBand = pool
	}
	endReason := fleet.QuotaStopEndReason(pool, time.Time{}, key, newJob)
	if !validNodeEndReason(endReason) {
		return giveUp("end_reason_too_long", "length", len(endReason))
	}
	// Place nothing while the old pane is busy or unreadable.
	name := heldAgentName(holder, record)
	probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
	if probe.state == heldPaneUnknown || probe.state == heldPanePresent && (!nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd)) {
		return skip("pane_not_idle", "probe", probe.reason, "status", probe.row.AgentStatus)
	}
	request, err := quotaReplacementRequest(holder.Request, event.JobID, newJob, pool, exclude, role)
	if err != nil {
		return skip("request_id")
	}
	next, err := fleet.ValidateSubmission(request)
	if err != nil || !validLaneReference(next.Owner) {
		return giveUp("invalid_replacement")
	}
	// Close the old pane before the successor exists, so a single job holds
	// the task: a pane that changed since the probe is refused by pane.close
	// and nothing is placed. A closed pane with no successor is ended by the
	// node's watcher after its grace, as a quota stop with no replacement.
	if probe.state == heldPanePresent {
		if closed, _ := h.closeHeldPane(ctx, holder, name, probe.row, event.JobID); !closed {
			return skip("pane_not_closed", "pane", holder.Outcome.Pane)
		}
	}
	mark := func() bool {
		// The pointer can only be written once and the watcher's fallback
		// would write a plain record, so a transient failure is retried
		// inside the grace.
		backoff := h.quotaMarkBackoff
		if backoff <= 0 {
			backoff = quotaMarkBackoff
		}
		var lost bool
		var reason string
		for attempt := 0; attempt < quotaMarkAttempts && !lost; attempt++ {
			if attempt > 0 {
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
				}
			}
			lost, reason = h.markHolderJobLostBecause(ctx, app, holder, name, endReason)
		}
		if !lost {
			h.logger.Warn("quota-stop replacement: the stopped job's record was not written", "job", event.JobID, "replacement", newJob, "reason", reason)
		}
		return lost
	}
	// An implementer is handed over in the order the ticket gives: the
	// record ends with handed_over_to first, then the task is spawned. The
	// spawn refuses a task still bound to a job that is not lost, so the old
	// record must end before the successor is admitted. When the spawn then
	// fails the pointer names a job that never ran, and the owner is told by
	// the spawn failure notice.
	if role != "reviewer" && !mark() {
		return "", false
	}
	if prior.HoldsTaskRole() {
		if priorID, _, _, err := store.spawnRoleOwner(ctx, prior.RoleKey()); err == nil && priorID == holder.Request.RequestID {
			// The stopped job still holds the role; its pane is closed.
			_ = store.releaseCompletedSpawnRole(ctx, priorID, prior.RoleKey())
		}
	}
	if _, _, err := store.claimSpawnFrom(ctx, request, newJob, next.Owner, h.now().UTC(), event.JobID); err != nil {
		return giveUp("claim_refused", "error", err.Error())
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	wait := h.quotaWait
	if wait <= 0 {
		wait = quotaReplaceWait
	}
	task, settled := h.awaitSpawnOutcome(ctx, request.RequestID, wait)
	outcome := task.Outcome
	if role == "reviewer" {
		// A reviewer is replaced only when the successor is placed (no other
		// pool: no replacement). An operation still running may yet be
		// completed by a late node result, and hk cannot repair the old record
		// after the watcher ends it, so a successor that can still run gets
		// its pointer too: a pointer to a successor that later fails is a
		// recoverable chain, a running successor nobody points to is
		// duplicate work. An operation that was never bound to a node
		// (accepted) can never place a job, and one that settled as a failure
		// did not.
		if settled && outcome.RC != 0 || !settled && !spawnMayStillRun(task) {
			h.logger.Info("quota-stop replacement not placed", "job", event.JobID, "replacement", newJob, "pool", pool, "status", task.Status, "rc", outcome.RC, "reason", outcome.Reason)
			h.giveUpQuotaStop(ctx, holder, prior.Owner, event, "not_placed")
			return "", false
		}
		if !settled {
			h.logger.Warn("quota-stop replacement undecided; writing its pointer", "job", event.JobID, "replacement", newJob, "status", task.Status)
		}
		if !mark() {
			return "", false
		}
	} else if settled && outcome.RC != 0 {
		h.logger.Warn("quota-stop hand-over not placed; its pointer names a job that never ran", "job", event.JobID, "replacement", newJob, "pool", pool, "rc", outcome.RC, "reason", outcome.Reason)
		return "", false
	}
	h.logger.Info("quota-stop replacement", "job", event.JobID, "replacement", newJob, "role", role, "pool", pool, "host", outcome.Host, "profile", outcome.Profile)
	h.logger.Info("quota-stop carry-over", "outcome", "carried_on", "job", event.JobID, "successor", newJob, "role", role, "pool", stopBand, "record_pool", pool, "host", outcome.Host, "profile", outcome.Profile)
	if role == "reviewer" {
		// The requester resolves the old job to its successor through the old
		// record's replaced_by and hears nothing else on a carry-over (hk 5690).
		return newJob, true
	}
	to, place := "with", fmt.Sprintf("%s/%s", outcome.Host, outcome.Profile)
	if role != "reviewer" {
		to = "to"
	}
	if !settled {
		place = "(placement still pending)"
	}
	text := fmt.Sprintf("%s %s (%s quota) %s %s on %s", verb, event.JobID, pool, to, newJob, place)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "quota-replace:" + newJob, JobID: newJob, OwnerLane: next.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1, Notice: ownerNoticeFor(newJob, "quota_replace", false)}, nil)
	return newJob, true
}

// giveUpQuotaStop ends a reviewer's quota stop that no other pool could take:
// the pane is closed if it is still there, the old record is written lost with
// the plain end_reason "quota_stop pool=<pool> reset_at=<t>" (mergewake hk 3302,
// no replaced_by), and the owner is told once. The notice's event id is the
// job's, so a repeat of the replacement (a hub restart, a second announcement)
// sends no second one. A pane that is busy or unreadable is not given up: the
// stop may be over, and the node's watcher decides when its grace runs out. The
// record and the log line are what a count of carried on and given up reads
// (docs/quota-stop.md, "The record of a carry-over").
func (h *HubServer) giveUpQuotaStop(ctx context.Context, holder hubSpawnTask, owner string, event hubJobEventPayload, why string) {
	if !validLaneReference(owner) {
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if err != nil || !exists || (state != "claimed" && state != "spawned") {
		return
	}
	stopPool, resetAt := quotaStopReasonFields(event.Reason)
	if stopPool == "" || !spawnargs.ValuePattern.MatchString(stopPool) {
		stopPool = pool
	}
	var reset time.Time
	if at, err := time.Parse(time.RFC3339, resetAt); err == nil {
		reset = at
	}
	endReason := fleet.QuotaStopEndReason(stopPool, reset, "", "")
	if !validNodeEndReason(endReason) {
		endReason = fleet.QuotaStopEndReason(stopPool, time.Time{}, "", "")
	}
	name := heldAgentName(holder, record)
	probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
	switch {
	case probe.state == heldPanePresent && nodePaneIdleStatus[probe.row.AgentStatus] && validNodeCommandPath(probe.row.Cwd):
		if closed, _ := h.closeHeldPane(ctx, holder, name, probe.row, event.JobID); !closed {
			return
		}
	case probe.state != heldPaneAbsent:
		return
	}
	if lost, _ := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); !lost {
		h.logger.Warn("quota-stop give-up: the stopped job's record was not written", "job", event.JobID, "reason", why)
		return
	}
	h.logger.Info("quota-stop carry-over", "outcome", "given_up", "job", event.JobID, "role", "reviewer", "pool", stopPool, "reason", why)
	text := fmt.Sprintf("gave up %s (%s quota): no other pool could take the review (%s)", event.JobID, stopPool, why)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "quota-giveup:" + event.JobID, JobID: event.JobID, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1, Notice: ownerNoticeFor(event.JobID, "quota_giveup", true)}, nil)
}

// spawnMayStillRun reports whether an operation that has not settled can still
// place a job: one with a destination that may still be sent (bound), one the
// node may hold (dispatching, reconciling), or one whose late result can still
// arrive (deadline_exceeded with a node binding). An accepted operation has no
// node, and past its deadline is completed retryable without being dispatched.
func spawnMayStillRun(task hubSpawnTask) bool {
	switch task.Status {
	case "bound", "dispatching", "reconciling":
		return true
	case "deadline_exceeded":
		return task.Node != nil
	}
	return false
}

// spawnStatusActive lists the operation states that are not settled for the
// caller: the coordinator still works them (laneInboxStore.pendingSpawns), or
// the start-condition queue holds them (waiting). A waiting row is not final:
// it is released once its predicates hold or ended at its limit.
func spawnStatusActive(status string) bool {
	switch status {
	case "accepted", "bound", "dispatching", "reconciling", "deadline_exceeded", "waiting":
		return true
	}
	return false
}

// awaitSpawnOutcome waits for the successor's operation to reach a final state
// (settled) and returns it; settled is false when the wait ended with the
// operation still running.
func (h *HubServer) awaitSpawnOutcome(ctx context.Context, requestID string, limit time.Duration) (hubSpawnTask, bool) {
	poll := h.quotaReplacePoll
	if poll <= 0 {
		poll = quotaReplacePoll
	}
	deadline := time.Now().Add(limit)
	for {
		task, err := h.inbox.store.spawnTask(ctx, requestID)
		// accepted, dispatching, reconciling and deadline_exceeded are the
		// states the coordinator still retries (pendingSpawns), and waiting is
		// held by the start-condition queue; a node may yet complete the
		// successor from reconciling. Only succeeded, failed and retryable are
		// final.
		if err == nil && !spawnStatusActive(task.Status) {
			return task, true
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return task, false
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return task, false
		}
	}
}

// quotaReconcileEvery is how often the hub looks for a replacement a restart
// interrupted.
const quotaReconcileEvery = 2 * time.Minute

var quotaSuccessorJob = regexp.MustCompile(`^(.+)-q[0-9]+$`)

// reconcileQuotaReplacements finishes a reviewer replacement the hub was
// restarted in the middle of: the successor was claimed and runs, but the old
// job's record was never written with its pointer, and the watcher's fallback
// would write a plain quota_stop. For each successor that placed (or may still
// place) whose predecessor's hk record is still live, it closes the
// predecessor's pane if one is left and writes the record with replaced_by. A
// implementer's pointer is written before its successor is claimed, so it
// needs no recovery. It reports how many records it wrote.
func (h *HubServer) reconcileQuotaReplacements(ctx context.Context) int {
	if h.inbox == nil {
		return 0
	}
	h.loopTick(loopQuotaLadder)
	successors, err := h.inbox.store.quotaSuccessorSpawns(ctx)
	if err != nil {
		h.loopNoteSilentError(loopQuotaLadder, "hub_quota_replace.go:quotaSuccessorSpawns", err)
		return 0
	}
	h.loopClearSilent("hub_quota_replace.go:quotaSuccessorSpawns")
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		h.loopNoteSilentError(loopQuotaLadder, "hub_quota_replace.go:newSpawnApp", err)
		return 0
	}
	h.loopClearSilent("hub_quota_replace.go:newSpawnApp")
	for _, successor := range successors {
		h.loopNoteMade(loopQuotaLadder, successor.Job)
	}
	written := 0
	for _, successor := range successors {
		if !spawnMayStillRun(successor) && successor.Status != "succeeded" {
			continue
		}
		// A replacement younger than its own budget is still being finished by
		// replaceQuotaStopped.
		if h.now().Sub(successor.Created) < quotaReplaceBudget {
			continue
		}
		old := successor.ReplacedFrom
		if h.reconcileQuotaReplacement(ctx, app, successor, old) {
			written++
			h.loopNoteDone(loopQuotaLadder, successor.Job)
		}
	}
	return written
}

func (h *HubServer) reconcileQuotaReplacement(ctx context.Context, app *fleet.App, successor hubSpawnTask, old string) bool {
	unlock, free := h.lockWithoutPoolAction(ctx, old)
	if !free {
		return false
	}
	defer unlock()
	holder, err := h.inbox.store.latestSpawnForJob(ctx, old)
	if err != nil || holder.Status != "succeeded" {
		return false
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil || prior.Role != "reviewer" {
		return false
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, old)
	cancel()
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if err != nil || !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
		return false
	}
	endReason := fleet.QuotaStopEndReason(pool, time.Time{}, "replaced_by", successor.Job)
	if !validNodeEndReason(endReason) {
		return false
	}
	name := heldAgentName(holder, record)
	probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
	switch {
	case probe.state == heldPanePresent && nodePaneIdleStatus[probe.row.AgentStatus] && validNodeCommandPath(probe.row.Cwd):
		if closed, _ := h.closeHeldPane(ctx, holder, name, probe.row, old); !closed {
			return false
		}
	case probe.state != heldPaneAbsent:
		return false
	}
	if lost, _ := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); lost {
		h.logger.Info("quota-stop replacement reconciled", "job", old, "replacement", successor.Job)
		return true
	}
	return false
}

// runQuotaReconcile runs reconcileQuotaReplacements shortly after start (nodes
// need time to reconnect) and then periodically.
func (h *HubServer) runQuotaReconcile(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	delay := quotaReconcileEvery / 4
	for {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		h.reconcileQuotaReplacements(ctx)
		h.expireStagedLadderSteps(ctx)
		delay = quotaReconcileEvery
	}
}
