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
// reviewer is replaced on another pool at once; a worker or tester the
// quota-stop manager has handed over is replaced the same way. The node
// announces the stop with job.lost reason "quota_stop" and leaves the hk
// record alone, because hk never rewrites a lost record and the pointer to the
// successor can only be written with it. The hub places the successor, then
// closes the old pane with a screen capture and writes the old hk record lost
// with end_reason "quota_stop pool=<p> replaced_by=<job>" (a worker or tester:
// handed_over_to=<job>). When no other pool can take the work the hub does
// nothing more, and the node ends the job itself without a successor.

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
	if h.inbox == nil || event.Reason != "quota_stop" {
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
// a reviewer) checkout directory; and --exclude-pool for the stopped pool. A
// worker or tester also gets one line naming the earlier job's report and
// branch to continue from.
func quotaReplacementRequest(old fleet.Submission, oldJob, newJob, pool string, role string) (fleet.Submission, error) {
	id, err := fleet.NewRequestID()
	if err != nil {
		return fleet.Submission{}, err
	}
	next := old
	next.RequestID = id
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
		if p != "" && p != pool {
			pools = append(pools, p)
		}
	}
	setArg("--exclude-pool", strings.Join(append(pools, pool), ","))
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
	// A reviewer's quota stop is replaced at once. A worker or tester only
	// announces job.lost when the quota-stop manager handed it over.
	if role != "reviewer" && role != "worker" && role != "tester" {
		return skip("role", "role", role)
	}
	// A task-bound job, or a tester or reviewer that inherited its builder's
	// task (--parent-job), is replaced with the same arguments.
	if prior.Task == "" && prior.ParentJob == "" || (prior.Profile != "" && prior.Profile != "auto") {
		return skip("not_replaceable")
	}
	if holder.Status != "succeeded" || spawnHolderHost(holder) != machine {
		return skip("holder_state", "status", holder.Status)
	}
	newJob, n, ok := quotaReplacementJob(event.JobID)
	if !ok {
		return skip("replacement_limit", "replacements", n-1)
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
	// The pointer to the successor is written once, with state=lost, so its
	// whole value must fit hk's limit before anything is closed or placed.
	key, verb := "replaced_by", "replaced"
	if role != "reviewer" {
		key, verb = "handed_over_to", "handed over"
	}
	endReason := fleet.QuotaStopEndReason(pool, time.Time{}, key, newJob)
	if !validNodeEndReason(endReason) {
		return skip("end_reason_too_long", "length", len(endReason))
	}
	// Place nothing while the old pane is busy or unreadable.
	name := heldAgentName(holder, record)
	probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
	if probe.state == heldPaneUnknown || probe.state == heldPanePresent && (!nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd)) {
		return skip("pane_not_idle", "probe", probe.reason, "status", probe.row.AgentStatus)
	}
	request, err := quotaReplacementRequest(holder.Request, event.JobID, newJob, pool, role)
	if err != nil {
		return skip("request_id")
	}
	next, err := fleet.ValidateSubmission(request)
	if err != nil || !validLaneReference(next.Owner) {
		return skip("invalid_replacement")
	}
	// Close the old pane before the successor exists, so a single job holds
	// the task: a pane that changed since the probe is refused by pane.close
	// and nothing is placed. A closed pane with no successor is ended by the
	// node's watcher after its grace, as a quota stop with no replacement.
	if probe.state == heldPanePresent && !h.closeHeldPane(ctx, holder, name, probe.row) {
		return skip("pane_not_closed", "pane", holder.Outcome.Pane)
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
	// A worker or tester is handed over in the order the ticket gives: the
	// record ends with handed_over_to first, then the task is spawned. The
	// spawn refuses a task still bound to a job that is not lost, so the old
	// record must end before the successor is admitted. When the spawn then
	// fails the pointer names a job that never ran, and the owner is told by
	// the spawn failure notice.
	if role != "reviewer" && !mark() {
		return "", false
	}
	if prior.Task != "" && prior.ParentJob == "" {
		if priorID, _, _, err := store.spawnRoleOwner(ctx, prior.RoleKey()); err == nil && priorID == holder.Request.RequestID {
			// The stopped job still holds the role; its pane is closed.
			_ = store.releaseCompletedSpawnRole(ctx, priorID, prior.RoleKey())
		}
	}
	if _, _, err := store.claimSpawn(ctx, request, newJob, next.Owner, h.now().UTC()); err != nil {
		return skip("claim_refused", "error", err.Error())
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
	to, place := "with", fmt.Sprintf("%s/%s", outcome.Host, outcome.Profile)
	if role != "reviewer" {
		to = "to"
	}
	if !settled {
		place = "(placement still pending)"
	}
	text := fmt.Sprintf("%s %s (%s quota) %s %s on %s", verb, event.JobID, pool, to, newJob, place)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "quota-replace:" + newJob, JobID: newJob, OwnerLane: next.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return newJob, true
}

// spawnMayStillRun reports whether an operation that has not settled can still
// place a job: one the node holds (dispatching, reconciling) or whose late
// result can still arrive (deadline_exceeded with a node binding). An accepted
// operation has no node, and past its deadline is completed retryable without
// being dispatched.
func spawnMayStillRun(task hubSpawnTask) bool {
	switch task.Status {
	case "dispatching", "reconciling":
		return true
	case "deadline_exceeded":
		return task.Node != nil
	}
	return false
}

// spawnStatusActive lists the operation states the coordinator still works
// (laneInboxStore.pendingSpawns).
func spawnStatusActive(status string) bool {
	switch status {
	case "accepted", "dispatching", "reconciling", "deadline_exceeded":
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
		// states the coordinator still retries (pendingSpawns); a node may yet
		// complete the successor from reconciling. Only succeeded, failed and
		// retryable are final.
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
// worker or tester's pointer is written before its successor is claimed, so it
// needs no recovery. It reports how many records it wrote.
func (h *HubServer) reconcileQuotaReplacements(ctx context.Context) int {
	if h.inbox == nil {
		return 0
	}
	successors, err := h.inbox.store.quotaSuccessorSpawns(ctx)
	if err != nil {
		return 0
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return 0
	}
	written := 0
	for _, successor := range successors {
		m := quotaSuccessorJob.FindStringSubmatch(successor.Job)
		if m == nil || !spawnMayStillRun(successor) && successor.Status != "succeeded" {
			continue
		}
		// A replacement younger than its own budget is still being finished by
		// replaceQuotaStopped.
		if h.now().Sub(successor.Created) < quotaReplaceBudget {
			continue
		}
		old := m[1]
		holder, err := h.inbox.store.latestSpawnForJob(ctx, old)
		if err != nil || holder.Status != "succeeded" {
			continue
		}
		prior, err := fleet.ValidateSubmission(holder.Request)
		if err != nil || prior.Role != "reviewer" {
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, err := app.ExistingJob(readCtx, old)
		cancel()
		state, _ := record["state"].(string)
		pool, _ := record["pool"].(string)
		if err != nil || !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
			continue
		}
		endReason := fleet.QuotaStopEndReason(pool, time.Time{}, "replaced_by", successor.Job)
		if !validNodeEndReason(endReason) {
			continue
		}
		name := heldAgentName(holder, record)
		probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
		switch {
		case probe.state == heldPanePresent && nodePaneIdleStatus[probe.row.AgentStatus] && validNodeCommandPath(probe.row.Cwd):
			if !h.closeHeldPane(ctx, holder, name, probe.row) {
				continue
			}
		case probe.state != heldPaneAbsent:
			continue
		}
		if lost, _ := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); lost {
			written++
			h.logger.Info("quota-stop replacement reconciled", "job", old, "replacement", successor.Job)
		}
	}
	return written
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
		delay = quotaReconcileEvery
	}
}
