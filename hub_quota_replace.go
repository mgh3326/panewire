package panewire

import (
	"context"
	"database/sql"
	"errors"
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
// or a credit rejection, hk 5690) is replaced on another pool at once. An
// implementer the quota-stop manager has handed over goes through the guarded
// worktree hand-over (hub_quota_handover.go, hk 5780), never the close below:
// that close has none of the checks an implementer's worktree needs. The node
// announces the stop with job.lost reason "quota_stop" and leaves the hk
// record alone, because hk never rewrites a lost record and the pointer to the
// successor can only be written with it. The hub places the successor, then
// closes the old pane with a screen capture and writes the old hk record lost
// with end_reason "quota_stop pool=<p> replaced_by=<job>" (an implementer:
// handed_over_to=<job>); a reviewer's requester is told nothing. When no other
// pool can take a reviewer the hub gives it up at once (giveUpQuotaStop): the
// record ends with the plain reason and the owner gets one notice. An implementer
// the guarded hand-over refuses (dirty or unpushed work it cannot snapshot, a
// pinned host, no other host) is left in its pane with one owner decision.

// isQuotaStopReason reports whether reason is a relayed quota stop: the bare
// "quota_stop" an older node sends, or the form the node's relay builds from the
// announcement (hub_jobs_client.go), "quota_stop pool=<p> [reset_at=<t>]" (the
// reset only when the node observed it, hk 5777). The hub
// compared the bare word only until hk 5690, so no announced stop was ever
// replaced: the relayed reason always carries the pool.
func isQuotaStopReason(reason string) bool {
	return reason == "quota_stop" || strings.HasPrefix(reason, "quota_stop ")
}

// quotaLineageHolds reports whether successor is a replacement of predecessor (the
// job named old): its claim wrote replaced_from as replacing it, for the same owner
// and the same task or parent job. A job that merely carries the next name in the
// chain is not (hk 5690 review).
func quotaLineageHolds(successor, predecessor hubSpawnTask, old string) bool {
	// replaced_from counts only when the claim itself wrote it (replaced_reason):
	// the startup backfill derives it from the job's name, which is no proof.
	if successor.ReplacedFrom != old || successor.ReplacedReason != "claim" || successor.Owner != predecessor.Owner {
		return false
	}
	for _, flag := range []string{"--task", "--parent-job"} {
		if hubSpawnArg(successor.Request.Args, flag) != hubSpawnArg(predecessor.Request.Args, flag) {
			return false
		}
	}
	return true
}

// quotaStopSeq reads the pane state a stop was observed in from the relayed reason:
// the pointer to pass as pane.close's stop_seq, and whether there was one.
func quotaStopSeq(reason string) (*int64, bool) {
	seq, ok := quotaStopReasonSeq(reason)
	if !ok {
		return nil, false
	}
	return &seq, true
}

// quotaPaneStopped reports whether a held pane's status is one a quota stop's close
// may act on: idle (or done), or, for a close fenced to the stop's own sequence, a
// blocked dialog at exactly that sequence is the stop (hk 5690 review).
func quotaPaneStopped(status string, fenced bool) bool {
	return nodePaneIdleStatus[status] || fenced && status == "blocked"
}

// quotaReplaceMax is the replacements one task role (one review pass) gets.
const quotaReplaceMax = 2

var quotaReplaceChain = regexp.MustCompile(`(-q[0-9]+)+$`)

// quotaReplacementJob names the successor of old: "<old>-q<n>" (shortened to
// herdr's agent name limit by successorName), where n counts the replacements
// already in old's id. It refuses past quotaReplaceMax and a name that is not
// a valid agent name and job id.
func quotaReplacementJob(old string) (job string, n int, ok bool) {
	n = 1
	if chain := quotaReplaceChain.FindString(old); chain != "" {
		n += strings.Count(chain, "-q")
	}
	if n > quotaReplaceMax {
		return "", n, false
	}
	job, ok = successorName(old, "q", n)
	return job, n, ok
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
	h.startQuotaStop(machine, event, nil, nil)
}

// startQuotaStop answers a stop announced by machine's node with a job.lost whose
// reason isQuotaStopReason, off the node's websocket reader. The reader never
// reads the store: the role comes from the stored submission the replacement
// itself loads (replaceQuotaStoppedAck), after the drain check. settled runs once
// the stop is durably owned (the node may then drop it from its outbox); relay
// files the event as the plain loss for a job the hub does not carry on (a
// consult, a job it never spawned): until hk 5780 the reader did that after its
// own store read. Both may be nil.
func (h *HubServer) startQuotaStop(machine string, event hubJobEventPayload, settled func(), relay func(context.Context)) {
	if h.inbox == nil || !isQuotaStopReason(event.Reason) {
		return
	}
	if h.spawnGrantDrain || h.spawnRollbackDrain {
		// A rollback drain creates no successor: an older hub could misread
		// its *_grant row (docs/hub-spawn-operations.md). Nothing is read, filed
		// or acknowledged: the node sends the stop again after the drain.
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
		h.replaceQuotaStoppedAck(h.closeCtx, machine, event, settled, &quotaAnnouncement{relay: relay})
	}()
}

// quotaAnnouncement marks a replacement the node announced (a relayed job.lost),
// as opposed to one the hub started itself (an account's credit, hk 5138). Only
// an announced stop splits by role: a reviewer takes the close below, an
// implementer the guarded worktree hand-over, anything else is filed as a loss.
type quotaAnnouncement struct {
	relay func(context.Context)
}

// fileLoss files the stop as a plain loss, for a job the hub does not carry on.
func (a *quotaAnnouncement) fileLoss(ctx context.Context) {
	if a != nil && a.relay != nil {
		a.relay(ctx)
	}
}

// quotaReplacementRequest is the holder's submission for the successor: same
// task, role, review pass, owner and brief; a new job id, agent label and (for
// a reviewer) checkout directory; and --exclude-pool for exclude, the band the
// stop spent (the stop's own pool through fleet.BandKey, not always the record
// pool). pool names the stopped job in the implementer's continue line. An
// implementer also gets one line naming the earlier job's report and branch to
// continue from.
func quotaReplacementRequest(old fleet.Submission, oldJob, newJob, pool, exclude string, role string) (fleet.Submission, error) {
	wantJob, n, ok := quotaReplacementJob(oldJob)
	if !ok || wantJob != newJob {
		return fleet.Submission{}, fmt.Errorf("%q is not the quota replacement of %q", newJob, oldJob)
	}
	label := hubSpawnArg(old.Args, "-l")
	successorLabel, _ := successorName(label, "q", n)
	if label != "" && !fleet.ValidAgentName(successorLabel) {
		return fleet.Submission{}, fmt.Errorf("the successor label of %q is not a valid agent name", label)
	}
	id, err := fleet.NewRequestID()
	if err != nil {
		return fleet.Submission{}, err
	}
	next := old
	next.RequestID = id
	// A hub-built successor was never typed by a submitter: it must not
	// arm its parent's close, whatever the replaced request carried (hk 5195).
	next.ParentJobExplicit = false
	// The stopped job already started, so any start condition it waited on was
	// met. Carried over, it would park the successor in waiting, where it could
	// be admitted after the review is given up (hk 5690).
	next.StartConditions = nil
	if next.DeadlineSeconds < 1 || next.DeadlineSeconds > int(quotaReplaceDeadline/time.Second) {
		next.DeadlineSeconds = int(quotaReplaceDeadline / time.Second)
	}
	next.Args = slices.Clone(old.Args)
	next.RetryHost = ""
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
	if label != "" {
		setArg("-l", successorLabel)
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
	return h.replaceQuotaStoppedAck(ctx, machine, event, nil, nil)
}

// replaceQuotaStoppedAck is replaceQuotaStopped that calls ackStop, once, when
// the stop is durably owned: the successor is claimed (the reconcile pass
// repairs the rest) or the replacement reached a decision that a resend cannot
// change. Every exit is an explicit outcome: a final one (skip, giveUp) settles
// the stop, a retry (retryLater) does not, and a claim settles it too (hk 5690).
func (h *HubServer) replaceQuotaStoppedAck(ctx context.Context, machine string, event hubJobEventPayload, ackStop func(), announced *quotaAnnouncement) (string, bool) {
	settle := func() {
		if ackStop != nil {
			ackStop()
			ackStop = nil
		}
	}
	retry := false
	defer func() {
		if !retry {
			settle()
		}
	}()
	// fileLoss hands the stop to the ordinary loss relay, which owns its receipt:
	// the node's outbox row is not acknowledged here (hk 5780 review).
	fileLoss := func() {
		if announced != nil {
			announced.fileLoss(ctx)
			retry = true
		}
	}
	skip := func(reason string, kv ...any) (string, bool) {
		h.logger.Info("quota-stop replacement skipped", append([]any{"job", event.JobID, "node", machine, "reason", reason}, kv...)...)
		return "", false
	}
	// retryLater leaves the announcement with its node: the hub could not look or
	// act yet, nothing is claimed, and a later delivery decides.
	retryLater := func(reason string, kv ...any) (string, bool) {
		retry = true
		return skip(reason, kv...)
	}
	budget := h.quotaReplaceBudget
	if budget <= 0 {
		budget = quotaReplaceBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// The existing quota replacement also closes and writes this job. Order
	// it with pool-action admission so neither can overwrite the other's end.
	unlockJob, free := h.lockWithoutPoolAction(ctx, event.JobID)
	if !free {
		if announced != nil && h.handoverRowOwns(ctx, event.JobID, event.EventID) {
			// This announcement's own hand-over row owns the job: it has its outcome.
			return skip("handover_owned")
		}
		return retryLater("pool_action_pending")
	}
	defer unlockJob()
	store := h.inbox.store
	if announced != nil {
		// An announcement with a saved outcome is answered from it, before the
		// holder, the node or its capabilities are looked at (hk 5780 review).
		if outcome, found := h.savedStopOutcome(ctx, event); found {
			if outcome == quotaStopSettled {
				return skip("outcome_saved")
			}
			return retryLater("outcome_unavailable")
		}
	}
	holder, err := store.latestSpawnForJob(ctx, event.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		// A job the hub did not spawn has no stored submission: not one it carries on.
		fileLoss()
		return skip("unknown_job")
	}
	if err != nil {
		// An operational failure says nothing about the job: retry. Nothing is
		// filed either: a role the hub could not look up is decided when the
		// node sends the stop again, not on a guess.
		return retryLater("holder_unavailable", "error", err.Error())
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		fileLoss()
		return skip("unreadable_request")
	}
	role := prior.Role
	// An announced reviewer stop is replaced at once. An implementer only
	// announces job.lost when the quota-stop manager handed it over.
	if role != "reviewer" && role != "implementer" {
		fileLoss()
		return skip("role", "role", role)
	}
	// An announced implementer is handed over by the guarded worktree hand-over
	// (hk 4633, hk 5780), not by the close below, which has none of its checks.
	guarded := announced != nil && role == "implementer"
	// A successor already claimed for this job (a replay after a hub restart, or a
	// second delivery) is finished before anything else, and before the age gate: a
	// claimed successor that may still run needs its replaced_by write, which the
	// periodic pass would otherwise leave until its own age (hk 5690 review).
	if claimed, _, ok := quotaReplacementJob(event.JobID); ok && !guarded {
		successor, err := store.latestSpawnForJob(ctx, claimed)
		switch {
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			// One finalizer: while a successor claim may exist the stop is never
			// acknowledged as stale or replaced on a failed read.
			return retryLater("successor_lookup_unavailable", "error", err.Error())
		case err == nil:
			// A job with that name is a replacement only with the claim's own
			// lineage: replaced_from written by the claim (not the name backfill),
			// for the same owner and task. A name collision is refused and the
			// record kept; the predecessor is never closed without the lineage and a
			// verified stop sequence (hk 5690).
			predecessor := holder
			if !quotaLineageHolds(successor, predecessor, event.JobID) {
				return skip("successor_name_collision", "job", claimed)
			}
			if quotaSuccessorSent(successor) {
				app, err := h.newSpawnApp(io.Discard, io.Discard)
				if err != nil {
					return retryLater("spawn_config_unavailable")
				}
				stopSeq, _ := quotaStopSeq(event.Reason)
				if !h.finishQuotaReplacement(ctx, app, successor, event.JobID, stopSeq) {
					// The pointer is not written yet: the node keeps the announcement
					// and the next delivery tries again.
					return retryLater("successor_finish_pending", "replacement", claimed)
				}
				return skip("already_replaced", "replacement", claimed)
			}
			if spawnStatusActive(successor.Status) {
				// A claim still queued (accepted, waiting) is not terminal: the
				// worker that made it, or the periodic pass, owns it. Not
				// acknowledged, never taken for stale or replaced.
				return retryLater("successor_in_flight", "status", successor.Status)
			}
			// The claimed successor ended without being placed: no other pool could
			// take the review, and this delivery finishes the give-up.
			if predecessor.Status == "succeeded" && role == "reviewer" {
				if h.giveUpQuotaStop(ctx, predecessor, prior.Owner, event, "not_placed") {
					return retryLater("successor_give_up_pending", "replacement", claimed)
				}
			}
			return skip("already_replaced", "replacement", claimed)
		}
	}
	// The watcher ends an unchanged stopped job itself when its grace runs out,
	// and hk never rewrites a lost record: a replacement that cannot finish inside
	// what is left of the grace could place a successor after that plain loss
	// (hk 5690 review). A stop announced too long ago is left to the watcher. A
	// hand-over row owns its own close and survives that loss (it fails safe).
	if announcedAt, ok := quotaStopReasonAt(event.Reason); ok && !guarded && h.now().Sub(announcedAt) > fleet.QuotaStopHandOffGrace-budget {
		return skip("announcement_stale", "age", h.now().Sub(announcedAt).Round(time.Second))
	}
	// A task-bound job, or a reviewer that inherited its builder's
	// task (--parent-job), is replaced with the same arguments. The guarded
	// hand-over refuses a task-less job itself, with an owner decision, and drops
	// a pinned profile.
	if !guarded && (prior.Task == "" && prior.ParentJob == "" || (prior.Profile != "" && prior.Profile != "auto")) {
		return skip("not_replaceable")
	}
	// An original spawn still settling (bound, dispatching, reconciling, past its
	// deadline with a node) can still become the placed holder: keep the stop
	// retryable and look again on the next delivery. A terminal status or another
	// host is final.
	if holder.Status != "succeeded" && spawnMayStillRun(holder) {
		return retryLater("holder_unsettled", "status", holder.Status)
	}
	if holder.Status != "succeeded" || spawnHolderHost(holder) != machine {
		return skip("holder_state", "status", holder.Status)
	}
	if guarded {
		// The job's lock is held through the hand-over: it serializes overlapping
		// deliveries of one announcement.
		switch h.handOverAnnouncedImplementer(ctx, machine, event, holder, prior, announced) {
		case quotaStopSettled:
			return "", false
		case quotaStopFiled:
			retry = true // the ordinary relay owns the receipt
			return "", false
		}
		return retryLater("handover_pending")
	}
	// A reviewer the hub cannot carry on is given up: its record ends lost now
	// (not after the watcher's grace) and the owner is told once (hk 5690).
	// An implementer's record is already written before its successor is
	// claimed, and a spawn that fails tells its owner itself.
	giveUp := func(reason string, kv ...any) (string, bool) {
		if role == "reviewer" && h.giveUpQuotaStop(ctx, holder, prior.Owner, event, reason) {
			return retryLater("give_up_"+reason+"_retry", kv...)
		}
		return skip(reason, kv...)
	}
	newJob, n, ok := quotaReplacementJob(event.JobID)
	if !ok {
		return giveUp("replacement_limit", "replacements", n-1)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return retryLater("spawn_config_unavailable")
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if err != nil {
		// hk could not be read (a timeout, a 503): the job may be live and carriable,
		// so nothing is decided and the node keeps the announcement (hk 5690).
		return retryLater("job_record_unavailable", "error", err.Error())
	}
	if !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
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
	// A reviewer's close is fenced to the pane state its stop was observed in
	// (stop_seq); a stop that carries none (a row reserved before the upgrade) never
	// closes a pane and is left to the watcher's grace (hk 5690 review).
	stopSeq, fenced := quotaStopSeq(event.Reason)
	if probe.state == heldPaneUnknown || probe.state == heldPanePresent && (!quotaPaneStopped(probe.row.AgentStatus, role == "reviewer" && fenced) || !validNodeCommandPath(probe.row.Cwd)) {
		return retryLater("pane_not_idle", "probe", probe.reason, "status", probe.row.AgentStatus)
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
		if role == "reviewer" && !fenced {
			return retryLater("stop_seq_unknown", "pane", holder.Outcome.Pane)
		}
		if closed, _ := h.closeHeldPaneStopped(ctx, holder, name, probe.row, event.JobID, stopSeq); !closed {
			return retryLater("pane_not_closed", "pane", holder.Outcome.Pane)
		}
	}
	// mark writes the old record's pointer. A reviewer's pointer passes a start
	// proof read afresh before every write attempt (hk 5776): a refusal, a fence or
	// an unbind that landed since the wait read the successor leaves it unwritten.
	mark := func(proven func() bool) (lost, unproven bool) {
		// The pointer can only be written once and the watcher's fallback
		// would write a plain record, so a transient failure is retried
		// inside the grace.
		backoff := h.quotaMarkBackoff
		if backoff <= 0 {
			backoff = quotaMarkBackoff
		}
		var reason string
		for attempt := 0; attempt < quotaMarkAttempts && !lost; attempt++ {
			if attempt > 0 {
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
				}
			}
			if proven != nil && !proven() {
				reason, unproven = "start_unproven", true
				break
			}
			lost, reason = h.markHolderJobLostBecause(ctx, app, holder, name, endReason)
		}
		if !lost {
			h.logger.Warn("quota-stop replacement: the stopped job's record was not written", "job", event.JobID, "replacement", newJob, "reason", reason)
		}
		return lost, unproven
	}
	// An implementer is handed over in the order the ticket gives: the
	// record ends with handed_over_to first, then the task is spawned. The
	// spawn refuses a task still bound to a job that is not lost, so the old
	// record must end before the successor is admitted. When the spawn then
	// fails the pointer names a job that never ran, and the owner is told by
	// the spawn failure notice.
	if role != "reviewer" {
		if lost, _ := mark(nil); !lost {
			return "", false
		}
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
	task, settled, known := h.awaitSpawnOutcomeKnown(ctx, request.RequestID, wait)
	if !known && !settled {
		// The successor is claimed but its state could not be read (a store error):
		// that is unknown, not "not running". It is never given up; the stop stays
		// with its node and the next delivery finds the claimed successor and
		// finishes it (hk 5690 review).
		return retryLater("successor_state_unknown", "replacement", newJob)
	}
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
		withdrawn := false
		if !settled && !spawnMayStillRun(task) && task.Status == "accepted" {
			// A successor still queued at the wait's end could bind later and
			// run with no pointer to it. Withdraw it before the review is given
			// up (hk 5690 review).
			withdrawn, _ = h.withdrawHandoverSuccessor(ctx, task, "quota_giveup")
			if !withdrawn {
				// The withdrawal did not commit: the operation moved on, or is
				// held. The state read afresh decides, and a successor that can
				// still run is pointed to (a pointer to one that later fails is a
				// recoverable chain; a running successor nobody points to is
				// duplicate work). A given-up review needs the proof that it
				// cannot.
				if fresh, err := store.spawnTask(ctx, request.RequestID); err == nil {
					task = fresh
					outcome = task.Outcome
					settled = !spawnStatusActive(task.Status)
				}
			}
		}
		if !withdrawn && !settled && spawnStatusActive(task.Status) {
			// Still live (accepted or waiting, unwithdrawn): it may yet run, but its
			// pointer waits for a start proof (below).
			h.logger.Warn("quota-stop successor could not be withdrawn; its pointer waits for a start proof", "job", event.JobID, "replacement", newJob, "status", task.Status)
		} else if settled && outcome.RC != 0 || !settled && !spawnMayStillRun(task) {
			h.logger.Info("quota-stop replacement not placed", "job", event.JobID, "replacement", newJob, "pool", pool, "status", task.Status, "rc", outcome.RC, "reason", outcome.Reason)
			if h.giveUpQuotaStop(ctx, holder, prior.Owner, event, "not_placed") {
				retry = true
			}
			return "", false
		}
		// The pointer is written only for a successor with a start proof, read
		// afresh: one still queued, bound, refused, fenced or unbound at the wait's
		// end has none, and its job may never start (hk 5776). The announcement stays
		// with its node; a later delivery or the reconcile pass writes the pointer
		// once the proof exists, or gives the review up when the successor ends.
		proven := func() bool {
			_, ok := h.quotaStartProof(ctx, request.RequestID)
			return ok
		}
		if !proven() {
			return retryLater("successor_start_unproven", "replacement", newJob, "status", task.Status)
		}
		if !settled {
			h.logger.Warn("quota-stop replacement undecided; writing its pointer", "job", event.JobID, "replacement", newJob, "status", task.Status)
		}
		if lost, unproven := mark(proven); !lost {
			if unproven {
				// The proof held at the wait's end and was gone by a write attempt (a
				// cleaned-up refusal requeued the operation): the stop stays with its
				// node, and a later delivery finishes, or gives up, the successor.
				return retryLater("successor_start_unproven", "replacement", newJob)
			}
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
// the plain end_reason "quota_stop pool=<pool>" with reset_at=<t> only when the
// node observed the reset (hk 5777; mergewake hk 3302, no replaced_by), and the
// owner is told once. The notice's event id is the
// job's, so a repeat of the replacement (a hub restart, a second announcement)
// sends no second one. A pane that is busy or unreadable is not given up: the
// stop may be over, and the node's watcher decides when its grace runs out. The
// record and the log line are what a count of carried on and given up reads
// (docs/quota-stop.md, "The record of a carry-over").
func (h *HubServer) giveUpQuotaStop(ctx context.Context, holder hubSpawnTask, owner string, event hubJobEventPayload, why string) (retry bool) {
	if !validLaneReference(owner) {
		return false
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return true
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if err != nil {
		// hk could not be read: the give-up is not decided on a guess, and the
		// announcement stays with its node (hk 5690 review).
		return true
	}
	if !exists || (state != "claimed" && state != "spawned") {
		return false
	}
	stopPool, resetAt := quotaStopReasonFields(event.Reason)
	if stopPool == "" || !spawnargs.ValuePattern.MatchString(stopPool) {
		stopPool = pool
	}
	if !quotaStopReasonResetObserved(event.Reason) {
		// Only a reset the node observed is written. A previous-release node
		// relays its own default as reset_at (hk 5777 review), and that value
		// must not reach the immutable end_reason.
		resetAt = ""
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
	stopSeq, fenced := quotaStopSeq(event.Reason)
	switch {
	case probe.state == heldPanePresent && quotaPaneStopped(probe.row.AgentStatus, fenced) && validNodeCommandPath(probe.row.Cwd):
		if !fenced {
			// No verified stop sequence: this close is never made; the
			// announcement stays with its node and the watcher's grace ends it.
			return true
		}
		if closed, _ := h.closeHeldPaneStopped(ctx, holder, name, probe.row, event.JobID, stopSeq); !closed {
			return false
		}
	case probe.state != heldPaneAbsent:
		return false
	}
	if lost, _ := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); !lost {
		h.logger.Warn("quota-stop give-up: the stopped job's record was not written", "job", event.JobID, "reason", why)
		return false
	}
	h.logger.Info("quota-stop carry-over", "outcome", "given_up", "job", event.JobID, "role", "reviewer", "pool", stopPool, "reason", why)
	text := fmt.Sprintf("gave up %s (%s quota): no other pool could take the review (%s)", event.JobID, stopPool, why)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "quota-giveup:" + event.JobID, JobID: event.JobID, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1, Notice: ownerNoticeFor(event.JobID, "quota_giveup", true)}, nil)
	return false
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
	task, settled, _ := h.awaitSpawnOutcomeKnown(ctx, requestID, limit)
	return task, settled
}

// awaitSpawnOutcomeKnown is awaitSpawnOutcome that also says whether the last read
// of the operation worked. A store error leaves the last status read (the operation
// is not "not running" because it could not be read): known is false then, and the
// caller treats the state as unknown (hk 5690).
func (h *HubServer) awaitSpawnOutcomeKnown(ctx context.Context, requestID string, limit time.Duration) (task hubSpawnTask, settled, known bool) {
	poll := h.quotaReplacePoll
	if poll <= 0 {
		poll = quotaReplacePoll
	}
	deadline := time.Now().Add(limit)
	for {
		read, err := h.inbox.store.spawnTask(ctx, requestID)
		known = err == nil
		if known {
			task = read
		}
		// accepted, dispatching, reconciling and deadline_exceeded are the
		// states the coordinator still retries (pendingSpawns), and waiting is
		// held by the start-condition queue; a node may yet complete the
		// successor from reconciling. Only succeeded, failed and retryable are
		// final.
		if known && !spawnStatusActive(task.Status) {
			return task, true, true
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return task, false, known
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return task, false, known
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
		// A successor that is placed or was sent to a node, at any age: a restart
		// between the claim and the pointer is repaired well inside the watcher's
		// grace instead of after the replacement budget. A claim still queued
		// (accepted, waiting), only bound (never sent) or ended unplaced is not a
		// pointer target: it may never start, and the replay or the give-up owns it
		// (hk 5690).
		if !quotaSuccessorSent(successor) {
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

// quotaSuccessorSent reports whether a claimed successor's snapshot is placed or
// was sent to a node. It is necessary for a replaced_by pointer, never enough: a
// snapshot goes stale, and a sent row may be refused, fenced or unbound. Only
// quotaStartProof, which reads the row afresh, decides a pointer (hk 5776).
func quotaSuccessorSent(successor hubSpawnTask) bool {
	if successor.Status == "succeeded" {
		return true
	}
	if successor.Node == nil {
		return false
	}
	switch successor.Status {
	case "dispatching", "reconciling", "deadline_exceeded":
		return true
	}
	return false
}

// quotaStartProof is the one gate of a replaced_by pointer (hk 5776). It reads the
// successor's operation afresh, so a snapshot taken before the lock or the wait
// never decides, and answers true only when the successor started or must:
//
//   - the operation succeeded (placed, its job landed), or
//   - it is bound to a node, was sent (dispatching, reconciling or past its
//     deadline with that node), and carries the start receipt: the node it is
//     bound to reported that this node request id landed (job.spawn.started,
//     sent from the durable landing point of that execution), and, for a
//     grant-required attempt, the hub granted that id. The state, the receipt and
//     the grant are read in one statement (spawnStartReceipt).
//
// The receipt names the attempt: it is keyed by the unique node request id the
// hub bound, accepted only from that machine while the attempt is sent, and a
// bind clears it. A job-name heartbeat (claimed_ms) or an hk record found by job
// id is no evidence: either can come from a node-local job under the generated
// successor name. A fenced attempt never reports a landing. An older node sends
// no receipt, so its successor has a proof only once its result succeeded. A row
// that is accepted, waiting, only bound, refused or unbound has no proof,
// whatever an earlier read said. The dispatch worker holds the request's action
// fence for the whole attempt, so this gate cannot share that lock. An
// unreadable row is no proof.
func (h *HubServer) quotaStartProof(ctx context.Context, requestID string) (hubSpawnTask, bool) {
	if h.inbox == nil {
		return hubSpawnTask{}, false
	}
	task, err := h.inbox.store.spawnTask(ctx, requestID)
	if err != nil {
		return hubSpawnTask{}, false
	}
	if task.Status == "succeeded" {
		return task, true
	}
	if !quotaSuccessorSent(task) {
		return task, false
	}
	claimed, err := h.inbox.store.spawnStartReceipt(ctx, requestID, task.Attempt, task.Node.RequestID)
	return task, err == nil && claimed
}

// reconcileQuotaReplacement finishes one claimed successor's pointer. It never
// waits for the predecessor's lock: a replacement that holds it is working the job,
// and the pass revisits it next time, so the jobs behind it are reached before
// their grace ends. The successor is read again by finishQuotaReplacement, since
// the snapshot the scan took may have been withdrawn since (hk 5690 review).
func (h *HubServer) reconcileQuotaReplacement(ctx context.Context, app *fleet.App, successor hubSpawnTask, old string) bool {
	unlock, free := h.tryLockWithoutPoolAction(ctx, old)
	if !free {
		return false
	}
	defer unlock()
	return h.finishQuotaReplacement(ctx, app, successor, old, nil)
}

// finishQuotaReplacement writes the replaced_by record for a claimed successor
// whose predecessor is still live. The caller holds the job's pool-action lock.
//
// successor is only the caller's snapshot: it names the request, and the start
// proof is read afresh from the store, before the predecessor's pane is touched
// and again right before the write, which comes after a pane probe and close that
// can take seconds (hk 5776).
func (h *HubServer) finishQuotaReplacement(ctx context.Context, app *fleet.App, successor hubSpawnTask, old string, stopSeq *int64) bool {
	// The one rule for a pointer, whoever calls: the successor has a start proof.
	successor, proven := h.quotaStartProof(ctx, successor.Request.RequestID)
	if !proven {
		return false
	}
	holder, err := h.inbox.store.latestSpawnForJob(ctx, old)
	if err != nil || holder.Status != "succeeded" || !quotaLineageHolds(successor, holder, old) {
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
	case probe.state == heldPanePresent && quotaPaneStopped(probe.row.AgentStatus, stopSeq != nil) && validNodeCommandPath(probe.row.Cwd):
		// The predecessor's pane is closed only fenced to a verified stop sequence;
		// the periodic pass has none and leaves a present pane alone (hk 5690).
		if stopSeq == nil {
			return false
		}
		if closed, _ := h.closeHeldPaneStopped(ctx, holder, name, probe.row, old, stopSeq); !closed {
			return false
		}
	case probe.state != heldPaneAbsent:
		return false
	}
	if _, proven := h.quotaStartProof(ctx, successor.Request.RequestID); !proven {
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
