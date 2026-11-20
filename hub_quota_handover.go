package panewire

import (
	"context"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// An implementer's announced stop goes through the guarded worktree hand-over
// (hk 5780; docs/quota-stop.md "Implementer"). The node's quota-stop manager
// decides a screen stop (a Command Code window limit, say) is a hand-over and
// announces it with job.lost reason quota_stop; nothing else drives the hub's
// pool sweep for it, so before hk 5780 the watcher wrote a plain loss after its
// grace and no successor was placed. The legacy close the reviewer takes has
// none of the hk 4633 checks (dirty or unpushed work), so an implementer never
// takes it: it gets the hand-over row of hub_pool_handover.go, which continues a
// clean, pushed branch on another host, saves dirty or unpushed work as a
// verified snapshot where the node offers handover_snapshot_v1, and otherwise
// refuses with one owner decision and leaves the pane open.

// quotaStopOutcome is what the guarded hand-over made of an announced stop.
type quotaStopOutcome int

const (
	// quotaStopRetry: nothing durable yet; the node keeps the stop and sends it again.
	quotaStopRetry quotaStopOutcome = iota
	// quotaStopSettled: the stop has a durable outcome of its own (the hand-over
	// row, the owner's decision, or nothing left to hand over): the node may drop it.
	quotaStopSettled
	// quotaStopFiled: the stop was handed to the ordinary loss relay, which owns
	// its receipt; it is not acknowledged as a control event (hk 5780 review).
	quotaStopFiled
)

// handOverAnnouncedImplementer routes the stop the node announced for holder, an
// implementer the hub spawned on machine. A node that cannot close a pane at the
// stop's own sequence (no pane_close_op_stop_seq_v1, or a stop that names none)
// cannot prove the pane is still the stopped one, and its stop is filed as a
// loss as it was before hk 5780; so is a node without the hand-over
// capabilities.
func (h *HubServer) handOverAnnouncedImplementer(ctx context.Context, machine string, event hubJobEventPayload, holder hubSpawnTask, prior fleet.Spawn, announced *quotaAnnouncement) quotaStopOutcome {
	const role = "implementer"
	if h.spawnGrantDrain || h.spawnRollbackDrain || h.inbox.store.spawnFrozen {
		// No successor may be created while grant rows drain (hk 4661).
		return quotaStopRetry
	}
	stopSeq, fenced := quotaStopSeq(event.Reason)
	if !fenced || !h.nodeHasCapability(machine, hubCapabilityCloseOps) || !h.nodeHasCapability(machine, hubCapabilityHandoverCheck) || !h.nodeHasCapability(machine, hubCapabilityCloseOpStopSeq) {
		h.logger.Info("quota-stop hand-over needs a node with close ops, the hand-over check and a stop-sequence fence", "job", event.JobID, "node", machine, "fenced", fenced)
		announced.fileLoss(ctx)
		return quotaStopFiled
	}
	// A resend finds its own outcome before anything is probed or prepared: the
	// worktree may have changed since the first delivery (an owner who read the
	// refusal committed and pushed), and a second answer would contradict it.
	owner := poolNoticeOwner(holder)
	noticeID := poolStopNoticeID(holder.Job, event.EventID)
	if event.EventID != "" {
		rows, err := h.inbox.store.poolActionsOf(ctx, holder.Job)
		if err != nil {
			return quotaStopRetry
		}
		for _, row := range rows {
			if handover := row.Data.Handover; row.Data.Kind == poolActionKindHandover && handover != nil && handover.StopEvent == event.EventID {
				return quotaStopSettled
			}
		}
		if owned, err := h.inbox.store.poolNoticeOwned(ctx, owner, noticeID); err != nil {
			return quotaStopRetry
		} else if owned {
			return quotaStopSettled
		}
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return quotaStopRetry
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	if err != nil {
		// hk could not be read: the job may be live and carriable.
		return quotaStopRetry
	}
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
		// The record is no longer live: nothing is left to hand over.
		h.logger.Info("quota-stop hand-over skipped", "job", event.JobID, "node", machine, "reason", "job_record", "state", state)
		return quotaStopSettled
	}
	// The effective task: a --parent-job successor inherits it, so the job
	// record names it, not the submission.
	taskID := prior.Task
	if id := hkRecordTaskID(record); id != "" {
		taskID = id
	}
	// The stop's own band is the pool the successor leaves; an account-scoped
	// stop leaves only the account (handoverExclude).
	exhaustion := poolExhaustion{Pool: pool, Account: quotaStopReasonAccount(event.Reason), Cause: poolCauseQuotaStop, StopSeq: stopSeq, StopEvent: event.EventID}
	if stopPool, _ := quotaStopReasonFields(event.Reason); stopPool != "" && spawnargs.ValuePattern.MatchString(stopPool) {
		exhaustion.Band = fleet.BandKey(stopPool)
	}
	in := h.readHandoverInputs(ctx, taskID)
	if in.unreadable {
		// A read that failed says nothing about the binding: only a confirmed
		// rebind is refused.
		return quotaStopRetry
	}
	var result poolJobResult
	if in.bound != holder.Job {
		// Another job holds the task now; replacing this one would start a second
		// implementer beside it.
		result = h.notifyPoolExhaustedAs(ctx, exhaustion, holder, role, "task_rebound", noticeID)
	} else {
		result = h.queueImplementerHandover(ctx, exhaustion, holder, prior, app, record, taskID)
	}
	h.logger.Info("quota-stop implementer hand-over", "job", event.JobID, "node", machine, "action", result.action, "reason", result.why)
	if result.acted() || result.why == "already_replaced" {
		return quotaStopSettled
	}
	return quotaStopRetry
}

// handoverRowOwns reports whether a hand-over row that is not final owns job: a
// resend of the announcement that opened it, or a later one, has its outcome.
func (h *HubServer) handoverRowOwns(ctx context.Context, job string) bool {
	rows, err := h.inbox.store.poolActionsOf(ctx, job)
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.Data.Kind == poolActionKindHandover && row.Phase != poolActionDone && row.Phase != poolActionRefused {
			return true
		}
	}
	return false
}
