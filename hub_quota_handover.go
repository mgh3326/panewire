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

// handOverAnnouncedImplementer routes the stop the node announced for holder, an
// implementer the hub spawned on machine. It reports whether the stop is settled
// (the node may drop it from its outbox): the hand-over row is stored, the owner
// has the one decision, or there is nothing left to do; false leaves it with the
// node to send again. A node without the hand-over capabilities cannot be
// checked, so its stop is filed as a loss as it was before hk 5780.
func (h *HubServer) handOverAnnouncedImplementer(ctx context.Context, machine string, event hubJobEventPayload, holder hubSpawnTask, prior fleet.Spawn, announced *quotaAnnouncement) bool {
	const role = "implementer"
	if h.spawnGrantDrain || h.spawnRollbackDrain || h.inbox.store.spawnFrozen {
		// No successor may be created while grant rows drain (hk 4661).
		return false
	}
	if !h.nodeHasCapability(machine, hubCapabilityCloseOps) || !h.nodeHasCapability(machine, hubCapabilityHandoverCheck) {
		h.logger.Info("quota-stop hand-over needs a node with close ops and the hand-over check", "job", event.JobID, "node", machine)
		announced.fileLoss(ctx)
		return true
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return false
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, event.JobID)
	cancel()
	if err != nil {
		// hk could not be read: the job may be live and carriable.
		return false
	}
	state, _ := record["state"].(string)
	pool, _ := record["pool"].(string)
	if !exists || (state != "claimed" && state != "spawned") || !spawnargs.ValuePattern.MatchString(pool) {
		// The record is no longer live: nothing is left to hand over.
		h.logger.Info("quota-stop hand-over skipped", "job", event.JobID, "node", machine, "reason", "job_record", "state", state)
		return true
	}
	if owner, err := h.inbox.store.poolActionOwns(ctx, holder.Job); err != nil {
		return false
	} else if owner != "" {
		// One row per job: a hand-over (or a close) already owns it.
		return true
	}
	// The effective task: a --parent-job successor inherits it, so the job
	// record names it, not the submission.
	taskID := prior.Task
	if id := hkRecordTaskID(record); id != "" {
		taskID = id
	}
	// The stop's own band is the pool the successor leaves; an account-scoped
	// stop leaves only the account (handoverExclude).
	exhaustion := poolExhaustion{Pool: pool, Account: quotaStopReasonAccount(event.Reason), Cause: poolCauseQuotaStop}
	if stopPool, _ := quotaStopReasonFields(event.Reason); stopPool != "" && spawnargs.ValuePattern.MatchString(stopPool) {
		exhaustion.Band = fleet.BandKey(stopPool)
	}
	var result poolJobResult
	if in := h.readHandoverInputs(ctx, taskID); in.bound != holder.Job {
		// Another job holds the task now; replacing this one would start a second
		// implementer beside it. An unreadable binding is not proof.
		result = h.notifyPoolExhausted(ctx, exhaustion, holder, role, "task_rebound")
	} else {
		result = h.queueImplementerHandover(ctx, exhaustion, holder, prior, app, record, taskID)
	}
	h.logger.Info("quota-stop implementer hand-over", "job", event.JobID, "node", machine, "action", result.action, "reason", result.why)
	return result.acted() || result.why == "already_replaced"
}
