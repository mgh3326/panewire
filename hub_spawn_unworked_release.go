package panewire

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A spawn whose pane exists but whose brief never landed (rc 75, or any other
// landing-stage failure) leaves an empty pane and a claimed hk job. Only an
// operator closing that pane released the (task, role) reservation, and a
// reviewer spawn for the same task was refused `held` until then (hk 3179).
//
// A new submission releases such a holder when every check below proves the
// pane never started work. A check that fails or cannot be read keeps the
// reservation, so the submitter still gets the "close pane …" fix.

const (
	// defaultUnworkedLandingGrace is how long after the failed spawn's deadline
	// an idle pane may still be waiting for a late brief.
	defaultUnworkedLandingGrace = 10 * time.Minute

	unworkedSpawnReason = "failed_spawn_unworked"
	// spawnHeldPaneActWait bounds one acting node command; pane.close alone
	// spends nodePaneCloseSettle on its two re-reads.
	spawnHeldPaneActWait = hubNodeCommandWait
)

// failedBeforeBrief reports a failed launch or landing where brief delivery
// was never proven. An rc 75 outside those stages is stored as reconciling,
// which never reaches a reclaim.
func failedBeforeBrief(holder hubSpawnTask) bool {
	return holder.Status == "failed" && (holder.Outcome.Stage == "launch" || holder.Outcome.Stage == "landing")
}

// nodeCall sends one node command and decodes its result into out. It reports
// false for an unreachable node, a node error, a late reply or an unreadable
// result, so every caller reads "could not tell" as "keep the reservation".
func (h *HubServer) nodeCall(ctx context.Context, machine, op string, args any, wait time.Duration, out any) bool {
	raw, err := json.Marshal(args)
	if err != nil {
		return false
	}
	pending, id, _, sent := h.sendNodeCommand(machine, op, raw)
	if !sent {
		return false
	}
	defer h.forgetNodeCommand(id)
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	select {
	case reply := <-pending.done:
		return reply.Error == "" && json.Unmarshal(reply.Result, out) == nil
	case <-ctx.Done():
		return false
	}
}

// closeHeldPaneAndMarkLost saves the holder's pane screen, closes the pane and
// marks its hk job lost, all through the holder's node. pane.close re-reads the
// pane itself and refuses one that changed, so it runs before the irreversible
// hk write; a refused or unreadable step returns false with the node's reason.
func (h *HubServer) closeHeldPaneAndMarkLost(ctx context.Context, app *fleet.App, holder hubSpawnTask, name string, row nodeAgentRow, questionJob string) (bool, string) {
	return h.closeHeldPaneAndMarkLostBecause(ctx, app, holder, name, row, "", questionJob)
}

// closeHeldPaneAndMarkLostBecause is closeHeldPaneAndMarkLost with the hk
// end_reason the lost record takes.
func (h *HubServer) closeHeldPaneAndMarkLostBecause(ctx context.Context, app *fleet.App, holder hubSpawnTask, name string, row nodeAgentRow, endReason, questionJob string) (bool, string) {
	if closed, reason := h.closeHeldPane(ctx, holder, name, row, questionJob); !closed {
		return false, reason
	}
	return h.markHolderJobLostBecause(ctx, app, holder, name, endReason)
}

// closeHeldPane saves the holder's pane screen and closes the pane through the
// holder's node. pane.close re-reads the pane and refuses one that changed, so
// a true result means the pane is closed or already gone.
func (h *HubServer) closeHeldPane(ctx context.Context, holder hubSpawnTask, name string, row nodeAgentRow, questionJob string) (bool, string) {
	machine := spawnHolderHost(holder)
	var captured nodePaneCaptureResult
	capture := nodePaneCaptureArgs{PaneID: row.PaneID, Name: name, Cwd: row.Cwd, AllowUnnamed: row.Name == "", MergedAfter: 0, Write: true}
	if !h.nodeCall(ctx, machine, nodeCommandPaneCapture, capture, spawnHeldPaneActWait, &captured) || captured.Outcome != nodeCaptureScreen {
		return false, "capture_failed"
	}
	var closed nodePaneCloseResult
	closeArgs := nodePaneCloseArgs{PaneID: row.PaneID, Name: name, AgentStatus: row.AgentStatus, Cwd: row.Cwd, AllowUnnamed: row.Name == "", QuestionJob: questionJob}
	if !h.nodeCall(ctx, machine, nodeCommandPaneClose, closeArgs, spawnHeldPaneActWait, &closed) {
		return false, "close_unreadable"
	}
	switch closed.Outcome {
	case nodePaneClosed, nodePaneGone:
		return true, ""
	case nodePaneQuestionOpen:
		return false, "question_open"
	}
	return false, "pane_changed"
}

// releaseUnworkedSpawn closes the empty pane of a failed-before-brief holder
// and marks its hk job lost, so the caller can release the reservation. Nothing
// is closed or written unless every proof holds. pane.close re-reads the pane
// itself and refuses a pane that went to work in the meantime, so it runs
// before the irreversible hk write.
func (h *HubServer) releaseUnworkedSpawn(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, row nodeAgentRow) (released bool, markReason string) {
	machine, checkout := spawnHolderHost(holder), holder.Request.Repository.Checkout
	name := hubSpawnArg(holder.Request.Args, "-l")
	if state, _ := record["state"].(string); state != "claimed" || !failedBeforeBrief(holder) || checkout == nil || machine == "" || !validNodeJobName(name) || !validNodeJobName(holder.Job) {
		return false, ""
	}
	if !nodePaneIdleStatus[row.AgentStatus] || !spawnReceiptCheckoutPath(row.Cwd, checkout.Dir, hubSpawnArg(holder.Request.Args, "-L")) || !validNodeCommandPath(row.Cwd) {
		return false, ""
	}
	if h.now().Before(holder.Deadline.Add(h.unworkedLandingGrace)) {
		return false, ""
	}
	var ledger nodeJobResult
	if !h.nodeCall(ctx, machine, nodeCommandJobResult, nodeJobResultArgs{Job: holder.Job}, spawnHeldPaneProbeWait, &ledger) {
		return false, ""
	}
	// A nil count is a node that predates the field: nothing is proven.
	if !ledger.Found || ledger.Completed || ledger.Report.Present || ledger.ActivityEvents == nil || *ledger.ActivityEvents != 0 {
		return false, ""
	}
	if marked, reason := h.closeHeldPaneAndMarkLost(ctx, app, holder, name, row, ""); !marked {
		return false, reason
	}
	slog.Info("released an unworked spawn failure", "job", holder.Job, "request", holder.Request.RequestID, "machine", machine, "pane", row.PaneID, "reason", unworkedSpawnReason)
	return true, ""
}
