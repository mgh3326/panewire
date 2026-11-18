package panewire

import (
	"context"
	"log/slog"
	"path/filepath"
	"regexp"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// nodeCommandCheckoutSweep is the operator's checkout sweep (hk 5091,
// docs/pane-sweep.md): it lists the *-checkout worktrees of the node's clones
// under its work root whose job is not live and removes them through the same
// guarded path the pane sweep's worktree tail uses. dry_run reports without
// changing anything.
const nodeCommandCheckoutSweep = "checkout.sweep"

const (
	// nodeCheckoutSweepRunTimeout bounds the whole pass; it outlasts the
	// default command budget because a host can hold many stale checkouts,
	// and stays under the hub's wait (hubCheckoutSweepWait) so the node's
	// answer arrives first.
	nodeCheckoutSweepRunTimeout = 90 * time.Second
	// hubCheckoutSweepWait is the hub endpoint's wait for this op, longer
	// than the node's run budget and shorter than the CLI client's.
	hubCheckoutSweepWait = 2 * time.Minute
)

type nodeCheckoutSweepArgs struct {
	DryRun bool `json:"dry_run"`
}

// nodeCheckoutSweepResult lists every *-checkout worktree found, in path
// order, with its verdict; Checkouts is empty, not null, when there are none.
type nodeCheckoutSweepResult struct {
	DryRun    bool                     `json:"dry_run"`
	Checkouts []fleet.CheckoutSweepRow `json:"checkouts"`
}

var nodeCheckoutSweepReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func validNodeCheckoutSweepResult(result nodeCheckoutSweepResult) bool {
	if result.Checkouts == nil {
		return false
	}
	for _, row := range result.Checkouts {
		if !validNodeCommandPath(row.Path) || (row.Job != "" && !validNodeJobName(row.Job)) ||
			(row.Reason != "" && !nodeCheckoutSweepReasonPattern.MatchString(row.Reason)) {
			return false
		}
		switch row.Outcome {
		case "kept":
		case "would_remove":
			if !result.DryRun {
				return false
			}
		case "removed":
			if result.DryRun {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// nodeCheckoutSweep runs one pass. It holds nodeCommandMu for the whole pass:
// the removals are git mutations that must not interleave with another acting
// node command, and a manual sweep is rare enough that the queue is fine.
func (client *HubClient) nodeCheckoutSweep(ctx context.Context, args nodeCheckoutSweepArgs) (any, string) {
	if client.jobsInboxRoot == "" {
		return nil, "jobs_root_unset"
	}
	workRoot, err := client.nodeWorkRoot()
	if err != nil {
		return nil, "work_root_unset"
	}
	app, err := fleet.New(slogWarnWriter{}, slogWarnWriter{})
	if err != nil {
		return nil, "sweep_failed"
	}
	app.Jobs = filepath.Join(client.jobsInboxRoot, "jobs")
	app.HerdrSession = client.nodeSpawnSession()
	owners := client.nodeCheckoutOrphanOwners(ctx)
	rows, err := (&fleet.CheckoutSweep{
		WorkRoot: workRoot,
		DryRun:   args.DryRun,
		OrphanOwner: func(dir string) string {
			return owners[nodeResolvedPath(dir)]
		},
		Info: func(message string, attrs ...any) { slog.Info(message, attrs...) },
	}).Pass(ctx, app)
	if err != nil {
		slog.Warn("checkout sweep failed", "reason", "pass-failed")
		return nil, "sweep_failed"
	}
	if rows == nil {
		rows = []fleet.CheckoutSweepRow{}
	}
	return nodeCheckoutSweepResult{DryRun: args.DryRun, Checkouts: rows}, ""
}

// nodeCheckoutRevisitInterval paces the daily kept-worktree revisit (hk 5117,
// track A2): once a day the node runs the node sweep-checkouts logic itself
// over the worktrees the sweep tail kept, through the same guarded removal,
// and never touches a worktree in use. The manual command stays.
const nodeCheckoutRevisitInterval = 24 * time.Hour

// runCheckoutRevisit runs the kept-worktree revisit once a day, starting with
// one pass at node start like the quota-pool reconcile does. Every pass holds
// nodeCommandMu: the removals are git mutations that must not interleave with
// another acting node command (a pane close works through the same lock), and
// a manual sweep is rare enough that the queue is fine. The lock wraps the
// pass seam too, so a test pass observes the same serialization.
func (client *HubClient) runCheckoutRevisit(ctx context.Context) {
	if client.jobsInboxRoot == "" {
		return
	}
	pass := client.checkoutRevisitPass
	if pass == nil {
		pass = client.revisitKeptWorktrees
	}
	guarded := func(ctx context.Context) {
		passCtx, cancel := context.WithTimeout(ctx, nodeCheckoutSweepRunTimeout)
		defer cancel()
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		pass(passCtx)
	}
	guarded(ctx)
	ticks := client.checkoutRevisitTicks
	if ticks == nil {
		ticker := time.NewTicker(nodeCheckoutRevisitInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			guarded(ctx)
		}
	}
}

// revisitKeptWorktrees runs one non-dry-run checkout sweep and counts it in
// the worktree flow mechanism: one item per judged *-checkout worktree, out
// when the guarded removal takes it. A failure counts in the day's errors.
// runCheckoutRevisit serializes it against other acting node commands; a
// direct caller holds nodeCommandMu itself.
func (client *HubClient) revisitKeptWorktrees(ctx context.Context) {
	// The ledger ticks at the top of every pass: the tick is the liveness
	// signal between the daily removals.
	client.nodeLoopTick(nodeWorktreeFlow)
	result, code := client.nodeCheckoutSweep(ctx, nodeCheckoutSweepArgs{DryRun: false})
	if code != "" {
		client.nodeLoopNoteError(nodeWorktreeFlow)
		return
	}
	rows, ok := result.(nodeCheckoutSweepResult)
	if !ok {
		client.nodeLoopNoteError(nodeWorktreeFlow)
		return
	}
	for _, row := range rows.Checkouts {
		// A checkout kept for a live user leaves on its own when that work
		// ends, so it never enters the flow: counting it would trip the
		// progress rule on a healthy node with many active checkouts.
		if row.Outcome == "kept" && fleet.CheckoutSweepLiveKeep(row.Reason) {
			continue
		}
		id := row.Job
		if id == "" {
			id = row.Path
		}
		client.nodeLoopNoteMade(nodeWorktreeFlow, id)
		if row.Outcome == "removed" {
			client.nodeLoopNoteDone(nodeWorktreeFlow, id)
		}
	}
}

// nodeCheckoutOrphanOwners attributes a *-checkout directory to its job when
// the job's local record is gone, from the spawn journal's completed results.
// A node without a readable journal attributes nothing and the sweep keeps
// the orphans it cannot prove.
func (client *HubClient) nodeCheckoutOrphanOwners(ctx context.Context) map[string]string {
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		return nil
	}
	defer client.releaseSpawnJournal()
	owners, err := journal.checkoutOwners(ctx)
	if err != nil {
		slog.Warn("checkout sweep: spawn journal unreadable; orphans stay unattributed")
		return nil
	}
	return owners
}
