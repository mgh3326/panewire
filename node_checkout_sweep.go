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
