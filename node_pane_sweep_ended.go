package panewire

import (
	"context"
	"log/slog"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// nodeCommandPaneSweep is the hub's ended-job sweep (hk 4512, docs/pane-sweep.md):
// the hub asks each node to close the panes of jobs that ended at least age
// ago, whatever herdr shows them doing. Only the hub sends it, on its own timer;
// authorizeNodeCommand refuses it on the public endpoint.
const nodeCommandPaneSweep = "pane.sweep"

// nodePaneSweepMaxAgeSeconds bounds age_s to a year. No configured age nears
// it, and the bound keeps the duration conversion from overflowing.
const nodePaneSweepMaxAgeSeconds = int64(365 * 24 * 60 * 60)

// nodePaneSweepArgs carries the age, in seconds, past which an ended job's pane
// may stay open.
type nodePaneSweepArgs struct {
	AgeS int64 `json:"age_s"`
}

// nodePaneSweepResult lists the panes this pass closed, in close order. The
// list is empty, not null, when nothing closed.
type nodePaneSweepResult struct {
	Closed []fleet.SweptPane `json:"closed"`
}

func validNodePaneSweepArgs(args nodePaneSweepArgs) bool {
	return args.AgeS >= 0 && args.AgeS <= nodePaneSweepMaxAgeSeconds
}

func validNodePaneSweepResult(result nodePaneSweepResult) bool {
	if result.Closed == nil {
		return false
	}
	for _, swept := range result.Closed {
		if !validNodeJobName(swept.Job) || !nodeCommandPaneIDPattern.MatchString(swept.Pane) || swept.Role == "" || swept.Reason == "" {
			return false
		}
	}
	return true
}

// nodePaneSweep runs one ended-job pass. It holds taskPaneSweepMu, as the local
// pass does, and never nodeCommandMu: each close takes nodeCommandMu through the
// sweeper's Exclusive, so holding it here would deadlock.
func (client *HubClient) nodePaneSweep(ctx context.Context, args nodePaneSweepArgs) (any, string) {
	if client.jobsInboxRoot == "" {
		return nil, "jobs_root_unset"
	}
	sweeper := client.nodeTaskPaneSweeper()
	client.taskPaneSweepMu.Lock()
	defer client.taskPaneSweepMu.Unlock()
	app, reason := client.taskPaneSweepApp(sweeper)
	if reason != "" {
		slog.Warn("pane sweep skipped", "reason", reason)
		return nil, "sweep_failed"
	}
	closed, err := sweeper.SweepEnded(ctx, app, time.Duration(args.AgeS)*time.Second)
	if err != nil {
		slog.Warn("pane sweep failed", "reason", "pass-failed")
		return nil, "sweep_failed"
	}
	if closed == nil {
		closed = []fleet.SweptPane{}
	}
	return nodePaneSweepResult{Closed: closed}, ""
}
