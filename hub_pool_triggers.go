package panewire

import (
	"context"
	"io"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// Two triggers start the exhausted-pool sweep (hub_pool_exhausted.go) for a
// pool that publishes no quota reading of its own (hk 4625; docs/quota-stop.md,
// "An exhausted pool"):
//
//   - every catalog row of the pool is retired while live jobs still run on it;
//   - a job's pane on the pool shows the provider's credit-exhausted rejection.
//
// Each pass reads the catalog and the live jobs' panes, so a pool that comes
// back (a row un-retired) re-arms both triggers.

const (
	// defaultPoolTriggerInterval is how often a running hub looks for the two
	// pool triggers; a zero interval keeps them off.
	defaultPoolTriggerInterval = 10 * time.Minute
	// poolTriggerScreenLines is the visible tail a job's pane is read with; the
	// quota-stop reader reads 60 lines too.
	poolTriggerScreenLines = 60
	// poolTriggerNodeWait bounds each node command one pass sends.
	poolTriggerNodeWait = 5 * time.Second
)

// runPoolTriggers checks the pool triggers once at startup and then once per
// poolTriggerInterval, until ctx ends. The first pass runs at startup so a hub
// restart notifies the owners of jobs left on an exhausted pool.
func (h *HubServer) runPoolTriggers(ctx context.Context) {
	if h.poolTriggerInterval <= 0 {
		return
	}
	ticker := time.NewTicker(h.poolTriggerInterval)
	defer ticker.Stop()
	for {
		h.checkPoolTriggers(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// checkPoolTriggers starts the sweep for every live pool with a retired
// catalog or a credit-exhausted job pane.
func (h *HubServer) checkPoolTriggers(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		h.logger.Warn("pool trigger could not list live jobs", "error", err)
		return
	}
	pools := liveSpawnPools(tasks)
	if len(pools) == 0 {
		return
	}
	h.checkRetiredPools(ctx, pools)
	for _, pool := range h.creditExhaustedPools(ctx, tasks) {
		h.startPoolSweep(poolExhaustion{Pool: pool, Cause: poolCauseCreditExhausted})
	}
}

// liveSpawnPools is the sorted set of pools a live job was placed on.
func liveSpawnPools(tasks []hubSpawnTask) []string {
	seen := map[string]bool{}
	var pools []string
	for _, task := range tasks {
		if task.Status != "succeeded" || task.Outcome.Pool == "" || seen[task.Outcome.Pool] {
			continue
		}
		seen[task.Outcome.Pool] = true
		pools = append(pools, task.Outcome.Pool)
	}
	slices.Sort(pools)
	return pools
}

// checkRetiredPools fires the retired cause for every live pool whose catalog
// rows are all retired, and clears both of a pool's held triggers once a row is
// live again.
func (h *HubServer) checkRetiredPools(ctx context.Context, pools []string) {
	rows, err := h.poolCatalogRows(ctx)
	if err != nil {
		h.logger.Debug("pool trigger catalog read failed", "error", err)
		return
	}
	for _, pool := range pools {
		found, live := catalogPool(rows, pool)
		switch {
		case !found:
			// A pool the catalog does not list says nothing either way.
		case live:
			// A row is usable again: the pool came back, so both of its
			// triggers re-arm.
			retiredKey := poolExhaustion{Pool: pool, Cause: poolCauseRetired}.key()
			if h.poolSweep.held(retiredKey) {
				h.poolSweep.forget(retiredKey)
				h.poolSweep.forget(poolExhaustion{Pool: pool, Cause: poolCauseCreditExhausted}.key())
			}
		default:
			h.startPoolSweep(poolExhaustion{Pool: pool, Cause: poolCauseRetired})
		}
	}
}

// catalogPool reports whether the catalog lists pool at all, and whether any
// of its rows is still live (not retired).
func catalogPool(rows []fleet.CatalogRow, pool string) (found, live bool) {
	for _, row := range rows {
		if row.Pool != pool {
			continue
		}
		found = true
		if row.RetiredAt == nil {
			live = true
		}
	}
	return found, live
}

// poolCatalogRows is the bench catalog as the trigger reads it: a stale catalog
// is refused, so a notice never rests on a day-old row set.
func (h *HubServer) poolCatalogRows(ctx context.Context) ([]fleet.CatalogRow, error) {
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return nil, err
	}
	return app.CatalogRows(ctx)
}

// creditExhaustedPools reads the pane of every live job and collects the pools
// whose provider credit-exhausted rejection it shows. A pane is read only when
// the job recorded a host and pane, and the rejection must name the job's own
// pool, so one pool's closure never reaches another pool's jobs.
func (h *HubServer) creditExhaustedPools(ctx context.Context, tasks []hubSpawnTask) []string {
	byMachine := map[string][]hubSpawnTask{}
	for _, task := range tasks {
		if task.Status != "succeeded" || task.Outcome.Pool == "" || task.Outcome.Pane == "" {
			continue
		}
		if machine := spawnHolderHost(task); machine != "" {
			byMachine[machine] = append(byMachine[machine], task)
		}
	}
	machines := make([]string, 0, len(byMachine))
	for machine := range byMachine {
		machines = append(machines, machine)
	}
	slices.Sort(machines)
	exhausted := map[string]bool{}
	for _, machine := range machines {
		panes := h.nodePaneRows(ctx, machine)
		if panes == nil {
			continue
		}
		for _, task := range byMachine[machine] {
			row, listed := panes[task.Outcome.Pane]
			if !listed || row.Name == "" {
				continue
			}
			read := PaneReadResult{}
			args := nodePaneReadArgs{PaneID: row.PaneID, Name: row.Name, Cwd: row.Cwd, TailLines: poolTriggerScreenLines}
			if !h.nodeCall(ctx, machine, nodeCommandPaneRead, args, poolTriggerNodeWait, &read) || read.Outcome != nodePaneReadOK {
				continue
			}
			if pool, ok := quotastop.CreditExhausted(read.ScreenTail); ok && pool == task.Outcome.Pool {
				exhausted[pool] = true
			}
		}
	}
	pools := make([]string, 0, len(exhausted))
	for pool := range exhausted {
		pools = append(pools, pool)
	}
	slices.Sort(pools)
	return pools
}

// nodePaneRows lists machine's panes by pane id. An agent row wins over a pane
// row, since only the agent row carries the label and cwd pane.read matches on.
// A nil result means the node did not answer with a complete list.
func (h *HubServer) nodePaneRows(ctx context.Context, machine string) map[string]nodeAgentRow {
	var list nodeAgentsListResult
	if !h.nodeCall(ctx, machine, nodeCommandAgentsList, struct{}{}, poolTriggerNodeWait, &list) || !list.PanesOK {
		return nil
	}
	rows := make(map[string]nodeAgentRow, len(list.Panes)+len(list.Agents))
	for _, row := range list.Panes {
		rows[row.PaneID] = row
	}
	for _, row := range list.Agents {
		rows[row.PaneID] = row
	}
	return rows
}
