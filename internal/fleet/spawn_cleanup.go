package fleet

import (
	"context"
	"path/filepath"
)

// A deadline can expire after the harness starts working. Preserve an unknown
// or working pane and its leases; timeout alone never authorizes closing it.
func (a *App) closeFailedSpawn(ctx context.Context, s Spawn, pane, kind string) error {
	agents, err := a.agents(ctx)
	if err != nil {
		return err
	}
	matches := 0
	for _, agent := range agents {
		if agent.Pane != pane {
			continue
		}
		matches++
		if agent.Name != s.Label || agent.Kind != kind || agent.CWD == "" || filepath.Clean(agent.CWD) != filepath.Clean(s.CWD) || (agent.Status != "idle" && agent.Status != "done") {
			return fail(exitStateConflict, "failed spawn pane identity or status is unconfirmed; inspect panewire find %s on the executing host before cleanup", s.Label)
		}
	}
	if matches == 0 {
		return nil
	}
	if matches != 1 {
		return fail(exitStateConflict, "duplicate pane identity; inspect the executing node before cleanup")
	}
	return a.must(ctx, "herdr", "pane", "close", pane)
}
