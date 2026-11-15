package fleet

import (
	"context"
	"path/filepath"
	"time"
)

const FailedSpawnCloseSettle = 2 * time.Second

// FailedSpawnUnconfirmed is the closed phrase of the refusal a failed spawn's
// cleanup gives when the pane is not provably the one it launched. The hub
// matches it in a landing refusal to re-check the pane (hub_spawn_async.go).
const FailedSpawnUnconfirmed = "pane identity or status is unconfirmed"

// A deadline can expire after the harness starts working. Preserve an unknown
// or working pane and its leases; timeout alone never authorizes closing it.
func (a *App) closeFailedSpawn(ctx context.Context, s Spawn, pane, kind string) error {
	err := a.closeFailedSpawnPane(ctx, s, pane, kind)
	if err == nil && s.registeredLane != nil {
		// The pane is closed, so its lane row routes to nothing. A failure
		// leaves the row for the hub's release of rows whose panes are gone.
		if e := a.removeLane(ctx, *s.registeredLane); e != nil {
			a.warn("lane retained: %v", e)
		}
	}
	if err != nil {
		// The pane stays open; the receipt lets reap tools find it by id.
		if e := a.event(ctx, s.Job, "job.cleanup_refused", map[string]any{"pane_id": pane, "label": s.Label, "kind": kind, "reason": err.Error()}); e != nil {
			a.warn("cleanup receipt for pane %s failed: %v", pane, e)
		}
	}
	return err
}

func (a *App) closeFailedSpawnPane(ctx context.Context, s Spawn, pane, kind string) error {
	first, err := a.failedSpawnPane(ctx, s, pane, kind)
	if err != nil {
		return err
	}
	if err = a.Sleep(ctx, FailedSpawnCloseSettle); err != nil {
		return err
	}
	second, err := a.failedSpawnPane(ctx, s, pane, kind)
	if err != nil {
		return err
	}
	if *first.StateChangeSeq != *second.StateChangeSeq {
		return fail(exitStateConflict, "failed spawn pane changed during settle; inspect panewire find %s before cleanup", s.Label)
	}
	return a.must(ctx, "herdr", "pane", "close", pane)
}

func (a *App) failedSpawnPane(ctx context.Context, s Spawn, pane, kind string) (Agent, error) {
	agents, err := a.agents(ctx)
	if err != nil {
		return Agent{}, err
	}
	matches := 0
	var matched Agent
	for _, agent := range agents {
		if agent.Pane != pane {
			continue
		}
		matches++
		if agent.Name != s.Label || agent.Kind != kind || agent.CWD == "" || filepath.Clean(agent.CWD) != filepath.Clean(s.CWD) || agent.Focused || agent.StateChangeSeq == nil || (agent.Status != "idle" && agent.Status != "done") {
			return Agent{}, fail(exitStateConflict, "failed spawn %s; inspect panewire find %s on the executing host before cleanup", FailedSpawnUnconfirmed, s.Label)
		}
		matched = agent
	}
	if matches == 0 {
		return Agent{}, fail(exitStateConflict, "failed spawn pane has no agent identity; inspect panewire find %s before cleanup", s.Label)
	}
	if matches != 1 {
		return Agent{}, fail(exitStateConflict, "duplicate pane identity; inspect the executing node before cleanup")
	}
	return matched, nil
}
