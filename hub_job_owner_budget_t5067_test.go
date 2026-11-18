package panewire

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

type slowFirstOwnerRead struct{ *ownerFenceHK }

func (s slowFirstOwnerRead) Get(ctx context.Context, id string) (map[string]any, error) {
	if id == "slow-job" {
		// Simulated backend latency: the first lookup consumes its sub-budget.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.ownerFenceHK.Get(ctx, id)
}

func TestJobOwnerFenceUnknownJobsShareHeartbeatBudget_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, _ := ownerFenceHub(t, hk)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	hub.closeCtx = ctx
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: slowFirstOwnerRead{hk}, Out: out, Err: errout}, nil
	}
	hub.observeActiveJobs("machine-a", []HubActiveJob{
		{JobID: "slow-job", AgentLabel: "worker-a", Epoch: 1},
		{JobID: "fast-job", AgentLabel: "worker-b", Epoch: 1},
	}, hub.now())
	if hub.jobs["slow-job"] != nil || hub.jobs["fast-job"] == nil {
		t.Fatalf("one slow read starved another job: slow=%+v fast=%+v", hub.jobs["slow-job"], hub.jobs["fast-job"])
	}
}
