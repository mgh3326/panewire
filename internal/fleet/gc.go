package fleet

import (
	"context"
	"fmt"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// gc reconciles the local ledger with Herdr: it expires path/linear_permit
// leases, cleans quota_pool records whose agent is gone and releases leased
// jobs that hold nothing and have no agent. An agent in status unknown, or a
// claim without an exact agent label, changes nothing and fails closed.
func (a *App) gc(ctx context.Context, args []string) error {
	f := flags("gc")
	asJSON := f.Bool("json", false, "")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() != 0 {
		return fail(exitUsage, "gc accepts no arguments")
	}
	res, e := a.ledger().GC(ctx, func(ctx context.Context) ([]arbiter.Agent, error) {
		agents, e := a.agents(ctx)
		if e != nil {
			return nil, e
		}
		out := make([]arbiter.Agent, 0, len(agents))
		for _, ag := range agents {
			out = append(out, arbiter.Agent{Status: ag.Status, Name: ag.Name})
		}
		return out, nil
	})
	if e != nil {
		return ledgerFail(e, "gc")
	}
	for _, s := range res.Skipped {
		a.warn("gc skipped orphan cleanup: job=%s reason=%s", s.Job, s.Reason)
	}
	if *asJSON {
		return writeJSON(a.Out, map[string]any{
			"checked": res.Checked, "expired": res.Expired, "transitioned": res.Transitioned,
			"stale_quota_pool": res.StaleQuota, "orphan_released": res.OrphanReleased, "skipped": res.Skipped,
			"live_agents": res.LiveAgents, "herdr_agents": res.HerdrAgents,
		})
	}
	fmt.Fprintf(a.Out, "checked=%d expired=%d transitioned=%d orphan_released=%d\n", res.Checked, res.Expired, len(res.Transitioned), len(res.OrphanReleased))
	for _, t := range res.Transitioned {
		fmt.Fprintf(a.Out, "  %s/%s job=%s expires_at=%s -> expired\n", t.Resource, t.Kind, t.Job, t.ExpiresAt)
	}
	for _, s := range res.StaleQuota {
		fmt.Fprintf(a.Out, "  job=%s pool=%s label=%s -> stale-cleaned\n", s.Job, s.Pool, s.AgentLabel)
	}
	for _, o := range res.OrphanReleased {
		fmt.Fprintf(a.Out, "  job=%s label=%s -> orphan-released\n", o.Job, o.AgentLabel)
	}
	return nil
}
