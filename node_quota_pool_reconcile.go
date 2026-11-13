package panewire

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	quotaPoolReconcileInterval = 5 * time.Minute
	quotaPoolReconcileTimeout  = 15 * time.Second
)

// runQuotaPoolReconcile periodically compares the local arbiter ledger with
// this node's Herdr agents. The ledger's identity and startup guards decide
// which absent jobs are safe to release.
func (client *HubClient) runQuotaPoolReconcile(ctx context.Context) {
	if client.jobsInboxRoot == "" {
		return
	}
	pass := client.quotaReconcile.pass
	if pass == nil {
		pass = client.reconcileNodeQuotaPools
	}
	pass(ctx)
	ticks := client.quotaReconcile.ticks
	if ticks == nil {
		ticker := time.NewTicker(quotaPoolReconcileInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			pass(ctx)
		}
	}
}

func (client *HubClient) reconcileNodeQuotaPools(ctx context.Context) {
	newApp := client.quotaReconcile.app
	if newApp == nil {
		newApp = func() (*fleet.App, error) { return fleet.New(slogWarnWriter{}, slogWarnWriter{}) }
	}
	app, err := newApp()
	if err != nil {
		slog.Warn("quota_pool reconciliation skipped", "reason", "app unavailable")
		return
	}
	app.Jobs = filepath.Join(client.jobsInboxRoot, "jobs")
	app.HerdrSession = client.nodeSpawnSession()
	passCtx, cancel := context.WithTimeout(ctx, quotaPoolReconcileTimeout)
	defer cancel()
	result, err := app.ReconcileLedger(passCtx, func(ctx context.Context) ([]arbiter.Agent, error) {
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		rows, err := client.readNodeAgents(ctx)
		if err != nil {
			return nil, err
		}
		agents := make([]arbiter.Agent, 0, len(rows))
		for _, row := range rows {
			if row.Name == "" || row.AgentStatus == "" {
				return nil, errors.New("herdr agent snapshot has incomplete identity")
			}
			agents = append(agents, arbiter.Agent{Status: row.AgentStatus, Name: row.Name})
		}
		return agents, nil
	})
	if err != nil {
		slog.Warn("quota_pool reconciliation failed", "error", err)
		return
	}
	if len(result.StaleQuota) != 0 || len(result.OrphanReleased) != 0 {
		slog.Info("quota_pool reconciliation completed", "stale_records", len(result.StaleQuota), "orphan_jobs", len(result.OrphanReleased))
	}
}
