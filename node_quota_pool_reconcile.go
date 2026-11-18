package panewire

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sort"
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
	if client.jobsInboxRoot == "" || !client.quotaPoolReconcile {
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
	if !client.quotaPoolReconcile {
		return
	}
	// The ledger ticks at the top of every pass, including passes that find
	// nothing to release: the tick is the liveness signal.
	client.nodeLoopTick(nodeQuotaPoolReconcile)
	newApp := client.quotaReconcile.app
	if newApp == nil {
		newApp = func() (*fleet.App, error) { return fleet.New(slogWarnWriter{}, slogWarnWriter{}) }
	}
	app, err := newApp()
	if err != nil {
		slog.Warn("quota_pool reconciliation skipped", "reason", "app unavailable")
		client.nodeLoopNoteError(nodeQuotaPoolReconcile)
		return
	}
	app.Jobs = filepath.Join(client.jobsInboxRoot, "jobs")
	app.HerdrSession = client.nodeSpawnSession()
	passCtx, cancel := context.WithTimeout(ctx, quotaPoolReconcileTimeout)
	defer cancel()
	readAgents := client.quotaReconcile.agents
	if readAgents == nil {
		readAgents = client.readNodeAgentsInSession
	}
	result, err := app.ReconcileLedgerForJobs(passCtx, func(ctx context.Context, jobs []string) ([]arbiter.Agent, error) {
		sessionJobs := make(map[string]int)
		for _, job := range jobs {
			session, err := client.nodeJobHerdrSession(job)
			if err != nil {
				return nil, err
			}
			sessionJobs[firstNonempty(session, app.HerdrSession)]++
		}
		names := make([]string, 0, len(sessionJobs))
		for session := range sessionJobs {
			names = append(names, session)
		}
		if len(names) == 0 {
			names = append(names, app.HerdrSession)
		}
		sort.Strings(names)
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		agents := make([]arbiter.Agent, 0)
		for _, session := range names {
			rows, err := readAgents(ctx, session)
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				if row.AgentStatus == "" {
					return nil, errors.New("herdr agent snapshot has incomplete identity")
				}
				if row.Name == "" {
					slog.Warn("herdr agent snapshot has row without a name", "session", session, "status", row.AgentStatus)
					if sessionJobs[session] > 0 {
						return nil, errors.New("herdr agent snapshot has incomplete identity for session holding jobs")
					}
				}
				agents = append(agents, arbiter.Agent{Status: row.AgentStatus, Name: row.Name})
			}
		}
		if len(jobs) > 0 && len(agents) == 0 {
			return nil, errors.New("herdr agent snapshot is empty while jobs are held")
		}
		return agents, nil
	})
	if err != nil {
		slog.Warn("quota_pool reconciliation failed", "error", err)
		client.nodeLoopNoteError(nodeQuotaPoolReconcile)
		return
	}
	// Flow edges: one item per ledger row the pass saw. Stale quota records
	// and orphan jobs enter when first listed; an orphan release exits when
	// the ledger drops it.
	for _, row := range result.StaleQuota {
		client.nodeLoopNoteMade(nodeQuotaPoolReconcile, row.Job)
	}
	for _, row := range result.OrphanReleased {
		client.nodeLoopNoteMade(nodeQuotaPoolReconcile, row.Job)
		client.nodeLoopNoteDone(nodeQuotaPoolReconcile, row.Job)
	}
	if len(result.StaleQuota) != 0 || len(result.OrphanReleased) != 0 {
		slog.Info("quota_pool reconciliation completed", "stale_records", len(result.StaleQuota), "orphan_jobs", len(result.OrphanReleased))
	}
}
