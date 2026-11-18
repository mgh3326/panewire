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
	result, err := app.ReconcileLedgerForJobs(passCtx, func(ctx context.Context, jobs []string) (arbiter.AgentSnapshot, error) {
		sessionJobIDs := make(map[string][]string)
		heldJobs := make(map[string]bool)
		for _, job := range jobs {
			session, err := client.nodeJobHerdrSession(job)
			if err != nil {
				return arbiter.AgentSnapshot{}, err
			}
			s := firstNonempty(session, app.HerdrSession)
			sessionJobIDs[s] = append(sessionJobIDs[s], job)
			heldJobs[job] = true
		}
		names := make([]string, 0, len(sessionJobIDs))
		for session := range sessionJobIDs {
			names = append(names, session)
		}
		if len(names) == 0 {
			names = append(names, app.HerdrSession)
		}
		sort.Strings(names)

		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		if client.quotaEmptySince == nil {
			client.quotaEmptySince = make(map[string]time.Time)
		}
		// A session that holds no job this pass ends its window. Otherwise a job
		// that joins it later would inherit the old start and skip its own
		// 10-minute confirmation.
		for session := range client.quotaEmptySince {
			if len(sessionJobIDs[session]) == 0 {
				delete(client.quotaEmptySince, session)
			}
		}

		now := client.nowUTC()
		agents := make([]arbiter.Agent, 0)
		unverified := make(map[string]string)

		for _, session := range names {
			rows, err := readAgents(ctx, session)
			if err != nil {
				delete(client.quotaEmptySince, session)
				return arbiter.AgentSnapshot{}, err
			}
			if len(rows) == 0 {
				if len(sessionJobIDs[session]) > 0 {
					since, ok := client.quotaEmptySince[session]
					if !ok {
						client.quotaEmptySince[session] = now
						since = now
					}
					if now.Sub(since) < arbiter.OrphanStartupGrace {
						for _, j := range sessionJobIDs[session] {
							unverified[j] = "herdr session " + session + " listed no agents"
						}
					}
				}
				continue
			}

			delete(client.quotaEmptySince, session)
			for _, row := range rows {
				if row.AgentStatus == "" {
					return arbiter.AgentSnapshot{}, errors.New("herdr agent snapshot has incomplete identity")
				}
				if row.Name == "" {
					slog.Info("herdr agent snapshot row without a name", "session", session, "status", row.AgentStatus, "pane", row.PaneID)
					if row.PaneID != "" {
						binding, ok := nodeJobHerdrBindingByPaneFromRoot(client.jobsInboxRoot, row.PaneID)
						if ok && heldJobs[binding.jobID] {
							unverified[binding.jobID] = "unnamed herdr row on the job's pane"
						}
					} else {
						for _, j := range sessionJobIDs[session] {
							unverified[j] = "unnamed herdr row without pane id in session"
						}
					}
					continue
				}
				agents = append(agents, arbiter.Agent{Status: row.AgentStatus, Name: row.Name})
			}
		}
		return arbiter.AgentSnapshot{
			Agents:     agents,
			Unverified: unverified,
		}, nil
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
