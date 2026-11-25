package panewire

import (
	"context"
	"io"
	"time"
)

// The eager role release (hk 6282, decision 494 option A, decision 496). A
// task role used to be freed only when a later spawn of the same role collided
// with it (reclaimSpawnRole), so a finished job that was never spawned again
// held its role for good. This background pass frees the role of a job that is
// proven over, without waiting for that spawn. Proven over means both:
//
//  1. hk says the job ended: its record is completed, lost or escalated
//     (hkTerminalState, read through the spawn App's ExistingJob), and
//  2. the job's node answered a complete pane list without the job's pane or
//     agent label (probeHeldPane, the probe the lazy path uses).
//
// Anything else keeps the role and the next pass looks again: a present pane,
// an unknown pane state (no recorded pane, an unreachable or disconnected node,
// a timeout, an error, panes_ok false), an hk read error, a job a pool action
// owns, or a job whose owner moved to another node. The pass runs on its own
// goroutine, off spawn admission and off the node event handler, and probes at
// most roleEagerBatch holders per pass, and never holds a job's pool-action
// lock while it probes; admission keeps its lazy release as
// the fallback. Only task_role goes to NULL: the reservation row stays.
// Contract: docs/job-close.md, docs/hub-spawn-operations.md.

// roleEagerInterval is how often the pass runs; roleEagerBatch bounds the
// holders one pass reads and probes. Variables so a test can shorten them.
var (
	roleEagerInterval = time.Minute
	roleEagerBatch    = 16
)

// runEagerRoleRelease runs releaseProvenOverRoles once per roleEagerInterval
// until ctx ends. Each pass resumes after the last request id the previous one
// read, so every held role is visited in turn however many stay held.
func (h *HubServer) runEagerRoleRelease(ctx context.Context) {
	ticker := time.NewTicker(roleEagerInterval)
	defer ticker.Stop()
	after := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			after = h.releaseProvenOverRoles(ctx, after)
		}
	}
}

// heldEndedSpawnRoles lists up to limit (request id, role key) pairs whose
// reservation still holds a task role while its operation is succeeded or
// failed, in request id order after the cursor after.
func (s *laneInboxStore) heldEndedSpawnRoles(parent context.Context, after string, limit int) (ids, roles []string, err error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT r.request_id, r.task_role FROM spawn_reservations r JOIN spawn_operations o ON o.request_id=r.request_id WHERE r.task_role IS NOT NULL AND o.status IN ('succeeded','failed') AND r.request_id>? ORDER BY r.request_id LIMIT ?`), after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, role string
		if err := rows.Scan(&id, &role); err != nil {
			return nil, nil, err
		}
		ids, roles = append(ids, id), append(roles, role)
	}
	return ids, roles, rows.Err()
}

// releaseEndedSpawnRole is releaseCompletedSpawnRole's write, reporting whether
// it cleared the row (false when the role was released or moved meanwhile).
func (s *laneInboxStore) releaseEndedSpawnRole(parent context.Context, id, roleKey string) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE request_id=? AND task_role=? AND EXISTS(SELECT 1 FROM spawn_operations WHERE request_id=? AND status IN ('succeeded','failed','cancelled'))`), id, roleKey, id)
	if err != nil {
		return false, err
	}
	n, err := s.dialect.rowsMatched(result)
	return n == 1, err
}

// releaseProvenOverRoles is one bounded pass. It returns the cursor the next
// pass starts after: "" once the list is exhausted, so the next pass wraps.
func (h *HubServer) releaseProvenOverRoles(ctx context.Context, after string) string {
	if h.inbox == nil {
		return ""
	}
	ids, roles, err := h.inbox.store.heldEndedSpawnRoles(ctx, after, roleEagerBatch)
	if err != nil || len(ids) == 0 {
		return ""
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return after
	}
	// A host that did not answer is not asked again in this pass: it would
	// only cost every later holder there the probe's wait.
	silent := map[string]bool{}
	for i, id := range ids {
		if ctx.Err() != nil {
			return after
		}
		h.releaseProvenOverRole(ctx, app, id, roles[i], silent)
	}
	if len(ids) < roleEagerBatch {
		return ""
	}
	return ids[len(ids)-1]
}

// releaseProvenOverRole frees one holder's role when its job is proven over,
// and reports whether it did.
func (h *HubServer) releaseProvenOverRole(ctx context.Context, app hubLostJobApp, id, roleKey string, silent map[string]bool) bool {
	holder, err := h.inbox.store.spawnTask(ctx, id)
	if err != nil || holder.Status != "succeeded" && holder.Status != "failed" {
		return false
	}
	host := spawnHolderHost(holder)
	if host == "" || silent[host] {
		return false
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, holder.Job)
	cancel()
	if state, _ := record["state"].(string); err != nil || !exists || !hkTerminalState(state) {
		return false
	}
	// A pool action that owns the job keeps the role (I1/I2). The probe runs
	// without the job's lock, so admission never waits on it; the fence is
	// checked again under the lock, taken without waiting, before the write.
	if owner, err := h.inbox.store.poolActionOwns(ctx, holder.Job); err != nil || owner != "" {
		return false
	}
	if h.jobMovedFrom(holder.Job, host) {
		return false
	}
	probe := h.probeHeldPane(ctx, host, holder.Outcome.Pane, heldAgentName(holder, record))
	if probe.state != heldPaneAbsent {
		if probe.state == heldPaneUnknown && probe.reason != "no_pane_id" && probe.reason != "no_host" {
			silent[host] = true
		}
		return false
	}
	unlock, free := h.tryLockWithoutPoolAction(ctx, holder.Job)
	if !free {
		return false
	}
	defer unlock()
	if h.jobMovedFrom(holder.Job, host) {
		return false
	}
	// The write names this holder's request id and role: a spawn admitted
	// meanwhile holds the role under its own request id and is never cleared.
	if released, err := h.inbox.store.releaseEndedSpawnRole(ctx, id, roleKey); err != nil || !released {
		return false
	}
	h.logger.Info("hub released a proven-over task role", "role_key", roleKey, "request", id, "job", holder.Job, "host", host, "probe_reason", probe.reason)
	return true
}

// jobMovedFrom reports whether the hub's job record puts job on another node
// or a later epoch than its first, so host's pane list cannot prove it over.
func (h *HubServer) jobMovedFrom(job, host string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.jobs[job]
	return record != nil && (record.Node != host || record.Epoch > 1)
}
