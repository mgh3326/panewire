package arbiter

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"
)

// OrphanStartupGrace: a claimed job can precede its Herdr pane, so absence
// from the agent list is not retirement evidence until this long after the
// latest claim or spawn.
const OrphanStartupGrace = 10 * time.Minute

// Agent is one entry of Herdr's authoritative agent list. Identities are kept
// fielded: a job is matched on its exact persisted label, never by prefix.
type Agent struct {
	Status string // idle | working | blocked | done | unknown
	JobID  string
	Name   string
	Label  string
}

var agentStatuses = map[string]bool{"idle": true, "working": true, "blocked": true, "done": true, "unknown": true}

// AgentSource reads the agent list. GC and ReleaseOrphan call it between a
// state snapshot and the write transaction, so a reclaim that lands during the
// external read cannot be released on stale Herdr evidence.
type AgentSource func(context.Context) ([]Agent, error)

type snapshot struct {
	state string
	claim sql.NullInt64
}

func jobSnapshot(c *conn, job string) (*snapshot, error) {
	var sn snapshot
	err := c.row("SELECT state FROM jobs WHERE job_id = ?", job).Scan(&sn.state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, sqlErr(err)
	}
	err = c.row("SELECT MAX(seq) FROM events WHERE job_id = ? AND kind IN ('job.claim', 'job.reclaim')", job).Scan(&sn.claim)
	return &sn, sqlErr(err)
}

func sameSnapshot(before, now *snapshot) bool { return before != nil && now != nil && *before == *now }

func (s *Store) snapshots(ctx context.Context, job string) (map[string]*snapshot, error) {
	c, e := s.ro(ctx)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	out := map[string]*snapshot{}
	ids := []string{job}
	if job == "" {
		ids = nil
		rows, err := c.rows("SELECT job_id FROM jobs")
		if err != nil {
			return nil, sqlErr(err)
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, sqlErr(err)
			}
			ids = append(ids, id)
		}
		rows.Close()
	}
	for _, id := range ids {
		sn, err := jobSnapshot(c, id)
		if err != nil {
			return nil, err
		}
		out[id] = sn
	}
	return out, nil
}

type orphanInfo struct {
	exists   bool
	state    string
	leases   []string // "resource/kind"
	quotas   []string
	eligible bool
}

func orphanState(c *conn, job string) (orphanInfo, error) {
	var o orphanInfo
	err := c.row("SELECT state FROM jobs WHERE job_id = ?", job).Scan(&o.state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return o, sqlErr(err)
	default:
		o.exists = true
	}
	rows, err := c.rows("SELECT resource, kind FROM leases WHERE job_id = ? ORDER BY resource, kind", job)
	if err != nil {
		return o, sqlErr(err)
	}
	for rows.Next() {
		var r, k string
		if err = rows.Scan(&r, &k); err != nil {
			rows.Close()
			return o, sqlErr(err)
		}
		o.leases = append(o.leases, r+"/"+k)
	}
	rows.Close()
	rows, err = c.rows("SELECT pool FROM quota_pool_records WHERE job_id = ? ORDER BY pool", job)
	if err != nil {
		return o, sqlErr(err)
	}
	for rows.Next() {
		var p string
		if err = rows.Scan(&p); err != nil {
			rows.Close()
			return o, sqlErr(err)
		}
		o.quotas = append(o.quotas, p)
	}
	rows.Close()
	o.eligible = o.exists && o.state == "leased" && len(o.leases) == 0 && len(o.quotas) == 0
	return o, nil
}

// startupBlocker is why a leased job still looks too new to retire, or "".
func startupBlocker(c *conn, job string, moment time.Time) (string, error) {
	rows, err := c.rows("SELECT kind, created_at FROM events WHERE job_id = ? AND kind IN ('job.claim', 'job.reclaim', 'job.spawned')", job)
	if err != nil {
		return "", sqlErr(err)
	}
	defer rows.Close()
	var claims, spawns []time.Time
	for rows.Next() {
		var kind, at string
		if err = rows.Scan(&kind, &at); err != nil {
			return "", sqlErr(err)
		}
		t, perr := parseISO(at)
		if perr != nil {
			return "", perr
		}
		if kind == "job.spawned" {
			spawns = append(spawns, t)
		} else {
			claims = append(claims, t)
		}
	}
	if err = rows.Err(); err != nil {
		return "", sqlErr(err)
	}
	latest := func(ts []time.Time) time.Time {
		var m time.Time
		for _, t := range ts {
			if t.After(m) {
				m = t
			}
		}
		return m
	}
	switch {
	case len(claims) == 0:
		return "claim timestamp unavailable; retaining during startup grace", nil
	case moment.Sub(latest(claims)) < OrphanStartupGrace:
		return "latest claim/reclaim is inside the 10-minute startup grace", nil
	case len(spawns) > 0 && moment.Sub(latest(spawns)) < OrphanStartupGrace:
		return "latest spawn is inside the 10-minute startup grace", nil
	}
	return "", nil
}

// matchedStatuses are the statuses of agents matching the exact persisted
// identity. A Herdr job_id counts only when the persisted identity is that
// very job id (the default claim mapping).
func matchedStatuses(agents []Agent, job, label string) (statuses []string, matched bool) {
	seen := map[string]bool{}
	for _, a := range agents {
		nameMatch := label != "" && (a.Name == label || a.Label == label)
		idMatch := label == job && a.JobID == job
		if nameMatch || idMatch {
			matched = true
			seen[a.Status] = true
		}
	}
	for st := range seen {
		statuses = append(statuses, st)
	}
	sort.Strings(statuses)
	return statuses, matched
}

func hasStatus(statuses []string, want string) bool {
	for _, s := range statuses {
		if s == want {
			return true
		}
	}
	return false
}

func checkAgents(agents []Agent) error {
	for _, a := range agents {
		if !agentStatuses[a.Status] {
			return errf(ExitStorage, "herdr agent status is not understood: %q", a.Status)
		}
	}
	return nil
}

func orphanReason(matched bool, statuses []string) string {
	if !matched {
		return "job absent from herdr agent list"
	}
	return "herdr agent status=" + strings.Join(statuses, ",")
}

// Transition is an exclusive lease gc moved to expired.
type Transition struct{ Resource, Kind, Job, ExpiresAt string }

// StaleQuota is a quota_pool record gc cleaned because its agent is gone.
type StaleQuota struct {
	Job, Pool, Profile, StartedAt, AgentLabel string
	HerdrStatus                               []string
}

// Orphan is a leased job gc released.
type Orphan struct {
	Job, PriorState, AgentLabel, Reason string
	HerdrStatus                         []string
	EventSeq                            int
}

// Skip is a job gc left alone, with the reason.
type Skip struct{ Job, Reason string }

// GCResult is what a gc pass did.
type GCResult struct {
	Checked, Expired int
	Transitioned     []Transition
	StaleQuota       []StaleQuota
	OrphanReleased   []Orphan
	Skipped          []Skip
	LiveAgents       int
	HerdrAgents      int
}

// GC does three things in one write transaction:
//
//  1. moves expired path/linear_permit leases to expired (archived and
//     evented, never deleted);
//  2. deletes quota_pool records whose exact agent is absent from Herdr (an
//     agent listed with any known status owns its pane and keeps its record);
//  3. releases leased jobs that hold nothing and whose agent is gone.
//
// An agent in status unknown, or a claim without an exact agent_label, is not
// stale evidence: gc then changes nothing and fails closed (ExitStorage). A
// job reclaimed after the Herdr snapshot, or still inside the startup grace,
// is skipped.
func (s *Store) GC(ctx context.Context, agents AgentSource) (GCResult, error) {
	var res GCResult
	snaps, e := s.snapshots(ctx, "")
	if e != nil {
		return res, e
	}
	list, e := agents(ctx)
	if e != nil {
		return res, e
	}
	if e = checkAgents(list); e != nil {
		return res, e
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return res, e
	}
	defer c.Close()
	res.HerdrAgents = len(list)
	for _, a := range list {
		if a.Status == "working" {
			res.LiveAgents++
		}
	}
	e = c.write(func() error {
		moment := s.now()
		rows, err := c.rows("SELECT resource, kind, job_id, token, acquired_at, expires_at FROM leases WHERE kind IN ('path', 'linear_permit')")
		if err != nil {
			return sqlErr(err)
		}
		var held []*leaseRow
		for rows.Next() {
			var r leaseRow
			if err = rows.Scan(&r.resource, &r.kind, &r.job, &r.token, &r.acquired, &r.expires); err != nil {
				rows.Close()
				return sqlErr(err)
			}
			held = append(held, &r)
		}
		rows.Close()
		res.Checked = len(held)
		for _, r := range held {
			until, perr := parseISO(r.expires)
			if perr != nil {
				return perr
			}
			if until.After(moment) {
				continue
			}
			res.Expired++
			// The row stays: expiry is an audited state transition.
			fresh, aerr := archive(c, r, "expired", moment)
			if aerr != nil {
				return aerr
			}
			if fresh {
				if _, err = s.recordEvent(c, r.job, "lease.expire", map[string]any{"resource": r.resource, "kind": r.kind, "expires_at": r.expires}, nil); err != nil {
					return err
				}
				res.Transitioned = append(res.Transitioned, Transition{r.resource, r.kind, r.job, r.expires})
			}
		}
		type quotaRow struct{ job, pool, profile, started string }
		rows, err = c.rows("SELECT job_id, pool, profile, started_at FROM quota_pool_records ORDER BY job_id, pool")
		if err != nil {
			return sqlErr(err)
		}
		var quotas []quotaRow
		for rows.Next() {
			var q quotaRow
			if err = rows.Scan(&q.job, &q.pool, &q.profile, &q.started); err != nil {
				rows.Close()
				return sqlErr(err)
			}
			quotas = append(quotas, q)
		}
		rows.Close()
		rows, err = c.rows(`SELECT j.job_id FROM jobs AS j WHERE j.state = 'leased'
			AND NOT EXISTS (SELECT 1 FROM leases AS l WHERE l.job_id = j.job_id)
			AND NOT EXISTS (SELECT 1 FROM quota_pool_records AS q WHERE q.job_id = j.job_id)
			ORDER BY j.job_id`)
		if err != nil {
			return sqlErr(err)
		}
		var rowless []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return sqlErr(err)
			}
			rowless = append(rowless, id)
		}
		rows.Close()

		// Quota-backed and row-less jobs go through the same identity, status,
		// snapshot and startup-grace guards. A row-less job is released only
		// when its exact owner is absent from Herdr.
		type check struct {
			job   string
			quota *quotaRow
		}
		var checks []check
		for i := range quotas {
			checks = append(checks, check{quotas[i].job, &quotas[i]})
		}
		for _, id := range rowless {
			checks = append(checks, check{job: id})
		}
		type decision struct {
			job, label string
			statuses   []string
			matched    bool
			quota      *quotaRow
		}
		var stale, rowlessDec []decision
		var unmapped, unknown []string
		for _, ck := range checks {
			label, lerr := claimAgentLabel(c, ck.job)
			if lerr != nil {
				return lerr
			}
			if label == "" {
				unmapped = append(unmapped, ck.job)
				continue
			}
			statuses, matched := matchedStatuses(list, ck.job, label)
			if hasStatus(statuses, "unknown") {
				unknown = append(unknown, ck.job)
				continue
			}
			if len(statuses) > 0 {
				continue // a listed agent with a known status owns its pane
			}
			cur, serr := jobSnapshot(c, ck.job)
			if serr != nil {
				return serr
			}
			if !sameSnapshot(snaps[ck.job], cur) {
				res.Skipped = append(res.Skipped, Skip{ck.job, "job state or claim sequence changed after Herdr snapshot"})
				continue
			}
			blocker, berr := startupBlocker(c, ck.job, moment)
			if berr != nil {
				return berr
			}
			if blocker != "" {
				res.Skipped = append(res.Skipped, Skip{ck.job, blocker})
				continue
			}
			d := decision{ck.job, label, statuses, matched, ck.quota}
			if ck.quota == nil {
				rowlessDec = append(rowlessDec, d)
			} else {
				stale = append(stale, d)
			}
		}
		if len(unmapped) > 0 || len(unknown) > 0 {
			var detail []string
			if len(unmapped) > 0 {
				detail = append(detail, "unmapped="+strings.Join(unmapped, ","))
			}
			if len(unknown) > 0 {
				detail = append(detail, "unknown="+strings.Join(unknown, ","))
			}
			return errf(ExitStorage, "gc identity reconciliation refused; no quota_pool records or orphan jobs were changed (%s)", strings.Join(detail, "; "))
		}

		type candidate struct {
			label, reason string
			statuses      []string
		}
		candidates := map[string]candidate{}
		for _, d := range stale {
			reason := orphanReason(d.matched, d.statuses)
			if _, err = c.exec("DELETE FROM quota_pool_records WHERE job_id = ? AND pool = ?", d.quota.job, d.quota.pool); err != nil {
				return sqlErr(err)
			}
			if _, err = s.recordEvent(c, d.job, "quota_pool.stale_cleanup", map[string]any{
				"job_id": d.job, "pool": d.quota.pool, "profile": d.quota.profile, "started_at": d.quota.started,
				"agent_label": d.label, "reason": reason, "herdr_status": nonNil(d.statuses),
			}, nil); err != nil {
				return err
			}
			res.StaleQuota = append(res.StaleQuota, StaleQuota{d.job, d.quota.pool, d.quota.profile, d.quota.started, d.label, d.statuses})
			candidates[d.job] = candidate{d.label, reason, d.statuses}
		}
		for _, d := range rowlessDec {
			candidates[d.job] = candidate{d.label, orphanReason(d.matched, d.statuses), d.statuses}
		}
		ids := make([]string, 0, len(candidates))
		for id := range candidates {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			cand := candidates[id]
			o, oerr := orphanState(c, id)
			if oerr != nil {
				return oerr
			}
			if !o.eligible {
				continue
			}
			if _, err = c.exec("UPDATE jobs SET state = 'released', updated_at = ? WHERE job_id = ? AND state = 'leased'", iso(moment), id); err != nil {
				return sqlErr(err)
			}
			seq, rerr := s.recordEvent(c, id, "job.orphan_release", map[string]any{
				"job_id": id, "prior_state": o.state, "agent_label": cand.label,
				"herdr_status": nonNil(cand.statuses), "reason": cand.reason, "forced": false,
			}, nil)
			if rerr != nil {
				return rerr
			}
			res.OrphanReleased = append(res.OrphanReleased, Orphan{id, o.state, cand.label, cand.reason, cand.statuses, seq})
		}
		return nil
	})
	return res, e
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// ReleaseOrphan is the operator's forced release of one leased job whose agent
// is gone: the same identity, status and startup-grace guards as gc, for one
// job. A missing job is ExitNotFound; a refusal is ExitFencing, or ExitStorage
// when the agent's status is unknown.
func (s *Store) ReleaseOrphan(ctx context.Context, job string, agents AgentSource) (Orphan, error) {
	var out Orphan
	snaps, e := s.snapshots(ctx, job)
	if e != nil {
		return out, e
	}
	list, e := agents(ctx)
	if e != nil {
		return out, e
	}
	if e = checkAgents(list); e != nil {
		return out, e
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return out, e
	}
	defer c.Close()
	e = c.write(func() error {
		cur, err := jobSnapshot(c, job)
		if err != nil {
			return err
		}
		if (snaps[job] != nil || cur != nil) && !sameSnapshot(snaps[job], cur) {
			return errf(ExitFencing, "orphan release refused: job state or claim sequence changed after Herdr snapshot (job=%s)", job)
		}
		o, err := orphanState(c, job)
		if err != nil {
			return err
		}
		if !o.exists {
			d := errf(ExitNotFound, "job not found: %s", job)
			d.Detail = map[string]any{"job_id": job}
			return d
		}
		moment := s.now()
		label, err := claimAgentLabel(c, job)
		if err != nil {
			return err
		}
		if label == "" {
			return errf(ExitStorage, "orphan release identity reconciliation refused; job.claim has no exact agent_label mapping (unmapped=%s)", job)
		}
		statuses, matched := matchedStatuses(list, job, label)
		var blockers []string
		if o.state != "leased" {
			blockers = append(blockers, "job state="+o.state+" (expected leased)")
		}
		switch {
		case hasStatus(statuses, "unknown"):
			blockers = append(blockers, "agent="+label+" herdr_status=unknown")
		case matched:
			blockers = append(blockers, "agent="+label+" herdr_status="+strings.Join(statuses, ",")+" (pane exists)")
		}
		for _, l := range o.leases {
			blockers = append(blockers, "lease="+l)
		}
		for _, q := range o.quotas {
			blockers = append(blockers, "quota_pool="+q)
		}
		if len(blockers) == 0 {
			b, berr := startupBlocker(c, job, moment)
			if berr != nil {
				return berr
			}
			if b != "" {
				blockers = append(blockers, b)
			}
		}
		if len(blockers) > 0 {
			code := ExitFencing
			if hasStatus(statuses, "unknown") {
				code = ExitStorage
			}
			return errf(code, "orphan release refused: job=%s leaves %s", job, strings.Join(blockers, ", "))
		}
		if !o.eligible {
			return errf(ExitFencing, "orphan release refused: job=%s is not an eligible leased orphan", job)
		}
		reason := orphanReason(matched, statuses)
		if _, err = c.exec("UPDATE jobs SET state = 'released', updated_at = ? WHERE job_id = ? AND state = 'leased'", iso(moment), job); err != nil {
			return sqlErr(err)
		}
		seq, err := s.recordEvent(c, job, "job.orphan_release", map[string]any{
			"job_id": job, "prior_state": o.state, "agent_label": label,
			"herdr_status": nonNil(statuses), "reason": reason, "forced": true,
		}, nil)
		out = Orphan{job, o.state, label, reason, statuses, seq}
		return err
	})
	return out, e
}
