package arbiter

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"time"
)

// LeaseReq asks for an exclusive lease (path or linear_permit).
type LeaseReq struct {
	Job, Resource, Kind string
	TTL                 time.Duration // used when HasTTL; otherwise the kind's default
	HasTTL              bool
}

// Lease is a granted exclusive lease. Token is the secret fencing token; only
// its sequence half is ever logged.
type Lease struct {
	Job, Resource, Kind, Token string
	AcquiredAt, ExpiresAt      string
	EventSeq                   int
	Superseded                 string // job whose expired lease this one replaced
}

type leaseRow struct{ resource, kind, job, token, acquired, expires string }

func getLease(c *conn, resource, kind string) (*leaseRow, error) {
	var r leaseRow
	e := c.row("SELECT resource, kind, job_id, token, acquired_at, expires_at FROM leases WHERE resource = ? AND kind = ?", resource, kind).
		Scan(&r.resource, &r.kind, &r.job, &r.token, &r.acquired, &r.expires)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	return &r, sqlErr(e)
}

func requireJob(c *conn, job string) error {
	var id string
	e := c.row("SELECT job_id FROM jobs WHERE job_id = ?", job).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return errf(ExitDenied, "job is not registered: %s — claim it first", job)
	}
	return sqlErr(e)
}

// archive keeps a lease generation as audit evidence; false means it was
// already archived in that state.
func archive(c *conn, r *leaseRow, state string, moment time.Time) (bool, error) {
	res, e := c.exec("INSERT OR IGNORE INTO lease_history (resource, kind, job_id, token, acquired_at, expires_at, state, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		r.resource, r.kind, r.job, r.token, r.acquired, r.expires, state, iso(moment))
	if e != nil {
		return false, sqlErr(e)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Acquire grants an exclusive lease atomically. A held, unexpired lease is
// ExitDenied; an expired one is archived as superseded and replaced. The job
// must be registered locally.
func (s *Store) Acquire(ctx context.Context, req LeaseReq) (Lease, error) {
	var out Lease
	if req.Kind != KindPath && req.Kind != KindLinearPermit {
		return out, errf(ExitUsage, "kind %q is not an exclusive lease kind", req.Kind)
	}
	if req.Resource == "" || req.TTL < 0 {
		return out, errf(ExitUsage, "a lease needs a resource and a non-negative ttl")
	}
	ttl := req.TTL
	if !req.HasTTL {
		ttl = DefaultTTL[req.Kind]
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return out, e
	}
	defer c.Close()
	moment := s.now()
	expires := moment.Add(ttl)
	e = c.write(func() error {
		if err := requireJob(c, req.Job); err != nil {
			return err
		}
		held, err := getLease(c, req.Resource, req.Kind)
		if err != nil {
			return err
		}
		out = Lease{Job: req.Job, Resource: req.Resource, Kind: req.Kind, AcquiredAt: iso(moment), ExpiresAt: iso(expires)}
		if held != nil {
			until, perr := parseISO(held.expires)
			if perr != nil {
				return perr
			}
			if until.After(moment) {
				d := errf(ExitDenied, "lease held: %s/%s is held by job=%s until %s", req.Resource, req.Kind, held.job, held.expires)
				d.Detail = map[string]any{"resource": req.Resource, "kind": req.Kind, "held_by": held.job, "expires_at": held.expires}
				return d
			}
			// An expired generation is kept as audit evidence before it is replaced.
			if _, err = archive(c, held, "superseded", moment); err != nil {
				return err
			}
			if _, err = c.exec("DELETE FROM leases WHERE resource = ? AND kind = ?", req.Resource, req.Kind); err != nil {
				return sqlErr(err)
			}
			out.Superseded = held.job
		}
		if out.Token, err = nextToken(c); err != nil {
			return sqlErr(err)
		}
		if _, err = c.exec("INSERT INTO leases (resource, kind, job_id, token, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)", req.Resource, req.Kind, req.Job, out.Token, iso(moment), iso(expires)); err != nil {
			return sqlErr(err)
		}
		if _, err = c.exec("UPDATE jobs SET state = 'leased', updated_at = ? WHERE job_id = ?", iso(moment), req.Job); err != nil {
			return sqlErr(err)
		}
		var superseded any
		if out.Superseded != "" {
			superseded = out.Superseded
		}
		out.EventSeq, err = s.recordEvent(c, req.Job, "lease.acquire", map[string]any{
			"resource": req.Resource, "kind": req.Kind, "resolved_from": "resource",
			"token": maskToken(out.Token), "expires_at": iso(expires), "superseded": superseded,
		}, nil)
		return err
	})
	return out, e
}

// Release returns an exclusive lease. Without force the caller must be the
// holder and present the fencing token; a stale owner or wrong token is
// ExitFencing. force is the operator override and records a force event.
func (s *Store) Release(ctx context.Context, job, resource, kind, token string, force bool) error {
	if kind != KindPath && kind != KindLinearPermit {
		return errf(ExitUsage, "kind %q is not an exclusive lease kind", kind)
	}
	if !force && token == "" {
		return errf(ExitUsage, "a normal lease release needs the token (force is the exception)")
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return e
	}
	defer c.Close()
	moment := s.now()
	return c.write(func() error {
		row, err := getLease(c, resource, kind)
		if err != nil {
			return err
		}
		if row == nil {
			return errf(ExitFencing, "no lease to release: %s/%s", resource, kind)
		}
		if !force && (row.job != job || row.token != token) {
			d := errf(ExitFencing, "stale owner refused: %s/%s is held by job=%s token=%s", resource, row.kind, row.job, maskToken(row.token))
			d.Detail = map[string]any{"resource": resource, "kind": row.kind, "held_by": row.job, "held_token": maskToken(row.token)}
			return d
		}
		state, evKind := "released", "lease.release"
		if force {
			state, evKind = "force-released", "lease.release.force"
		}
		if _, err = archive(c, row, state, moment); err != nil {
			return err
		}
		if _, err = c.exec("DELETE FROM leases WHERE resource = ? AND kind = ?", row.resource, row.kind); err != nil {
			return sqlErr(err)
		}
		if err = settleJob(c, row.job, moment); err != nil {
			return err
		}
		_, err = s.recordEvent(c, row.job, evKind, map[string]any{"resource": row.resource, "kind": row.kind, "token": maskToken(row.token), "forced": force}, nil)
		return err
	})
}

// settleJob marks a job released once it holds nothing: no quota_pool record
// and no unexpired exclusive lease. A job that still holds one resource stays
// leased, so it cannot be reclaimed (and a second generation cannot start)
// while the first still consumes a pool or a lease. An expired lease no longer
// holds anything; gc archives it.
func settleJob(c *conn, job string, moment time.Time) error {
	var held int
	err := c.row(`SELECT (SELECT count(*) FROM quota_pool_records WHERE job_id = ?)
		+ (SELECT count(*) FROM leases WHERE job_id = ? AND expires_at > ?)`, job, job, iso(moment)).Scan(&held)
	if err != nil {
		return sqlErr(err)
	}
	if held > 0 {
		_, err = c.exec("UPDATE jobs SET updated_at = ? WHERE job_id = ?", iso(moment), job)
		return sqlErr(err)
	}
	_, err = c.exec("UPDATE jobs SET state = 'released', updated_at = ? WHERE job_id = ?", iso(moment), job)
	return sqlErr(err)
}

// Gate is what a passed quota gate says about the run: the pool, and the
// optional audit fields copied through verbatim.
type Gate struct {
	Pool  string
	Audit map[string]string
}

var (
	gateProfile = regexp.MustCompile(`\bprofile=(\S+)`)
	gatePool    = regexp.MustCompile(`\bpool=(\S+)`)
	// auditKeys appear only in gate output. The values are copied as the gate
	// printed them — ref_resolution is the gate's own "recorded claim, not
	// proof of identity or consent" label, so summarising it would promote an
	// unverified claim to a verified one.
	auditKeys = []string{"escalation_override", "operator_request_ref", "requested_by", "ref_resolution"}
)

// ParseGate resolves a profile's quota pool from the gate output or verified
// launch context. When both supply a pool they must match. A profile named in
// the output must be the one asked for.
// An audit key that appears with two different values is refused, because
// either choice would forge the record.
func ParseGate(profile, text, expectedPool string) (Gate, error) {
	g := Gate{Pool: expectedPool, Audit: map[string]string{}}
	if m := gateProfile.FindStringSubmatch(text); m != nil && m[1] != profile {
		return g, errf(ExitStorage, "gate output profile differs: %q != %q", m[1], profile)
	}
	m := gatePool.FindStringSubmatch(text)
	if m != nil {
		if g.Pool != "" && g.Pool != m[1] {
			return g, errf(ExitStorage, "gate output pool differs from launch")
		}
		g.Pool = m[1]
	}
	if g.Pool == "" {
		return g, errf(ExitStorage, "gate output names no pool (profile=%s)", profile)
	}
	for _, key := range auditKeys {
		seen := map[string]bool{}
		for _, v := range regexp.MustCompile(`\b`+key+`=(\S+)`).FindAllStringSubmatch(text, -1) {
			seen[v[1]] = true
		}
		if len(seen) > 1 {
			vals := make([]string, 0, len(seen))
			for v := range seen {
				vals = append(vals, v)
			}
			sort.Strings(vals)
			return g, errf(ExitStorage, "gate output %s has differing values %v — an injected audit token is not recorded", key, vals)
		}
		for v := range seen {
			g.Audit[key] = v
		}
	}
	return g, nil
}

// QuotaRun is the quota_pool record of one run.
type QuotaRun struct {
	Job, Profile, LaunchProfile string
	Gate                        Gate
}

// QuotaResult is the recorded run.
type QuotaResult struct {
	Job, Pool, Profile, StartedAt string
	EventSeq                      int
}

// RecordQuotaPool writes the quota_pool execution record (no TTL, no fencing)
// for a registered job and marks it leased. A second record for the same
// (job, pool) is ExitDuplicate.
func (s *Store) RecordQuotaPool(ctx context.Context, q QuotaRun) (QuotaResult, error) {
	var out QuotaResult
	if q.Gate.Pool == "" || q.Profile == "" {
		return out, errf(ExitUsage, "a quota_pool record needs a profile and a resolved pool")
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return out, e
	}
	defer c.Close()
	moment := s.now()
	out = QuotaResult{Job: q.Job, Pool: q.Gate.Pool, Profile: q.Profile, StartedAt: iso(moment)}
	e = c.write(func() error {
		if err := requireJob(c, q.Job); err != nil {
			return err
		}
		if _, err := c.exec("INSERT INTO quota_pool_records (job_id, pool, profile, started_at) VALUES (?, ?, ?, ?)", q.Job, q.Gate.Pool, q.Profile, iso(moment)); err != nil {
			if isUnique(err) {
				d := errf(ExitDuplicate, "quota_pool record duplicate: job=%s pool=%s", q.Job, q.Gate.Pool)
				d.Detail = map[string]any{"job": q.Job, "pool": q.Gate.Pool, "profile": q.Profile}
				return d
			}
			return sqlErr(err)
		}
		if _, err := c.exec("UPDATE jobs SET state = 'leased', updated_at = ? WHERE job_id = ?", iso(moment), q.Job); err != nil {
			return sqlErr(err)
		}
		p := map[string]any{"job_id": q.Job, "pool": q.Gate.Pool, "profile": q.Profile, "started_at": iso(moment)}
		if q.LaunchProfile != "" {
			p["launch_profile"] = q.LaunchProfile
		}
		for k, v := range q.Gate.Audit {
			p[k] = v
		}
		var err error
		out.EventSeq, err = s.recordEvent(c, q.Job, "quota_pool.record", p, nil)
		return err
	})
	return out, e
}

// QuotaPools lists the pools a job holds a quota_pool record for.
// A ledger that does not exist yet holds none.
func (s *Store) QuotaPools(ctx context.Context, job string) ([]string, error) {
	if _, e := os.Stat(s.DB); errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	}
	st, e := s.Status(ctx, job, "")
	if e != nil {
		return nil, e
	}
	var pools []string
	for _, r := range st.Quota {
		pools = append(pools, r.Pool)
	}
	return pools, nil
}

// ReleaseQuotaPool deletes a job's quota_pool record and marks the job
// released. A missing record is ExitFencing.
func (s *Store) ReleaseQuotaPool(ctx context.Context, job, pool string, force bool) error {
	c, e := s.rw(ctx, false)
	if e != nil {
		return e
	}
	defer c.Close()
	moment := s.now()
	return c.write(func() error {
		var profile, started string
		err := c.row("SELECT profile, started_at FROM quota_pool_records WHERE job_id = ? AND pool = ?", job, pool).Scan(&profile, &started)
		if errors.Is(err, sql.ErrNoRows) {
			return errf(ExitFencing, "no quota_pool record to release: job=%s pool=%s", job, pool)
		}
		if err != nil {
			return sqlErr(err)
		}
		if _, err = c.exec("DELETE FROM quota_pool_records WHERE job_id = ? AND pool = ?", job, pool); err != nil {
			return sqlErr(err)
		}
		if err = settleJob(c, job, moment); err != nil {
			return err
		}
		kind := "quota_pool.release"
		if force {
			kind += ".force"
		}
		_, err = s.recordEvent(c, job, kind, map[string]any{"job_id": job, "pool": pool, "profile": profile, "started_at": started, "forced": force}, nil)
		return err
	})
}
