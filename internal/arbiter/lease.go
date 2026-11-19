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
		out.EventSeq, err = s.recordEvent(ctx, c, req.Job, "lease.acquire", map[string]any{
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
		_, err = s.recordEvent(ctx, c, row.job, evKind, map[string]any{"resource": row.resource, "kind": row.kind, "token": maskToken(row.token), "forced": force}, nil)
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

// QuotaRun is the quota_pool record of one run. Epoch is the hub grant epoch
// of the attempt acquiring it (hk 5294); zero records no epoch, as a row an
// earlier binary wrote has none.
type QuotaRun struct {
	Job, Profile, LaunchProfile string
	Gate                        Gate
	Epoch                       uint64
}

// QuotaResult is the recorded run.
type QuotaResult struct {
	Job, Pool, Profile, StartedAt string
	EventSeq                      int
}

// RecordQuotaPool writes the quota_pool execution record (no TTL, no fencing)
// for a registered job and marks it leased. A second record for the same
// (job, pool) is ExitDuplicate, except that an attempt with a greater epoch
// takes over the row of a lower one: the hub revoked that attempt, and its
// late release will find the row no longer its own and leave it be.
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
		var superseded uint64
		if q.Epoch > 0 {
			held, err := quotaEpoch(c, q.Job, q.Gate.Pool)
			if err != nil {
				return err
			}
			if held > 0 && held < q.Epoch {
				if _, err = c.exec("DELETE FROM quota_pool_records WHERE job_id = ? AND pool = ?", q.Job, q.Gate.Pool); err != nil {
					return sqlErr(err)
				}
				superseded = held
			}
		}
		if _, err := c.exec("INSERT INTO quota_pool_records (job_id, pool, profile, started_at) VALUES (?, ?, ?, ?)", q.Job, q.Gate.Pool, q.Profile, iso(moment)); err != nil {
			if isUnique(err) {
				d := errf(ExitDuplicate, "quota_pool record duplicate: job=%s pool=%s", q.Job, q.Gate.Pool)
				d.Detail = map[string]any{"job": q.Job, "pool": q.Gate.Pool, "profile": q.Profile}
				return d
			}
			return sqlErr(err)
		}
		// A side row left by a record this one replaced or an older binary
		// deleted is overwritten; with no epoch it must not outlive its row.
		if q.Epoch > 0 {
			_, err := c.exec("INSERT OR REPLACE INTO quota_pool_epochs (job_id, pool, epoch, started_at) VALUES (?, ?, ?, ?)", q.Job, q.Gate.Pool, q.Epoch, iso(moment))
			if err != nil {
				return sqlErr(err)
			}
		} else if err := dropQuotaEpoch(c, q.Job, q.Gate.Pool); err != nil {
			return err
		}
		if _, err := c.exec("UPDATE jobs SET state = 'leased', updated_at = ? WHERE job_id = ?", iso(moment), q.Job); err != nil {
			return sqlErr(err)
		}
		p := map[string]any{"job_id": q.Job, "pool": q.Gate.Pool, "profile": q.Profile, "started_at": iso(moment)}
		if q.LaunchProfile != "" {
			p["launch_profile"] = q.LaunchProfile
		}
		if q.Epoch > 0 {
			p["epoch"] = q.Epoch
		}
		if superseded > 0 {
			p["superseded_epoch"] = superseded
		}
		for k, v := range q.Gate.Audit {
			p[k] = v
		}
		var err error
		out.EventSeq, err = s.recordEvent(ctx, c, q.Job, "quota_pool.record", p, nil)
		return err
	})
	return out, e
}

// quotaEpoch is the epoch that acquired the (job, pool) row, or 0 when the
// row has none: it predates the side table, an older binary recorded it, or
// there is no row.
func quotaEpoch(c *conn, job, pool string) (uint64, error) {
	var epoch int64
	err := c.row(`SELECT e.epoch FROM quota_pool_epochs AS e
		JOIN quota_pool_records AS q ON q.job_id = e.job_id AND q.pool = e.pool AND q.started_at = e.started_at
		WHERE e.job_id = ? AND e.pool = ?`, job, pool).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) || err == nil && epoch < 0 {
		return 0, nil
	}
	return uint64(epoch), sqlErr(err)
}

func dropQuotaEpoch(c *conn, job, pool string) error {
	_, err := c.exec("DELETE FROM quota_pool_epochs WHERE job_id = ? AND pool = ?", job, pool)
	return sqlErr(err)
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

// QuotaEpoch is the epoch that acquired the (job, pool) row; zero when the row
// has none, or there is no row.
func (s *Store) QuotaEpoch(ctx context.Context, job, pool string) (uint64, error) {
	if _, e := os.Stat(s.DB); errors.Is(e, fs.ErrNotExist) {
		return 0, nil
	}
	c, e := s.rw(ctx, false)
	if e != nil {
		return 0, e
	}
	defer c.Close()
	return quotaEpoch(c, job, pool)
}

// ReleaseQuotaPool deletes a job's quota_pool record whatever attempt
// acquired it, and marks the job released if it holds nothing else. A missing
// record is ExitFencing. It is for the operator override and for a caller that
// just recorded the row itself; an attempt ending releases with
// ReleaseQuotaPoolEpoch.
func (s *Store) ReleaseQuotaPool(ctx context.Context, job, pool string, force bool) error {
	_, err := s.release(ctx, job, pool, force, func(uint64) bool { return true })
	return err
}

// ReleaseQuotaPoolEpoch releases the (job, pool) row an attempt acquired, and
// only that: a row acquired at another epoch belongs to another attempt and is
// left alone (released is false), however the current grant compares.
//
// A row with no epoch predates this record and cannot be attributed to an
// attempt. It is released only when terminal, asked only for such a row, says
// the job itself has ended here, never by matching a grant. terminal runs
// under the ledger's write transaction, so it must not call back into the
// Store. A missing row is ExitFencing.
func (s *Store) ReleaseQuotaPoolEpoch(ctx context.Context, job, pool string, epoch uint64, terminal func() bool) (released bool, err error) {
	if epoch == 0 {
		return false, errf(ExitUsage, "a quota_pool release by epoch needs the attempt's epoch")
	}
	return s.release(ctx, job, pool, false, func(row uint64) bool {
		if row == 0 {
			return terminal != nil && terminal()
		}
		return row == epoch
	})
}

func (s *Store) release(ctx context.Context, job, pool string, force bool, mine func(rowEpoch uint64) bool) (bool, error) {
	c, e := s.rw(ctx, false)
	if e != nil {
		return false, e
	}
	defer c.Close()
	moment := s.now()
	released := false
	e = c.write(func() error {
		var profile, started string
		err := c.row("SELECT profile, started_at FROM quota_pool_records WHERE job_id = ? AND pool = ?", job, pool).Scan(&profile, &started)
		if errors.Is(err, sql.ErrNoRows) {
			return errf(ExitFencing, "no quota_pool record to release: job=%s pool=%s", job, pool)
		}
		if err != nil {
			return sqlErr(err)
		}
		epoch, err := quotaEpoch(c, job, pool)
		if err != nil {
			return err
		}
		if !mine(epoch) {
			return nil
		}
		if _, err = c.exec("DELETE FROM quota_pool_records WHERE job_id = ? AND pool = ?", job, pool); err != nil {
			return sqlErr(err)
		}
		if err = dropQuotaEpoch(c, job, pool); err != nil {
			return err
		}
		if err = settleJob(c, job, moment); err != nil {
			return err
		}
		kind := "quota_pool.release"
		if force {
			kind += ".force"
		}
		p := map[string]any{"job_id": job, "pool": pool, "profile": profile, "started_at": started, "forced": force}
		if epoch > 0 {
			p["epoch"] = epoch
		}
		if _, err = s.recordEvent(ctx, c, job, kind, p, nil); err != nil {
			return err
		}
		released = true
		return nil
	})
	return released && e == nil, e
}
