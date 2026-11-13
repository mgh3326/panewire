package arbiter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
)

// Claim is the host-local registration of a job: a jobs row plus the
// job.claim event the readers key on. Cross-host duplicate arbitration is the
// hk job record's business; the row here still serialises two spawns of one id
// on this host and carries the state the leases and gc read.
type Claim struct {
	Job, Lane, T      string
	Role              string // "implementer" (default) or "builder"
	ParentLane        string // required for, and only for, a builder
	AgentLabel        string // exact Herdr name/label; defaults to Job
	TaskID            string
	TaskLinkOverride  string
	TaskInheritedFrom string
	ReclaimReleased   bool // re-claim a released job instead of refusing a duplicate
}

// ClaimResult describes the registered job.
type ClaimResult struct {
	Job, OwnerLane, TLevel, State, AgentLabel, Role, ParentLane string
	CreatedAt, UpdatedAt                                        string
	EventSeq                                                    int
	Reclaimed                                                   bool
	PreviousCreatedAt                                           string
}

var tLevels = map[string]bool{"T0": true, "T1": true, "T2": true, "T3": true}

// ReleaseClaim releases a failed launch before it has acquired resources.
// It must be called only by the attempt that successfully claimed this row.
func (s *Store) ReleaseClaim(ctx context.Context, job string) error {
	c, err := s.rw(ctx, false)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.write(func() error {
		var state string
		if err := c.row("SELECT state FROM jobs WHERE job_id = ?", job).Scan(&state); err != nil {
			return sqlErr(err)
		}
		if state != "claimed" {
			return errf(ExitDenied, "only a resource-free claimed job can be released")
		}
		var held int
		if err := c.row(`SELECT (SELECT count(*) FROM quota_pool_records WHERE job_id = ?) + (SELECT count(*) FROM leases WHERE job_id = ? AND expires_at > ?)`, job, job, iso(s.now())).Scan(&held); err != nil {
			return sqlErr(err)
		}
		if held != 0 {
			return errf(ExitDenied, "claim still holds resources")
		}
		moment := s.now()
		if err := settleJob(c, job, moment); err != nil {
			return err
		}
		_, err := s.recordEvent(c, job, "job.spawn_aborted", map[string]any{"retryable": true}, nil)
		return err
	})
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (cl Claim) payload(label string) map[string]any {
	p := map[string]any{"owner_lane": cl.Lane, "t_level": cl.T, "role": cl.Role, "agent_label": label}
	if cl.ParentLane != "" {
		p["parent_lane"] = cl.ParentLane
	} else {
		p["parent_lane"] = nil
	}
	if cl.TaskID != "" {
		if n, e := strconv.Atoi(cl.TaskID); isDigits(cl.TaskID) && e == nil {
			p["task_id"] = n
		} else {
			p["task_id"] = cl.TaskID
		}
	}
	if cl.TaskLinkOverride != "" {
		p["task_link_override"] = cl.TaskLinkOverride
	}
	if cl.TaskInheritedFrom != "" {
		p["task_inherited_from"] = cl.TaskInheritedFrom
	}
	return p
}

// Claim registers a job. A job that is already registered is ExitDuplicate,
// except that a released job is re-claimed atomically when ReclaimReleased is
// set. The state database is created here if it does not exist.
func (s *Store) Claim(ctx context.Context, cl Claim) (ClaimResult, error) {
	var res ClaimResult
	if e := validJobID(cl.Job); e != nil {
		return res, e
	}
	if cl.Role == "" {
		cl.Role = "implementer"
	}
	switch {
	case cl.Role != "implementer" && cl.Role != "builder":
		return res, errf(ExitUsage, "unknown role %q", cl.Role)
	case cl.Role == "builder" && cl.ParentLane == "":
		return res, errf(ExitUsage, "a builder claim needs a parent lane")
	case cl.Role != "builder" && cl.ParentLane != "":
		return res, errf(ExitUsage, "a parent lane is for a builder claim only")
	case cl.Lane == "" || !tLevels[cl.T]:
		return res, errf(ExitUsage, "a claim needs a lane and a T level (T0..T3)")
	}
	label := cl.AgentLabel
	if label == "" {
		label = cl.Job
	}
	c, e := s.rw(ctx, true)
	if e != nil {
		return res, e
	}
	defer c.Close()
	moment := s.now()
	res = ClaimResult{Job: cl.Job, OwnerLane: cl.Lane, TLevel: cl.T, State: "claimed", AgentLabel: label, Role: cl.Role, ParentLane: cl.ParentLane, CreatedAt: iso(moment), UpdatedAt: iso(moment)}
	e = c.write(func() error {
		var owner, state, t, created string
		err := c.row("SELECT owner_lane, state, t_level, created_at FROM jobs WHERE job_id = ?", cl.Job).Scan(&owner, &state, &t, &created)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err = c.exec("INSERT INTO jobs (job_id, owner_lane, state, t_level, created_at, updated_at) VALUES (?, ?, 'claimed', ?, ?, ?)", cl.Job, cl.Lane, cl.T, iso(moment), iso(moment)); err != nil {
				return sqlErr(err)
			}
			res.EventSeq, err = s.recordEvent(c, cl.Job, "job.claim", cl.payload(label), nil)
			return err
		case err != nil:
			return sqlErr(err)
		}
		if !cl.ReclaimReleased || state != "released" {
			d := errf(ExitDuplicate, "job duplicate: %s is already registered (lane=%s state=%s t=%s created_at=%s)", cl.Job, owner, state, t, created)
			d.Detail = map[string]any{"job": cl.Job, "owner_lane": owner, "state": state, "t_level": t, "created_at": created}
			return d
		}
		// The row is evidence, not a disposable reservation: BEGIN IMMEDIATE
		// makes the released -> claimed test and update atomic.
		res.Reclaimed, res.PreviousCreatedAt, res.CreatedAt = true, created, created
		if _, err = c.exec("UPDATE jobs SET owner_lane = ?, state = 'claimed', t_level = ?, updated_at = ? WHERE job_id = ?", cl.Lane, cl.T, iso(moment), cl.Job); err != nil {
			return sqlErr(err)
		}
		p := cl.payload(label)
		p["previous_created_at"] = created
		p["previous_owner_lane"] = owner
		res.EventSeq, err = s.recordEvent(c, cl.Job, "job.reclaim", p, nil)
		return err
	})
	return res, e
}

// JobView is the durable recovery view of one exact job id.
type JobView struct {
	Job, OwnerLane, State, TLevel, CreatedAt, UpdatedAt string
	AgentLabel                                          string            // "" when the claim predates the mapping
	Receipt                                             map[string]string // newest job.spawned receipt; nil when absent
	RecentEventKinds                                    []string          // up to 10, oldest first
}

// JobGet reads one job. An unknown id is ExitNotFound.
func (s *Store) JobGet(ctx context.Context, job string) (JobView, error) {
	var v JobView
	c, e := s.ro(ctx)
	if e != nil {
		return v, e
	}
	defer c.Close()
	v.Job = job
	err := c.row("SELECT owner_lane, state, t_level, created_at, updated_at FROM jobs WHERE job_id = ?", job).Scan(&v.OwnerLane, &v.State, &v.TLevel, &v.CreatedAt, &v.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		d := errf(ExitNotFound, "job not found: %s", job)
		d.Detail = map[string]any{"job_id": job}
		return v, d
	}
	if err != nil {
		return v, sqlErr(err)
	}
	rows, err := c.rows("SELECT kind FROM events WHERE job_id = ? ORDER BY seq DESC LIMIT 10", job)
	if err != nil {
		return v, sqlErr(err)
	}
	for rows.Next() {
		var k string
		if err = rows.Scan(&k); err != nil {
			rows.Close()
			return v, sqlErr(err)
		}
		v.RecentEventKinds = append([]string{k}, v.RecentEventKinds...)
	}
	rows.Close()
	if v.AgentLabel, err = claimAgentLabel(c, job); err != nil {
		return v, err
	}
	v.Receipt, err = spawnReceipt(c, job)
	return v, err
}

// eventPayload decodes the payload of a stored event body.
func eventPayload(body string) (map[string]any, bool, error) {
	var doc map[string]any
	if e := json.Unmarshal([]byte(body), &doc); e != nil {
		return nil, false, e
	}
	p, ok := doc["payload"].(map[string]any)
	return p, ok, nil
}

// spawnReceipt is the newest job.spawned receipt, nil when there is none.
func spawnReceipt(c *conn, job string) (map[string]string, error) {
	var body, created string
	err := c.row("SELECT payload, created_at FROM events WHERE job_id = ? AND kind = 'job.spawned' ORDER BY seq DESC LIMIT 1", job).Scan(&body, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, sqlErr(err)
	}
	p, ok, err := eventPayload(body)
	if err != nil {
		return nil, errf(ExitStorage, "job.spawned event is unreadable: job=%s: %v", job, err)
	}
	if !ok {
		return nil, errf(ExitStorage, "job.spawned event payload is not an object: job=%s", job)
	}
	pane, _ := p["pane_id"].(string)
	if pane == "" {
		return nil, errf(ExitStorage, "job.spawned event pane_id is invalid: job=%s", job)
	}
	r := map[string]string{"pane_id": pane, "spawned_at": created}
	for _, k := range []string{"label", "profile", "workspace", "e6_arm"} {
		v, present := p[k]
		if !present || v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			return nil, errf(ExitStorage, "job.spawned event %s is not a string: job=%s", k, job)
		}
		r[k] = str
	}
	return r, nil
}

// claimAgentLabel is the current exact Herdr identity from claim and reclaim
// evidence: the label at the latest claim/reclaim. Claims that predate the
// mapping return "", and gc then refuses them rather than guess a label from a
// job-id prefix.
func claimAgentLabel(c *conn, job string) (string, error) {
	rows, err := c.rows("SELECT kind, payload FROM events WHERE job_id = ? AND kind IN ('job.claim', 'job.reclaim') ORDER BY seq", job)
	if err != nil {
		return "", sqlErr(err)
	}
	defer rows.Close()
	latest := ""
	original := map[string]bool{}
	for rows.Next() {
		var kind, body string
		if err = rows.Scan(&kind, &body); err != nil {
			return "", sqlErr(err)
		}
		p, ok, e := eventPayload(body)
		if e != nil {
			return "", errf(ExitStorage, "job claim/reclaim event is unreadable: job=%s kind=%s: %v", job, kind, e)
		}
		if !ok {
			continue
		}
		raw, present := p["agent_label"]
		if !present || raw == nil {
			continue
		}
		label, isStr := raw.(string)
		if !isStr || label == "" {
			return "", errf(ExitStorage, "job claim/reclaim event agent_label is invalid: job=%s", job)
		}
		if kind == "job.claim" {
			original[label] = true
		}
		latest = label
	}
	if err = rows.Err(); err != nil {
		return "", sqlErr(err)
	}
	if len(original) > 1 {
		labels := make([]string, 0, len(original))
		for l := range original {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		return "", errf(ExitStorage, "job.claim agent_label is ambiguous: job=%s labels=%v", job, labels)
	}
	return latest, nil
}

// Status is a read-only snapshot of the ledger.
type Status struct {
	Now     string
	Jobs    []JobRow
	Leases  []LeaseRow
	Quota   []QuotaRecord
	History []LeaseHistory
}

type JobRow struct{ Job, OwnerLane, State, TLevel, CreatedAt, UpdatedAt string }
type LeaseRow struct {
	Resource, Kind, Job, Token, AcquiredAt, ExpiresAt string
	Expired                                           bool
}
type QuotaRecord struct{ Job, Pool, Profile, StartedAt string }
type LeaseHistory struct {
	Resource, Kind, Job, Token, AcquiredAt, ExpiresAt, State, RecordedAt string
}

// Status reads the ledger. job or lane narrows it (not both); tokens are
// masked to their fencing sequence.
func (s *Store) Status(ctx context.Context, job, lane string) (Status, error) {
	var st Status
	if job != "" && lane != "" {
		return st, errf(ExitUsage, "job and lane cannot be combined")
	}
	c, e := s.ro(ctx)
	if e != nil {
		return st, e
	}
	defer c.Close()
	moment := s.now()
	st.Now = iso(moment)
	q, args := "SELECT job_id, owner_lane, state, t_level, created_at, updated_at FROM jobs", []any(nil)
	switch {
	case job != "":
		q, args = q+" WHERE job_id = ?", []any{job}
	case lane != "":
		q, args = q+" WHERE owner_lane = ?", []any{lane}
	}
	rows, err := c.rows(q+" ORDER BY job_id", args...)
	if err != nil {
		return st, sqlErr(err)
	}
	ids := map[string]bool{}
	for rows.Next() {
		var r JobRow
		if err = rows.Scan(&r.Job, &r.OwnerLane, &r.State, &r.TLevel, &r.CreatedAt, &r.UpdatedAt); err != nil {
			rows.Close()
			return st, sqlErr(err)
		}
		st.Jobs = append(st.Jobs, r)
		ids[r.Job] = true
	}
	rows.Close()
	scoped := job != "" || lane != ""
	rows, err = c.rows("SELECT resource, kind, job_id, token, acquired_at, expires_at FROM leases ORDER BY resource, kind")
	if err != nil {
		return st, sqlErr(err)
	}
	for rows.Next() {
		var r LeaseRow
		if err = rows.Scan(&r.Resource, &r.Kind, &r.Job, &r.Token, &r.AcquiredAt, &r.ExpiresAt); err != nil {
			rows.Close()
			return st, sqlErr(err)
		}
		if scoped && !ids[r.Job] {
			continue
		}
		exp, perr := parseISO(r.ExpiresAt)
		if perr != nil {
			rows.Close()
			return st, perr
		}
		r.Expired = !exp.After(moment)
		r.Token = maskToken(r.Token)
		st.Leases = append(st.Leases, r)
	}
	rows.Close()
	rows, err = c.rows("SELECT job_id, pool, profile, started_at FROM quota_pool_records ORDER BY job_id, pool")
	if err != nil {
		return st, sqlErr(err)
	}
	for rows.Next() {
		var r QuotaRecord
		if err = rows.Scan(&r.Job, &r.Pool, &r.Profile, &r.StartedAt); err != nil {
			rows.Close()
			return st, sqlErr(err)
		}
		if !scoped || ids[r.Job] {
			st.Quota = append(st.Quota, r)
		}
	}
	rows.Close()
	rows, err = c.rows("SELECT resource, kind, job_id, token, acquired_at, expires_at, state, recorded_at FROM lease_history ORDER BY id")
	if err != nil {
		return st, sqlErr(err)
	}
	for rows.Next() {
		var r LeaseHistory
		if err = rows.Scan(&r.Resource, &r.Kind, &r.Job, &r.Token, &r.AcquiredAt, &r.ExpiresAt, &r.State, &r.RecordedAt); err != nil {
			rows.Close()
			return st, sqlErr(err)
		}
		if !scoped || ids[r.Job] {
			r.Token = maskToken(r.Token)
			st.History = append(st.History, r)
		}
	}
	rows.Close()
	return st, sqlErr(rows.Err())
}
