package fleet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// JobStore is the hk /v1/jobs surface panewire uses: claim the job id on the
// executing host, then merge allowlisted fields as the job moves
// claimed → spawned → completed | escalated | lost. The design is
// hk:doc design/2026-10-30/cross-host-jobs (§2.3 write path, §3.2 ids).
type JobStore interface {
	Claim(ctx context.Context, jobID, taskID, host string) error
	Put(ctx context.Context, jobID string, fields map[string]any) error
	Get(ctx context.Context, jobID string) (map[string]any, error)
}

var (
	errJobTaken    = errors.New("job_id_taken")
	errJobNotFound = errors.New("not_found")
	ErrJobNotFound = errJobNotFound
)

// hkJobs is the production JobStore over the shared handoffkeep HTTP client.
// now stamps each claim's client time (hk claimed_at).
type hkJobs struct {
	q   *hkQuota
	now func() time.Time
}

func newHKJobs(q *hkQuota, now func() time.Time) JobStore { return &hkJobs{q: q, now: now} }

func jobError(e error) error {
	var be *BackendError
	if errors.As(e, &be) {
		switch {
		case be.Status == http.StatusConflict && be.Code == "job_id_taken":
			return errJobTaken
		case be.Status == http.StatusNotFound:
			return errJobNotFound
		}
	}
	return e
}

func (j *hkJobs) Claim(ctx context.Context, jobID, taskID, host string) error {
	// A task-less job claims with a null task_id (hk docs/jobs.md: claims may
	// omit task_id or send null). A non-empty task must be numeric.
	body := map[string]any{"job_id": jobID, "task_id": nil, "host": host, "at": j.now().UTC()}
	if taskID != "" {
		task, e := strconv.ParseInt(taskID, 10, 64)
		if e != nil {
			return fmt.Errorf("job record claim needs a numeric task id")
		}
		body["task_id"] = task
	}
	return jobError(j.q.request(ctx, http.MethodPost, "/v1/jobs", body, nil))
}
func (j *hkJobs) Put(ctx context.Context, jobID string, fields map[string]any) error {
	return jobError(j.q.request(ctx, http.MethodPut, "/v1/jobs/"+url.PathEscape(jobID), fields, nil))
}
func (j *hkJobs) Get(ctx context.Context, jobID string) (map[string]any, error) {
	var rec map[string]any
	if e := j.q.request(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, &rec); e != nil {
		return nil, jobError(e)
	}
	return rec, nil
}

// jobStore resolves the record client: the injected one in tests, else the
// shared handoffkeep endpoint that quota reads already use (same URL and
// bearer credential, so no new credential surface).
func (a *App) jobStore() (JobStore, error) {
	if a.JobStore != nil {
		return a.JobStore, nil
	}
	c, e := a.quotaClient()
	if e != nil {
		return nil, e
	}
	a.JobStore = newHKJobs(c, a.Now)
	return a.JobStore, nil
}

// NewJobStore returns the hk /v1/jobs client for an explicit handoffkeep
// endpoint and bearer credential. It is how a node writes a job record with its
// own credential (spawn.json handoffkeep_env) rather than the hub's.
func NewJobStore(baseURL, token, cfID, cfSecret string) (JobStore, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return newHKJobs(client, time.Now), nil
}

// newNodeHKClient validates an explicit handoffkeep endpoint and credential
// and returns the shared HTTP client over them.
func newNodeHKClient(baseURL, token, cfID, cfSecret string) (*hkQuota, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("invalid handoffkeep endpoint")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, errors.New("invalid handoffkeep endpoint scheme")
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) && !isTailnetHost(parsed.Hostname()) {
		return nil, errors.New("handoffkeep endpoint requires HTTPS")
	}
	if token == "" || (cfID == "") != (cfSecret == "") {
		return nil, errors.New("invalid handoffkeep credential")
	}
	return &hkQuota{base: strings.TrimRight(baseURL, "/"), token: token, cfID: cfID, cfSecret: cfSecret}, nil
}

// NodeID reports the host id this app claims job records under: HUB_MACHINE_ID
// from the node env file. It is the host whose hk host grant this app holds.
func (a *App) NodeID() (string, bool) { return a.nodeID() }

// nodeEnvMaxBytes bounds the node env read, as in pacelever: a larger file is
// refused, not truncated.
const nodeEnvMaxBytes = 1 << 20

var nodeIDRE = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// nodeEnvPath locates the panewire node env file: PANEWIRE_NODE_ENV, then
// [quota].node_env, then the panewire default. Same source pacelever uses
// since PL-1.
func (a *App) nodeEnvPath() string {
	if v := strings.TrimSpace(os.Getenv("PANEWIRE_NODE_ENV")); v != "" {
		return expand(v)
	}
	if v := strings.TrimSpace(str(a.Config.Quota, "node_env")); v != "" {
		return expand(v)
	}
	home, _ := os.UserHomeDir()
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".config", "panewire", "hub-node.env")
}

// nodeID reads HUB_MACHINE_ID from the node env file — the executing host's
// id for job claims. Only that one value leaves the parser; the token in the
// same file is discarded. Parser kept identical to pacelever's.
func (a *App) nodeID() (string, bool) {
	path := a.nodeEnvPath()
	if path == "" {
		return "", false
	}
	f, e := os.Open(path)
	if e != nil {
		return "", false
	}
	defer f.Close()
	if info, e := f.Stat(); e != nil || !info.Mode().IsRegular() || info.Size() > nodeEnvMaxBytes {
		return "", false
	}
	id := ""
	scanner := bufio.NewScanner(io.LimitReader(f, nodeEnvMaxBytes))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return "", false
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			var e error
			if value, e = strconv.Unquote(value); e != nil {
				return "", false
			}
		}
		if key == "HUB_MACHINE_ID" {
			id = value
		}
	}
	if scanner.Err() != nil || !nodeIDRE.MatchString(id) {
		return "", false
	}
	return id, true
}

// claimJobRecord runs the hk job claim on the executing host before the
// arbiter claim. A taken id is the same refusal as the arbiter's duplicate
// (exit 74); hk unreachable fails closed like the task claim (exit 70).
func (a *App) claimJobRecord(ctx context.Context, s Spawn) error {
	store, e := a.jobStore()
	if e != nil {
		return fail(exitConfig, "job record claim unavailable: %v", e)
	}
	host, ok := a.nodeID()
	if !ok {
		return fail(exitConfig, "job record claim unavailable: no node id (HUB_MACHINE_ID in the node env file)")
	}
	if e = store.Claim(ctx, s.Job, s.Task, host); e != nil {
		if errors.Is(e, errJobTaken) {
			return fail(exitDuplicate, "job id %s already claimed", s.Job)
		}
		return fail(exitConfig, "job record claim failed: %v", e)
	}
	// hk answers an identical claim as a replay whatever state the record is
	// in. Job ids are never reused, so a replay that is past "claimed" is a
	// finished or running job, not a fresh claim: refuse it like a duplicate.
	rec, e := store.Get(ctx, s.Job)
	if e != nil {
		return fail(exitConfig, "job record claim unreadable: %v", e)
	}
	if st, _ := rec["state"].(string); st != "claimed" {
		return fail(exitDuplicate, "job id %s already used (record state %q)", s.Job, st)
	}
	return nil
}

// markSpawnRetryable preserves the identical host/task claim after verified
// pre-landing cleanup. A failed attempt is evidence, not a terminal job id. The
// hk record stays as it is: hk takes a result only with state=completed, so
// this fact lives in the local event and the hub's spawn operation.
func (a *App) markSpawnRetryable(ctx context.Context, id string) {
	_ = a.event(ctx, id, "job.spawn_retryable", map[string]any{"retry": "repeat the same spawn command on the same host"})
}

// abandonJobRecord marks a claimed record lost when the spawn aborts before
// the pane exists, so waiters stop on it instead of reading a claim that
// never runs. Best effort: a failure leaves job.record_failed.
func (a *App) abandonJobRecord(ctx context.Context, id string) {
	if e := a.putJobRecord(ctx, id, map[string]any{"state": "lost", "at": a.Now().UTC()}); e != nil {
		a.jobRecordFailed(ctx, id, "abandon", e)
	}
}

// putJobRecord merges allowlisted fields into the job's hk record.
func (a *App) putJobRecord(ctx context.Context, id string, fields map[string]any) error {
	store, e := a.jobStore()
	if e != nil {
		return e
	}
	return store.Put(ctx, id, fields)
}

// jobRecordFailed records a best-effort record-write failure: the durable
// job.record_failed event plus a warning, as hk doc §2.4 specifies for writes
// after the pane exists.
func (a *App) jobRecordFailed(ctx context.Context, id, op string, e error) {
	a.warn("job record %s write failed: %v", op, e)
	_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": op})
}

// spawnedJobRecord writes the spawned record after the brief lands. Only
// allowlisted metadata crosses: no brief text, command lines, pane output or
// absolute local paths.
func (a *App) spawnedJobRecord(ctx context.Context, s Spawn, seat Seat, launch Launch, pane, pool, owner string) {
	rec := map[string]any{
		"state":      "spawned",
		"at":         a.Now().UTC(),
		"role":       s.Role,
		"label":      s.Label,
		"family":     seat.Family,
		"profile":    s.Profile,
		"model":      launch.ModelID,
		"harness":    seat.Kind,
		"effort":     launch.Effort,
		"pool":       pool,
		"owner_lane": owner,
		"pane_id":    pane,
		"selection":  jobSelection(s, jobSelectionSource(s)),
	}
	if s.Parent != "" {
		rec["parent_lane"] = s.Parent
	}
	if s.Inherited != "" {
		rec["parent_job"] = s.Inherited
	}
	if wt := a.worktreeRef(ctx, s.CWD); wt != nil {
		rec["worktree"] = wt
	}
	putSpawnedJobRecord(ctx, a, s.Job, rec)
}

// worktreeRef builds the {origin, branch|null, head} record for the job's
// checkout, or nil when origin is absent or not a shareable remote (hk
// refuses local paths and file URLs).
func (a *App) worktreeRef(ctx context.Context, cwd string) map[string]any {
	r := a.call(ctx, "git", "-C", cwd, "remote", "get-url", "origin")
	if r.Code != 0 || !validWorktreeOrigin(strings.TrimSpace(r.Out)) {
		return nil
	}
	origin := strings.TrimSpace(r.Out)
	r = a.call(ctx, "git", "-C", cwd, "rev-parse", "HEAD")
	head := strings.TrimSpace(r.Out)
	if r.Code != 0 || head == "" {
		return nil
	}
	wt := map[string]any{"origin": origin, "head": head}
	if r = a.call(ctx, "git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"); r.Code == 0 {
		if b := strings.TrimSpace(r.Out); b != "" && b != "HEAD" {
			wt["branch"] = b
		}
	}
	return wt
}

var scpOrigin = regexp.MustCompile(`^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+:.+$`)

// validWorktreeOrigin mirrors the hk allowlist: an http/https/ssh/git URL
// without userinfo, a bare remote name, or the scp-style git@host:path form.
func validWorktreeOrigin(o string) bool {
	if o == "" || strings.HasPrefix(o, "/") || strings.HasPrefix(o, "~") || strings.HasPrefix(o, ".") || regexp.MustCompile(`^[A-Za-z]:[\\/]`).MatchString(o) {
		return false
	}
	if u, e := url.Parse(o); e == nil && u.Scheme != "" {
		return slices.Contains([]string{"http", "https", "ssh", "git"}, strings.ToLower(u.Scheme)) && u.User == nil && u.Host != ""
	}
	if scpOrigin.MatchString(o) {
		return true
	}
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`).MatchString(o)
}

// parentResolveTimeout bounds the hk wait when --parent-job names a job
// whose record has not propagated yet (hk doc: wait ≤30 s, then exit 75).
const parentResolveTimeout = 30 * time.Second

// resolveParent returns the local job record, else the hk one after a
// bounded wait for a lagging record.
func (a *App) resolveParent(ctx context.Context, id string) (Job, error) {
	j, e := a.job(id)
	if e == nil {
		return j, nil
	}
	return a.parentFromHK(ctx, id)
}

func (a *App) parentFromHK(ctx context.Context, id string) (Job, error) {
	deadline := a.Now().Add(parentResolveTimeout)
	var last error = errJobNotFound
	for {
		if store, e := a.jobStore(); e == nil {
			if rec, e := store.Get(ctx, id); e == nil {
				return jobFromRecord(id, rec), nil
			} else {
				last = e
			}
		} else {
			last = e
		}
		if !a.Now().Before(deadline) {
			break
		}
		if e := a.Sleep(ctx, 5*time.Second); e != nil {
			return Job{}, e
		}
	}
	return Job{}, fmt.Errorf("parent job %s: %w", id, last)
}

// jobFromRecord projects the fields spawn needs out of an hk job record.
func jobFromRecord(id string, rec map[string]any) Job {
	return Job{ID: id, Task: scalar(rec["task_id"]), Family: str(rec, "family"), Profile: str(rec, "profile"), Owner: str(rec, "owner_lane"), Label: str(rec, "label")}
}

// jobRecordOutcome maps a delegated job's hk record to a wait outcome — the
// W4 rule in the design: a terminal record state answers the wait; a pending
// or missing record and an unreadable hk answer nothing, and the caller's
// bound and local signals decide. warned is dedicated to the hk-unavailable
// message so an earlier unrelated wait warning cannot suppress it.
func (a *App) jobRecordOutcome(ctx context.Context, id string, warned *bool) waitOutcome {
	hkWarn := func() {
		if !*warned {
			*warned = true
			a.warn("job record unreadable; watching local signals (reason hk-unavailable)")
		}
	}
	store, e := a.jobStore()
	if e != nil {
		hkWarn()
		return waitOutcome{}
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		if !errors.Is(e, errJobNotFound) {
			hkWarn()
		}
		return waitOutcome{}
	}
	switch str(rec, "state") {
	case "completed":
		return waitOutcome{name: "completed", report: str(rec, "report_doc")}
	case "escalated":
		return waitOutcome{name: "escalate", detail: "hk job record escalated"}
	case "lost":
		return waitOutcome{name: "lost"}
	}
	return waitOutcome{}
}

// A live/unverified pane is not a lost job. Keep its immutable claim while the
// hub reconciles the attempt; no completion watcher is started before landing.
func (a *App) markSpawnUncertain(ctx context.Context, id string) {
	_ = a.event(ctx, id, "job.cleanup_pending", map[string]any{"reason": "pane_or_resources_unverified"})
}

// MarkJobLost records a claimed job as lost. The hub calls it once the pane
// behind a failed spawn is verifiably gone, so the job no longer holds its
// task and role.
func (a *App) MarkJobLost(ctx context.Context, id string) error {
	return a.putJobRecord(ctx, id, map[string]any{"state": "lost", "at": a.Now().UTC()})
}

// MarkJobLostBecause is MarkJobLost with an hk end_reason; an empty one writes
// none.
func (a *App) MarkJobLostBecause(ctx context.Context, id, endReason string) error {
	record := map[string]any{"state": "lost", "at": a.Now().UTC()}
	if endReason != "" {
		record["end_reason"] = endReason
	}
	return a.putJobRecord(ctx, id, record)
}
