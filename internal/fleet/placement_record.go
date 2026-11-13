package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

const placementDeliveryBudget = 2 * time.Second

// PlacementSignals copies only the hub's numeric placement inputs. It never
// serializes arbitrary hub metadata, account logins, or command output.
type PlacementSignals struct {
	SignalAgeS   *float64 `json:"signal_age_s,omitempty"`
	CPUBusyShort *float64 `json:"cpu_busy_short,omitempty"`
	CPUBusy5m    *float64 `json:"cpu_busy_5m,omitempty"`
	MemAvailable *float64 `json:"mem_available,omitempty"`
	SwapInMBPS   *float64 `json:"swap_in_mbps,omitempty"`
	PowerSource  string   `json:"power_source,omitempty"`
	BridgedJobs  *int     `json:"bridged_jobs,omitempty"`
	ActiveJobs   *int     `json:"active_jobs,omitempty"`
	Tasks        *int     `json:"tasks,omitempty"`
	TaskSlots    *int     `json:"task_slots,omitempty"`
	Power        *float64 `json:"power,omitempty"`
	// LiveJobs, Load1PerCore and Terms are the score v2 inputs: the terms are
	// the factors of the candidate's free fraction (docs/placement.md).
	LiveJobs     *int            `json:"live_jobs,omitempty"`
	Load1PerCore *float64        `json:"load1_per_core,omitempty"`
	Terms        *PlacementTerms `json:"terms,omitempty"`
}

// PlacementTerms are the hub's five score factors for one candidate, each 0..1.
type PlacementTerms struct {
	CPU  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
	IO   float64 `json:"io"`
	Load float64 `json:"load"`
	Jobs float64 `json:"jobs"`
}

type placementCandidateRecord struct {
	Host    string   `json:"host"`
	Outcome string   `json:"outcome"`
	Reason  string   `json:"reason"`
	Score   *float64 `json:"score,omitempty"`
	PlacementSignals
}

// placementExecution is emitted by the executing panewire in its OK receipt.
// The initiator must not guess the model from its own catalog: admission on
// the destination can resolve a different launch rung.
type placementExecution struct {
	Kind       string `json:"-"`
	Host       string `json:"chosen_host,omitempty"`
	Family     string `json:"family,omitempty"`
	Model      string `json:"model,omitempty"`
	Role       string `json:"role,omitempty"`
	Profile    string `json:"profile,omitempty"`
	Pool       string `json:"pool,omitempty"`
	PaneID     string `json:"pane_id,omitempty"`
	Repo       string `json:"repo,omitempty"`
	AccountKey string `json:"account_key,omitempty"`
}

type placementRecord struct {
	JobID       string                     `json:"job_id"`
	Attempt     int                        `json:"attempt"`
	TaskID      int64                      `json:"task_id,omitempty"`
	DecidedAt   string                     `json:"decided_at"`
	ToolVersion string                     `json:"tool_version"`
	Source      string                     `json:"source"`
	Ranking     string                     `json:"ranking"`
	StateSource string                     `json:"state_source"`
	Outcome     string                     `json:"outcome"`
	Reason      string                     `json:"reason"`
	Candidates  []placementCandidateRecord `json:"candidates"`
	placementExecution
}

type placementDecision struct {
	plan          *placePlan
	execution     placementExecution
	contactedHost string
	started       bool
	placed        bool
	recorded      bool
}

// pendingPlacement is local-only. Old executors may return an OK receipt
// without execution metadata; resolve their shared job record before posting
// the immutable payload, including when hk is temporarily unavailable.
type pendingPlacement struct {
	Record     placementRecord `json:"record"`
	ResolveJob bool            `json:"resolve_job,omitempty"`
}

// placementRole is the hk placement record's role: review, consult or worker
// (the work class), not a spawn role.
func placementRole(s Spawn) string {
	role := firstNonempty(s.RoutingRole, s.Role)
	switch {
	case s.Role == "reviewer" || role == "reviewer" || role == "judge":
		return "review"
	case role == "consult":
		return "consult"
	default:
		return "worker"
	}
}

func placementAccount(key string) string {
	if key == "" {
		return ""
	}
	pool, fp, ok := strings.Cut(key, ":")
	if ok && validID(pool) && accountFP.MatchString(fp) {
		return key
	}
	// An opaque alias is allowed too, but an unrecognized key must never
	// accidentally put a login or a credential in the log.
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("account:%x", sum[:8])
}

func (e placementExecution) receipt() string {
	e.AccountKey = placementAccount(e.AccountKey)
	b, _ := json.Marshal(e)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (d *placementDecision) remoteReceipt(host, out string) {
	if d == nil {
		return
	}
	for _, line := range strings.Split(out, "\n") {
		failed := strings.HasPrefix(line, "FAILED placement=")
		if !failed && !strings.HasPrefix(line, "OK pane=") {
			continue
		}
		if !failed {
			d.started, d.placed = true, true
			d.execution = placementExecution{Host: host}
		}
		for _, word := range strings.Fields(line) {
			if raw, ok := strings.CutPrefix(word, "placement="); ok {
				b, err := base64.RawURLEncoding.DecodeString(raw)
				var e placementExecution
				if err == nil && json.Unmarshal(b, &e) == nil && e.Host == host && e.Family != "" && (failed || e.PaneID != "") {
					e.AccountKey = placementAccount(e.AccountKey)
					d.started = true
					d.execution = e
					return
				}
			}
		}
	}
}

func (a *App) finishPlacement(ctx context.Context, s Spawn, err error) {
	if s.DryRun || !validID(s.Job) || s.placement.recorded {
		return
	}
	d := s.placement
	d.recorded = true
	if s.Delegated {
		if err != nil && d.started && !d.placed {
			fmt.Fprintf(a.Out, "FAILED placement=%s\n", d.execution.receipt())
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), placementDeliveryBudget)
	defer cancel()
	r := placementRecord{JobID: s.Job, DecidedAt: a.Now().UTC().Format(time.RFC3339Nano), ToolVersion: BuildVersion,
		Source: "auto", Ranking: "unknown", StateSource: "unknown", Outcome: "refused", Candidates: []placementCandidateRecord{}}
	r.TaskID, _ = strconv.ParseInt(s.Task, 10, 64)
	r.Profile, r.Role, r.AccountKey = s.Profile, placementRole(s), placementAccount(s.AccountKey)
	if seat, ok := a.Policy.Profiles[s.Profile]; ok {
		r.Family = seat.Family
	}
	if s.Host != "auto" {
		r.Source, r.Ranking = "forced", "forced"
	}
	// Selection can refuse before a placePlan exists. Keep its host trace
	// too, projecting only the host, scores, and structured skip reasons.
	if d.plan == nil {
		r.Candidates = append(r.Candidates, selectionCandidates(s.Sel, d.execution.Host)...)
	}
	if d.plan != nil {
		r.Pool, r.Repo = d.plan.pool, d.plan.repo
		if r.Source == "auto" {
			switch d.plan.ranking() {
			case "score", "wake":
				r.Ranking = d.plan.ranking()
			case "raw_load":
				r.Ranking = "load"
			}
		}
		for _, c := range d.plan.candidates {
			outcome, reason := "skipped", c.outcome
			if c.name == d.execution.Host && (d.placed || d.started) {
				outcome, reason = "chosen", "available"
			} else if reason == "" {
				reason = "not_chosen"
			}
			signals := c.signals
			if signals.PowerSource != "ac" && signals.PowerSource != "battery" {
				signals.PowerSource = ""
			}
			r.Candidates = append(r.Candidates, placementCandidateRecord{Host: c.name, Outcome: outcome, Reason: placementSkipReason(reason), Score: c.score, PlacementSignals: signals})
		}
	}
	if len(r.Candidates) == 0 && d.contactedHost != "" {
		outcome, reason := "refused", exitReason(exitCode(err))
		if d.started || d.placed {
			outcome, reason = "chosen", "available"
		}
		r.Candidates = append(r.Candidates, placementCandidateRecord{Host: d.contactedHost, Outcome: outcome, Reason: reason})
	}
	stateReason := ""
	if a.fleetLog != nil && a.fleetLog.hosts != nil {
		r.StateSource = a.fleetLog.hosts.Source
		stateReason = a.fleetLog.hosts.Reason
		r.Reason = stateReason
	}
	if err != nil {
		r.Reason = exitReason(exitCode(err))
		if d.started {
			r.Outcome = "failed"
		}
		switch exitCode(err) {
		case exitUsage, exitSeatRefused, exitMockRefused, exitLocalDisabled, exitPlacement, exitRoleWait, exitRoleEscalate, exitAuthUnavailable, exitQuotaUnknown:
			r.Outcome = "refused"
		}
	}
	if d.execution.Host != "" && (d.started || d.placed) {
		family, pool, repo, account := r.Family, r.Pool, r.Repo, r.AccountKey
		r.placementExecution = d.execution
		r.Family = firstNonempty(r.Family, family)
		r.Pool = firstNonempty(r.Pool, pool)
		r.Repo = firstNonempty(r.Repo, repo)
		r.AccountKey = firstNonempty(r.AccountKey, account)
		r.Role = firstNonempty(r.Role, placementRole(s))
		r.Profile = firstNonempty(r.Profile, s.Profile)
	}
	if d.placed {
		r.Outcome, r.Reason = "placed", stateReason
	}
	if r.Repo == "" {
		r.Repo = a.placementRepo(ctx, s.CWD)
	}
	if !validID(r.Repo) {
		r.Repo = ""
	}
	r.AccountKey = placementAccount(r.AccountKey)
	p := pendingPlacement{Record: r, ResolveJob: d.placed && d.execution.Family == ""}
	if e := a.spoolPlacement(&p); e != nil {
		a.warn("placement record could not be spooled")
		return
	}
	a.deliverPlacements(ctx)
}

// selectionCandidates projects a selection's host trace into one candidate row
// per host. launched says the host named chosen took the job.
func selectionCandidates(sel map[string]any, chosen string) []placementCandidateRecord {
	var out []placementCandidateRecord
	hosts, _ := sel["hosts"].([]any)
	at, untried := map[string]int{}, map[string]bool{}
	for _, raw := range hosts {
		host, _ := raw.(map[string]any)
		name := str(host, "host")
		if pass, _ := host["pass"].(int); pass > 1 {
			// The credit band retries a host pass 1 already listed. Merge its
			// row so the refusal it carries is not lost: it replaces a pass 1
			// row that tried nothing, and otherwise pass 1 keeps the entry.
			if i, ok := at[name]; ok {
				if reason, failed := creditBandRefusal(host); failed && untried[name] && out[i].Outcome != "chosen" {
					out[i].Reason = placementSkipReason(reason)
				}
				continue
			}
		}
		row := placementCandidateRecord{Host: name, Outcome: "skipped", Reason: placementSkipReason(str(host, "outcome"))}
		// A chosen host carries no refusal, whether or not execution started:
		// the selection chose it, so "chosen" is never a skip reason.
		if name == chosen {
			row.Outcome, row.Reason = "chosen", "available"
		}
		if score, ok := number(host, "score"); ok {
			row.Score = &score
		}
		tried, _ := host["tried"].([]any)
		at[name], untried[name] = len(out), len(tried) == 0
		out = append(out, row)
	}
	return out
}

// creditBandRefusal names why a host's credit band attempt did not place: the
// gate's quota result when it gave one, else the host row's own outcome. It
// reports false for a row that placed or never tried, which carries no refusal.
func creditBandRefusal(host map[string]any) (string, bool) {
	switch got := str(host, "admission_outcome"); got {
	case "quota_exhausted", "quota_unknown":
		return got, true
	case "no_rung":
		if tried, _ := host["tried"].([]any); len(tried) > 0 {
			return str(host, "outcome"), true
		}
	}
	return "", false
}

func placementSkipReason(reason string) string {
	// A transport's repo:<error> can contain paths and stderr. Only its
	// structured prefix belongs in this record.
	if strings.HasPrefix(reason, "repo:") {
		return "repo_unavailable"
	}
	return fleetevent.Code(fleetevent.Redact(reason))
}

func (a *App) placementRepo(ctx context.Context, cwd string) string {
	repo, _ := a.placementRepoCheckout(ctx, cwd)
	return repo
}

// placementRepoCheckout answers the repo key and whether the spawn carries a
// linked-worktree checkout the target node can clone instead of relying on
// its cwd_map.
func (a *App) placementRepoCheckout(ctx context.Context, cwd string) (string, bool) {
	if cwd == "" {
		return "", false
	}
	bounded, cancel := context.WithTimeout(ctx, placementDeliveryBudget)
	defer cancel()
	if repo := a.localRepo(bounded, cwd); repo != nil {
		return RepoKey(repo.name), a.carriesCheckout(repo)
	}
	return "", false
}

// Attempt markers survive successful delivery. O_EXCL allocates without a
// shared lock, so concurrent invocations cannot overwrite one another's key.
func (a *App) spoolPlacement(p *pendingPlacement) error {
	dir := filepath.Join(a.stateDir(), "placement-attempts", p.Record.JobID)
	if e := noSymlinks(dir); e != nil {
		return e
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	for n := 1; ; n++ {
		f, e := os.OpenFile(filepath.Join(dir, strconv.Itoa(n)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if os.IsExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if e = f.Close(); e != nil {
			return e
		}
		p.Record.Attempt = n
		b, e := json.Marshal(p)
		if e != nil {
			return e
		}
		return atomicFile(filepath.Join(a.stateDir(), "placements", p.Record.JobID+"."+strconv.Itoa(n)+".json"), b, 0600)
	}
}

func (a *App) flushPlacements() {
	ctx, cancel := context.WithTimeout(context.Background(), placementDeliveryBudget)
	defer cancel()
	a.deliverPlacements(ctx)
}

func (a *App) deliverPlacements(ctx context.Context) {
	dir := filepath.Join(a.stateDir(), "placements")
	if noSymlinks(dir) != nil {
		return
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) == 0 {
		return
	}
	c, e := a.quotaClient()
	if e != nil {
		a.warn("placement delivery unavailable; records remain spooled")
		return
	}
	cursorPath := filepath.Join(a.stateDir(), "placement-cursor")
	cursor, _ := os.ReadFile(cursorPath)
	start := sort.Search(len(entries), func(i int) bool { return entries[i].Name() > string(cursor) })
	lastAttempted := ""
	rejected := false
	defer func() {
		if rejected {
			a.capRejectedPlacements()
		}
		if lastAttempted != "" {
			if e := atomicFile(cursorPath, []byte(lastAttempted), 0600); e != nil {
				a.warn("placement delivery cursor could not be saved")
			}
		}
	}()
	attempted := 0
	for offset := 0; offset < len(entries); offset++ {
		if attempted >= 32 || ctx.Err() != nil {
			break
		}
		ent := entries[(start+offset)%len(entries)]
		if ent.Type()&os.ModeSymlink != 0 || ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		attempted++
		previous := lastAttempted
		lastAttempted = ent.Name()
		path := filepath.Join(dir, ent.Name())
		b, e := os.ReadFile(path)
		var p pendingPlacement
		if e != nil || json.Unmarshal(b, &p) != nil {
			a.warn("unreadable placement spool entry")
			continue
		}
		if p.ResolveJob {
			store, err := a.jobStore()
			if err != nil {
				break
			}
			job, err := store.Get(ctx, p.Record.JobID)
			if err != nil || str(job, "family") == "" || str(job, "pane_id") == "" {
				a.warn("placement execution metadata unavailable; record remains spooled")
				continue
			}
			r := &p.Record
			r.Family, r.Model, r.Profile, r.Pool, r.PaneID = str(job, "family"), str(job, "model"), str(job, "profile"), str(job, "pool"), str(job, "pane_id")
			p.ResolveJob = false
			b, _ = json.Marshal(p)
			// Freeze the resolved payload before its first POST, so an
			// ambiguous HTTP result always retries identical metadata.
			if atomicFile(path, b, 0600) != nil {
				continue
			}
		}
		e = c.request(ctx, http.MethodPost, "/v1/placements", p.Record, nil)
		var be *BackendError
		conflict := errors.As(e, &be) && be.Status == http.StatusConflict
		if errors.As(e, &be) && (be.Status == http.StatusBadRequest || be.Status == http.StatusUnprocessableEntity) {
			rejectedDir := filepath.Join(a.stateDir(), "placements-rejected")
			moveErr := noSymlinks(rejectedDir)
			if moveErr == nil {
				moveErr = os.MkdirAll(rejectedDir, 0700)
			}
			if moveErr == nil {
				moveErr = os.Chmod(path, 0600)
			}
			if moveErr == nil {
				moveErr = os.Rename(path, filepath.Join(rejectedDir, ent.Name()))
			}
			if moveErr != nil {
				_, sourceErr := os.Stat(path)
				if !os.IsNotExist(moveErr) || !os.IsNotExist(sourceErr) {
					lastAttempted = previous
					a.warn("placement record could not be quarantined; records remain spooled")
					break
				}
			}
			rejected = true
			// The sidecar keeps hk's answer next to the record so the refusal
			// can be read after the fact; a failed write only loses the note.
			note, _ := json.Marshal(map[string]any{"status": be.Status, "error": be.Code, "reason": be.Reason})
			if atomicFile(filepath.Join(rejectedDir, ent.Name()+".reason"), note, 0600) != nil {
				a.warn("placement rejection reason could not be saved")
			}
			if be.Reason != "" {
				a.warn("placement record %s rejected: hk HTTP %d %s: %s", ent.Name(), be.Status, be.Code, be.Reason)
			} else {
				a.warn("placement record %s rejected: hk HTTP %d %s", ent.Name(), be.Status, be.Code)
			}
			continue
		}
		if e != nil && !conflict {
			lastAttempted = previous
			// Name hk's answer (status and error code, never the body) so a
			// refusal such as a token scope can be told from an outage.
			if errors.As(e, &be) {
				a.warn("placement delivery failed: hk HTTP %d %s; records remain spooled", be.Status, be.Code)
			} else {
				a.warn("placement delivery failed: hk unreachable; records remain spooled")
			}
			break
		}
		if conflict {
			a.warn("placement delivery HTTP 409; conflicting record dropped")
		}
		if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
			a.warn("placement spool entry could not be removed")
		}
	}
}

func (a *App) capRejectedPlacements() {
	dir := filepath.Join(a.stateDir(), "placements-rejected")
	entries, err := os.ReadDir(dir)
	if err != nil {
		a.warn("placement quarantine could not be read")
		return
	}
	type file struct {
		name     string
		modified time.Time
	}
	var files []file
	for _, entry := range entries {
		// A rejection-reason sidecar is not a record: it goes with its record.
		if entry.Type().IsRegular() && !strings.HasSuffix(entry.Name(), ".reason") {
			info, err := entry.Info()
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				a.warn("placement quarantine entry could not be inspected")
				return
			}
			files = append(files, file{entry.Name(), info.ModTime()})
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modified.Equal(files[j].modified) {
			return files[i].name < files[j].name
		}
		return files[i].modified.Before(files[j].modified)
	})
	dropped := 0
	for _, f := range files[:max(0, len(files)-1000)] {
		if err := os.Remove(filepath.Join(dir, f.name)); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			a.warn("placement quarantine entry could not be removed")
			break
		}
		_ = os.Remove(filepath.Join(dir, f.name+".reason"))
		dropped++
	}
	if dropped > 0 {
		a.warn("placement quarantine dropped %d oldest records", dropped)
	}
}
