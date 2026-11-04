package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

type Spawn struct {
	CWD, Profile, Prompt, Workspace, Label, T, Job, Owner, Effort, Role, Lane, Parent, Task, ParentJob, Host, Report, ExecutionLane, OperatorRequest, RequestedBy, Purpose, LaneDefer, ReportRelative string
	OverrideReason, WaitTimeout, Urgency                                                                                                                                                              string
	Keep, DupOK, Bypass, DryRun, Wait, AllowEscalation                                                                                                                                                bool
	Inherited, ParentFamily                                                                                                                                                                           string
	// Tags lists the capability tags a host must carry and ExcludeHosts the
	// hosts placement must not pick. Delegated marks a spawn delivered by
	// another initiator. Sel carries the selection decision record; all four
	// are initiator-local and never cross the delegation wire.
	Tags         string `json:"-"`
	ExcludeHosts string `json:"-"`
	// BuilderFamily and BuilderJob name the author's family and job for a
	// reviewer or tester whose builder job ran on another host, so its
	// metadata is not readable here (--parent-job needs local metadata).
	BuilderFamily string         `json:"-"`
	BuilderJob    string         `json:"-"`
	Delegated     bool           `json:"-"`
	Sel           map[string]any `json:"-"`
	// route is the routing decision that admitted this spawn (auto
	// selection or admitPinnedProfile); launch admission reads its
	// family_effort opt-in. Initiator-local like Sel.
	route *routing
	// Grade is the task grade a caller states itself (`panewire delegate`),
	// used instead of the "(grade X, Tn" marker in the bound task's title.
	Grade string `json:"-"`
	// ExcludeFamilies and OnlyFamily narrow automatic selection to seats of
	// other families, or of one family (`panewire delegate --panel`).
	ExcludeFamilies []string `json:"-"`
	OnlyFamily      string   `json:"-"`
	// RoutingRole and AccountKey bind delegated admission to the selection.
	RoutingRole string `json:"routing_role,omitempty"`
	AccountKey  string `json:"account_key,omitempty"`
	// FreshTTLS is the shared-quota freshness window, in seconds, under which
	// the placing host admitted AccountKey. The executing host rechecks the
	// account with the same window, so one reading cannot be fresh at plan
	// time and stale at spawn time only because the two hosts set (or
	// default) fresh_ttl_s differently.
	FreshTTLS int64 `json:"fresh_ttl_s,omitempty"`
	// SelHost is the host host-first selection admitted the account on;
	// placement tries it first among hosts in the same load bucket.
	// TaskGrade is the grade placement checks against a host's grades
	// list, empty for lead and consult.
	SelHost   string `json:"-"`
	TaskGrade string `json:"-"`
	placement *placementDecision
}

func parseSpawn(args []string) (Spawn, error) {
	s := Spawn{Role: "worker", Host: "auto"}
	f := flags("spawn")
	for _, p := range []struct {
		k string
		v *string
	}{{"c", &s.CWD}, {"m", &s.Profile}, {"profile", &s.Profile}, {"p", &s.Prompt}, {"w", &s.Workspace}, {"l", &s.Label}, {"t", &s.T}, {"job", &s.Job}, {"owner", &s.Owner}, {"effort", &s.Effort}, {"role", &s.Role}, {"lane", &s.Lane}, {"parent", &s.Parent}, {"task", &s.Task}, {"parent-job", &s.ParentJob}, {"host", &s.Host}, {"report", &s.Report}, {"L", &s.ExecutionLane}, {"operator-request", &s.OperatorRequest}, {"requested-by", &s.RequestedBy}, {"purpose", &s.Purpose}, {"lane-defer", &s.LaneDefer}, {"report-relative", &s.ReportRelative}, {"tags", &s.Tags}, {"exclude-host", &s.ExcludeHosts}, {"override-reason", &s.OverrideReason}, {"wait-timeout", &s.WaitTimeout}, {"urgency", &s.Urgency}, {"builder-family", &s.BuilderFamily}, {"builder-job", &s.BuilderJob}, {"routing-role", &s.RoutingRole}} {
		f.StringVar(p.v, p.k, *p.v, "")
	}
	f.StringVar(&s.Grade, "grade", "", "task grade (workers only)")
	f.StringVar(&s.Workspace, "workspace", "", "local herdr workspace")
	f.BoolVar(&s.Keep, "keep", false, "")
	f.BoolVar(&s.DupOK, "job-dup-ok", false, "")
	f.BoolVar(&s.Bypass, "task-hk-bypass", false, "")
	f.BoolVar(&s.DryRun, "dry-run", false, "")
	f.BoolVar(&s.Wait, "wait", false, "")
	f.BoolVar(&s.AllowEscalation, "allow-escalation", false, "")
	if e := f.Parse(args); e != nil {
		return s, fail(exitUsage, "%v", e)
	}
	if f.NArg() != 0 {
		return s, fail(exitUsage, "unexpected spawn arguments")
	}
	if s.Job == "" {
		s.Job = s.Label
	}
	if s.Role == "captain" {
		s.Role = "builder"
	}
	// An operator request is the operator-request purpose: the astra gate admits
	// only director, architect or operator-request, and a spawn that cited the
	// operator's request but forgot --purpose was refused with role_restricted.
	if s.OperatorRequest != "" && s.Purpose == "" {
		s.Purpose = "operator-request"
	}
	if flagPassed(f, "grade") && (topRoutingRole(builtinRoleMap[s.Role]) || topRoutingRole(s.RoutingRole)) {
		return s, fail(exitConfig, "%s: --grade is not accepted for lead or consult", noLeadGradeRule)
	}
	return s, nil
}

type Task struct {
	ID        any            `json:"id"`
	Title     string         `json:"title"`
	State     string         `json:"state"`
	ClaimedBy string         `json:"claimed_by"`
	Refs      map[string]any `json:"refs"`
	CreatedAt string         `json:"created_at"`
	Priority  any            `json:"priority"`
}

// unlinkedReviewer is a reviewer that names the author's family but no
// builder job: it reviews a task it neither inherits nor claims, so several
// passes can run on the same task.
// unlinkedReviewer reads the resolved fields that cross the ssh envelope
// (ParentFamily, Inherited, ParentJob); BuilderFamily/BuilderJob do not, so a
// test of those would read false on the receiving host.
func (s Spawn) unlinkedReviewer() bool {
	return s.Role == "reviewer" && s.ParentFamily != "" && s.Inherited == "" && s.ParentJob == ""
}

func (a *App) taskPrecheck(ctx context.Context, s Spawn) (Task, bool, error) {
	r := a.call(ctx, "handoffkeep", "tasks", "show", s.Task)
	var t Task
	if r.Code != 0 {
		if strings.Contains(r.Out+r.Err, "not_found") {
			return t, false, fail(exitUsage, "task does not exist")
		}
		if s.Bypass {
			return t, true, nil
		}
		return t, false, fail(exitConfig, "task precheck unavailable: %s", r.Err)
	}
	if e := json.Unmarshal([]byte(r.Out), &t); e != nil || t.State == "" {
		return t, false, fail(exitConfig, "unreadable task precheck")
	}
	if t.ID != nil && scalar(t.ID) != s.Task {
		return t, false, fail(exitConfig, "task response ID mismatch")
	}
	if s.Inherited != "" || s.unlinkedReviewer() {
		return t, false, nil
	}
	if slices.Contains([]string{"merged", "dropped", "hold", "needs_decision"}, t.State) {
		return t, false, fail(exitUsage, "task unavailable: %s", t.State)
	}
	if j := str(t.Refs, "job_id"); j != "" && j != s.Job {
		return t, false, fail(exitUsage, "task bound to another job")
	}
	if str(t.Refs, "job_id") == "" && t.ClaimedBy != "" && t.ClaimedBy != s.Label {
		return t, false, fail(exitUsage, "task has another claimant without a job binding")
	}
	return t, false, nil
}
func (a *App) taskLink(ctx context.Context, s Spawn, bypassed bool) error {
	status := "inherited"
	if s.unlinkedReviewer() {
		// No claim: the task stays bound to whatever it was bound to.
		status = "unlinked"
	} else if s.Inherited == "" {
		r := a.call(ctx, "handoffkeep", "tasks", "claim", s.Task, "--by", s.Label, "--job-id", s.Job)
		var t Task
		if r.Code != 0 {
			if !bypassed || !s.Bypass || strings.Contains(r.Out+r.Err, "conflict") || strings.Contains(r.Out+r.Err, "refused") {
				return fail(exitConfig, "task claim refused: %s", r.Err)
			}
			status = "bypassed"
		} else {
			if e := json.Unmarshal([]byte(r.Out), &t); e != nil || t.ClaimedBy != s.Label || str(t.Refs, "job_id") != s.Job {
				return fail(exitConfig, "task claim did not confirm claimed_by and refs.job_id")
			}
			status = "linked"
		}
	}
	return a.event(ctx, s.Job, "job.task_link", map[string]any{"task_id": s.Task, "job_id": s.Job, "claimed_by": s.Label, "status": status, "inherited_from": s.Inherited})
}
func (a *App) Spawn(ctx context.Context, s Spawn) (err error) {
	startedAdmission := time.Now()
	admissionRecorded := false
	defer func() {
		if !admissionRecorded {
			a.stage("placement", startedAdmission)
		}
	}()
	// Keep every herdr operation and lane identity on the same session.
	if a.HerdrSession == "" {
		a.HerdrSession = firstNonempty(os.Getenv("HERDR_SESSION"), "default")
	}
	if s.Workspace == "" {
		s.Workspace = str(a.Config.Local, "workspace")
		if s.Delegated && s.Workspace == "" {
			return fail(exitConfig, "WORKSPACE_UNCONFIGURED: set hosts.<host>.workspace or executing local.workspace")
		}
	}
	s.placement = &placementDecision{}
	defer func() {
		if a.ObserveReceipt != nil {
			x := s.placement.execution
			a.ObserveReceipt(ExecutionReceipt{CWD: s.CWD, Label: s.Label, Job: s.Job, Host: x.Host, Pane: x.PaneID, Profile: firstNonempty(x.Profile, s.Profile), Model: x.Model, Kind: x.Kind, Role: firstNonempty(x.Role, s.Role)})
		}
	}()

	defer func() { a.finishPlacement(ctx, s, err) }()
	defer func() { a.spawnEvents(s, err) }()
	if s.Grade != "" && (topRoutingRole(builtinRoleMap[s.Role]) || topRoutingRole(s.RoutingRole)) {
		return fail(exitConfig, "%s: --grade is not accepted for lead or consult", noLeadGradeRule)
	}
	if s.CWD == "" || s.Prompt == "" || s.Label == "" || !slices.Contains([]string{"T0", "T1", "T2", "T3"}, s.T) {
		return fail(exitUsage, "spawn requires -c -p -l --t T0..T3; supply the missing flags (workspace defaults to executing local.workspace)")
	}
	if !validID(s.Job) || !validID(s.Label) || !slices.Contains([]string{"worker", "builder", "tester", "reviewer", "counsel"}, s.Role) {
		return fail(exitUsage, "invalid job, label or role")
	}
	if s.Role == "builder" && (s.Lane == "" || s.Parent == "" || s.Owner != "" || s.Task == "") {
		return fail(exitUsage, "builder requires --lane --parent --task; --owner is not accepted")
	}
	if s.LaneDefer != "" && (s.Role != "builder" || !validID(s.LaneDefer)) {
		return fail(exitUsage, "--lane-defer requires builder and a host name")
	}
	if s.Task != "" && s.ParentJob != "" {
		return fail(exitUsage, "--task and --parent-job are exclusive")
	}
	resolved, aliasErr := a.resolveHost(s.Host)
	if aliasErr != nil {
		return aliasErr
	}
	if resolved != s.Host {
		fmt.Fprintf(a.Out, "host_alias=%s machine=%s\n", s.Host, resolved)
		s.Host = resolved
	}
	autoSel := s.Profile == "" || s.Profile == "auto"
	if !s.Delegated && (!autoSel || s.Host != "auto") && strings.TrimSpace(s.OverrideReason) == "" {
		return fail(exitUsage, "explicit -m or --host requires --override-reason")
	}
	if s.Urgency != "" && !slices.Contains([]string{"normal", "urgent"}, s.Urgency) {
		return fail(exitUsage, "--urgency must be normal or urgent")
	}
	if s.Wait && s.DryRun {
		return fail(exitUsage, "--wait needs a real spawn")
	}
	if s.WaitTimeout != "" && !s.Wait {
		return fail(exitUsage, "--wait-timeout requires --wait")
	}
	if s.Wait {
		if _, e := a.waitDeadline(s); e != nil {
			return e
		}
	}
	if s.BuilderFamily != "" || s.BuilderJob != "" {
		// A reviewer may name only the author's family: the author's job is
		// not bound to the task (not launched through panewire), so there is
		// nothing to inherit, and the pass runs as a top-level job.
		if s.BuilderFamily == "" || (s.BuilderJob == "" && s.Role != "reviewer") || (s.BuilderJob != "" && !validID(s.BuilderJob)) || s.ParentJob != "" || s.Task == "" || (s.Role != "reviewer" && s.Role != "tester") {
			return fail(exitUsage, "--builder-family and --builder-job go together (--builder-family alone only with --role reviewer), with --task and --role reviewer|tester, not --parent-job")
		}
		if s.BuilderJob != "" && s.Report == "" && s.ReportRelative == "" {
			return fail(exitUsage, "--builder-job requires --report")
		}
		s.ParentFamily = s.BuilderFamily
		s.Inherited = s.BuilderJob
	}
	if s.ParentJob == "" && s.Task == "" {
		s.ParentJob = os.Getenv("ARBITER_JOB")
		if s.ParentJob == "" {
			s.Task = os.Getenv("HK_TASK_ID")
		}
	}
	builderFamily := s.ParentFamily
	if s.ParentJob != "" {
		j, e := a.resolveParent(ctx, s.ParentJob)
		if errors.Is(e, errJobNotFound) {
			return &parentJobNotFound{fail(exitStateConflict, "parent job %s not found: --parent-job must name an existing job", s.ParentJob)}
		}
		if e != nil {
			return fail(exitStateConflict, "parent metadata unreadable: %v", e)
		}
		s.Task = j.Task
		s.Inherited = s.ParentJob
		builderFamily = j.Family
		if builderFamily == "" {
			if seat, ok := a.Policy.Profiles[j.Profile]; ok {
				builderFamily = seat.Family
			}
		}
		if s.Owner == "" {
			s.Owner = j.Owner
		}
		s.ParentFamily = builderFamily
		if s.Report == "" && s.ReportRelative == "" {
			return fail(exitUsage, "--parent-job requires --report")
		}
	}
	if !regexp.MustCompile(`^[0-9]+$`).MatchString(s.Task) {
		return fail(exitUsage, "spawn requires a numeric task binding")
	}
	if _, e := parseTags(s.Tags); e != nil {
		return e
	}
	excluded, e := parseHosts(s.ExcludeHosts)
	if e != nil {
		return e
	}
	if forced := s.Host; forced != "auto" {
		if forced == "local" {
			forced = a.machine()
		}
		if slices.Contains(excluded, forced) {
			return fail(exitUsage, "--host %s is excluded by --exclude-host", s.Host)
		}
	}
	brief, e := os.ReadFile(expand(s.Prompt))
	if e != nil {
		return e
	}
	if len(strings.TrimSpace(string(brief))) == 0 {
		return fail(exitUsage, "brief is empty")
	}
	selEffort := ""
	if autoSel {
		sel, e := a.selectProfile(ctx, s)
		if sel != nil {
			s.Sel = sel.record()
		}
		if s.DryRun && s.Sel != nil {
			b, _ := encode(s.Sel)
			fmt.Fprintf(a.Out, "urgency=%s source=%s tiers=%s\n", sel.req.urgency, sel.req.urgencySource, strings.Join(sel.order, ">"))
			fmt.Fprintf(a.Out, "selection=%s\n", b)
		}
		if e != nil {
			return e
		}
		s.Profile = sel.chosen.seatName
		s.AccountKey, s.RoutingRole, s.SelHost, s.route = sel.chosen.accountKey, sel.req.route.role, sel.chosen.host, sel.req.route
		selEffort = sel.chosen.row.Effort
		if selEffort != "" {
			s.Effort = selEffort
		}
	} else {
		if e := a.admitPinnedProfile(ctx, &s); e != nil {
			return e
		}
	}
	// An unknown -m seat can never admit anywhere: refuse before placement
	// burns a remote round on hosts that must all answer rc70.
	if _, ok := a.Policy.Profiles[s.Profile]; !ok {
		return fail(exitConfig, "missing seat policy for %s", s.Profile)
	}
	var placementRec map[string]any
	if s.Host != "local" {
		handled, rec, e := a.place(ctx, s, string(brief))
		a.stage("placement", startedAdmission)
		admissionRecorded = true
		if e != nil {
			return e
		}
		if handled {
			a.finishPlacement(ctx, s, nil)
			return a.afterSpawn(ctx, s, true)
		}
		placementRec = rec
	}
	if s.Workspace == "" {
		return fail(exitConfig, "WORKSPACE_UNCONFIGURED: set local.workspace on the executing host or pass -w WORKSPACE_ID")
	}
	payload, e := a.prepareBrief(s, string(brief))
	if e != nil {
		return e
	}
	if s.DryRun {
		// The local preview must still refuse what Seat.Check would refuse:
		// a capability-bound seat on a host that lacks the flag.
		if seat := a.Policy.Profiles[s.Profile]; seat.Capability != "standard" && !boolean(a.Config.Local, seat.Capability, false) {
			return fail(exitConfig, "host %s lacks capability %s", a.machine(), seat.Capability)
		}
		fmt.Fprintf(a.Out, "dry-run host=%s profile=%s role=%s job=%s\n%s\n", a.machine(), s.Profile, s.Role, s.Job, payload)
		return nil
	}
	if !boolean(a.Config.Local, "spawn", true) {
		return fail(exitLocalDisabled, "local spawns disabled")
	}
	cwd, e := filepath.Abs(expand(s.CWD))
	if e != nil {
		return e
	}
	s.CWD = cwd
	if s.ExecutionLane != "" {
		if s.ExecutionLane != "live" && s.ExecutionLane != "mock" {
			return fail(exitUsage, "-L must be live or mock")
		}
		if info, e := os.Stat(filepath.Join(cwd, s.ExecutionLane)); e == nil && info.IsDir() {
			s.CWD = filepath.Join(cwd, s.ExecutionLane)
		}
	}
	if info, e := os.Stat(s.CWD); e != nil || !info.IsDir() {
		return fail(exitUsage, "cwd is not a directory")
	}
	if s.ReportRelative != "" {
		if filepath.IsAbs(s.ReportRelative) || strings.Contains(s.ReportRelative, "..") {
			return fail(exitUsage, "unsafe report-relative")
		}
		s.Report = filepath.Join(a.Jobs, s.ReportRelative)
	}
	if s.Report != "" {
		if e := safeReport(a.Jobs, s.Job, firstNonempty(s.ParentJob, s.Inherited), s.Report); e != nil {
			return e
		}
	}
	seat, ok := a.Policy.Profiles[s.Profile]
	if !ok {
		return fail(exitConfig, "missing seat policy for %s", s.Profile)
	}
	effort := s.Effort
	if effort == "" {
		effort = seat.Effort
	}
	if seat.Pin && effort != seat.Effort {
		return fail(exitSeatRefused, "profile pins effort %s", seat.Effort)
	}
	task, bypassed, e := a.taskPrecheck(ctx, s)
	if e != nil {
		return e
	}
	grade := s.Grade
	if s.Grade == "" {
		if m := taskMarker.FindStringSubmatch(task.Title); m != nil {
			grade = strings.ToUpper(m[1])
			if strings.ToUpper(m[2]) != s.T {
				return fail(exitSeatRefused, "task scale does not match --t")
			}
		} else if !bypassed && !topRoutingRole(s.RoutingRole) {
			return fail(exitSeatRefused, "task title lacks (grade X, Tn marker")
		}
	}
	if s.Sel != nil && s.Sel["requested"] == nil {
		s.Sel["requested"] = map[string]any{"grade": grade, "role": s.Role, "t": s.T, "effort": effort}
	}
	launch, gate, e := a.admitLaunch(ctx, s, seat, effort, selEffort, grade, builderFamily, autoSel)
	if e != nil {
		return e
	}
	granted, e := admissionGate(seat.gateName(), s.AccountKey, launch.Pool, gate)
	if e != nil {
		return ledgerFail(e, "lease")
	}
	if !s.DryRun {
		var resolveErr error
		s.Workspace, resolveErr = a.resolveWorkspace(ctx, s.Workspace)
		if resolveErr != nil {
			return resolveErr
		}
	}
	if s.AccountKey == "" {
		key, _ := a.remoteAccount(ctx, firstNonempty(launch.Pool, gatePool(gate)), a.machine(), a.Config.Local)
		s.AccountKey = key
	}
	a.stampSelection(&s, a.machine())
	s.placement.execution = placementExecution{Host: a.machine(), Family: seat.Family, Model: launch.ModelID, Role: placementRole(s), Profile: s.Profile, Pool: firstNonempty(launch.Pool, gatePool(gate)), AccountKey: placementAccount(s.AccountKey)}
	owner := s.Owner
	if owner == "" {
		owner = "default"
	}
	if s.Role == "builder" {
		owner = s.Lane
	}
	claim := arbiter.Claim{Job: s.Job, AgentLabel: s.Label, Lane: owner, T: s.T, Role: "worker", TaskID: s.Task, TaskInheritedFrom: s.Inherited}
	if s.Role == "builder" {
		claim.Role, claim.ParentLane = "builder", s.Parent
	}
	if s.Bypass {
		claim.TaskLinkOverride = "declared"
	}
	// The hk job claim runs on the executing host before the local claim:
	// 409 job_id_taken refuses with the same exit 74, and hk down fails
	// closed like the task claim.
	if !admissionRecorded {
		a.stage("placement", startedAdmission)
		admissionRecorded = true
	}
	startedClaim := time.Now()
	if e = a.claimJobRecord(ctx, s); e != nil {
		a.stage("claim", startedClaim)
		return e
	}
	a.stage("claim", startedClaim)
	committed := false
	retrySafe := true
	ownedClaim := false
	hadPool := false
	defer func() {
		// A duplicate loses to a live spawn of the same id (an identical hk
		// claim replays as claimed); its record belongs to that spawn, so a
		// duplicate never marks it lost.
		var ee *ExitError
		if err != nil && !committed && !(errors.As(err, &ee) && ee.Code == exitDuplicate) {
			if retrySafe {
				if ownedClaim && !hadPool {
					if e := a.ledger().ReleaseClaim(context.WithoutCancel(ctx), s.Job); e != nil {
						retrySafe = false
						a.warn("claim cleanup failed: %v", e)
					}
				}
				if retrySafe {
					a.markSpawnRetryable(context.WithoutCancel(ctx), s.Job)
				}
			}
			if a.ObserveRetryable != nil {
				a.ObserveRetryable(retrySafe)
			}
			if !retrySafe {
				a.markSpawnUncertain(context.WithoutCancel(ctx), s.Job)
			}
		}
	}()
	// The local ledger arbitrates concurrent duplicate claims on this host;
	// never infer a free job from pane status.
	startedLocalClaim := time.Now()
	_, e = a.ledger().Claim(ctx, claim)
	a.stage("claim", startedLocalClaim)
	if e != nil && arbiter.Code(e) == arbiter.ExitDuplicate {
		d, de := a.durable(ctx, s.Job)
		if de != nil {
			return fail(exitStateConflict, "duplicate job state unreadable")
		}
		if d.State == "released" {
			claim.ReclaimReleased = true
			startedLocalClaim = time.Now()
			_, e = a.ledger().Claim(ctx, claim)
			a.stage("claim", startedLocalClaim)
		} else if !s.DupOK {
			return fail(exitDuplicate, "active duplicate job %s", s.Job)
		} else {
			a.warn("--job-dup-ok used for %s", s.Job)
			if e = a.event(ctx, s.Job, "job.duplicate_override", map[string]any{"label": s.Label}); e != nil {
				return e
			}
			e = nil
		}
	}
	if e != nil {
		return ledgerFail(e, "claim")
	}
	ownedClaim = true
	if !s.Delegated {
		if placementRec == nil {
			tags, _ := parseTags(s.Tags)
			placementRec = map[string]any{"source": "forced", "tags": tags, "profile": s.Profile, "host": a.machine()}
		}
		a.attachSelection(placementRec, s)
		if e = a.event(ctx, s.Job, "job.placement", placementRec); e != nil {
			a.warn("placement decision record failed: %v", e)
		}
	}
	pane := ""
	pool := ""
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if pane != "" {
				a.captureFailure(cleanup, s.Job, pane)
				if closeErr := a.closeFailedSpawn(cleanup, s, pane, seat.Kind); closeErr != nil {
					retrySafe = false
					a.warn("pane cleanup failed; inspect with panewire find %s before retry: %v", s.Label, closeErr)
				}
			}
			if pool != "" && retrySafe {
				if e := a.ledger().ReleaseQuotaPool(cleanup, s.Job, pool, false); e != nil {
					retrySafe = false
					a.warn("quota cleanup failed: %v", e)
				}
			}
		}
	}()
	startedQuotaClaim := time.Now()
	rec, e := a.ledger().RecordQuotaPool(ctx, arbiter.QuotaRun{Job: s.Job, Profile: seat.gateName(), LaunchProfile: launchProfile(seat.Canon, launch.Effort), Gate: granted})
	a.stage("claim", startedQuotaClaim)
	if e != nil {
		return ledgerFail(e, "lease")
	}
	pool = rec.Pool
	hadPool = true
	mockEnv, e := a.mock(ctx, s, seat)
	if e != nil {
		return e
	}
	argv, env, e := a.launchArgs(seat, launch)
	if e != nil {
		return e
	}
	for _, v := range mockEnv {
		if path, ok := strings.CutPrefix(v, "PANEWIRE_MOCK_CONFIG="); ok {
			argv = append(argv, "--strict-mcp-config", "--mcp-config", path)
		}
	}
	env = append(env, mockEnv...)
	env = append(env, "ARBITER_JOB="+s.Job, "ARBITER_RESOURCE="+pool, "ARBITER_KIND=quota_pool", "ARBITER_PROFILE="+seat.gateName(), "HK_TASK_ID="+s.Task)
	if seat.Kind == "claude" && s.Purpose != "director" {
		env = append(env, "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION=false")
	}
	if e = a.workspaceTrust(ctx, s, seat, env); e != nil {
		return fail(exitConfig, "workspace trust: %v", e)
	}
	startedLaunch := time.Now()
	launchRecorded := false
	defer func() {
		if !launchRecorded {
			a.stage("launch", startedLaunch)
		}
	}()
	tabArgs := []string{"tab", "create", "--workspace", s.Workspace, "--cwd", s.CWD, "--label", s.Label, "--no-focus"}
	for _, v := range env {
		tabArgs = append(tabArgs, "--env", v)
	}
	s.placement.started = true
	r := a.call(ctx, "herdr", tabArgs...)
	if e = check(r, "herdr tab create"); e != nil {
		return e
	}
	var created map[string]any
	if e = json.Unmarshal([]byte(r.Out), &created); e != nil {
		return e
	}
	created = unwrap(created)
	root, _ := created["root_pane"].(map[string]any)
	pane = str(root, "pane_id")
	s.placement.execution.PaneID = pane
	s.placement.execution.Kind = seat.Kind
	tab := str(created, "tab_id")
	if pane == "" {
		return fail(exitConfig, "tab create omitted pane_id")
	}
	if e = a.start(ctx, s, seat, launch, pane, argv); e != nil {
		return e
	}
	a.stage("launch", startedLaunch)
	launchRecorded = true
	startedLanding := time.Now()
	landingRecorded := false
	defer func() {
		if !landingRecorded {
			a.stage("landing", startedLanding)
		}
	}()
	pointer, e := a.writeDeliveredBrief(s.Job, payload)
	if e != nil {
		return e
	}
	if e = a.deliver(ctx, seat.Kind, pane, pointer); e != nil {
		return e
	}
	if seat.Kind == "omp" || seat.Kind == "cmd" {
		if e = a.modelProof(ctx, seat, launch, s.CWD, pane, pointer); e != nil {
			return e
		}
	}
	if e = a.taskLink(ctx, s, bypassed); e != nil {
		return e
	}
	a.stage("landing", startedLanding)
	landingRecorded = true
	a.spawnedJobRecord(ctx, s, seat, launch, pane, pool, owner)
	lane := s.Label
	parent := owner
	if s.Role == "builder" {
		lane = s.Lane
		parent = s.Parent
	}
	machine := a.machine()
	laneOK := false
	if s.LaneDefer == "" {
		if e := a.registerLane(ctx, lane, parent, pane, s.Role, machine); e != nil {
			a.warn("lane registration failed (session=%s machine=%s pane=%s): %v", a.herdrSession(), machine, pane, e)
		} else {
			laneOK = true
		}
	}
	receipt := map[string]any{"pane_id": pane, "tab_id": tab, "herdr_session": a.herdrSession(), "label": s.Label, "profile": s.Profile, "launch_profile": launchProfile(seat.Canon, launch.Effort), "model_id": launch.ModelID, "effort": launch.Effort, "family": seat.Family, "role": s.Role, "lane_role": s.Role, "owner_lane": owner, "parent_lane": s.Parent, "task_id": s.Task, "report": s.Report, "cwd": s.CWD, "keep": s.Keep, "pool": pool, "host": machine}
	if laneOK {
		receipt["hub_lane"] = lane
		receipt["hub_lane_machine"] = machine
	}
	if e = a.event(ctx, s.Job, "job.spawned", receipt); e != nil {
		return e
	}
	committed = true
	s.placement.placed = true
	s.placement.execution.Repo = a.placementRepo(ctx, s.CWD)
	if e = a.startWatch(s.Job); e != nil {
		a.warn("completion watcher failed: %v", e)
	}
	laneStatus := ""
	if laneOK {
		laneStatus = " lane=" + lane + "@" + machine
	} else if s.Role == "builder" {
		laneStatus = " lane=UNREGISTERED"
	}
	fmt.Fprintf(a.Out, "OK pane=%s session=%s model=%s label=%s status=working landed=yes job=%s quota_record=%s/quota_pool%s placement=%s\n", pane, a.herdrSession(), s.Profile, s.Label, s.Job, pool, laneStatus, s.placement.execution.receipt())
	a.finishPlacement(ctx, s, nil)
	if s.Role == "builder" && !laneOK && a.envOr("PANEWIRE_LANE_STRICT", "WRK_LANE_STRICT") == "1" {
		return fail(exitLaneUnregistered, "builder pane exists but lane is UNREGISTERED")
	}
	return a.afterSpawn(ctx, s, false)
}

// admitLaunch resolves the launch rung for a selected seat and asks the quota
// gate and the hub for the spawn. It is the admission half shared by spawn and
// `panewire delegate --headless`; it claims and writes nothing.
func (a *App) admitLaunch(ctx context.Context, s Spawn, seat Seat, effort, selEffort, grade, builderFamily string, autoSel bool) (Launch, string, error) {
	qr := QuotaRequest{Profile: seat.Canon, GateProfile: seat.gateName(), Effort: effort, Role: s.Role, OperatorRequest: s.OperatorRequest, RequestedBy: s.RequestedBy, Purpose: s.Purpose, AccountKey: s.AccountKey}
	launch, e := a.Quota.Launch(ctx, qr)
	if e != nil {
		// Preserve the gate's role/quota refusal even when the catalog also
		// refuses this launch; no fallback launch data is ever invented.
		if _, gateErr := a.Quota.Gate(ctx, qr); gateErr != nil {
			return Launch{}, "", gateErr
		}
		return Launch{}, "", e
	}
	if effort != "" && launch.Effort != effort {
		return Launch{}, "", fail(exitConfig, "catalog effort does not match request")
	}
	if autoSel && selEffort != "" && launch.Effort != selEffort {
		return Launch{}, "", fail(exitConfig, "launch rung does not match selected catalog rung")
	}
	if launch.Profile != seat.Canon {
		return Launch{}, "", fail(exitConfig, "catalog profile mismatch")
	}
	if topRoutingRole(s.RoutingRole) {
		grade = ""
		if !(&routing{role: s.RoutingRole}).admitsSeat(seat, launch.ModelID) {
			return Launch{}, "", fail(exitSeatRefused, "NO_ROLE_CANDIDATE: model %s in harness %s cannot lead or consult", launch.ModelID, seat.Kind)
		}
	}
	familyGate := s.route.familyAdmitsGate(launch.row())
	if e = seat.Check(launch, s.Role, grade, builderFamily, a.envOr("PANEWIRE_E6_ARM", "SCOPEFUEL_E6_ARM"), a.Config.Local, familyGate); e != nil {
		return Launch{}, "", e
	}
	// A gate alias is the pool gate for a whole family of spellings, so it
	// cannot tell an escalation rung from a default one; the launch row can.
	if seat.Gate != "" && launch.Gate != "default" && !s.AllowEscalation && !familyGate {
		return Launch{}, "", fail(exitSeatRefused, "%s is a %s rung; pass --allow-escalation", seat.Canon, launch.Gate)
	}
	qr.Effort = launch.Effort
	accountPool, _, _ := strings.Cut(s.AccountKey, ":")
	pool, err := boundPool(launch.Pool, accountPool)
	if err != nil {
		return Launch{}, "", err
	}
	policy, err := a.poolPolicy()
	if err != nil {
		return Launch{}, "", err
	}
	if s.AccountKey != "" {
		key, acc := a.remoteAccount(ctx, pool, a.machine(), a.Config.Local)
		if why := selectedAccountReason(s.AccountKey, key, acc, policy.isCredit(pool)); why != "" {
			return Launch{}, "", fail(exitAuthUnavailable, "AUTH_UNAVAILABLE: executing host %s cannot confirm the selected account %s: %s", a.machine(), s.AccountKey, why)
		}
	}
	if policy.isCredit(pool) {
		if err := a.creditPoolStatus(ctx, pool); err != nil {
			return Launch{}, "", err
		}
	}
	gate, e := a.Quota.Gate(ctx, qr)
	if e != nil {
		return Launch{}, "", e
	}
	granted, err := admissionGate(seat.gateName(), s.AccountKey, pool, gate)
	if err != nil {
		return Launch{}, "", err
	}
	if pool == "" && policy.isCredit(granted.Pool) {
		if err := a.creditPoolStatus(ctx, granted.Pool); err != nil {
			return Launch{}, "", err
		}
	}
	launch.Pool = granted.Pool
	if e = a.hubGate(ctx, s.CWD, launch.Pool); e != nil {
		return Launch{}, "", e
	}
	return launch, gate, nil
}
func (a *App) attachSelection(rec map[string]any, s Spawn) {
	state := a.fleetStateRecord()
	if s.Sel != nil {
		if state != nil {
			s.Sel["state"] = state
		}
		rec["selection"] = s.Sel
	}
	if state != nil {
		rec["state"] = state
	}
	if s.OverrideReason != "" {
		rec["override_reason"] = s.OverrideReason
	}
}
func firstNonempty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
func unwrap(m map[string]any) map[string]any {
	if r, ok := m["result"].(map[string]any); ok {
		return r
	}
	return m
}
func (a *App) machine() string {
	if mapping, ok := a.Config.Hub["session_machine_ids"].(map[string]any); ok {
		session := a.herdrSession()
		if id := str(mapping, session); id != "" {
			return id
		}
	}
	for _, m := range []map[string]any{a.Config.Local, a.Config.Hub} {
		if v := str(m, "machine_id"); v != "" {
			return v
		}
	}
	return "local"
}
func (a *App) captureFailure(ctx context.Context, job, pane string) {
	r := a.call(ctx, "herdr", "pane", "read", pane, "--source", "visible", "--lines", "80")
	if r.Code != 0 {
		return
	}
	dir := filepath.Join(a.Jobs, job)
	if os.MkdirAll(dir, 0700) != nil {
		return
	}
	f, e := os.CreateTemp(dir, "spawn-failure-*.txt")
	if e == nil {
		_, _ = f.WriteString(r.Out)
		_ = f.Close()
	}
}
func safeReport(root, job, parent, path string) error {
	if !filepath.IsAbs(path) {
		return fail(exitUsage, "report must be absolute")
	}
	for _, id := range []string{job, parent} {
		if id != "" && within(filepath.Join(root, id), path) {
			return noSymlinks(path)
		}
	}
	return fail(exitUsage, "report must be under the job or parent job directory")
}
func within(root, path string) bool {
	rel, e := filepath.Rel(root, path)
	return e == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
func noSymlinks(path string) error {
	for p := filepath.Clean(path); p != filepath.Dir(p); p = filepath.Dir(p) {
		s, e := os.Lstat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if s.Mode()&os.ModeSymlink != 0 {
			return fail(exitUsage, "symlink path refused: %s", p)
		}
	}
	return nil
}

// writeDeliveredBrief stores the full brief in the job dir on this host and
// returns the one-line pointer that is typed into the pane instead. Typing the
// whole brief lost everything past about 1 KB on a remote host, and the pane
// still reported a landed brief; a file has no such limit.
func (a *App) writeDeliveredBrief(job, payload string) (string, error) {
	dir := filepath.Join(a.Jobs, job)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return "", e
	}
	path := filepath.Join(dir, "brief-delivered.md")
	if e := os.WriteFile(path, []byte(payload), 0600); e != nil {
		return "", e
	}
	sum := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("Your brief is the file %s (%d bytes, sha256 %x). Read the whole file now, then follow it exactly; it is your only instruction.", path, len(payload), sum[:6]), nil
}
