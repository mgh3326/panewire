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

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/panewire/internal/arbiter"
)

type Spawn struct {
	// hubRetryHost is the host the hub's own verified-cleanup retry pinned,
	// copied from Submission.RetryHost by ValidateSubmission. Nothing parses
	// into it; validateContainment accepts exactly this host without an
	// operator reference.
	hubRetryHost string
	// registeredLane is the hub lane row the spawn created for its pane. A
	// failed spawn that closes the pane removes the row with it.
	registeredLane                                                                                                                                                                         *Job
	requestedLocal                                                                                                                                                                         bool
	CWD, Profile, Prompt, Workspace, Label, T, Job, Owner, Effort, Role, Lane, Parent, Task, ParentJob, Host, Report, ExecutionLane, OperatorRequest, RequestedBy, Purpose, ReportRelative string
	OverrideReason, Containment, WaitTimeout, Urgency                                                                                                                                      string
	// ReviewPass is the explicit review pass ("1".."9") of a reviewer spawn. It
	// only widens the hub reservation key (RoleKey); panewire never parses it out
	// of a label.
	ReviewPass                                         string   `json:"review_pass,omitempty"`
	After                                              []string `json:"-"`
	Keep, DupOK, Bypass, DryRun, Wait, AllowEscalation bool
	Inherited, ParentFamily                            string
	// Tags lists the capability tags a host must carry and ExcludeHosts the
	// hosts placement must not pick. Delegated marks a spawn delivered by
	// another initiator. Sel carries the selection decision record; all four
	// are initiator-local and never cross the delegation wire.
	Tags         string `json:"-"`
	ExcludeHosts string `json:"-"`
	// ExcludePools lists the quota pools placement must not pick for this one
	// request (--exclude-pool). Like ExcludeHosts it is a standing exclusion,
	// not a pin: it needs no containment.
	ExcludePools string `json:"-"`
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
	// ownerOmitted records that the command line carried no --owner. Spawn then
	// resolves the owner from the task's hk project instead of inheriting a
	// caller lane. A Spawn built in-process leaves it false.
	ownerOmitted bool
	// laneUnregistered is set once hub lane registration failed after its
	// retry; --wait output and the execution receipt name it.
	laneUnregistered bool
	// laneError is the bounded, redacted reason of the last failed lane
	// registration, kept in the receipt and the LANE_UNREGISTERED notice.
	laneError string
	// Grade is the task grade a caller states itself (`panewire delegate`),
	// used instead of the "(grade X, Tn" marker in the bound task's title.
	Grade string `json:"-"`
	// E6Arm is the resolved experiment marker (<canon>@<effort>): the value a
	// hub submission carried, else this process's environment. It travels to
	// the node as --e6-arm and is recorded on the job.
	E6Arm string `json:"-"`
	// e6ArmLocked marks a spawn whose marker is already authoritative — a hub
	// submission (ValidateSubmission) or a node-delivered spawn
	// (bindNodeSpawn). Its process environment never fills an empty E6Arm;
	// only a local panewire spawn resolves PANEWIRE_E6_ARM/SCOPEFUEL_E6_ARM.
	e6ArmLocked bool
	// ExcludeFamilies and OnlyFamily narrow automatic selection to seats of
	// other families, or of one family (`panewire delegate --panel`).
	ExcludeFamilies []string `json:"-"`
	OnlyFamily      string   `json:"-"`
	failedHarnesses []string
	autoSelected    bool
	requestedEffort string
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
	// list, empty for consult.
	SelHost   string `json:"-"`
	TaskGrade string `json:"-"`
	placement *placementDecision
	// replacedRung is the parent job a --parent-job spawn resolved in this
	// process. A ladder step writes its row from it (outcome_ladder.go).
	replacedRung *Job
}

var containmentPattern = regexp.MustCompile(`^hk:[1-9][0-9]{0,8}$`)

func validateContainment(s Spawn) error {
	if s.Containment != "" && !containmentPattern.MatchString(s.Containment) {
		return fail(exitUsage, "invalid containment: expected hk:<task id>")
	}
	if s.Delegated {
		return nil
	}
	hostPinned := s.Host != "" && s.Host != "auto" && s.Host != "local" && !s.requestedLocal
	// The hub's verified-cleanup retry re-pins the host its own stored
	// outcome named; that is not a submitter's placement pin.
	if s.hubRetryHost != "" && s.Host == s.hubRetryHost {
		hostPinned = false
	}
	pinned := (s.Profile != "" && s.Profile != "auto") || hostPinned
	// --exclude-host is not gated: standing exclusions (mergewake keeps
	// reviewers off its own host on every spawn) are policy, not containment
	// (operator re-decision 2026-11-05, hk 2963).
	if pinned && s.Containment == "" && strings.TrimSpace(s.OperatorRequest) == "" {
		return fail(exitUsage, "containment_required: placement pin requires authorization")
	}
	return nil
}

// retiredLeadSeatPointer is the one line `--role builder` and `panewire lead`
// are refused with: lead is no longer a seat panewire spawns.
const retiredLeadSeatPointer = "the delegated lead seat is retired (--role builder, panewire lead): lead is an operator-opened session that works through subagents; see the rstack wake and robin-mode skills"

var reviewPassPattern = regexp.MustCompile(`^[1-9]$`)

// RoleKey is the hub's reservation key for a spawn bound to a task: one
// holder per (task, role), and per review pass when --review-pass is set, so
// the p1 and p2 reviewers of one task run together. Every reader and writer
// of spawn_reservations.task_role builds the key here. Stored keys without a
// pass stay "task:role".
func (s Spawn) RoleKey() string {
	return SpawnRoleKey(s.Task, s.Role, s.ReviewPass)
}

// SpawnRoleKey is RoleKey for a caller that holds the parts, not a Spawn.
func SpawnRoleKey(task, role, pass string) string {
	key := task + ":" + role
	if pass != "" {
		key += ":p" + pass
	}
	return key
}

func parseSpawn(args []string) (Spawn, error) {
	s := Spawn{Role: "worker", Host: "auto"}
	f := flags("spawn")
	f.Var((*repeatedStringFlag)(&s.After), "after", "start condition (repeatable)")
	for _, p := range []struct {
		k string
		v *string
	}{{"c", &s.CWD}, {"m", &s.Profile}, {"profile", &s.Profile}, {"p", &s.Prompt}, {"w", &s.Workspace}, {"l", &s.Label}, {"t", &s.T}, {"job", &s.Job}, {"owner", &s.Owner}, {"effort", &s.Effort}, {"role", &s.Role}, {"lane", &s.Lane}, {"parent", &s.Parent}, {"task", &s.Task}, {"parent-job", &s.ParentJob}, {"host", &s.Host}, {"report", &s.Report}, {"L", &s.ExecutionLane}, {"operator-request", &s.OperatorRequest}, {"requested-by", &s.RequestedBy}, {"purpose", &s.Purpose}, {"report-relative", &s.ReportRelative}, {"tags", &s.Tags}, {"exclude-host", &s.ExcludeHosts}, {"exclude-pool", &s.ExcludePools}, {"override-reason", &s.OverrideReason}, {"containment", &s.Containment}, {"wait-timeout", &s.WaitTimeout}, {"urgency", &s.Urgency}, {"builder-family", &s.BuilderFamily}, {"builder-job", &s.BuilderJob}, {"routing-role", &s.RoutingRole}, {"e6-arm", &s.E6Arm}} {
		f.StringVar(p.v, p.k, *p.v, "")
	}
	f.StringVar(&s.Grade, "grade", "", "task grade (workers only)")
	f.StringVar(&s.ReviewPass, "review-pass", "", "review pass 1..9 (reviewer only)")
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
	s.ownerOmitted = !flagPassed(f, "owner")
	if flagPassed(f, "containment") && s.Containment == "" {
		return s, fail(exitUsage, "invalid containment: expected hk:<task id>")
	}
	if f.NArg() != 0 {
		return s, fail(exitUsage, "unexpected spawn arguments")
	}
	if flagPassed(f, "review-pass") && (s.Role != "reviewer" || !reviewPassPattern.MatchString(s.ReviewPass)) {
		return s, fail(exitUsage, "--review-pass N (1..9) is accepted only with --role reviewer")
	}
	if _, err := ParseStartConditions(s.After, s.Task); err != nil {
		return s, fail(exitUsage, "%v", err)
	}
	if s.Job == "" {
		s.Job = s.Label
	}
	if s.Role == "builder" || s.Role == "captain" {
		return s, fail(exitUsage, "%s", retiredLeadSeatPointer)
	}
	// An operator request records the operator-request purpose so receipts and
	// placement see the request's purpose when --purpose is omitted.
	if s.OperatorRequest != "" && s.Purpose == "" {
		s.Purpose = "operator-request"
	}
	if flagPassed(f, "grade") && (topRoutingRole(builtinRoleMap[s.Role]) || topRoutingRole(s.RoutingRole)) {
		s.Grade = strings.ToUpper(s.Grade)
		if _, ok := gradeOrder[s.Grade]; !ok {
			return s, fail(exitUsage, "--grade needs C|B|A|A+|S|S+|SS")
		}
	}
	return s, nil
}

// e6Arm resolves the experiment marker for a spawn. A value the spawn
// carries wins; a locked spawn (a hub submission or a node-delivered spawn,
// whose marker is already authoritative) keeps an empty marker empty; every
// other spawn reads the environment exactly as a local spawn always has. It
// never invents a marker.
func (a *App) e6Arm(carried string, locked bool) string {
	if locked {
		return carried
	}
	return firstNonempty(carried, a.envOr("PANEWIRE_E6_ARM", "SCOPEFUEL_E6_ARM"))
}

// ValidE6Arm reports whether arm is the experiment marker for canon: canon@
// for a rung whose catalog effort is empty, or canon@<effort>. Submission, the
// dry run and the node all call it. It checks the form only; the seat gate
// decides whether the rung the marker names is armed.
func ValidE6Arm(arm, canon string) bool {
	at := strings.IndexByte(arm, '@')
	if at <= 0 || arm[:at] != canon || strings.ContainsAny(arm, " \t\r\n") {
		return false
	}
	effort := arm[at+1:]
	return effort == "" || validID(effort)
}

type Task struct {
	ID        any            `json:"id"`
	Title     string         `json:"title"`
	State     string         `json:"state"`
	ClaimedBy string         `json:"claimed_by"`
	Refs      map[string]any `json:"refs"`
	Project   string         `json:"project"`
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

// taskPrecheck reads the task over hk HTTP (hkGetTask) and refuses a spawn
// the task cannot take. --bypass covers an hk the precheck cannot reach or
// that errs, never an answer it cannot read (errHKUnreadable).
func (a *App) taskPrecheck(ctx context.Context, s Spawn) (client.Task, bool, error) {
	t, err := hkGetTask(ctx, s.Task)
	if errors.Is(err, errHKUnreadable) {
		return t, false, fail(exitConfig, "unreadable task precheck")
	}
	if err != nil {
		if strings.Contains(err.Error(), "not_found") {
			return t, false, fail(exitUsage, "task does not exist")
		}
		if s.Bypass {
			return t, true, nil
		}
		return t, false, fail(exitConfig, "task precheck unavailable: %s", hkErrText(err))
	}
	if t.State == "" {
		return t, false, fail(exitConfig, "unreadable task precheck")
	}
	if !hkTaskIDIs(t, s.Task) {
		return t, false, fail(exitConfig, "task response ID mismatch")
	}
	return t, false, taskTakesSpawn(t, s)
}

// taskTakesSpawn refuses a spawn the task cannot take: it is in a state that
// ends or parks the work, or is bound to another job or claimant. An
// inherited spawn and an unlinked reviewer take any task.
func taskTakesSpawn(t client.Task, s Spawn) error {
	if s.Inherited != "" || s.unlinkedReviewer() {
		return nil
	}
	if slices.Contains([]string{"merged", "dropped", "hold", "needs_decision"}, t.State) {
		return fail(exitUsage, "task unavailable: %s", t.State)
	}
	if j := t.Refs.JobID; j != "" && j != s.Job {
		return fail(exitUsage, "task bound to another job")
	}
	if t.Refs.JobID == "" && t.ClaimedBy != "" && t.ClaimedBy != s.Label {
		return fail(exitUsage, "task has another claimant without a job binding")
	}
	return nil
}

// taskMarkerGrade is the grade the task title's (grade X, Tn) marker names,
// or s.Grade when the spawn carries one. A marker whose scale differs from
// --t, or no marker where one is required, refuses the spawn. A bypassed
// precheck and a consult need none.
func taskMarkerGrade(t client.Task, s Spawn, bypassed bool) (string, error) {
	if s.Grade != "" {
		return s.Grade, nil
	}
	if m := taskMarker.FindStringSubmatch(t.Title); m != nil {
		if strings.ToUpper(m[2]) != s.T {
			return "", fail(exitSeatRefused, "task scale does not match --t")
		}
		return strings.ToUpper(m[1]), nil
	}
	if !bypassed && !topRoutingRole(s.RoutingRole) {
		return "", fail(exitSeatRefused, "task title lacks (grade X, Tn marker")
	}
	return "", nil
}

// replacesParent reports whether this inherited spawn can take the parent's
// place on its task. Only an implementer seat can: a reviewer, tester,
// consult or delegated reader or verifier works beside the parent, even one
// that has completed, and must not take the task and its review routing away.
// It reads the fields that cross the hub and ssh envelopes.
func (s Spawn) replacesParent() bool {
	if s.Inherited == "" || s.Inherited == s.Job {
		return false
	}
	role := firstNonempty(s.RoutingRole, builtinRoleMap[s.Role])
	return role == "implementer"
}

// rebindInherited moves the task to this job when its parent is terminal and
// the task is still bound to that parent, and returns the job.task_link status.
// The spawn has landed by now, so a failure is recorded on ev and never returned.
// Rule: docs/config.md, "--parent-job".
func (a *App) rebindInherited(ctx context.Context, s Spawn, task client.Task, ev map[string]any) string {
	refused := func(why string) string {
		a.warn("task %s stays bound to job %s: %s", s.Task, s.Inherited, why)
		ev["rebind_error"] = strings.ToValidUTF8(clip(why, 240), "")
		return "inherited"
	}
	switch bound := task.Refs.JobID; bound {
	case "":
		return "inherited"
	case s.Job:
		// An earlier attempt of this request rebound the task and then failed.
		return "rebound"
	case s.Inherited:
	default:
		ev["bound_job"] = bound
		return "inherited"
	}
	store, e := a.jobStore()
	if e != nil {
		return refused("parent job record unavailable")
	}
	parent, e := store.Get(ctx, s.Inherited)
	if e != nil {
		return refused("parent job record unreadable")
	}
	state := str(parent, "state")
	if !slices.Contains([]string{"lost", "failed", "completed"}, state) {
		return "inherited"
	}
	t, e := hkRebindTask(ctx, s.Task, s.Inherited, s.Job, s.Label, "respawn continues "+state+" parent job "+s.Inherited)
	if e != nil && !errors.Is(e, errHKUnreadable) {
		return refused("task rebind refused: " + hkErrText(e))
	}
	if e != nil || t.ClaimedBy != s.Label || t.Refs.JobID != s.Job {
		return refused("task rebind did not confirm claimed_by and refs.job_id")
	}
	return "rebound"
}

// hkRebindTask moves the task's job link from fromJob to toJob over hk HTTP.
// A refusal (such as wrk_receipt_required) is the server's error code.
// An answer the CLI path's parser cannot read is errHKUnreadable.
func hkRebindTask(ctx context.Context, task, fromJob, toJob, by, reason string) (client.Task, error) {
	return hkTaskCall(task, func(c client.Client, id int64) (client.Task, error) {
		return c.RebindTaskJob(ctx, id, fromJob, toJob, by, reason)
	})
}

// taskLinkedTo reports whether hk's task record already binds the task to
// the job (refs.job_id). A task that cannot be read counts as not linked.
func (a *App) taskLinkedTo(ctx context.Context, task, job string) bool {
	t, err := hkGetTask(ctx, task)
	return err == nil && job != "" && hkTaskIDIs(t, task) && t.Refs.JobID == job
}

type taskClaimFailure struct{ reason string }

func (e *taskClaimFailure) Error() string { return e.reason }

// hkClaimTask claims the task for label, linked to job, over hk HTTP. An
// answer the CLI path's parser cannot read is errHKUnreadable.
func hkClaimTask(ctx context.Context, task, label, job string) (client.Task, error) {
	return hkTaskCall(task, func(c client.Client, id int64) (client.Task, error) {
		return c.ClaimTask(ctx, id, label, job, "")
	})
}

// hkClaimRefused reports whether a claim error is a refusal a bypassed spawn
// must not ignore: its text names a conflict or a refusal, as the hk CLI's
// stderr did. The CLI reworded invalid_context — a server that predates
// job_id claims — as "claim_job_id_rejected: server refused the claim", so
// that code counts too.
func hkClaimRefused(err error) bool {
	text := err.Error()
	return strings.Contains(text, "conflict") || strings.Contains(text, "refused") || client.ErrorCode(err) == "invalid_context"
}

func (a *App) taskLink(ctx context.Context, s Spawn, task client.Task, bypassed bool) error {
	ev := map[string]any{"task_id": s.Task, "job_id": s.Job, "claimed_by": s.Label, "inherited_from": s.Inherited}
	status := "inherited"
	var claimFailure *taskClaimFailure
	if s.replacesParent() {
		status = a.rebindInherited(ctx, s, task, ev)
	} else if s.unlinkedReviewer() {
		// No claim: the task stays bound to whatever it was bound to.
		status = "unlinked"
	} else if s.Inherited == "" {
		t, err := hkClaimTask(ctx, s.Task, s.Label, s.Job)
		if err != nil && !errors.Is(err, errHKUnreadable) {
			if !bypassed || !s.Bypass || hkClaimRefused(err) {
				if a.taskLinkedTo(ctx, s.Task, s.Job) {
					// hk already binds the task to this job (a design session
					// rebound it before the spawn): nothing to link (hk 3426).
					status = "already_linked"
				} else {
					claimFailure = &taskClaimFailure{reason: fmt.Sprintf("task claim exited with status %d", hkCLIErrorExit)}
					status = "failed"
				}
			} else {
				status = "bypassed"
			}
		} else {
			// hk answered: an answer that cannot be read did not confirm
			// the binding, and --bypass does not cover it.
			if err != nil || slices.Contains([]string{"merged", "dropped", "hold", "needs_decision"}, t.State) || t.ClaimedBy != s.Label || t.Refs.JobID != s.Job {
				claimFailure = &taskClaimFailure{reason: "task claim did not confirm claimed_by and refs.job_id"}
				status = "failed"
			} else {
				status = "linked"
			}
		}
	}
	ev["status"] = status
	if claimFailure != nil {
		ev["reason"] = claimFailure.reason
	}
	if err := a.event(ctx, s.Job, "job.task_link", ev); err != nil {
		return err
	}
	if claimFailure != nil {
		return claimFailure
	}
	return nil
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
			a.ObserveReceipt(ExecutionReceipt{CWD: s.CWD, Label: s.Label, Job: s.Job, Host: x.Host, Pane: x.PaneID, Profile: firstNonempty(x.Profile, s.Profile), Model: x.Model, Kind: x.Kind, Role: firstNonempty(x.Role, s.Role), LaneUnregistered: s.laneUnregistered})
		}
	}()

	defer func() { a.finishPlacement(ctx, s, err) }()
	defer func() { a.spawnEvents(s, err) }()
	if s.CWD == "" || s.Prompt == "" || s.Label == "" || !slices.Contains([]string{"T0", "T1", "T2", "T3"}, s.T) {
		return fail(exitUsage, "spawn requires -c -p -l --t T0..T3; supply the missing flags (workspace defaults to executing local.workspace)")
	}
	if !validID(s.Job) || !validID(s.Label) || !slices.Contains([]string{"worker", "tester", "reviewer", "counsel"}, s.Role) {
		return fail(exitUsage, "invalid job, label or role")
	}
	if s.Task != "" && s.ParentJob != "" {
		return fail(exitUsage, "--task and --parent-job are exclusive")
	}
	if s.E6Arm == "" {
		s.E6Arm = a.e6Arm("", s.e6ArmLocked)
	}
	if err := validateContainment(s); err != nil {
		return err
	}
	resolved, aliasErr := a.resolveHost(s.Host)
	if aliasErr != nil {
		return aliasErr
	}
	if resolved != s.Host {
		fmt.Fprintf(a.Out, "host_alias=%s machine=%s\n", s.Host, resolved)
		s.Host = resolved
	}
	// A pinned hub host sends these flags to a node, whose own argument rule
	// refuses the whole job.spawn on a mismatch. Refuse here so a dry-run names
	// the flag before admission answers for another reason. A local or ssh host
	// never sees the hub wire, and an auto placement decides its host later.
	if str(a.Config.Hosts[s.Host], "via") == "hub" {
		if reason := hubSpawnArgRefusal(a.Jobs, s); reason != "" {
			return fail(exitUsage, "hub spawn: %s", reason)
		}
	}
	autoSel := s.Profile == "" || s.Profile == "auto"
	s.autoSelected = autoSel
	s.requestedEffort = s.Effort
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
		s.replacedRung = &j
		builderFamily = j.Family
		if builderFamily == "" {
			if seat, ok := a.Policy.Profiles[j.Profile]; ok {
				builderFamily = seat.Family
			}
		}
		if s.Owner == "" && !s.ownerOmitted {
			s.Owner = j.Owner
		}
		s.ParentFamily = builderFamily
		if s.Report == "" && s.ReportRelative == "" {
			return fail(exitUsage, "--parent-job requires --report")
		}
	}
	if s.ownerOmitted {
		if e := a.resolveOwner(ctx, &s); e != nil {
			return e
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
	if _, e := parsePools(s.ExcludePools); e != nil {
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
	brief, e := a.readBrief(ctx, s.Prompt, 0)
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
	if s.DryRun && s.Host == "auto" {
		a.warn("dry-run plans from this host; the hub coordinator places the real spawn from its own host list and may choose another node")
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
	// A dry run starts nothing, so disabled local spawns do not refuse its preview.
	if !s.DryRun && !boolean(a.Config.Local, "spawn", true) {
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
	grade, e := taskMarkerGrade(task, s, bypassed)
	if e != nil {
		return e
	}
	s.TaskGrade = grade
	if s.Sel != nil && s.Sel["requested"] == nil {
		s.Sel["requested"] = map[string]any{"grade": grade, "role": s.Role, "t": s.T, "effort": effort}
	}
	if s.DryRun {
		// A dry run takes the seat gate a real spawn takes (the catalog row and
		// Seat.Check, E6 marker included) and nothing after it: no gate receipt,
		// lease, hub reservation or pane.
		if seat.Capability != "standard" && !boolean(a.Config.Local, seat.Capability, false) {
			return fail(exitConfig, "host %s lacks capability %s", a.machine(), seat.Capability)
		}
		// A pinned seat takes the launch-row gate here. An auto pick was
		// already resolved from the catalog by selectProfile, which this
		// dry run does not repeat.
		if !autoSel {
			if _, e := a.checkSpawnAdmission(ctx, s, seat, a.spawnQuotaRequest(s, seat, effort), effort, selEffort, grade, builderFamily, autoSel, a.Config.Local); e != nil {
				return e
			}
		}
		payload, e := a.prepareBrief(s, seat, Launch{Effort: firstNonempty(s.Effort, seat.Effort)}, string(brief))
		if e != nil {
			return e
		}
		a.dryRunLine(a.machine(), s, payload)
		return nil
	}
	launch, gate, e := a.admitLaunch(ctx, s, seat, effort, selEffort, grade, builderFamily, autoSel)
	if e != nil {
		return e
	}
	payload, e := a.prepareBrief(s, seat, launch, string(brief))
	if e != nil {
		return e
	}
	granted, e := admissionGate(seat.gateName(), s.AccountKey, launch.Pool, gate)
	if e != nil {
		return ledgerFail(e, "lease")
	}
	committed := false
	configuredWorkspace := s.Workspace
	var createdWorkspaces []*createdWorkspace
	if !s.DryRun {
		resolved, made, resolveErr := a.resolveWorkspace(ctx, s.Workspace)
		if resolveErr != nil {
			return resolveErr
		}
		s.Workspace = resolved
		if made != nil {
			createdWorkspaces = append(createdWorkspaces, made)
		}
	}
	// A workspace this spawn created stays alive on the tab it created there. On
	// failure remove that tab only; another spawn that adopted the workspace by
	// label keeps its own tab, and herdr closes the workspace with its last tab.
	defer func() {
		if committed || len(createdWorkspaces) == 0 {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		for _, w := range createdWorkspaces {
			if e := a.closeCreatedWorkspace(cleanup, w); e != nil {
				a.warn("created workspace cleanup failed; close tab %s (workspace %s) on the executing host: %v", w.tab, w.workspace, e)
			}
		}
	}()
	if s.AccountKey == "" {
		var receipt struct {
			ReadingKey string `json:"reading_account_key"`
		}
		if json.Unmarshal([]byte(gate), &receipt) == nil && receipt.ReadingKey != "" {
			if !validAccountKey(receipt.ReadingKey) || accountPool(receipt.ReadingKey) != firstNonempty(launch.Pool, gatePool(gate)) {
				return fail(exitConfig, "quota receipt reading account does not match the launch pool")
			}
			s.AccountKey = receipt.ReadingKey
		} else {
			key, _ := a.remoteAccount(ctx, firstNonempty(launch.Pool, gatePool(gate)), a.machine(), a.Config.Local)
			s.AccountKey = key
		}
	}
	a.stampSelection(&s, a.machine())
	s.placement.execution = placementExecution{Host: a.machine(), Family: seat.Family, Model: launch.ModelID, Role: placementRole(s), Profile: s.Profile, Pool: firstNonempty(launch.Pool, gatePool(gate)), AccountKey: placementAccount(s.AccountKey)}
	owner := s.Owner
	if owner == "" {
		owner = "default"
	}
	claim := arbiter.Claim{Job: s.Job, AgentLabel: s.Label, Lane: owner, T: s.T, Role: "worker", TaskID: s.Task, TaskInheritedFrom: s.Inherited}
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
	argv = withSessionName(seat.Kind, argv, s.Job)
	if seat.Kind == "omp" && (s.Role == "reviewer" || s.Role == "counsel") {
		configPath, err := writeOMPExtendedContextOverlay()
		if err != nil {
			return fail(exitConfig, "omp extended context settings: %v", err)
		}
		defer func() {
			if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
				a.warn("omp extended context settings cleanup failed: %v", err)
			}
		}()
		argv = append(argv, "--config", configPath)
	}
	for _, v := range mockEnv {
		if path, ok := strings.CutPrefix(v, "PANEWIRE_MOCK_CONFIG="); ok {
			argv = append(argv, "--strict-mcp-config", "--mcp-config", path)
		}
	}
	env = append(env, mockEnv...)
	env = append(env, "ARBITER_JOB="+s.Job, "ARBITER_RESOURCE="+pool, "ARBITER_KIND=quota_pool", "ARBITER_PROFILE="+seat.gateName(), "HK_TASK_ID="+s.Task)
	// The pane's hub lane is its label (registered below). Naming it here lets
	// the pane's `panewire inbox` commands default to their own lane.
	env = append(env, "PANEWIRE_LANE="+s.Label)
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
	createTab := func(workspace string) Result {
		args := []string{"tab", "create", "--workspace", workspace, "--cwd", s.CWD, "--label", s.Label, "--no-focus"}
		for _, v := range env {
			args = append(args, "--env", v)
		}
		return a.call(ctx, "herdr", args...)
	}
	s.placement.started = true
	r := createTab(s.Workspace)
	if e = check(r, "herdr tab create"); e != nil {
		// Another spawn's failed cleanup may close a workspace this spawn
		// adopted before the tab exists. A label can be recreated; an id cannot.
		if !workspaceGone(r) || herdrWorkspaceID.MatchString(configuredWorkspace) {
			return e
		}
		retryWorkspace, retryCreated, retryErr := a.resolveWorkspace(ctx, configuredWorkspace)
		if retryErr != nil {
			return retryErr
		}
		if retryCreated != nil {
			createdWorkspaces = append(createdWorkspaces, retryCreated)
		}
		s.Workspace = retryWorkspace
		r = createTab(s.Workspace)
		if e = check(r, "herdr tab create"); e != nil {
			return e
		}
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
	if e = a.taskLink(ctx, s, task, bypassed); e != nil {
		var claimFailure *taskClaimFailure
		if !errors.As(e, &claimFailure) {
			return e
		}
		a.warn("job %s task %s: %s; link the task manually", s.Job, s.Task, claimFailure.reason)
		id := sha256.Sum256([]byte(s.Job))
		text := fmt.Sprintf("[panewire] TASK_LINK_FAILED job=%s task=%s reason=%s; link the task manually", s.Job, s.Task, claimFailure.reason)
		r := a.call(ctx, "panewire", "emit", "--kind", "lane.event", "--lane", owner, "--type", "automation-failed", "--event-id", fmt.Sprintf("task-link-failed-%x", id[:]), "--text", text, "--inbox-root", filepath.Dir(a.Jobs))
		if r.Code != 0 {
			a.warn("task link notice failed (exit %d)", r.Code)
		}
	}
	a.stage("landing", startedLanding)
	landingRecorded = true
	a.spawnedJobRecord(ctx, s, seat, launch, pane, pool, owner)
	lane := s.Label
	parent := owner
	machine := a.machine()
	laneOK := false
	if e := a.registerLaneWithRetry(ctx, lane, parent, pane, s.Role, machine); e != nil {
		s.laneUnregistered = true
		s.laneError = laneFailureReason(e)
	} else {
		laneOK = true
		s.registeredLane = &Job{HubLane: lane, Machine: machine, Pane: pane, Role: s.Role}
	}
	receipt := map[string]any{"pane_id": pane, "tab_id": tab, "herdr_session": a.herdrSession(), "label": s.Label, "profile": s.Profile, "launch_profile": launchProfile(seat.Canon, launch.Effort), "model_id": launch.ModelID, "effort": launch.Effort, "family": seat.Family, "role": s.Role, "lane_role": s.Role, "owner_lane": owner, "parent_lane": s.Parent, "task_id": s.Task, "report": s.Report, "cwd": s.CWD, "keep": s.Keep, "pool": pool, "host": machine}
	receipt["harness_session"] = a.harnessSession(ctx, pane, seat.Kind)
	if s.AccountKey != "" {
		receipt["account_key"] = s.AccountKey
	}
	if s.E6Arm != "" {
		receipt["e6_arm"] = s.E6Arm
	}
	if laneOK {
		receipt["hub_lane"] = lane
		receipt["hub_lane_machine"] = machine
	}
	if s.laneUnregistered {
		receipt["lane_unregistered"] = true
		if s.laneError != "" {
			receipt["lane_error"] = s.laneError
		}
	}
	if e = a.event(ctx, s.Job, "job.spawned", receipt); e != nil {
		return e
	}
	committed = true
	s.placement.placed = true
	if rerr := a.recordLadderStep(ctx, s, launch, seat); rerr != nil {
		a.warn("ladder step not recorded: %v", rerr)
	}
	s.placement.execution.Repo = a.placementRepo(ctx, s.CWD)
	if e = a.startWatch(s.Job); e != nil {
		a.warn("completion watcher failed: %v", e)
	}
	if s.laneUnregistered {
		a.announceLaneUnregistered(ctx, s, lane, parent, pane, machine)
	}
	laneStatus := ""
	if laneOK {
		laneStatus = " lane=" + lane + "@" + machine
	} else if s.laneUnregistered {
		laneStatus = " lane=UNREGISTERED"
	}
	fmt.Fprintf(a.Out, "OK pane=%s session=%s model=%s label=%s status=working landed=yes job=%s quota_record=%s/quota_pool%s placement=%s\n", pane, a.herdrSession(), s.Profile, s.Label, s.Job, pool, laneStatus, s.placement.execution.receipt())
	a.finishPlacement(ctx, s, nil)
	if s.laneUnregistered && a.envOr("PANEWIRE_LANE_STRICT", "WRK_LANE_STRICT") == "1" {
		return fail(exitLaneUnregistered, "%s pane exists but lane is UNREGISTERED", s.Role)
	}
	return a.afterSpawn(ctx, s, false)
}

// spawnQuotaRequest is the catalog and gate request for a spawn's seat and rung.
func (a *App) spawnQuotaRequest(s Spawn, seat Seat, effort string) QuotaRequest {
	return QuotaRequest{Profile: seat.Canon, GateProfile: seat.gateName(), Effort: effort, Role: s.Role, OperatorRequest: s.OperatorRequest, RequestedBy: s.RequestedBy, Purpose: s.Purpose, AccountKey: s.AccountKey, E6Arm: a.e6Arm(s.E6Arm, s.e6ArmLocked), E6ArmLocked: s.e6ArmLocked}
}

// checkLaunchRow is the launch-row half of admitLaunch: the catalog row the
// request resolves to, then the seat's own rules (grade cover, E6 marker). A
// dry run runs it too, so the preview refuses what a real spawn refuses. It
// takes no gate receipt, lease or reservation.
func (a *App) checkLaunchRow(ctx context.Context, s Spawn, seat Seat, qr QuotaRequest, effort, selEffort, grade, builderFamily string, autoSel bool, host map[string]any) (Launch, error) {
	launch, e := a.Quota.Launch(ctx, qr)
	if e != nil {
		// Preserve the gate's role/quota refusal even when the catalog also
		// refuses this launch; no fallback launch data is ever invented.
		if _, gateErr := a.Quota.Gate(ctx, qr); gateErr != nil {
			return Launch{}, gateErr
		}
		return Launch{}, e
	}
	if effort != "" && launch.Effort != effort {
		return Launch{}, fail(exitConfig, "catalog effort does not match request")
	}
	if autoSel && selEffort != "" && launch.Effort != selEffort {
		return Launch{}, fail(exitConfig, "launch rung does not match selected catalog rung")
	}
	if launch.Profile != seat.Canon {
		return Launch{}, fail(exitConfig, "catalog profile mismatch")
	}
	if topRoutingRole(s.RoutingRole) {
		grade = ""
		if !(&routing{role: s.RoutingRole}).admitsSeat(seat, launch.ModelID) {
			return Launch{}, fail(exitSeatRefused, "NO_ROLE_CANDIDATE: model %s in harness %s cannot consult", launch.ModelID, seat.Kind)
		}
	}
	if e = seat.Check(launch, s.Role, grade, builderFamily, a.e6Arm(s.E6Arm, s.e6ArmLocked), host); e != nil {
		return Launch{}, e
	}
	return launch, nil
}

// checkSpawnAdmission covers the catalog row, seat rules and escalation gate
// shared by a dry-run and a real spawn. It does not read quota or claim a seat.
func (a *App) checkSpawnAdmission(ctx context.Context, s Spawn, seat Seat, qr QuotaRequest, effort, selEffort, grade, builderFamily string, autoSel bool, host map[string]any) (Launch, error) {
	launch, err := a.checkLaunchRow(ctx, s, seat, qr, effort, selEffort, grade, builderFamily, autoSel, host)
	if err != nil {
		return Launch{}, err
	}
	familyGate := s.route.familyAdmitsGate(launch.row())
	if (seat.Gate != "" || seat.Kind == "agy" && launch.Gate == escalationGate) && launch.Gate != defaultGate && !s.AllowEscalation && !familyGate {
		return Launch{}, fail(exitSeatRefused, "%s is a %s rung; pass --allow-escalation", seat.Canon, launch.Gate)
	}
	return launch, nil
}

func (a *App) checkDryRunAdmission(ctx context.Context, s Spawn, host map[string]any) error {
	seat, ok := a.Policy.Profiles[s.Profile]
	if !ok {
		return fail(exitConfig, "missing seat policy for %s", s.Profile)
	}
	effort := firstNonempty(s.Effort, seat.Effort)
	_, err := a.checkSpawnAdmission(ctx, s, seat, a.spawnQuotaRequest(s, seat, effort), effort, effort, s.TaskGrade, s.ParentFamily, false, host)
	return err
}

// admitLaunch resolves the launch rung for a selected seat and asks the quota
// gate and the hub for the spawn. It is the admission half shared by spawn and
// `panewire delegate --headless`; it claims and writes nothing.
func (a *App) admitLaunch(ctx context.Context, s Spawn, seat Seat, effort, selEffort, grade, builderFamily string, autoSel bool) (Launch, string, error) {
	qr := a.spawnQuotaRequest(s, seat, effort)
	launch, e := a.checkSpawnAdmission(ctx, s, seat, qr, effort, selEffort, grade, builderFamily, autoSel, a.Config.Local)
	if e != nil {
		return Launch{}, "", e
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
	if s.Containment != "" {
		rec["containment"] = s.Containment
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
