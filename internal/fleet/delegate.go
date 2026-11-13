package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// `panewire delegate` hands one bounded reading, verifying or cross-checking
// task to a free or spare-quota model and prints its answer: one call, one
// answer back, no PR and no worktree of its own. It is a thin layer on the
// spawn machinery.
//
// The data shape is one job per delegation, id delegate-<parent>-<n> (parent =
// the enclosing job, ARBITER_JOB; delegate_<host>_<n> outside one, host = the
// node id, so ids stay unique across hosts). The job dir holds
// the brief the model gets (brief.md), the result it files with
// `panewire done JOB --result` (result.json, {"answer": TEXT}), its report
// (report.md, the same answer in prose) and, with --patch, changes.patch.
//
//	pane (default)  Spawn{Profile: auto, Wait: true} through the same path as
//	                `panewire spawn`, then read the hk job record's result and
//	                close the pane. Placement may pick another host.
//	--headless      the selected seat's CLI is run once on this host with the
//	                brief and a bounded timeout; the job is claimed, leased and
//	                completed exactly like a pane job, so quota and hk records
//	                stay consistent.
//
// Roles map onto seat roles: reader and verifier are "worker" seats, reviewer
// is a "reviewer" seat and excludes the caller's own model family. Selection is
// the spawn/plan core under the normal policy (waste-risk pools, then the tier
// order); a free model is one candidate there, not a target. --panel N sends
// the prompt to N families at once (panel.go).
//
// Nothing is written in --cwd. With --patch the delegate works in a detached
// scratch worktree of --cwd and gets back changes.patch; the worktree is
// removed before delegate returns.
//
// Exit codes: 0 the answer was printed; 2 usage; 70 configuration; 75 parent
// job state unreadable; 77 no seat or rung admitted; 81 no host admitted;
// 83 the delegate escalated; 84 it was lost or revoked; 85 the wait deadline
// expired; 124 the headless CLI hit --timeout; 1 the job finished with no
// answer or the headless CLI failed. Spawn and quota refusals keep their own
// codes.

// delegateRoles maps a delegate role to the seat role selection works with.
var delegateRoles = map[string]string{"reader": "worker", "verifier": "worker", "reviewer": "reviewer"}

const delegateDefaultTimeout = 15 * time.Minute

type delegateOpts struct {
	role, prompt, grade, cwd, timeout, family, workspace string
	headless, patch, json                                bool
	panel                                                int
}

// delegateCaller is what delegate knows about the session calling it.
type delegateCaller struct {
	parent string // enclosing job id, "" outside a job
	task   string
	owner  string
	family string
}

// delegateReq is everything one delegation needs, shared by a single call and
// every member of a panel.
type delegateReq struct {
	o        delegateOpts
	seatRole string
	caller   delegateCaller
	body     string
	cwd      string
	timeout  time.Duration
}

// DelegateResult is one delegation, and the --json document of a single call.
// The answer is the last thing printed in either form.
type DelegateResult struct {
	Job     string         `json:"job"`
	Role    string         `json:"role"`
	Mode    string         `json:"mode"`
	Profile string         `json:"profile"`
	Family  string         `json:"family,omitempty"`
	Patch   string         `json:"patch,omitempty"`
	Result  map[string]any `json:"result,omitempty"`
	Answer  string         `json:"answer"`
}

func (a *App) delegate(ctx context.Context, args []string) error {
	var o delegateOpts
	f := flags("delegate")
	f.StringVar(&o.role, "role", "", "")
	f.StringVar(&o.prompt, "prompt", "", "")
	f.StringVar(&o.grade, "grade", "C", "")
	f.StringVar(&o.cwd, "cwd", "", "")
	f.StringVar(&o.timeout, "timeout", "", "")
	f.StringVar(&o.family, "family", "", "")
	f.StringVar(&o.workspace, "workspace", "", "")
	f.BoolVar(&o.headless, "headless", false, "")
	f.BoolVar(&o.patch, "patch", false, "")
	f.BoolVar(&o.json, "json", false, "")
	f.IntVar(&o.panel, "panel", 0, "")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() != 0 {
		return fail(exitUsage, "delegate takes no positional arguments")
	}
	d := delegateReq{o: o}
	var ok bool
	if d.seatRole, ok = delegateRoles[o.role]; !ok {
		return fail(exitUsage, "delegate needs --role reader|verifier|reviewer")
	}
	if o.panel != 0 && (o.panel < 2 || o.panel > 4) {
		return fail(exitUsage, "--panel takes 2..4")
	}
	d.o.grade = strings.ToUpper(o.grade)
	if _, ok := gradeOrder[d.o.grade]; !ok {
		return fail(exitUsage, "--grade must be C|B|A|A+|S|S+|SS")
	}
	d.timeout = delegateDefaultTimeout
	if o.timeout != "" {
		if d.timeout, ok = timeoutValue(o.timeout); !ok {
			return fail(exitUsage, "invalid --timeout %q", o.timeout)
		}
	}
	if o.prompt == "" {
		return fail(exitUsage, "delegate needs --prompt FILE")
	}
	body, e := os.ReadFile(expand(o.prompt))
	if e != nil {
		return e
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return fail(exitUsage, "prompt is empty")
	}
	d.body = string(body)
	d.cwd = o.cwd
	if d.cwd == "" {
		d.cwd = "."
	}
	if d.cwd, e = filepath.Abs(expand(d.cwd)); e != nil {
		return e
	}
	if info, e := os.Stat(d.cwd); e != nil || !info.IsDir() {
		return fail(exitUsage, "--cwd is not a directory")
	}
	if !o.headless {
		if d.o.workspace == "" {
			d.o.workspace = str(a.Config.Local, "workspace")
		}
		if d.o.workspace == "" {
			return fail(exitUsage, "pane mode needs --workspace (or [local] workspace); --headless needs none")
		}
	}
	if d.caller, e = a.delegateCaller(ctx, o.family); e != nil {
		return e
	}
	if d.seatRole == "reviewer" && d.caller.family == "" {
		return fail(exitUsage, "a reviewer excludes the caller's model family: run inside a panewire job or pass --family")
	}
	if !isNumeric(d.caller.task) {
		return fail(exitUsage, "delegate records a job and needs a task: run inside a panewire job (ARBITER_JOB) or set HK_TASK_ID")
	}
	if o.panel != 0 {
		return a.delegatePanel(ctx, d)
	}
	id, e := a.delegateJobID(d.caller.parent)
	if e != nil {
		return e
	}
	res, e := a.delegateRun(ctx, d, id, "", nil)
	if e != nil {
		return e
	}
	return a.printDelegate(res, o.json)
}

// delegateRun is one delegation under job id: brief, scratch tree if asked,
// pane or headless run, answer, pane close, patch. A panel member passes
// onlyFamily to pin the seat's family; the seat is still chosen by selection.
func (a *App) delegateRun(ctx context.Context, d delegateReq, id, onlyFamily string, exclude []string) (res DelegateResult, err error) {
	o := d.o
	dir := filepath.Join(a.Jobs, id)
	paths := delegatePaths{brief: filepath.Join(dir, "brief.md"), report: filepath.Join(dir, "report.md"), result: filepath.Join(dir, "result.json"), patch: filepath.Join(dir, "changes.patch")}
	work := d.cwd
	if o.patch {
		tree, cleanup, e := a.scratchTree(ctx, d.cwd)
		if e != nil {
			return res, e
		}
		defer cleanup()
		work = tree
	}
	if err = os.WriteFile(paths.brief, []byte(delegatePreamble(o.role, o.patch, o.panel > 0, id, paths)+"\n\n"+d.body), 0600); err != nil {
		return res, err
	}
	s := Spawn{CWD: work, Profile: "auto", Prompt: paths.brief, Workspace: o.workspace, Label: id, T: "T0", Job: id, Role: d.seatRole, RoutingRole: d.routingRole(), Host: "auto", Report: paths.report, Grade: o.grade, ParentFamily: d.caller.family, Task: d.caller.task, ParentJob: d.caller.parent, OnlyFamily: onlyFamily, ExcludeFamilies: exclude}
	if o.patch || o.headless {
		s.Host = "local"
		s.OverrideReason = "delegate " + map[bool]string{true: "--headless runs", false: "--patch uses a scratch checkout"}[o.headless] + " on this host"
	}
	res = DelegateResult{Job: id, Role: o.role, Mode: "pane"}
	if o.headless {
		res.Mode = "headless"
		res.Profile, err = a.delegateHeadless(ctx, s, d.caller, d.body, paths, d.timeout)
	} else {
		res.Profile, err = a.delegatePane(ctx, s, d.timeout)
	}
	if err != nil {
		return res, err
	}
	res.Family = a.Policy.Profiles[res.Profile].Family
	if res.Answer, res.Result, err = a.delegateAnswer(ctx, id, paths); err != nil {
		return res, err
	}
	if !o.headless {
		a.delegateClose(ctx, id)
	}
	if o.patch {
		if res.Patch, err = a.scratchPatch(ctx, work, paths.patch); err != nil {
			return res, err
		}
	}
	return res, nil
}

type delegatePaths struct{ brief, report, result, patch string }

func (d delegateReq) routingRole() string {
	if d.o.patch {
		return "implementer"
	}
	return map[string]string{"reader": "implementer", "verifier": "verifier", "reviewer": "reviewer"}[d.o.role]
}

func isNumeric(s string) bool {
	_, e := strconv.ParseUint(s, 10, 64)
	return e == nil
}

// delegateCaller reads the calling job (ARBITER_JOB) for its task, owner lane
// and model family. A readable job's family wins over --family; outside a job
// the flag and HK_TASK_ID stand in.
func (a *App) delegateCaller(ctx context.Context, flagFamily string) (delegateCaller, error) {
	c := delegateCaller{family: flagFamily, task: os.Getenv("HK_TASK_ID")}
	parent := os.Getenv("ARBITER_JOB")
	if parent == "" {
		return c, nil
	}
	if !validID(parent) {
		return c, fail(exitUsage, "ARBITER_JOB is not a valid job id")
	}
	j, e := a.resolveParent(ctx, parent)
	if e != nil {
		return c, fail(exitStateConflict, "parent metadata unreadable: %v", e)
	}
	c.parent, c.task, c.owner = parent, j.Task, j.Owner
	family := j.Family
	if family == "" {
		family = a.Policy.Profiles[j.Profile].Family
	}
	if family != "" {
		c.family = family
	}
	return c, nil
}

// delegateJobID reserves the next delegate-<parent>-<n> (or delegate_<host>_<n>
// outside a job) by creating its job dir; mkdir is the atomic claim, so
// concurrent delegations on this host never share an id. The counter is local,
// so outside a job the id carries the host's node id: the hk claim is global,
// and two hosts' delegate-1 would collide there. A parent job runs on one host
// and its id is already unique, so its delegations need no host part. The two
// forms differ in the separator after "delegate", because a node id can equal a
// parent job id; node ids have no "_", so the host form parses unambiguously.
func (a *App) delegateJobID(parent string) (string, error) {
	prefix := "delegate-"
	if parent != "" {
		prefix += parent + "-"
	} else {
		host, ok := a.nodeID()
		if !ok {
			return "", fail(exitConfig, "delegate needs HUB_MACHINE_ID in the node env file: its job ids must be unique across hosts")
		}
		prefix = "delegate_" + host + "_"
	}
	if e := os.MkdirAll(a.Jobs, 0700); e != nil {
		return "", e
	}
	for n := 1; n < 10000; n++ {
		id := prefix + strconv.Itoa(n)
		if !validID(id) {
			return "", fail(exitUsage, "delegate job id %q is not valid; the parent job id is too long", id)
		}
		switch e := os.Mkdir(filepath.Join(a.Jobs, id), 0700); {
		case e == nil:
			return id, nil
		case !os.IsExist(e):
			return "", e
		}
	}
	return "", fail(exitDuplicate, "no free delegate job id under %s", prefix)
}

// delegatePreamble is the role contract at the head of the delegate's brief,
// followed by how to file the answer. The fleet brief-policy preamble is added
// on top by spawn.
func delegatePreamble(role string, patch, panel bool, id string, p delegatePaths) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are a delegated %s for job %s. A lead session asked you one bounded question and is waiting for the answer. Do not open a PR, create a branch, commit or push.\n\n", role, id)
	if patch {
		b.WriteString("You are in a throwaway checkout of the lead's tree. You may edit files here to try the change; do not commit. Your edits are handed back to the lead as a patch it may or may not apply, so say what you changed and whether it worked.\n")
	} else {
		switch role {
		case "verifier":
			b.WriteString("You verify. Run only the acceptance commands given in the task below, exactly as written, and report each one with its exit status and the output that decides it. Never edit, create or delete any file in the working tree, never fix a failure you find, and run nothing else that changes state.\n")
		case "reviewer":
			b.WriteString("You review. Read the code and the diff the task names, run only read-only commands, and report findings ranked by severity with file:line and a concrete failure scenario. Never edit, create or delete any file in the working tree.\n")
		default:
			b.WriteString("You read. Read files, search and run read-only commands, then answer the question from what you saw, citing file:line. Never edit, create or delete any file in the working tree.\n")
		}
	}
	shape := "{\"answer\": \"<your answer>\"}"
	if panel {
		b.WriteString("\nOther models answer the same task independently and the lead compares the answers. Make each claim its own short point, cite file:line where it has one, and give one word as your overall verdict (for example ok, issues, fail, unclear).\n")
		shape = "{\"answer\": \"<your answer>\", \"verdict\": \"<one word>\", \"points\": [\"<one short claim, with file:line if any>\", ...]}"
	}
	fmt.Fprintf(&b, "\nWhen you have the answer, file it and stop:\n1. Write %s (one JSON object, under 64 KiB) to %s\n2. Write the same answer as plain text to %s\n3. Run: panewire done %s --report %s --result %s\nThese three files are the only files you may write outside the working tree.\n\nTask:", shape, p.result, p.report, id, p.report, p.result)
	return b.String()
}

// delegatePane spawns the delegate in a visible pane through the ordinary
// spawn path and waits for it. The spawn and wait chatter goes to stderr so
// stdout carries only the answer.
func (a *App) delegatePane(ctx context.Context, s Spawn, timeout time.Duration) (string, error) {
	s.Wait, s.WaitTimeout = true, timeout.String()
	if s.ParentJob != "" {
		s.Task = "" // the task is inherited from the parent job
	}
	spawn := a.spawnFn
	if spawn == nil {
		spawn = func(ctx context.Context, a *App, s Spawn) error { return a.SubmitSpawn(ctx, s) }
	}
	out := a.Out
	var chatter bytes.Buffer
	a.Out = &chatter
	e := spawn(ctx, a, s)
	a.Out = out
	a.Err.Write(chatter.Bytes())
	if e != nil {
		fmt.Fprintf(a.Err, "panewire: delegate job %s did not answer; its pane (if any) stays for `panewire reap`\n", s.Job)
		return "", e
	}
	j, _ := a.job(s.Job)
	return j.Profile, nil
}

// delegateAnswer reads what the delegate filed: the result object in the hk
// job record (`panewire done --result`), else the report text when the
// completion watcher finished the job on the delegate's terminal signal.
func (a *App) delegateAnswer(ctx context.Context, id string, p delegatePaths) (string, map[string]any, error) {
	var result map[string]any
	if store, e := a.jobStore(); e == nil {
		if rec, e := store.Get(ctx, id); e == nil && rec["result"] != nil {
			if b, e := json.Marshal(rec["result"]); e == nil {
				_ = json.Unmarshal(b, &result)
			}
		}
	}
	if ans := strings.TrimSpace(str(result, "answer")); ans != "" {
		return ans, result, nil
	}
	if b, e := os.ReadFile(p.report); e == nil && strings.TrimSpace(string(b)) != "" {
		return strings.TrimSpace(string(b)), result, nil
	}
	return "", result, fail(exitFailure, "delegate job %s finished without an answer", id)
}

// delegateClose closes the delegate's pane once its answer has been read, after
// checking the pane is still the one this job spawned. A delegate placed on
// another host has no local receipt; its pane is left for that host's reap.
func (a *App) delegateClose(ctx context.Context, id string) {
	j, e := a.job(id)
	if e != nil || j.Pane == "" {
		a.warn("delegate job %s has no local pane to close", id)
		return
	}
	agents, e := a.agents(ctx)
	if e != nil {
		a.warn("delegate pane %s not closed: %v", j.Pane, e)
		return
	}
	for _, ag := range agents {
		if ag.Pane != j.Pane {
			continue
		}
		if ag.Name != j.Label || j.Tab != "" && ag.Tab != j.Tab {
			a.warn("delegate pane %s now belongs to another agent; not closed", j.Pane)
			return
		}
		a.recordJobUsage(ctx, j, true)
		if e = a.must(ctx, "herdr", "pane", "close", j.Pane); e != nil {
			a.warn("delegate pane %s not closed: %v", j.Pane, e)
			return
		}
	}
	if e = a.removeLane(ctx, j); e != nil {
		a.warn("lane retained: %v", e)
	}
	if e = a.event(ctx, id, "job.reaped", map[string]any{"pane_id": j.Pane, "reason": "delegate-answered"}); e != nil {
		a.warn("reaped event failed: %v", e)
	}
}

func (a *App) printDelegate(r DelegateResult, asJSON bool) error {
	if asJSON {
		return writeJSON(a.Out, r)
	}
	fmt.Fprintf(a.Out, "delegate job=%s role=%s mode=%s profile=%s\n", r.Job, r.Role, r.Mode, r.Profile)
	if r.Patch != "" {
		fmt.Fprintf(a.Out, "patch=%s\n", r.Patch)
	}
	fmt.Fprintln(a.Out, r.Answer)
	return nil
}

// oneShot is how each agent kind runs a single prompt non-interactively: args
// before and after the launch flags, and whether the prompt goes on stdin or
// as the last argument. Kinds not listed have no headless form here.
var oneShot = map[string]struct {
	pre, post []string
	stdin     bool
}{
	"claude":   {post: []string{"-p"}, stdin: true},
	"codex":    {pre: []string{"exec"}, stdin: true},
	"devin":    {post: []string{"-p"}},
	"cmd":      {post: []string{"-p"}},
	"omp":      {post: []string{"-p"}},
	"opencode": {pre: []string{"run"}},
}

// delegateHeadless runs the selected seat's CLI once on this host. It follows
// spawn's order — select, admit, claim the hk record and the ledger, lease the
// quota pool — and closes the job through `done`, so a headless delegation
// leaves the same records as a pane one.
func (a *App) delegateHeadless(ctx context.Context, s Spawn, c delegateCaller, brief string, p delegatePaths, timeout time.Duration) (profile string, err error) {
	s.placement = &placementDecision{}
	defer func() { a.finishPlacement(ctx, s, err) }()
	if c.parent != "" {
		s.Inherited, s.Owner = c.parent, c.owner
	}
	sel, e := a.selectProfile(ctx, s)
	if sel != nil {
		s.Sel = sel.record()
	}
	if e != nil {
		return "", e
	}
	s.Profile, s.Effort = sel.chosen.seatName, firstNonempty(sel.chosen.row.Effort, s.Effort)
	s.AccountKey, s.RoutingRole, s.route = sel.chosen.accountKey, sel.req.route.role, sel.req.route
	seat := a.Policy.Profiles[s.Profile]
	shot, ok := oneShot[seat.Kind]
	if !ok {
		return "", fail(exitConfig, "%s (%s) has no headless form; run without --headless", s.Profile, seat.Kind)
	}
	effort := firstNonempty(s.Effort, seat.Effort)
	if seat.Pin && effort != seat.Effort {
		return "", fail(exitSeatRefused, "profile pins effort %s", seat.Effort)
	}
	launch, gate, e := a.admitLaunch(ctx, s, seat, effort, sel.chosen.row.Effort, s.Grade, s.ParentFamily, true)
	if e != nil {
		return "", e
	}
	s.placement.execution = placementExecution{Host: a.machine(), Family: seat.Family, Model: launch.ModelID, Role: placementRole(s), Profile: s.Profile, Pool: launch.Pool, AccountKey: placementAccount(s.AccountKey)}
	argv, env, e := a.launchArgs(seat, launch)
	if e != nil {
		return "", e
	}
	argv = withSessionName(seat.Kind, argv, s.Job)
	granted, e := admissionGate(seat.gateName(), s.AccountKey, launch.Pool, gate)
	if e != nil {
		return "", ledgerFail(e, "lease")
	}

	if e = a.claimJobRecord(ctx, s); e != nil {
		return "", e
	}
	pool := ""
	finished := false
	defer func() {
		if finished {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		a.abandonJobRecord(cleanup, s.Job)
		_ = a.event(cleanup, s.Job, "job.lost", map[string]any{"reason": "headless-failed"})
		if pool != "" {
			_ = a.ledger().ReleaseQuotaPool(cleanup, s.Job, pool, false)
		}
	}()
	owner := firstNonempty(s.Owner, "default")
	// The ledger knows two claim roles, worker and builder; a reviewer seat
	// claims as a worker, as spawn does.
	if _, e = a.ledger().Claim(ctx, arbiter.Claim{Job: s.Job, AgentLabel: s.Label, Lane: owner, T: s.T, Role: "worker", TaskID: s.Task, TaskInheritedFrom: s.Inherited}); e != nil {
		return "", ledgerFail(e, "claim")
	}
	rec := map[string]any{"source": "delegate", "mode": "headless", "profile": s.Profile, "host": a.machine()}
	a.attachSelection(rec, s)
	if e = a.event(ctx, s.Job, "job.placement", rec); e != nil {
		a.warn("placement decision record failed: %v", e)
	}
	lease, e := a.ledger().RecordQuotaPool(ctx, arbiter.QuotaRun{Job: s.Job, Profile: seat.gateName(), LaunchProfile: launchProfile(seat.Canon, launch.Effort), Gate: granted})
	if e != nil {
		return "", ledgerFail(e, "lease")
	}
	pool = lease.Pool
	receipt := map[string]any{"label": s.Label, "profile": s.Profile, "launch_profile": launchProfile(seat.Canon, launch.Effort), "model_id": launch.ModelID, "effort": launch.Effort, "family": seat.Family, "role": s.Role, "owner_lane": owner, "task_id": s.Task, "report": p.report, "cwd": s.CWD, "pool": pool, "host": a.machine(), "headless": true}
	if e = a.event(ctx, s.Job, "job.spawned", receipt); e != nil {
		return "", e
	}
	spawned := map[string]any{"state": "spawned", "at": a.Now().UTC(), "role": s.Role, "label": s.Label, "family": seat.Family, "profile": s.Profile, "model": launch.ModelID, "pool": pool, "owner_lane": owner, "selection": jobSelection(s, "delegate")}
	if s.Inherited != "" {
		spawned["parent_job"] = s.Inherited
	}
	putSpawnedJobRecord(ctx, a, s.Job, spawned)

	env = append(env, "ARBITER_JOB="+s.Job, "ARBITER_RESOURCE="+pool, "ARBITER_KIND=quota_pool", "ARBITER_PROFILE="+seat.gateName(), "HK_TASK_ID="+s.Task)
	cmdArgs := slices.Concat(shot.pre, argv, shot.post)
	cmd := Command{Name: seat.Kind, Args: cmdArgs, Env: env, Dir: s.CWD}
	prompt, e := os.ReadFile(p.brief)
	if e != nil {
		return "", e
	}
	if payload, e := a.prepareBrief(s, seat, launch, string(prompt)); e != nil {
		return "", e
	} else if shot.stdin {
		cmd.Input = payload
	} else {
		cmd.Args = append(cmd.Args, payload)
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	runner := a.Runner
	if _, ok := runner.(ExecRunner); ok {
		runner = ExecRunner{Timeout: timeout}
	}
	s.placement.started = true
	r := runner.Run(runCtx, cmd)
	if r.Code != 0 {
		return "", fail(r.Code, "headless %s: %s", s.Profile, strings.TrimSpace(r.Err+" "+r.Out))
	}
	s.placement.placed = true
	a.finishPlacement(ctx, s, nil)
	answer := strings.TrimSpace(r.Out)
	if answer == "" {
		return "", fail(exitFailure, "headless %s printed no answer", s.Profile)
	}
	result, _ := json.Marshal(map[string]any{"answer": answer})
	if e = os.WriteFile(p.result, result, 0600); e != nil {
		return "", e
	}
	if e = os.WriteFile(p.report, []byte(answer+"\n"), 0600); e != nil {
		return "", e
	}
	finished = true
	var done bytes.Buffer
	out := a.Out
	a.Out = &done
	e = a.lifecycle(ctx, "done", []string{s.Job, "--report", p.report, "--result", p.result})
	a.Out = out
	a.Err.Write(done.Bytes())
	return s.Profile, e
}

// scratchTree adds a detached worktree of cwd in a fresh temp dir: the tree at
// HEAD, plus the tracked changes the caller has not committed (git stash
// create makes that commit without touching the work tree, the index or any
// ref). The returned func removes the worktree and its dir.
func (a *App) scratchTree(ctx context.Context, cwd string) (string, func(), error) {
	r := a.call(ctx, "git", "-C", cwd, "rev-parse", "--is-inside-work-tree")
	if r.Code != 0 {
		return "", nil, fail(exitUsage, "--patch needs --cwd inside a git work tree")
	}
	base := "HEAD"
	if r = a.call(ctx, "git", "-C", cwd, "stash", "create"); r.Code == 0 && strings.TrimSpace(r.Out) != "" {
		base = strings.TrimSpace(r.Out)
	}
	dir, e := os.MkdirTemp("", "panewire-delegate-")
	if e != nil {
		return "", nil, e
	}
	cleanup := func() {
		bg := context.WithoutCancel(ctx)
		a.call(bg, "git", "-C", cwd, "worktree", "remove", "--force", dir)
		os.RemoveAll(dir)
		a.call(bg, "git", "-C", cwd, "worktree", "prune")
	}
	if e = check(a.call(ctx, "git", "-C", cwd, "worktree", "add", "--detach", dir, base), "git worktree add"); e != nil {
		cleanup()
		return "", nil, e
	}
	return dir, cleanup, nil
}

// scratchPatch writes everything the delegate changed in its scratch tree to
// dest and returns dest, or "" when nothing changed.
func (a *App) scratchPatch(ctx context.Context, tree, dest string) (string, error) {
	if e := check(a.call(ctx, "git", "-C", tree, "add", "-A"), "git add"); e != nil {
		return "", e
	}
	r := a.call(ctx, "git", "-C", tree, "diff", "--cached", "--binary", "HEAD")
	if e := check(r, "git diff"); e != nil {
		return "", e
	}
	if strings.TrimSpace(r.Out) == "" {
		return "", nil
	}
	return dest, os.WriteFile(dest, []byte(r.Out), 0600)
}
