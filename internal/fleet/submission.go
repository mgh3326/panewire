package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/spawnargs"
)

// Repository carries checkout evidence read on the submitting host. The hub
// never reads a pathname on another host; the node verifies the pinned checkout.
type Repository struct {
	Name      string       `json:"name"`
	Origin    string       `json:"origin"`
	Relative  string       `json:"relative"`
	Worktree  string       `json:"worktree"`
	LocalRoot string       `json:"local_root"`
	Checkout  *HubCheckout `json:"checkout,omitempty"`
}

type Submission struct {
	RequestID       string           `json:"request_id"`
	Args            []string         `json:"args"`
	Brief           string           `json:"brief"`
	Repository      Repository       `json:"repository"`
	OriginMachine   string           `json:"origin_machine"`
	DeadlineSeconds int              `json:"deadline_seconds"`
	StartConditions []StartCondition `json:"start_conditions,omitempty"`
	// ExpiresSeconds is the request's time limit from acceptance (--expires).
	// Zero means the role's default: 6 h for an implementer, 1 h for a
	// reviewer or consult (hk 4660). The attempt's DeadlineSeconds ends an
	// attempt, never the request.
	ExpiresSeconds int `json:"expires_seconds,omitempty"`
	// LadderStep identifies a hub-staged author replacement for replay.
	LadderStep *LadderStepIdentity `json:"ladder_step,omitempty"`
	// E6Arm is the exact <canon>@<effort> experiment marker the submitter saw
	// in its environment. Hub admission and the node launch use it exactly as
	// a local spawn reads the environment; empty means none, and an empty
	// marker stays empty — the hub's and the node's process environments
	// never fill it (hk 3518).
	E6Arm string `json:"e6_arm,omitempty"`
	// RetryHost is the hub-internal marker of a verified-cleanup retry: the
	// hub sets it, in memory, on the one request it rewrites to `--host <the
	// stored outcome host>`, and validateContainment then does not treat that
	// host as a submitter pin. It never travels on the wire or in the store
	// (json:"-"), and no flag parses into it.
	RetryHost string `json:"-"`
	// RetryHarnesses lists harness kinds already refused on RetryHost in this
	// attempt. Like RetryHost, this is internal hub state, never wire input.
	RetryHarnesses []string `json:"-"`
	// CheckoutSpec marks a request whose worktree came from the
	// --checkout-origin/--checkout-commit/--checkout-ref spec rather than a
	// local -c checkout (hk 4738). Submit asks the hub for checkout_spec_v1
	// first: a hub that predates it would run the job in the node's cwd_map
	// path instead of the named commit. Never on the wire.
	CheckoutSpec bool `json:"-"`
}

type LadderStepIdentity struct {
	Parent  string `json:"parent"`
	Round   int    `json:"round"`
	PR      string `json:"pr"`
	Profile string `json:"profile"`
	Effort  string `json:"effort"`
}

type Accepted struct{ Job, Request string }

type SpawnSubmitter interface {
	Submit(context.Context, Submission) (Accepted, error)
}

// Submit sends the request under its own request id. The hub keys the
// operation by that id and answers a replay with the operation it already
// holds, so the submit may be sent again (docs/hub-uninterrupted-calls.md).
func (c *hubHTTP) Submit(ctx context.Context, request Submission) (Accepted, error) {
	// A hub that predates a feature would drop the field and run the request
	// without it, so the submitter asks for the feature first.
	var needs []struct{ feature, flag string }
	if len(request.StartConditions) != 0 {
		needs = append(needs, struct{ feature, flag string }{"start_conditions_v1", "--after start conditions"})
	}
	if request.ExpiresSeconds != 0 {
		needs = append(needs, struct{ feature, flag string }{"spawn_expires_v1", "--expires"})
	}
	if request.CheckoutSpec {
		needs = append(needs, struct{ feature, flag string }{"checkout_spec_v1", "--checkout-origin"})
	}
	// A sparse checkout's lists are dropped by a hub that predates
	// checkout_sparse_v1 (hk 5098), so the request would run the full checkout
	// without saying so; ask first.
	if checkout := request.Repository.Checkout; checkout != nil && (len(checkout.Exclude) != 0 || len(checkout.Include) != 0) {
		needs = append(needs, struct{ feature, flag string }{"checkout_sparse_v1", "--checkout-exclude"})
	}
	// A hub that predates checkout_branch_v1 refuses the successor mode, so
	// the submitter states the cause instead of reading invalid_request
	// (hk 5118).
	if checkout := request.Repository.Checkout; checkout != nil && checkout.Mode == HubCheckoutSuccessor {
		needs = append(needs, struct{ feature, flag string }{"checkout_branch_v1", "--checkout-ref refs/heads/<branch> for an implementer spawn"})
	}
	if len(needs) != 0 {
		status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/features", nil, 5*time.Second)
		if err != nil {
			return Accepted{}, fail(exitConfig, "unable to verify hub support for %s: %v", needs[0].flag, err)
		}
		if status != http.StatusOK {
			return Accepted{}, fail(exitConfig, "unable to verify hub support for %s (HTTP %d)", needs[0].flag, status)
		}
		var features struct {
			Features []string `json:"features"`
		}
		if json.Unmarshal(raw, &features) != nil {
			return Accepted{}, fail(exitConfig, "hub returned an unreadable spawn feature response")
		}
		for _, need := range needs {
			if !slices.Contains(features.Features, need.feature) {
				return Accepted{}, fail(exitUsage, "hub does not support %s; upgrade the hub before submitting", need.flag)
			}
		}
	}
	status, raw, err := c.doIdempotent(ctx, http.MethodPost, "/v1/spawns", request, 15*time.Second)
	if err != nil {
		return Accepted{}, err
	}
	if status == http.StatusConflict {
		return Accepted{}, fail(exitDuplicate, "%s", conflictMessage(raw))
	}
	var response struct {
		Job     string `json:"job_id"`
		Request string `json:"request_id"`
		Error   string `json:"error"`
		Reason  string `json:"reason"`
		Fix     string `json:"fix"`
	}
	if json.Unmarshal(raw, &response) != nil || status != http.StatusAccepted || !validID(response.Job) || response.Request != request.RequestID {
		message := fmt.Sprintf("hub did not accept spawn (HTTP %d, %s)", status, response.Error)
		if response.Reason != "" {
			message += ": " + response.Reason
		}
		return Accepted{}, fail(exitConfig, "%s; fix: %s", message, firstNonempty(response.Fix, "inspect the hub operation and restore its configuration, then replay the saved request"))
	}
	return Accepted{Job: response.Job, Request: response.Request}, nil
}

// conflictMessage prints what the hub said about a 409: its error, the
// operation holding the claim, and the fix. A body that does not parse keeps
// the generic text.
func conflictMessage(raw []byte) string {
	var conflict struct {
		Error   string `json:"error"`
		Fix     string `json:"fix"`
		Request string `json:"request_id"`
		Job     string `json:"job"`
		Host    string `json:"host"`
		Pane    string `json:"pane"`
	}
	if json.Unmarshal(raw, &conflict) != nil || conflict.Error == "" {
		return "job id already claimed; inspect its existing accepted request"
	}
	message := "hub refused spawn: " + conflict.Error
	if conflict.Request != "" || conflict.Job != "" {
		message += fmt.Sprintf(" (held by request=%s job=%s host=%s pane=%s)", conflict.Request, conflict.Job, firstNonempty(conflict.Host, "unknown"), firstNonempty(conflict.Pane, "unknown"))
	}
	if conflict.Fix != "" {
		message += "; fix: " + conflict.Fix
	}
	return message
}

func (a *App) SubmitSpawn(ctx context.Context, s Spawn) error {
	defer a.reportHubRetries()
	wait := s.Wait
	if wait {
		if _, err := a.waitDeadline(s); err != nil {
			return err
		}
		s.Wait = false
	}
	request, err := a.PrepareSubmission(ctx, s)
	if err != nil {
		return err
	}
	hub, err := a.hub()
	if err != nil {
		return err
	}
	submitter, ok := hub.(SpawnSubmitter)
	if !ok {
		return fail(exitConfig, "hub transport cannot submit jobs; configure a current panewire hub")
	}
	request, err = a.saveSubmission(request)
	if err != nil {
		return err
	}
	accepted, err := submitter.Submit(ctx, request)
	if err != nil {
		return fail(ErrorCode(err), "spawn response is uncertain or refused; replay this same command and receipt request=%s: %v", request.RequestID, err)
	}
	fmt.Fprintf(a.Out, "ACCEPTED job=%s request=%s\n", accepted.Job, accepted.Request)
	if wait {
		waiter, ok := submitter.(interface {
			WaitSpawn(context.Context, string, SpawnWaitOptions) (SpawnAttempt, error)
		})
		if !ok {
			return fail(exitConfig, "hub transport cannot wait for spawn status; upgrade the configured transport")
		}
		timeout, err := a.waitDeadline(s)
		if err != nil {
			return err
		}
		attempt, err := waiter.WaitSpawn(ctx, accepted.Request, SpawnWaitOptions{Timeout: timeout, Notice: a.Err})
		if err != nil {
			return err
		}
		attempt.print(a.Out, accepted.Job)
		s.Wait = true
		s.laneUnregistered = attempt.LaneUnregistered
		return a.waitJob(ctx, s, true)
	}
	return nil
}

func (a *App) PrepareSubmission(ctx context.Context, s Spawn) (Submission, error) {
	if s.ParentJob == "" && s.Task == "" {
		s.ParentJob = os.Getenv("ARBITER_JOB")
		if s.ParentJob == "" {
			s.Task = os.Getenv("HK_TASK_ID")
		}
	}
	if s.Owner == "" {
		if s.ownerOmitted {
			if err := a.resolveOwner(ctx, &s); err != nil {
				return Submission{}, err
			}
		} else if s.ParentJob != "" {
			parent, err := a.resolveParent(ctx, s.ParentJob)
			if err != nil {
				return Submission{}, fail(exitStateConflict, "parent metadata unreadable; restore handoffkeep access before retrying")
			}
			s.Owner = parent.Owner
		}
	}

	spec, specName, err := s.checkoutSpec()
	if err != nil {
		return Submission{}, err
	}
	if s.CWD == "" && spec == nil || s.Prompt == "" || !validID(s.Job) || !validID(s.Label) || !isNumeric(s.Task) && s.ParentJob == "" {
		return Submission{}, fail(exitUsage, "spawn requires -c CWD (or %s) -p BRIEF -l LABEL --task TASK_ID (or --parent-job JOB)", checkoutSpecFlags)
	}
	// The id names jobs/<id> on this host and on the executing node; refuse a
	// case-only collision before the hk preflight spends a request or a plan
	// dispatches (hk 4824). A real submission claims the identity in
	// saveSubmission, immediately before its receipt creates the dir.
	if err := checkJobDirCase(a.Jobs, s.Job); err != nil {
		return Submission{}, err
	}
	if _, err := hubArgs(a.Jobs, s); err != nil {
		return Submission{}, fail(exitUsage, "%v; correct the flag before submitting through the hub", err)
	}
	if err := validateContainment(s); err != nil {
		return Submission{}, err
	}
	if err := a.submitPreflight(ctx, s); err != nil {
		return Submission{}, err
	}
	originalHost := s.Host
	resolved, err := a.resolveHost(s.Host)
	if err != nil {
		return Submission{}, err
	}
	if resolved == "local" {
		resolved = a.machine()
		if resolved == "local" {
			return Submission{}, fail(exitConfig, "set local.machine_id before requesting --host local")
		}
	}
	s.Host = resolved
	if originalHost != "" && originalHost != s.Host {
		fmt.Fprintf(a.Out, "host_alias=%s machine=%s\n", originalHost, s.Host)
	}
	if s.Profile == "" {
		s.Profile = "auto"
	}
	if s.Host == "" {
		s.Host = "auto"
	}
	if s.Role == "" {
		s.Role = "implementer"
	}
	if s.Profile != "auto" || s.Host != "auto" {
		if strings.TrimSpace(s.OverrideReason) == "" {
			return Submission{}, fail(exitUsage, "explicit -m or --host requires --override-reason")
		}
	}
	s.E6Arm = a.e6Arm("", false)
	if s.E6Arm != "" && !ValidE6Arm(s.E6Arm, s.Profile) {
		return Submission{}, fail(exitUsage, "PANEWIRE_E6_ARM must be <canon>@ or <canon>@<effort> with canon %s, got %q", s.Profile, s.E6Arm)
	}
	// hk 4738: a spawn may name its worktree instead of holding one. The spec
	// is the same pinned checkout the -c path derives, so everything after
	// this block sees one shape and the node prepares the same worktree.
	var repo *repoResolution
	cwd := ""
	if spec != nil {
		repo = &repoResolution{name: specName, url: spec.Origin, wt: spec.Dir}
	} else {
		cwd, err = filepath.Abs(expand(s.CWD))
		if err != nil {
			return Submission{}, err
		}
		cwd, err = filepath.EvalSymlinks(cwd)
		if err != nil {
			return Submission{}, fail(exitUsage, "cwd is unreadable; pass -c with an existing checkout")
		}
		s.CWD = cwd
		repo = a.localRepo(ctx, cwd)
		if repo == nil || repo.url == "" {
			return Submission{}, fail(exitUsage, "checkout has no usable origin; configure origin and push the branch before spawn")
		}
		if repo.rel != "" {
			return Submission{}, fail(exitPlacement, "hub_subdirectory_unsupported: pass -c with the checkout root before submitting")
		}
		if repo.wt == "" {
			repo.sourceDir = cwd
			repo.branch = strings.TrimSpace(a.call(ctx, "git", "-C", cwd, "branch", "--show-current").Out)
			repo.head = strings.TrimSpace(a.call(ctx, "git", "-C", cwd, "rev-parse", "HEAD").Out)
			if s.Role != "reviewer" && s.Role != "consult" && repo.branch != "" {
				defaultRef := a.call(ctx, "git", "-C", cwd, "symbolic-ref", "--short", "refs/remotes/origin/HEAD")
				if defaultRef.Code == 0 && strings.TrimSpace(defaultRef.Out) == "origin/"+repo.branch {
					return Submission{}, fail(exitPlacement, "create a worktree on a new branch (git worktree add -b wk/<slug> ...) and pass it as -c")
				}
			}
			repo.wt = checkoutWorktreeName(s.Job)
		}
	}
	brief, err := a.readBrief(ctx, s.Prompt, hubMaxBriefBytes)
	if _, doc, _ := briefDocKey(s.Prompt); doc && err != nil {
		return Submission{}, err
	}
	if err != nil || len(brief) > hubMaxBriefBytes || !utf8.Valid(brief) || strings.TrimSpace(string(brief)) == "" {
		return Submission{}, fail(exitUsage, "pass -p with a nonempty UTF-8 brief of at most 64 KiB")
	}
	// A writing role must have its branch on origin before submitting: without
	// refs/remotes/origin/<branch> the checkout spec falls back to a detached
	// HEAD at whatever origin ref holds HEAD, which is not the worktree the
	// caller passed. Reviewer and consult clear the branch below and keep that
	// fallback; a pinned checkout already carries its evidence.
	//
	// A branch whose origin tip is a descendant of HEAD (HEAD strictly behind)
	// is refused too: the node would find the origin tip differs from the sha
	// and answer branch-elsewhere. HEAD equal to or ahead of the tip keeps
	// today's behaviour. Both checks read only the local remote-tracking ref;
	// the submitter never fetches, so a stale ref passes (hk 3355).
	pinned := a.Repository != nil && a.Repository.Checkout != nil
	if !pinned && repo.branch != "" && s.Role != "reviewer" && s.Role != "consult" {
		originRef := "refs/remotes/origin/" + repo.branch
		tip := a.call(ctx, "git", "-C", cwd, "rev-parse", "--verify", "--quiet", originRef)
		if tip.Code != 0 {
			return Submission{}, fail(exitPlacement, "branch %s is not on origin; push it first: git -C %s push -u origin %s", repo.branch, cwd, repo.branch)
		}
		if strings.TrimSpace(tip.Out) != repo.head && a.call(ctx, "git", "-C", cwd, "merge-base", "--is-ancestor", repo.head, originRef).Code == 0 {
			return Submission{}, fail(exitPlacement, "worktree is behind origin/%s; run: git -C %s fetch && git -C %s merge --ff-only origin/%s", repo.branch, cwd, cwd, repo.branch)
		}
	}
	var checkout *HubCheckout
	if spec != nil {
		checkout = spec
	} else if repo.wt != "" {
		var why string
		checkoutRepo := *repo
		if s.Role == "reviewer" || s.Role == "consult" {
			checkoutRepo.branch = ""
		}
		checkout, why = a.hubCheckout(ctx, &checkoutRepo)
		if checkout == nil {
			return Submission{}, fail(exitPlacement, "%s", why)
		}
		if checkout.Mode == "branch" {
			checkout.Sha = repo.head
		}
	}
	report, err := hubReportRelative(a.Jobs, s)
	if err != nil {
		return Submission{}, fail(exitUsage, "%v", err)
	}
	s.Report, s.ReportRelative = "", report
	id, err := newRequestID()
	if err != nil {
		return Submission{}, err
	}
	submissionSpawn := s
	if originalHost == "local" {
		submissionSpawn.Host = "local"
	}
	if submissionSpawn.Host == "auto" && herdrWorkspaceID.MatchString(submissionSpawn.Workspace) {
		submissionSpawn.Workspace = ""
	}
	conditions, err := ParseStartConditions(s.After, s.Task)
	if err != nil {
		return Submission{}, fail(exitUsage, "%v", err)
	}
	expiresSeconds := 0
	if s.Expires != "" {
		if expiresSeconds, err = parseSpawnExpires(s.Expires); err != nil {
			return Submission{}, fail(exitUsage, "%v", err)
		}
	}
	request := Submission{RequestID: id, Brief: string(brief), Repository: Repository{Name: repo.name, Origin: checkout.Origin, Relative: repo.rel, Worktree: repo.wt, LocalRoot: repo.localRoot, Checkout: checkout}, CheckoutSpec: spec != nil, OriginMachine: a.machine(), DeadlineSeconds: 600, E6Arm: s.E6Arm, Args: submissionArgs(submissionSpawn), StartConditions: conditions, ExpiresSeconds: expiresSeconds}
	if _, err = ValidateSubmission(request); err != nil {
		return Submission{}, err
	}
	return request, nil
}

func ValidateSubmission(request Submission) (Spawn, error) {
	s, err := parseSpawn(request.Args)
	if err != nil {
		return s, err
	}
	if !ValidateStartConditions(request.StartConditions, s.Task) {
		return s, fail(exitUsage, "invalid start conditions; rebuild the request with panewire spawn")
	}
	if request.E6Arm != "" && !ValidE6Arm(request.E6Arm, s.Profile) {
		return s, fail(exitUsage, "e6_arm %q must be <canon>@ or <canon>@<effort> with canon %q", request.E6Arm, s.Profile)
	}
	s.E6Arm = request.E6Arm
	// The request's marker is authoritative from here on: an empty e6_arm
	// stays empty through admission and the node argv; the hub process
	// environment never fills it (hk 3518, panewire#307 follow-up).
	s.e6ArmLocked = true
	s.hubRetryHost = request.RetryHost
	s.failedHarnesses = append([]string(nil), request.RetryHarnesses...)
	if err := validateContainment(s); err != nil {
		return s, err
	}
	if s.Host == "local" {
		if !validID(request.OriginMachine) || request.OriginMachine == "local" {
			return s, fail(exitUsage, "invalid origin machine for --host local")
		}
		s.Host = request.OriginMachine
		s.requestedLocal = true
	}
	pinned := request.Repository.Checkout != nil
	if !regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(request.RequestID) || !validID(s.Job) || !validID(s.Label) || s.DryRun || s.Delegated || s.Wait || s.Prompt == "" || s.CWD != "" && !filepath.IsAbs(s.CWD) || s.CWD == "" && !pinned || len(request.Brief) > hubMaxBriefBytes || !utf8.ValidString(request.Brief) || strings.TrimSpace(request.Brief) == "" || request.DeadlineSeconds < 1 || request.DeadlineSeconds > 3600 || !ValidSpawnExpiresSeconds(request.ExpiresSeconds) {
		return s, fail(exitUsage, "invalid spawn request; rebuild it with panewire spawn")
	}
	if !isNumeric(s.Task) && s.ParentJob == "" {
		return s, fail(exitUsage, "supply --task TASK_ID or --parent-job JOB")
	}
	if s.Task != "" && s.ParentJob != "" {
		return s, fail(exitUsage, "--task and --parent-job are exclusive")
	}
	if s.Owner == "" {
		return s, fail(exitUsage, "supply --owner LANE so completion can be delivered")
	}
	if !slices.Contains([]string{"T0", "T1", "T2", "T3"}, s.T) || !slices.Contains(spawnRoles, s.Role) {
		return s, fail(exitUsage, "use --t T0..T3 and a supported --role")
	}
	repo := request.Repository
	// A request with no -c names its worktree by the pinned checkout alone,
	// so it has no local root to be absolute; every other request reads a
	// local checkout and names the root it lives in (hk 4738).
	if !validID(repo.Name) || repo.Worktree != "" && !validID(repo.Worktree) || s.CWD != "" && !filepath.IsAbs(repo.LocalRoot) || s.CWD == "" && repo.Worktree == "" || !cloneURL.MatchString(repo.Origin) || credentialedURL(repo.Origin) || strings.ContainsAny(repo.Origin+repo.LocalRoot, "\x00\r\n") {
		return s, fail(exitUsage, "invalid repository descriptor; use a credential-free origin and a safe checkout name")
	}
	if repo.Relative != "" {
		return s, fail(exitPlacement, "hub_subdirectory_unsupported: use the checkout root before submitting")
	}
	// A sparse spec reaches the node as these lists (hk 5098): refuse a bad
	// entry where the flag is read, naming its index.
	if repo.Checkout != nil {
		if reason := spawnargs.SparseSpecRefusal(repo.Checkout.Mode, repo.Checkout.Exclude, repo.Checkout.Include); reason != "" {
			return s, fail(exitUsage, "%s", reason)
		}
	}
	return s, nil
}

func submissionArgs(s Spawn) []string {
	var args []string
	for _, field := range []struct{ flag, value string }{
		{"-c", s.CWD}, {"-p", s.Prompt}, {"-m", s.Profile}, {"-w", s.Workspace}, {"-l", s.Label}, {"--job", s.Job}, {"--t", s.T}, {"--owner", s.Owner}, {"--effort", s.Effort}, {"--role", s.Role}, {"--lane", s.Lane}, {"--parent", s.Parent}, {"--task", s.Task}, {"--parent-job", s.ParentJob}, {"--host", s.Host}, {"--report-relative", s.ReportRelative}, {"-L", s.ExecutionLane}, {"--override-reason", s.OverrideReason}, {"--containment", s.Containment}, {"--tags", s.Tags}, {"--exclude-host", s.ExcludeHosts}, {"--exclude-pool", s.ExcludePools}, {"--urgency", s.Urgency}, {"--author-family", s.AuthorFamily}, {"--builder-job", s.BuilderJob}, {"--review-pass", s.ReviewPass}, {"--grade", s.Grade}, {"--consult-stamp", consultStampValue(s.ConsultStamp)}, {"--operator-request", s.OperatorRequest}, {"--requested-by", s.RequestedBy}, {"--purpose", s.Purpose},
	} {
		if field.value != "" {
			args = append(args, field.flag, field.value)
		}
	}
	for _, flag := range []struct {
		name string
		set  bool
	}{{"--keep", s.Keep}, {"--job-dup-ok", s.DupOK}, {"--task-hk-bypass", s.Bypass}, {"--allow-escalation", s.AllowEscalation}} {
		if flag.set {
			args = append(args, flag.name)
		}
	}
	return args
}

func (a *App) stage(name string, started time.Time) {
	if a.ObserveStage != nil {
		a.ObserveStage(name, time.Since(started))
	}
}

// spawnStatusFailureLimit bounds how many status reads in a row may fail before
// WaitSpawn gives up; the operation keeps running on the hub meanwhile.
const spawnStatusFailureLimit = 15

// SpawnAttempt names where the hub actually ran an operation. The submitter
// only chose "auto", and its own dry-run can differ from the hub's placement.
type SpawnAttempt struct {
	Host    string `json:"host"`
	Profile string `json:"profile"`
	Pane    string `json:"pane"`
	// LaneUnregistered: the pane runs but the hub has no lane for it.
	LaneUnregistered bool `json:"lane_unregistered,omitempty"`
}

func (s SpawnAttempt) String() string {
	return "host=" + firstNonempty(s.Host, "none") + " profile=" + firstNonempty(s.Profile, "none")
}

func (s SpawnAttempt) print(out io.Writer, job string) {
	fmt.Fprintf(out, "SPAWNED job=%s %s pane=%s%s\n", job, s, firstNonempty(s.Pane, "none"), laneUnregisteredSuffix(s.LaneUnregistered))
}

// SpawnStatusRecord is one spawn operation as the client reads it: the
// fields `spawn status` prints, under the names it prints them.
type SpawnStatusRecord struct {
	Request string `json:"request"`
	// State is the request state of spawn_request_state.go, today's name or
	// a design name.
	State       string `json:"state"`
	Reason      string `json:"reason,omitempty"`
	NextCheckAt string `json:"next_check_at,omitempty"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	// RC is the hub's recorded result code; zero means it recorded none.
	RC    int    `json:"rc,omitempty"`
	Stage string `json:"stage,omitempty"`
	Job   string `json:"job,omitempty"`
}

// line is the one line `spawn status` prints for the record.
func (r SpawnStatusRecord) line() string {
	line := "request=" + r.Request + " state=" + r.State
	if r.Reason != "" {
		line += " reason=" + r.Reason
	}
	if r.NextCheckAt != "" {
		line += " next_check_at=" + r.NextCheckAt
	}
	if r.ExpiresAt != "" {
		line += " expires_at=" + r.ExpiresAt
	}
	if r.RC != 0 {
		line += " rc=" + strconv.Itoa(r.RC)
	}
	if r.Stage != "" {
		line += " stage=" + r.Stage
	}
	if r.Job != "" {
		line += " job=" + r.Job
	}
	return line
}

// ReadSpawnStatus reads a hub spawn operation once (GET /v1/spawns/<id>) and
// returns its state and the fields a caller reports. An id that is not a
// plain id, a transport failure, a non-200 answer or a body with no status is
// an error.
func (c *hubHTTP) ReadSpawnStatus(ctx context.Context, id string) (SpawnStatusRecord, error) {
	if !validID(id) {
		return SpawnStatusRecord{}, fail(exitUsage, "invalid spawn operation id")
	}
	status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/"+id, nil, 5*time.Second)
	if err != nil {
		return SpawnStatusRecord{}, err
	}
	var answer struct {
		Job         string `json:"job_id"`
		Request     string `json:"request_id"`
		Status      string `json:"status"`
		Reason      string `json:"reason"`
		NextCheckAt string `json:"next_check_at"`
		ExpiresAt   string `json:"expires_at"`
		// Result.RC and Result.Stage alone are read: a reason's refusal text
		// may name a path on the executing node, and the wait already prints
		// only these three.
		Result struct {
			RC     int    `json:"rc"`
			Stage  string `json:"stage"`
			Reason string `json:"reason"`
		} `json:"result"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &answer) != nil || answer.Status == "" {
		return SpawnStatusRecord{}, fail(exitStateConflict, "spawn status unreadable; inspect operation %s", id)
	}
	return SpawnStatusRecord{
		Request:     firstNonempty(answer.Request, id),
		State:       answer.Status,
		Reason:      firstNonempty(answer.Reason, answer.Result.Reason),
		NextCheckAt: answer.NextCheckAt,
		ExpiresAt:   answer.ExpiresAt,
		RC:          answer.Result.RC,
		Stage:       answer.Result.Stage,
		Job:         answer.Job,
	}, nil
}

// SpawnStatus reads a hub spawn operation's status once (GET
// /v1/spawns/<id>). The value is the request state of spawn_request_state.go,
// today's names or a design name. An id that is not a plain id, a transport
// failure, a non-200 answer or a body with no status is an error.
func (c *hubHTTP) SpawnStatus(ctx context.Context, id string) (string, error) {
	record, err := c.ReadSpawnStatus(ctx, id)
	if err != nil {
		return "", err
	}
	return record.State, nil
}

// SpawnWaitOptions shapes one WaitSpawn.
type SpawnWaitOptions struct {
	// Timeout bounds how long the request may stay short of landed or
	// terminal. Zero leaves only the context to end the wait.
	Timeout time.Duration
	// Notice hears the lines a waiting caller is owed: a parked request's
	// reason and next check, and a state this client does not know. Nil hears
	// nothing.
	Notice io.Writer
}

// WaitSpawn follows a hub request's state to landed or a terminal state
// (spawn_request_state.go maps each state), before the job-completion wait
// takes over. Admission and node refusal codes stay visible to synchronous
// callers. A non-terminal or unknown state keeps waiting; the wait ends at
// opts.Timeout with exitWaitParked for a parked request and exitWaitTimeout
// otherwise. The bound runs on the wall clock: a frozen test clock would
// never expire it.
func (c *hubHTTP) WaitSpawn(ctx context.Context, id string, opts SpawnWaitOptions) (SpawnAttempt, error) {
	poll := c.statusPoll
	if poll == 0 {
		poll = 2 * time.Second
	}
	var deadline time.Time
	if opts.Timeout > 0 {
		deadline = time.Now().Add(opts.Timeout)
	}
	failures := 0
	var told spawnWaitNotices
	// last is the latest request state read; the bound's exit names it.
	lastKind, lastStatus := spawnActive, ""
	expired := func() error {
		if deadline.IsZero() || time.Now().Before(deadline) {
			return nil
		}
		if lastKind == spawnParked {
			return fail(exitWaitParked, "spawn request %s is still parked after the wait timeout; it stays queued on the hub (operation %s)", id, id)
		}
		return fail(exitWaitTimeout, "wait timeout for spawn request %s in state %s; it stays on the hub (operation %s)", id, firstNonempty(lastStatus, "unknown"), id)
	}
	for {
		// The bound covers the read, its retries, the failure sleeps and the
		// poll sleep alike: a read starts only inside it and ends at it.
		if err := expired(); err != nil {
			return SpawnAttempt{}, err
		}
		readCtx, cancel := ctx, context.CancelFunc(func() {})
		if !deadline.IsZero() {
			readCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		status, raw, err := c.doIdempotent(readCtx, http.MethodGet, "/v1/spawns/"+id, nil, 5*time.Second)
		cancel()
		if ctx.Err() != nil {
			return SpawnAttempt{}, ctx.Err()
		}
		if err != nil || status >= http.StatusInternalServerError {
			if e := expired(); e != nil {
				return SpawnAttempt{}, e
			}
			if failures++; failures > spawnStatusFailureLimit {
				return SpawnAttempt{}, fail(exitStateConflict, "spawn status unavailable; inspect operation %s and retry its saved request without choosing a new host", id)
			}
			if err = c.sleepWithin(ctx, poll, deadline); err != nil {
				return SpawnAttempt{}, err
			}
			continue
		}
		failures = 0
		var result struct {
			Status      string `json:"status"`
			Reason      string `json:"reason"`
			NextCheckAt string `json:"next_check_at"`
			// ExpiresAt is the request's time limit (hk 4660); the hub
			// reports it on every state.
			ExpiresAt string `json:"expires_at"`
			Result    struct {
				SpawnAttempt
				RC     int    `json:"rc"`
				Reason string `json:"reason"`
			} `json:"result"`
		}
		if status != 200 || json.Unmarshal(raw, &result) != nil {
			return SpawnAttempt{}, fail(exitStateConflict, "spawn status unreadable; inspect operation %s", id)
		}
		attempt := result.Result.SpawnAttempt
		kind := classifySpawnState(result.Status)
		switch kind {
		case spawnLanded:
			return attempt, nil
		case spawnRefused:
			return attempt, fail(spawnRefusalRC(result.Result.RC), "spawn refused %s: %s (operation %s)", attempt, result.Result.Reason, id)
		case spawnEnded:
			limit := ""
			if result.ExpiresAt != "" {
				limit = " expires_at=" + result.ExpiresAt
			}
			return attempt, fail(exitWaitLost, "spawn request %s before it landed %s: %s%s (operation %s)", result.Status, attempt, firstNonempty(result.Reason, result.Result.Reason, "no reason recorded"), limit, id)
		case spawnUnresolved:
			return attempt, fail(exitStateConflict, "spawn ownership is unresolved %s; inspect operation %s and its executing node before any retry", attempt, id)
		case spawnParked:
			told.parked(opts.Notice, id, firstNonempty(result.Reason, result.Result.Reason), result.NextCheckAt)
		case spawnUnknown:
			told.unknown(opts.Notice, result.Status)
		}
		lastKind, lastStatus = kind, result.Status
		if err = c.sleepWithin(ctx, poll, deadline); err != nil {
			return attempt, err
		}
	}
}

// sleepWithin sleeps d, or until deadline when that comes first (a zero
// deadline is none). Only the caller's own context ends it with an error.
func (c *hubHTTP) sleepWithin(ctx context.Context, d time.Duration, deadline time.Time) error {
	if !deadline.IsZero() {
		d = min(d, max(time.Until(deadline), 0))
	}
	return sleepContext(ctx, d)
}

// spawnRefusalRC is the exit code of a refused request: the hub's refusal
// code, or the placement failure when the hub recorded none (zero would read
// as success).
func spawnRefusalRC(rc int) int {
	if rc <= 0 {
		return exitPlacement
	}
	return rc
}

func isNumeric(s string) bool {
	_, e := strconv.ParseUint(s, 10, 64)
	return e == nil
}
