package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	if len(request.StartConditions) != 0 {
		status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/features", nil, 5*time.Second)
		if err != nil {
			return Accepted{}, fail(exitConfig, "unable to verify hub support for --after start conditions: %v", err)
		}
		if status != http.StatusOK {
			return Accepted{}, fail(exitConfig, "unable to verify hub support for --after start conditions (HTTP %d)", status)
		}
		var features struct {
			Features []string `json:"features"`
		}
		if json.Unmarshal(raw, &features) != nil {
			return Accepted{}, fail(exitConfig, "hub returned an unreadable spawn feature response")
		}
		if !slices.Contains(features.Features, "start_conditions_v1") {
			return Accepted{}, fail(exitUsage, "hub does not support --after start conditions; upgrade the hub before submitting")
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
		// RetryAfter is the seconds until the hold frees on its own; the hub
		// sends it only for a reviewer held by the landing grace.
		RetryAfter int `json:"retry_after_s"`
	}
	if json.Unmarshal(raw, &conflict) != nil || conflict.Error == "" {
		return "job id already claimed; inspect its existing accepted request"
	}
	message := "hub refused spawn: " + conflict.Error
	if conflict.Request != "" || conflict.Job != "" {
		message += fmt.Sprintf(" (held by request=%s job=%s host=%s pane=%s)", conflict.Request, conflict.Job, firstNonempty(conflict.Host, "unknown"), firstNonempty(conflict.Pane, "unknown"))
	}
	if conflict.RetryAfter > 0 {
		message += fmt.Sprintf(" retry_after_s=%d", conflict.RetryAfter)
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
			WaitSpawn(context.Context, string) (SpawnAttempt, error)
		})
		if !ok {
			return fail(exitConfig, "hub transport cannot wait for spawn status; upgrade the configured transport")
		}
		attempt, err := waiter.WaitSpawn(ctx, accepted.Request)
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

	if s.CWD == "" || s.Prompt == "" || !validID(s.Job) || !validID(s.Label) || !isNumeric(s.Task) && s.ParentJob == "" {
		return Submission{}, fail(exitUsage, "spawn requires -c CWD -p BRIEF -l LABEL --task TASK_ID (or --parent-job JOB)")
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
	cwd, err := filepath.Abs(expand(s.CWD))
	if err != nil {
		return Submission{}, err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return Submission{}, fail(exitUsage, "cwd is unreadable; pass -c with an existing checkout")
	}
	s.CWD = cwd
	brief, err := a.readBrief(ctx, s.Prompt, hubMaxBriefBytes)
	if _, doc, _ := briefDocKey(s.Prompt); doc && err != nil {
		return Submission{}, err
	}
	if err != nil || len(brief) > hubMaxBriefBytes || !utf8.Valid(brief) || strings.TrimSpace(string(brief)) == "" {
		return Submission{}, fail(exitUsage, "pass -p with a nonempty UTF-8 brief of at most 64 KiB")
	}
	repo := a.localRepo(ctx, cwd)
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
		repo.wt = s.Job
		if len(repo.wt) > 110 {
			sum := sha256.Sum256([]byte(s.Job))
			repo.wt = repo.wt[:40] + "-" + hex.EncodeToString(sum[:])
		}
		repo.wt += "-checkout"
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
	if repo.wt != "" {
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
	request := Submission{RequestID: id, Brief: string(brief), Repository: Repository{Name: repo.name, Origin: checkout.Origin, Relative: repo.rel, Worktree: repo.wt, LocalRoot: repo.localRoot, Checkout: checkout}, OriginMachine: a.machine(), DeadlineSeconds: 600, E6Arm: s.E6Arm, Args: submissionArgs(submissionSpawn), StartConditions: conditions}
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
	if !regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(request.RequestID) || !validID(s.Job) || !validID(s.Label) || s.DryRun || s.Delegated || s.Wait || s.Prompt == "" || !filepath.IsAbs(s.CWD) || len(request.Brief) > hubMaxBriefBytes || !utf8.ValidString(request.Brief) || strings.TrimSpace(request.Brief) == "" || request.DeadlineSeconds < 1 || request.DeadlineSeconds > 3600 {
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
	if !validID(repo.Name) || repo.Worktree != "" && !validID(repo.Worktree) || !filepath.IsAbs(repo.LocalRoot) || !cloneURL.MatchString(repo.Origin) || credentialedURL(repo.Origin) || strings.ContainsAny(repo.Origin+repo.LocalRoot, "\x00\r\n") {
		return s, fail(exitUsage, "invalid repository descriptor; use a credential-free origin and a safe checkout name")
	}
	if repo.Relative != "" {
		return s, fail(exitPlacement, "hub_subdirectory_unsupported: use the checkout root before submitting")
	}
	return s, nil
}

func submissionArgs(s Spawn) []string {
	var args []string
	for _, field := range []struct{ flag, value string }{
		{"-c", s.CWD}, {"-p", s.Prompt}, {"-m", s.Profile}, {"-w", s.Workspace}, {"-l", s.Label}, {"--job", s.Job}, {"--t", s.T}, {"--owner", s.Owner}, {"--effort", s.Effort}, {"--role", s.Role}, {"--lane", s.Lane}, {"--parent", s.Parent}, {"--task", s.Task}, {"--parent-job", s.ParentJob}, {"--host", s.Host}, {"--report-relative", s.ReportRelative}, {"-L", s.ExecutionLane}, {"--override-reason", s.OverrideReason}, {"--containment", s.Containment}, {"--tags", s.Tags}, {"--exclude-host", s.ExcludeHosts}, {"--exclude-pool", s.ExcludePools}, {"--urgency", s.Urgency}, {"--author-family", s.AuthorFamily}, {"--builder-job", s.BuilderJob}, {"--review-pass", s.ReviewPass}, {"--grade", s.Grade}, {"--operator-request", s.OperatorRequest}, {"--requested-by", s.RequestedBy}, {"--purpose", s.Purpose},
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

// SpawnStatus reads a hub spawn operation's status once (GET
// /v1/spawns/<id>): accepted, dispatching, succeeded, failed, retryable,
// reconciling or deadline_exceeded. An id that is not a plain id, a
// transport failure, a non-200 answer or a body with no status is an error.
func (c *hubHTTP) SpawnStatus(ctx context.Context, id string) (string, error) {
	if !validID(id) {
		return "", fail(exitUsage, "invalid spawn operation id")
	}
	status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/"+id, nil, 5*time.Second)
	if err != nil {
		return "", err
	}
	var result struct {
		Status string `json:"status"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &result) != nil || result.Status == "" {
		return "", fail(exitStateConflict, "spawn status unreadable; inspect operation %s", id)
	}
	return result.Status, nil
}

// WaitSpawn observes durable spawn state before the existing --wait completion
// path. Admission/node refusal codes remain visible to synchronous callers.
func (c *hubHTTP) WaitSpawn(ctx context.Context, id string) (SpawnAttempt, error) {
	poll := c.statusPoll
	if poll == 0 {
		poll = 2 * time.Second
	}
	failures := 0
	for {
		status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/"+id, nil, 5*time.Second)
		if err != nil || status >= http.StatusInternalServerError {
			if failures++; failures > spawnStatusFailureLimit {
				return SpawnAttempt{}, fail(exitStateConflict, "spawn status unavailable; inspect operation %s and retry its saved request without choosing a new host", id)
			}
			if err = sleepContext(ctx, poll); err != nil {
				return SpawnAttempt{}, err
			}
			continue
		}
		failures = 0
		var result struct {
			Status string `json:"status"`
			Result struct {
				SpawnAttempt
				RC     int    `json:"rc"`
				Reason string `json:"reason"`
			} `json:"result"`
		}
		if status != 200 || json.Unmarshal(raw, &result) != nil {
			return SpawnAttempt{}, fail(exitStateConflict, "spawn status unreadable; inspect operation %s", id)
		}
		attempt := result.Result.SpawnAttempt
		switch result.Status {
		case "succeeded":
			return attempt, nil
		case "failed", "retryable":
			return attempt, fail(result.Result.RC, "spawn refused %s: %s (operation %s)", attempt, result.Result.Reason, id)
		case "deadline_exceeded":
			return attempt, fail(exitStateConflict, "spawn ownership is unresolved %s; inspect operation %s and its executing node before any retry", attempt, id)
		case "accepted", "bound", "dispatching", "reconciling":
		default:
			return attempt, fail(exitStateConflict, "unknown spawn state; inspect operation %s", id)
		}
		if err = sleepContext(ctx, poll); err != nil {
			return attempt, err
		}
	}
}

func isNumeric(s string) bool {
	_, e := strconv.ParseUint(s, 10, 64)
	return e == nil
}
