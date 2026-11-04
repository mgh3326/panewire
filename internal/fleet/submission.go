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
	RequestID       string     `json:"request_id"`
	Args            []string   `json:"args"`
	Brief           string     `json:"brief"`
	Repository      Repository `json:"repository"`
	OriginMachine   string     `json:"origin_machine"`
	DeadlineSeconds int        `json:"deadline_seconds"`
}

type Accepted struct{ Job, Request string }

type SpawnSubmitter interface {
	Submit(context.Context, Submission) (Accepted, error)
}

func (c *hubHTTP) Submit(ctx context.Context, request Submission) (Accepted, error) {
	status, raw, err := c.do(ctx, http.MethodPost, "/v1/spawns", request, 15*time.Second)
	if err != nil {
		return Accepted{}, err
	}
	if status == http.StatusConflict {
		return Accepted{}, fail(exitDuplicate, "job id already claimed; inspect its existing accepted request")
	}
	var response struct {
		Job     string `json:"job_id"`
		Request string `json:"request_id"`
		Error   string `json:"error"`
		Fix     string `json:"fix"`
	}
	if json.Unmarshal(raw, &response) != nil || status != http.StatusAccepted || !validID(response.Job) || response.Request != request.RequestID {
		return Accepted{}, fail(exitConfig, "hub did not accept spawn (HTTP %d, %s); fix: %s", status, response.Error, firstNonempty(response.Fix, "inspect the hub operation and restore its configuration, then replay the saved request"))
	}
	return Accepted{Job: response.Job, Request: response.Request}, nil
}

func (a *App) SubmitSpawn(ctx context.Context, s Spawn) error {
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
	if s.ParentJob != "" && s.Owner == "" {
		parent, err := a.resolveParent(ctx, s.ParentJob)
		if err != nil {
			return Submission{}, fail(exitStateConflict, "parent metadata unreadable; restore handoffkeep access before retrying")
		}
		s.Owner = parent.Owner
	}

	if s.CWD == "" || s.Prompt == "" || !validID(s.Job) || !validID(s.Label) || !isNumeric(s.Task) && s.ParentJob == "" {
		return Submission{}, fail(exitUsage, "spawn requires -c CWD -p BRIEF -l LABEL --task TASK_ID (or --parent-job JOB)")
	}
	if _, err := hubArgs(a.Jobs, s); err != nil {
		return Submission{}, fail(exitUsage, "%v; remove the unsupported flag before submitting through the hub", err)
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
		s.Role = "worker"
	}
	if s.Profile != "auto" || s.Host != "auto" {
		if strings.TrimSpace(s.OverrideReason) == "" {
			return Submission{}, fail(exitUsage, "explicit -m or --host requires --override-reason")
		}
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
	brief, err := os.ReadFile(expand(s.Prompt))
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
		repo.wt = s.Job
		if len(repo.wt) > 110 {
			sum := sha256.Sum256([]byte(s.Job))
			repo.wt = repo.wt[:40] + "-" + hex.EncodeToString(sum[:])
		}
		repo.wt += "-checkout"
	}
	var checkout *HubCheckout
	if repo.wt != "" {
		var why string
		checkout, why = a.hubCheckout(ctx, repo)
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
	request := Submission{RequestID: id, Brief: string(brief), Repository: Repository{Name: repo.name, Origin: checkout.Origin, Relative: repo.rel, Worktree: repo.wt, LocalRoot: repo.localRoot, Checkout: checkout}, OriginMachine: a.machine(), DeadlineSeconds: 600, Args: submissionArgs(s)}
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
	if !regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(request.RequestID) || !validID(s.Job) || !validID(s.Label) || s.DryRun || s.Delegated || s.Wait || s.Prompt == "" || !filepath.IsAbs(s.CWD) || len(request.Brief) > hubMaxBriefBytes || !utf8.ValidString(request.Brief) || strings.TrimSpace(request.Brief) == "" || request.DeadlineSeconds < 1 || request.DeadlineSeconds > 3600 {
		return s, fail(exitUsage, "invalid spawn request; rebuild it with panewire spawn")
	}
	if !isNumeric(s.Task) && s.ParentJob == "" {
		return s, fail(exitUsage, "supply --task TASK_ID or --parent-job JOB")
	}
	if s.Task != "" && s.ParentJob != "" {
		return s, fail(exitUsage, "--task and --parent-job are exclusive")
	}
	if s.Role == "builder" {
		if s.Lane == "" || s.Parent == "" || s.Owner != "" || s.Task == "" {
			return s, fail(exitUsage, "builder requires --lane --parent --task without --owner")
		}
	} else if s.Owner == "" {
		return s, fail(exitUsage, "supply --owner LANE so completion can be delivered")
	}
	if !slices.Contains([]string{"T0", "T1", "T2", "T3"}, s.T) || !slices.Contains([]string{"worker", "builder", "reviewer", "tester", "counsel"}, s.Role) {
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
		{"-c", s.CWD}, {"-p", s.Prompt}, {"-m", s.Profile}, {"-w", s.Workspace}, {"-l", s.Label}, {"--job", s.Job}, {"--t", s.T}, {"--owner", s.Owner}, {"--effort", s.Effort}, {"--role", s.Role}, {"--lane", s.Lane}, {"--parent", s.Parent}, {"--task", s.Task}, {"--parent-job", s.ParentJob}, {"--host", s.Host}, {"--report-relative", s.ReportRelative}, {"-L", s.ExecutionLane}, {"--override-reason", s.OverrideReason}, {"--tags", s.Tags}, {"--exclude-host", s.ExcludeHosts}, {"--urgency", s.Urgency}, {"--builder-family", s.BuilderFamily}, {"--builder-job", s.BuilderJob}, {"--routing-role", s.RoutingRole}, {"--grade", s.Grade}, {"--operator-request", s.OperatorRequest}, {"--requested-by", s.RequestedBy}, {"--purpose", s.Purpose}, {"--lane-defer", s.LaneDefer},
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

// WaitSpawn observes durable spawn state before the existing --wait completion
// path. Admission/node refusal codes remain visible to synchronous callers.
func (c *hubHTTP) WaitSpawn(ctx context.Context, id string) (SpawnAttempt, error) {
	poll := c.statusPoll
	if poll == 0 {
		poll = 2 * time.Second
	}
	failures := 0
	for {
		status, raw, err := c.do(ctx, http.MethodGet, "/v1/spawns/"+id, nil, 5*time.Second)
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
		case "reconciling", "deadline_exceeded":
			return attempt, fail(exitStateConflict, "spawn ownership is unresolved %s; inspect operation %s and its executing node before any retry", attempt, id)
		case "accepted", "dispatching":
		default:
			return attempt, fail(exitStateConflict, "unknown spawn state; inspect operation %s", id)
		}
		if err = sleepContext(ctx, poll); err != nil {
			return attempt, err
		}
	}
}
