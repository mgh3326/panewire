package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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
	}
	if json.Unmarshal(raw, &response) != nil || status != http.StatusAccepted || !validID(response.Job) || response.Request == "" {
		return Accepted{}, fail(exitConfig, "hub did not accept spawn (HTTP %d, %s)", status, response.Error)
	}
	return Accepted{Job: response.Job, Request: response.Request}, nil
}

func (a *App) SubmitSpawn(ctx context.Context, s Spawn) error {
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
	accepted, err := submitter.Submit(ctx, request)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.Out, "ACCEPTED job=%s request=%s\n", accepted.Job, accepted.Request)
	return nil
}

func (a *App) PrepareSubmission(ctx context.Context, s Spawn) (Submission, error) {
	if s.CWD == "" || s.Prompt == "" || !validID(s.Job) || !validID(s.Label) || !isNumeric(s.Task) && s.ParentJob == "" {
		return Submission{}, fail(exitUsage, "spawn requires -c CWD -p BRIEF -l LABEL --task TASK_ID (or --parent-job JOB)")
	}
	if s.Wait {
		return Submission{}, fail(exitUsage, "spawn acceptance is asynchronous; use panewire jobs to inspect completion instead of --wait")
	}
	if s.Host == "local" {
		s.Host = a.machine()
		if s.Host == "local" {
			return Submission{}, fail(exitConfig, "set local.machine_id before requesting the submitting host with --host local")
		}
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
	request := Submission{RequestID: id, Brief: string(brief), Repository: Repository{Name: repo.name, Origin: repo.url, Relative: repo.rel, Worktree: repo.wt, LocalRoot: repo.localRoot, Checkout: checkout}, OriginMachine: a.machine(), DeadlineSeconds: 600, Args: submissionArgs(s)}
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
	if !validID(s.Job) || !validID(s.Label) || s.DryRun || s.Delegated || s.Wait || s.Prompt == "" || !filepath.IsAbs(s.CWD) || len(request.Brief) > hubMaxBriefBytes || !utf8.ValidString(request.Brief) || strings.TrimSpace(request.Brief) == "" || request.DeadlineSeconds < 1 || request.DeadlineSeconds > 3600 {
		return s, fail(exitUsage, "invalid spawn request; rebuild it with panewire spawn")
	}
	if !isNumeric(s.Task) && s.ParentJob == "" {
		return s, fail(exitUsage, "supply --task TASK_ID or --parent-job JOB")
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
	if repo.Relative != "" && !hubReportRelativeOK(repo.Relative) {
		return s, fail(exitUsage, "invalid repository subdirectory")
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
