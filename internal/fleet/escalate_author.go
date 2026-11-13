package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// `panewire escalate-author` is the hub's half of the review-round rule
// (hk:doc policy/model-families §The escalation ladder, operator decisions 137,
// 146 and 148): mergewake calls it at round 5, the CLI resolves the consult
// seat and the next ladder rung, and the hub starts the author again on the
// same worktree at that rung and records one info event for the design lane.
//
// The wire (agreed with mergewake-design, hk 4264):
//
//	panewire escalate-author --task ID --job JOB --pr owner/repo#N --round N --findings-file PATH
//
// rc 0 prints the new job and rung; rc 2 is a refusal (no_rung, quota_refused,
// or another named code) and does nothing to the worker; any other rc is an
// error mergewake logs and continues past.

const (
	// escalateAuthorFindingsMax bounds the findings JSON the CLI reads and the
	// hub accepts. mergewake's file is a small JSON array of thread summaries.
	escalateAuthorFindingsMax = 32 << 10
	// escalateAuthorTimeout is the hub call's deadline. The hub closes the old
	// pane and waits for the successor's placement inside it.
	escalateAuthorTimeout = 4 * time.Minute
)

var escalateAuthorPRPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+#[1-9][0-9]*$`)

// EscalateAuthorRequest is the CLI's request to the hub. From* name the rung
// the job record showed, so the hub can refuse a stale request; Profile and
// Effort are the rung to start the author at (empty with Step "" for no_rung).
type EscalateAuthorRequest struct {
	Task        string `json:"task"`
	Job         string `json:"job"`
	PR          string `json:"pr"`
	Round       int    `json:"round"`
	Findings    string `json:"findings"`
	Grade       string `json:"grade"`
	FromProfile string `json:"from_profile"`
	FromEffort  string `json:"from_effort"`
	Profile     string `json:"profile,omitempty"`
	Effort      string `json:"effort,omitempty"`
	Step        string `json:"step,omitempty"`
	ConsultSeat string `json:"consult_seat,omitempty"`
}

// EscalateAuthorResult is the hub's answer: a successor job and rung, or a
// refusal code with the reason.
type EscalateAuthorResult struct {
	Job    string `json:"job,omitempty"`
	Rung   string `json:"rung,omitempty"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// EscalateAuthorer is the hub transport surface the command needs.
type EscalateAuthorer interface {
	EscalateAuthor(context.Context, EscalateAuthorRequest) (EscalateAuthorResult, error)
}

// EscalateAuthor posts the request to the hub. It is not retried: the hub
// closes a pane and starts a job, and the operation is idempotent by
// (job, round) on its own, so a lost answer is re-run by the caller's next
// round rather than replayed here.
func (c *hubHTTP) EscalateAuthor(ctx context.Context, request EscalateAuthorRequest) (EscalateAuthorResult, error) {
	status, data, err := c.do(ctx, http.MethodPost, "/v1/jobs/escalate-author", request, escalateAuthorTimeout)
	if err != nil {
		return EscalateAuthorResult{}, err
	}
	var result EscalateAuthorResult
	if json.Unmarshal(data, &result) != nil {
		return EscalateAuthorResult{}, fail(exitConfig, "hub escalate-author answer unreadable (HTTP %d)", status)
	}
	if status != http.StatusOK {
		return result, fail(exitConfig, "hub refused escalate-author (HTTP %d, %s): %s", status, result.Code, result.Reason)
	}
	return result, nil
}

func (a *App) escalateAuthorCmd(ctx context.Context, args []string) error {
	var task, job, pr, findingsPath string
	var round int
	f := flags("escalate-author")
	f.StringVar(&task, "task", "", "task id")
	f.StringVar(&job, "job", "", "the author job to replace")
	f.StringVar(&pr, "pr", "", "owner/repo#number")
	f.IntVar(&round, "round", 0, "the review round that ended without a pass")
	f.StringVar(&findingsPath, "findings-file", "", "JSON array of findings")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "escalate-author takes no positional arguments")
	}
	if !isNumeric(task) {
		return fail(exitUsage, "escalate-author needs --task TASK_ID")
	}
	if !validID(job) {
		return fail(exitUsage, "escalate-author needs --job JOB")
	}
	if !escalateAuthorPRPattern.MatchString(pr) {
		return fail(exitUsage, "escalate-author needs --pr owner/repo#N")
	}
	if round < 1 {
		return fail(exitUsage, "escalate-author needs --round N (N >= 1)")
	}
	findings, err := readEscalateFindings(findingsPath)
	if err != nil {
		return err
	}
	record, err := a.escalateAuthorJobRecord(ctx, task, job)
	if err != nil {
		return err
	}
	grade, scale, err := TaskMarker(ctx, task)
	if err != nil {
		return err
	}
	from := LadderPick{
		Profile: str(record, "profile"),
		Effort:  str(record, "effort"),
		Pool:    str(record, "pool"),
		Family:  str(record, "family"),
		ModelID: str(record, "model"),
	}
	if from.Profile == "" {
		return fail(exitConfig, "job %s record names no profile; restore its hk record before escalating", job)
	}
	request := EscalateAuthorRequest{
		Task: task, Job: job, PR: pr, Round: round, Findings: findings, Grade: grade,
		FromProfile: from.Profile, FromEffort: from.Effort,
	}
	step, ok, err := a.NextLadderRung(ctx, from, grade, scale)
	if err != nil {
		if exitCode(err) == exitConfig {
			return err
		}
		return fail(exitUsage, "quota_refused: %v", err)
	}
	if ok {
		request.Profile, request.Effort, request.Step = step.Pick.Profile, step.Pick.Effort, step.Kind
	}
	if consult, err := a.ConsultSeat(ctx, grade, str(record, "owner_lane"), from.Family, scale); err == nil {
		request.ConsultSeat = consult.Profile + "@" + firstNonempty(consult.Effort, "default")
	} else {
		a.warn("consult seat not resolved: %v", err)
	}
	hub, err := a.hub()
	if err != nil {
		return err
	}
	escalator, ok := hub.(EscalateAuthorer)
	if !ok {
		return fail(exitConfig, "hub transport cannot escalate an author; upgrade the configured transport")
	}
	result, err := escalator.EscalateAuthor(ctx, request)
	if err != nil {
		return err
	}
	if result.Code != "" {
		return fail(exitUsage, "%s: %s", result.Code, result.Reason)
	}
	if !validID(result.Job) {
		return fail(exitConfig, "hub escalate-author returned no job")
	}
	fmt.Fprintf(a.Out, "ESCALATED job=%s rung=%s\n", result.Job, result.Rung)
	return nil
}

// escalateAuthorJobRecord reads the author's live job record and refuses a job
// that is not the task's, not running, or bound to a different task.
func (a *App) escalateAuthorJobRecord(ctx context.Context, task, job string) (map[string]any, error) {
	store, err := a.jobStore()
	if err != nil {
		return nil, err
	}
	record, err := store.Get(ctx, job)
	if err != nil {
		return nil, fail(exitUsage, "job %s has no readable record: %v", job, err)
	}
	switch state, _ := record["state"].(string); state {
	case "claimed", "spawned":
	default:
		return nil, fail(exitUsage, "job %s is not live (state %q); nothing to escalate", job, state)
	}
	if bound := scalar(record["task_id"]); bound != "" && bound != task {
		return nil, fail(exitUsage, "job %s is bound to task %s, not %s", job, bound, task)
	}
	return record, nil
}

// readEscalateFindings reads mergewake's findings file: a JSON array of
// finding objects, bounded and valid UTF-8. An unreadable or malformed file
// refuses before anything reaches the hub.
func readEscalateFindings(path string) (string, error) {
	if path == "" {
		return "", fail(exitUsage, "escalate-author needs --findings-file PATH")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fail(exitUsage, "findings file unreadable: %v", err)
	}
	if len(data) > escalateAuthorFindingsMax {
		return "", fail(exitUsage, "findings file is %d bytes; the limit is %d", len(data), escalateAuthorFindingsMax)
	}
	var findings []json.RawMessage
	if json.Unmarshal(data, &findings) != nil {
		return "", fail(exitUsage, "findings file is not a JSON array")
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "", fail(exitUsage, "findings file is empty")
	}
	return text, nil
}
