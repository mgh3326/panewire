package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/mgh3326/panewire/internal/spawnargs"
)

// Consult request planning (hk 4552 phase 1, operator decision 250). The hub
// owns the seat choice and the run: a consult asks one grade above the task's
// grade, in another family than the author's, and its report must carry the
// four sections in consultPreamble. The design is docs/consult-on-request.md.

// consultSections are the report headings the hub checks before it records an
// answer. mergewake parses Verdict and the Findings rows; the other two are
// passed through by key.
var consultSections = []string{"## Verdict", "## Root cause", "## Design", "## Findings"}

// consultVerdicts is the closed set of one-line verdicts.
var consultVerdicts = map[string]bool{"apply": true, "cut": true, "split": true, "escalate": true}

// consultPreamble is the fixed part of every consult brief. It changes no
// code: the consult answers a question document and writes the four sections.
const consultPreamble = `You are a consult. Read the question document below and answer it. Do not change any code, commit or push.

Write the answer as your job report with exactly these sections:

## Verdict
One line, one of: ` + "`apply`" + `, ` + "`cut`" + `, ` + "`split`" + `, ` + "`escalate`" + `.

## Root cause
Prose: the first wrong point the findings share.

## Design
The one design that closes the findings.

## Findings
A table with one row for every finding id in the question:

| finding id | action (fix/dismiss/defer) | reason |

`

// ConsultPlanInput is one request to resolve a consult seat and build the
// spawn submission for it.
type ConsultPlanInput struct {
	Task string
	// Question is the hk:doc/KEY document that carries the question.
	Question string
	// AuthorJob names the author's job; its record and stored submission are
	// the family and checkout evidence. AuthorFamily overrides the family.
	AuthorJob    string
	AuthorFamily string
	PR           string
	Head         string
	// Requester is the caller's lane or tool id. Owner is the lane the spawn
	// belongs to (the requester lane, or the hub's tool-consult lane).
	Requester string
	Owner     string
	// T is the task scale used when the task title has no (grade X, Tn)
	// marker; empty means T2.
	T string
	// Repository is the author job's stored repository evidence, supplied by
	// the hub from its own spawn operation. A consult changes no code, but it
	// still runs in a checkout of the PR head.
	Repository *Repository
	// ExcludeProfiles names seats a respawn must not reuse.
	ExcludeProfiles []string
}

// ConsultPlan is the resolved seat and the spawn submission for one consult.
type ConsultPlan struct {
	Seat    SeatChoice
	Job     string
	Request Submission
}

// PlanConsult resolves the consult seat and builds the one spawn submission
// for it. It writes nothing; the hub claims the request and records the
// consult row.
func (a *App) PlanConsult(ctx context.Context, in ConsultPlanInput) (ConsultPlan, error) {
	if !isNumeric(in.Task) {
		return ConsultPlan{}, fail(exitUsage, "consult task must be a task id")
	}
	if in.Owner == "" {
		return ConsultPlan{}, fail(exitUsage, "consult owner lane is required")
	}
	questionKey, ok := strings.CutPrefix(in.Question, "hk:doc/")
	if !ok || !spawnargs.ValidDocKey(questionKey) {
		return ConsultPlan{}, fail(exitUsage, "question must be hk:doc/KEY")
	}
	if in.Repository == nil || in.Repository.Origin == "" || in.Repository.Name == "" || !path.IsAbs(in.Repository.LocalRoot) {
		return ConsultPlan{}, fail(exitConfig, "consult repository evidence is unavailable")
	}

	scale := firstNonempty(strings.ToUpper(in.T), "T2")
	askerGrade := ""
	if taskGrade, markerScale, err := TaskMarker(ctx, in.Task); err == nil {
		askerGrade = taskGrade
		if markerScale != "" {
			scale = markerScale
		}
	}

	authorFamily := in.AuthorFamily
	if authorFamily == "" && in.AuthorJob != "" {
		store, err := a.jobStore()
		if err != nil {
			return ConsultPlan{}, fail(exitConfig, "author family unreadable: %v", err)
		}
		record, err := store.Get(ctx, in.AuthorJob)
		if err != nil {
			return ConsultPlan{}, fail(exitConfig, "author job %s unreadable: %v", in.AuthorJob, err)
		}
		authorFamily = a.jobBuilderFamily(jobFromRecord(in.AuthorJob, record))
	}
	if authorFamily == "" {
		return ConsultPlan{}, fail(exitUsage, "author family is required (--author-family or --author-job)")
	}

	lane := ""
	// A design-session lane asks at the SS floor; only a lane name can say so.
	// The requester is a lane when it carries no slash, and the hub validates
	// the name before planning.
	if in.Requester != "" && !strings.Contains(in.Requester, "/") {
		lane = in.Requester
	}
	seat, err := a.ConsultSeatExcluding(ctx, askerGrade, lane, authorFamily, scale, in.ExcludeProfiles)
	if err != nil {
		return ConsultPlan{}, err
	}
	// The seat is pinned below, so selection never runs where the job's record
	// is written. Carry the stamp the planner selected with (hk 4875).
	stamp, ok := consultStampFromRecord(seat.Consult)
	if !ok {
		return ConsultPlan{}, fail(exitConfig, "consult seat selection wrote no consult stamp")
	}
	if consultStampValue(stamp) == "" {
		// The wire rule cannot carry this asker's family (over-long or a control
		// character); the consult still runs, and its record lacks the stamp.
		a.warn("consult stamp for task %s cannot be carried (author family %q); the record will lack it", in.Task, stamp["asker_family"])
	}

	head, fetch, err := consultCheckoutRef(in)
	if err != nil {
		return ConsultPlan{}, err
	}
	suffix, err := consultSuffix()
	if err != nil {
		return ConsultPlan{}, err
	}
	job := "consult-" + in.Task + "-" + suffix
	dir := job + "-checkout"
	cwd := path.Join(in.Repository.LocalRoot, dir)

	brief, err := a.consultBrief(ctx, in, questionKey)
	if err != nil {
		return ConsultPlan{}, err
	}

	repo := *in.Repository
	repo.Worktree = dir
	repo.Checkout = &HubCheckout{Origin: in.Repository.Origin, Mode: "detached", Sha: head, Fetch: fetch, Dir: dir}

	reason := fmt.Sprintf("consult seat %s/%s for task %s", seat.Profile, firstNonempty(seat.Effort, "default"), in.Task)
	s := Spawn{
		CWD:            cwd,
		Prompt:         "/consult-brief",
		Profile:        seat.Profile,
		Effort:         seat.Effort,
		Role:           "consult",
		Task:           in.Task,
		T:              scale,
		Owner:          in.Owner,
		Job:            job,
		Label:          job,
		Host:           "auto",
		AuthorFamily:   authorFamily,
		OverrideReason: reason,
		Containment:    "hk:" + in.Task,
		ReportRelative: job + "/report.md",
		Grade:          askerGrade,
		ConsultStamp:   stamp,
	}
	requestID, err := newRequestID()
	if err != nil {
		return ConsultPlan{}, err
	}
	request := Submission{
		RequestID: requestID,
		Args:      submissionArgs(s),
		Brief:     brief,
		Repository: Repository{
			Name:      repo.Name,
			Origin:    repo.Origin,
			Relative:  repo.Relative,
			Worktree:  repo.Worktree,
			LocalRoot: repo.LocalRoot,
			Checkout:  repo.Checkout,
		},
		OriginMachine:   "hub",
		DeadlineSeconds: 600,
	}
	if _, err := ValidateSubmission(request); err != nil {
		return ConsultPlan{}, err
	}
	return ConsultPlan{Seat: seat, Job: job, Request: request}, nil
}

// consultCheckoutRef picks the commit and the origin ref the node fetches for
// the consult checkout: the requested head and ref, else the author job's
// pinned checkout, else its branch.
func consultCheckoutRef(in ConsultPlanInput) (sha, fetch string, err error) {
	repo := in.Repository
	if repo.Checkout != nil {
		sha, fetch = repo.Checkout.Sha, repo.Checkout.Fetch
	}
	if in.Head != "" {
		if !hubShaRule.MatchString(in.Head) {
			return "", "", fail(exitUsage, "head must be a 40-character commit id")
		}
		sha = in.Head
	}
	if in.PR != "" {
		number := in.PR[strings.LastIndexByte(in.PR, '#')+1:]
		fetch = "pull/" + number + "/head"
	} else if fetch == "" && repo.Checkout != nil && repo.Checkout.Branch != "" {
		fetch = "refs/heads/" + repo.Checkout.Branch
	}
	if sha == "" {
		return "", "", fail(exitUsage, "consult head is required (--head or the author job's checkout)")
	}
	if fetch == "" {
		return "", "", fail(exitUsage, "consult head ref is unknown; pass --pr REPO#N")
	}
	return sha, fetch, nil
}

// consultSuffix is the unique tail of a consult job id.
func consultSuffix() (string, error) {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// consultBrief is the fixed preamble plus the question document. The brief
// stays inside the hub's brief bound.
func (a *App) consultBrief(ctx context.Context, in ConsultPlanInput, questionKey string) (string, error) {
	doc, err := hkGetDocument(ctx, questionKey)
	if err != nil {
		return "", fail(exitConfig, "question document %s unreadable: %v", questionKey, hkErrText(err))
	}
	brief := consultPreamble + "Question document: hk:doc/" + questionKey + "\n\n" + doc.Body
	if len([]byte(brief)) > hubMaxBriefBytes {
		return "", fail(exitUsage, "question document exceeds the consult brief bound")
	}
	return brief, nil
}

// ConsultAnswerProblem reports why a completed consult report does not carry
// the required answer shape, "" when it does. The findings section must hold
// the table header mergewake parses.
func ConsultAnswerProblem(body string) string {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	index := map[string]int{}
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		for _, section := range consultSections {
			if trimmed == section {
				index[section] = i
			}
		}
	}
	for _, section := range consultSections {
		if _, ok := index[section]; !ok {
			return "missing section " + section
		}
	}
	verdict := ""
	for _, line := range lines[index["## Verdict"]+1:] {
		line = strings.TrimSpace(strings.Trim(strings.TrimSpace(line), "*`_"))
		if line != "" {
			verdict = strings.ToLower(line)
			break
		}
	}
	if !consultVerdicts[verdict] {
		return "## Verdict must be one line: apply, cut, split or escalate"
	}
	findings := lines[index["## Findings"]:]
	table := false
	for _, line := range findings {
		if strings.HasPrefix(strings.TrimSpace(line), "|") {
			table = true
			break
		}
	}
	if !table {
		return "## Findings must contain the finding table"
	}
	return ""
}

// TaskBinding is the job a task is currently bound to, "" when none.
func TaskBinding(ctx context.Context, task string) (string, error) {
	t, err := hkGetTask(ctx, task)
	if err != nil {
		return "", err
	}
	return t.Refs.JobID, nil
}

// CheckConsultAnswerDocument reads a stored consult report and reports why it
// does not carry the answer shape, "" when it does. The hub uses it before it
// records answer_doc.
func CheckConsultAnswerDocument(ctx context.Context, key string) (string, error) {
	doc, err := hkGetDocument(ctx, key)
	if err != nil {
		return "", err
	}
	return ConsultAnswerProblem(doc.Body), nil
}
