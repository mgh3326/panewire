package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Ladder-step rows (docs/reps.md §Ladder steps). A ladder step is a pinned
// re-spawn that replaces a rung on a PR that is still open: a --parent-job
// spawn whose override reason starts "ladder step". When that spawn lands, the
// replaced rung gets one completed = 0 row. The row has its own kind and origin
// id, so it never collides with the replaced job's outcome row, and a later
// merge of the PR leaves it alone.

// ladderStepPrefix opens the override reason a ladder-step re-spawn carries
// (rstack wake and fleet-review).
const ladderStepPrefix = "ladder step "

// ladderStepKind is notes.kind of a ladder-step row.
const ladderStepKind = "ladder_step"

// isLadderStep reports a spawn that replaces a rung: a re-spawn on the parent
// job it names, under a ladder-step override reason.
func (s Spawn) isLadderStep() bool {
	return s.ParentJob != "" && strings.HasPrefix(strings.TrimSpace(s.OverrideReason), ladderStepPrefix)
}

// recordLadderStep writes the ladder-step row for a ladder-step spawn that has
// just landed. The caller calls it once job.spawned is committed and logs an
// error; the spawn itself is kept either way. A retry of the same step carries
// the same origin id, so hk keeps the first row (AddOutcome reports it as not
// recorded).
func (a *App) recordLadderStep(ctx context.Context, s Spawn, launch Launch, seat Seat) error {
	if !s.isLadderStep() || s.replacedRung == nil {
		return nil
	}
	old := *s.replacedRung
	pr, _ := joinedPR(old)
	number := prNumber(pr)
	repo := prRepo(pr)
	reasonRepo, reasonNumber := ladderStepReasonPR(s.OverrideReason)
	switch {
	case number == 0:
		// Authors rarely record `job joined` (7 of 470 job ledgers on one host,
		// hk 5521), so the PR the step names in its own override reason is the
		// record of record: the hub writes "..., round N, owner/repo#N".
		repo, number = reasonRepo, reasonNumber
	case repo == "" && reasonNumber == number:
		// The joined PR gave no repository (a bare number): the step's own
		// reason names it for the same PR.
		repo = reasonRepo
	}
	if number == 0 {
		return fmt.Errorf("replaced job %s joined no PR", old.ID)
	}
	store, err := a.benchStore()
	if err != nil {
		return err
	}
	var rec map[string]any
	if jobStore, err := a.jobStore(); err == nil {
		rec, _ = jobStore.Get(ctx, old.ID)
	}
	profile := firstNonempty(old.Profile, str(old.Receipt, "profile"))
	effort := firstNonempty(old.Effort, str(old.Receipt, "effort"))
	model := firstNonempty(str(old.Receipt, "model_id"), str(rec, "model"))
	harness := outcomeHarness(old.Receipt, rec)
	rounds := outcomeRounds(old)
	notes, _ := json.Marshal(map[string]any{
		"source": "panewire", "schema": 1, "kind": ladderStepKind,
		"job": old.ID, "task": s.Task, "pr": number, "repo": repo,
		"grade": s.TaskGrade, "tier": s.T, "rounds": rounds,
		// reviewed_heads stays null: panewire keeps no record of the heads
		// mergewake reviewed (docs/reps.md §Ladder steps), so the count is
		// unknown, never zero.
		"reviewed_heads": nil,
		"replaced":       map[string]any{"profile": profile, "effort": effort, "model": model, "harness": harness},
		"takes_over": map[string]any{
			"job": s.Job, "profile": s.Profile, "effort": firstNonempty(launch.Effort, seat.Effort),
			"model": launch.ModelID, "harness": seat.Kind,
		},
	})
	completed := 0
	rep := Rep{
		OriginID: stableRepOriginID("panewire-ladder:" + old.ID),
		Profile:  profile, ModelID: model, TaskRef: "hk" + s.Task, Tier: s.T,
		Role: outcomeRole(old), Rounds: rounds,
		Completed: &completed, RecordedAt: a.Now().UTC().Format(time.RFC3339Nano),
		Effort: effort, Grade: s.TaskGrade, Notes: string(notes),
	}
	if catalog, err := a.benchCatalog(ctx); err == nil {
		rep.TableGrade = repTableGrade(catalog, profile, effort)
	}
	_, _, err = store.AddOutcome(ctx, rep)
	return err
}

// ladderStepPRPattern is the PR a ladder-step override reason ends with:
// owner/repo#N (the hub's) or "PR N" (rstack wake's).
var ladderStepPRPattern = regexp.MustCompile(`(?:([A-Za-z0-9._-]+/[A-Za-z0-9._-]+)#|\bPR )([0-9]+)\s*$`)

// ladderStepReasonPR reads the PR a ladder-step override reason names: its
// repository when it names one, and its number; 0 when it names none.
func ladderStepReasonPR(reason string) (repo string, number int) {
	m := ladderStepPRPattern.FindStringSubmatch(strings.TrimSpace(reason))
	if m == nil {
		return "", 0
	}
	n, err := strconv.Atoi(m[2])
	if err != nil || n < 1 {
		return "", 0
	}
	return m[1], n
}

// prRepo reads the owner/repo of a PR reference: a GitHub pull URL or
// owner/repo#N. A bare number names none.
func prRepo(pr string) string {
	pr = strings.TrimSpace(pr)
	if i := strings.Index(pr, "github.com/"); i >= 0 {
		parts := strings.Split(strings.TrimPrefix(pr[i:], "github.com/"), "/")
		if len(parts) >= 3 && parts[0] != "" && parts[1] != "" && parts[2] == "pull" {
			return parts[0] + "/" + parts[1]
		}
		return ""
	}
	if i := strings.LastIndex(pr, "#"); i > 0 && strings.Count(pr[:i], "/") == 1 {
		return pr[:i]
	}
	return ""
}
