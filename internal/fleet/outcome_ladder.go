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

// ladderReasonPR is the "PR <n>" a ladder-step override reason names.
var ladderReasonPR = regexp.MustCompile(`\bPR #?([0-9]+)\b`)

// isLadderStep reports a spawn that replaces a rung: a re-spawn on the parent
// job it names, under a ladder-step override reason.
func (s Spawn) isLadderStep() bool {
	return s.ParentJob != "" && strings.HasPrefix(strings.TrimSpace(s.OverrideReason), ladderStepPrefix)
}

// ladderStepPR is the PR a ladder step replaces a rung on. The replaced job's
// joined PR is the first source. A parent resolved from its hk record has no
// job events, so it has no joined PR on that path; the override reason's
// "PR <n>" is the source there. It returns 0 when neither names a PR.
func ladderStepPR(s Spawn, old Job) (int, string) {
	pr, _ := joinedPR(old)
	if n := prNumber(pr); n > 0 {
		return n, "joined"
	}
	if m := ladderReasonPR.FindStringSubmatch(s.OverrideReason); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			return n, "override_reason"
		}
	}
	return 0, ""
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
	number, prSource := ladderStepPR(s, old)
	if number == 0 {
		return fmt.Errorf("replaced job %s names no PR", old.ID)
	}
	store, err := a.benchStore()
	if err != nil {
		return err
	}
	var rec map[string]any
	if jobStore, err := a.jobStore(); err == nil {
		rec, _ = jobStore.Get(ctx, old.ID)
	}
	profile := firstNonempty(old.Profile, str(old.Receipt, "profile"), str(rec, "profile"))
	effort := firstNonempty(old.Effort, str(old.Receipt, "effort"), str(rec, "effort"))
	model := firstNonempty(str(old.Receipt, "model_id"), str(rec, "model"))
	harness := outcomeHarness(old.Receipt, rec)
	rounds := outcomeRounds(old)
	notes, _ := json.Marshal(map[string]any{
		"source": "panewire", "schema": 1, "kind": ladderStepKind,
		"job": old.ID, "task": s.Task, "pr": number, "pr_source": prSource,
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
