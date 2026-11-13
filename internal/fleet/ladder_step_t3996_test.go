package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ladderFixture is a ladder-step re-spawn: a sonnet rung (parent rebindParent)
// on PR 77 is replaced by the cmd seat of the spawn fixture, under a
// "ladder step" override reason. The bench store writes to a fake hk.
func ladderFixture(t *testing.T) (*rebindFixture, *hkBenchFake) {
	t.Helper()
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.s.OverrideReason = "ladder step sonnet high → cmd-dsflash41 high, PR 77"
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"task_id": "42", "t_level": "T1", "role": "implementer"})
	putEvent(t, fx.a, rebindParent, "job.spawned", map[string]any{
		"profile": "sonnet", "effort": "high", "model_id": "claude-sonnet-5-5",
		"harness_session": map[string]any{"kind": "claude", "id": "s1"}, "task_id": "42",
	})
	putEvent(t, fx.a, rebindParent, "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/77", "head": "abc123"})
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	fx.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: fx.a.Now}, now: fx.a.Now, pendingPath: filepath.Join(fx.a.Home, "reps.json")}
	fx.a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
		return []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "B"}}, nil
	}
	fx.a.TaskState = func(context.Context, string) (TaskStatus, error) {
		return TaskStatus{State: "in_progress", Title: "implement (grade B, T1)"}, nil
	}
	return fx, fake
}

// ladderNotes decodes a row's notes object.
func ladderNotes(t *testing.T, row map[string]any) map[string]any {
	t.Helper()
	raw, _ := row["notes"].(string)
	var notes map[string]any
	if err := json.Unmarshal([]byte(raw), &notes); err != nil {
		t.Fatalf("notes %q: %v", raw, err)
	}
	return notes
}

// (a) A ladder-step re-spawn on an open PR writes one ladder-step row that names
// the replaced rung, the PR and the rung that takes over.
func TestLadderStepSpawnWritesOneRowNamingReplacedAndTakingOverRung(t *testing.T) {
	fx, fake := ladderFixture(t)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d, want 1: %v", len(fake.reps), fake.reps)
	}
	row := fake.reps[0]
	if row["completed"] != float64(0) || row["profile"] != "sonnet" || row["effort"] != "high" ||
		row["model_id"] != "claude-sonnet-5-5" || row["task_ref"] != "hk42" || row["tier"] != "T1" ||
		row["grade"] != "B" || row["role"] != "impl" {
		t.Fatalf("row = %v", row)
	}
	notes := ladderNotes(t, row)
	takeover := fx.a.Policy.Profiles[fx.s.Profile].Kind
	replaced, _ := notes["replaced"].(map[string]any)
	taking, _ := notes["takes_over"].(map[string]any)
	if notes["kind"] != ladderStepKind || notes["job"] != rebindParent || notes["pr"] != float64(77) ||
		replaced["profile"] != "sonnet" || replaced["effort"] != "high" || replaced["model"] != "claude-sonnet-5-5" ||
		replaced["harness"] != "claude" || taking["job"] != fx.s.Job || taking["profile"] != fx.s.Profile ||
		taking["effort"] != "high" || taking["model"] != "provider/example-model" || taking["harness"] != takeover {
		t.Fatalf("notes = %v", notes)
	}
	if notes["reviewed_heads"] != nil {
		t.Fatalf("reviewed_heads = %v, want null (unknown)", notes["reviewed_heads"])
	}
	if row["origin_id"] != float64(stableRepOriginID("panewire-ladder:"+rebindParent)) {
		t.Fatalf("origin_id = %v, want the ladder origin of the replaced job", row["origin_id"])
	}
}

// (b) The same step written again, with a later clock and a different
// re-spawn job id, keeps the first row.
func TestLadderStepRetryDoesNotWriteSecondRow(t *testing.T) {
	fx, fake := ladderFixture(t)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	fx.a.Now = func() time.Time { return time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC) }
	retry := fx.s
	retry.Job = "job-a-retry"
	seat := fx.a.Policy.Profiles[fx.s.Profile]
	if err := fx.a.recordLadderStep(context.Background(), retry, Launch{Effort: "high", ModelID: "provider/example-model"}, seat); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d, want 1", len(fake.reps))
	}
	if taking, _ := ladderNotes(t, fake.reps[0])["takes_over"].(map[string]any); taking["job"] != fx.s.Job {
		t.Fatalf("kept row names %v, want the first re-spawn %s", taking["job"], fx.s.Job)
	}
}

// (c) A later merge of the PR writes the merging job's own outcome row, and the
// ladder-step row is left exactly as it was written.
func TestLadderStepRowSurvivesLaterMerge(t *testing.T) {
	fx, fake := ladderFixture(t)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows after spawn = %d, want the ladder row", len(fake.reps))
	}
	before, err := json.Marshal(fake.reps[0])
	if err != nil {
		t.Fatal(err)
	}
	merging := Job{ID: fx.s.Job, Task: "42", Profile: fx.s.Profile, Events: []Event{{Kind: "job.joined",
		Payload: map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/77", "head": "abc123"}}}}
	status := TaskStatus{State: "merged", Category: "completed", Title: "implement (grade B, T1)"}
	change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: "merged", Head: "abc123",
		OpenedAt: "2026-11-07T08:00:00Z", MergedAt: "2026-11-07T09:00:00Z"}
	if err := fx.a.recordMergedOutcome(context.Background(), merging, "42", status, change); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 2 {
		t.Fatalf("rows = %d, want the ladder row and the merging job's row", len(fake.reps))
	}
	after, err := json.Marshal(fake.reps[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("ladder row changed by the merge:\nbefore %s\nafter  %s", before, after)
	}
	merged := fake.reps[1]
	if merged["merged"] != true || merged["completed"] != float64(1) ||
		merged["origin_id"] != float64(stableRepOriginID("panewire-outcome:"+fx.s.Job)) {
		t.Fatalf("merging job row = %v", merged)
	}
}

// (d) reps list for the replaced rung shows the ladder-step row, and the rung
// that takes over does not claim it.
func TestLadderStepRowShowsInRepsListForReplacedRung(t *testing.T) {
	fx, _ := ladderFixture(t)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	var out, errOut bytes.Buffer
	if err := fx.a.repsCommand(context.Background(), []string{"list", "--profile", "sonnet", "--effort", "high"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "profile=sonnet model=claude-sonnet-5-5 effort=high") || !strings.Contains(out.String(), `"kind":"ladder_step"`) ||
		!strings.Contains(out.String(), "completed=0") {
		t.Fatalf("reps list for the replaced rung:\n%s", out.String())
	}
	out.Reset()
	if err := fx.a.repsCommand(context.Background(), []string{"list", "--profile", fx.s.Profile}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "ladder_step") {
		t.Fatalf("reps list for the taking-over rung shows the ladder row:\n%s", out.String())
	}
}

// (e) The replaced job's own failure row is written under its outcome origin
// with its fields as before, and it is separate from the ladder-step row.
func TestLadderStepLeavesReplacedJobFailureRowAlone(t *testing.T) {
	fx, fake := ladderFixture(t)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	old := mustFailureJob(t, fx.a, rebindParent)
	if final, err := fx.a.recordFailedOutcome(context.Background(), old, "42", "escalated", prFake("closed")); err != nil || !final {
		t.Fatalf("final=%v err=%v, want final and no error", final, err)
	}
	if len(fake.reps) != 2 {
		t.Fatalf("rows = %d, want the ladder row and the failure row", len(fake.reps))
	}
	ladder, failure := fake.reps[0], fake.reps[1]
	if failure["origin_id"] != float64(stableRepOriginID("panewire-outcome:"+rebindParent)) ||
		failure["completed"] != float64(0) || failure["profile"] != "sonnet" || failure["effort"] != "high" ||
		failure["model_id"] != "claude-sonnet-5-5" || failure["grade"] != "B" || failure["tier"] != "T1" {
		t.Fatalf("failure row = %v", failure)
	}
	if notes := ladderNotes(t, failure); notes["ended"] != "escalated" || notes["kind"] != nil {
		t.Fatalf("failure notes = %v", notes)
	}
	if ladder["origin_id"] == failure["origin_id"] {
		t.Fatal("ladder row shares the failure row's origin id")
	}
}

// A spawn that is not a ladder step, or a ladder step whose replaced job joined
// no PR, writes no ladder row.
func TestLadderStepNeedsParentJobAndReplacedPR(t *testing.T) {
	fx, fake := ladderFixture(t)
	plain := fx.s
	plain.OverrideReason = "operator pin"
	seat := fx.a.Policy.Profiles[fx.s.Profile]
	if err := fx.a.recordLadderStep(context.Background(), plain, Launch{Effort: "high"}, seat); err != nil {
		t.Fatal(err)
	}
	bare := fx.s
	bare.replacedRung = &Job{ID: rebindParent, Task: "42"}
	if err := fx.a.recordLadderStep(context.Background(), bare, Launch{Effort: "high"}, seat); err == nil {
		t.Fatal("ladder step over a parent with no PR was not refused")
	}
	if len(fake.reps) != 0 {
		t.Fatalf("rows = %d, want 0", len(fake.reps))
	}
}
