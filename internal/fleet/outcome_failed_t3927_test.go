package fleet

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mergedRepBodyAtBase is the exact PUT body a merged job's outcome sent at the
// base commit (7937361), before failure reps existed. A change to the failure
// path must not move one byte of it (docs/reps.md §Automatic outcomes).
const mergedRepBodyAtBase = `{"reps":[{"blockers_found":1,"completed":1,"critical_findings":0,"effort":"high","grade":"B","high_findings":1,"input_tokens":null,"merged":true,"model_id":"claude-sonnet-5-5","notes":"{\"effort\":\"high\",\"findings_by_severity\":{\"high\":1,\"low\":2},\"grade\":\"B\",\"harness\":\"claude\",\"head\":\"abc123\",\"job\":\"pw-job\",\"merged_at\":\"2026-11-07T09:00:00Z\",\"model\":\"claude-sonnet-5-5\",\"pr\":77,\"profile\":\"sonnet\",\"repo\":\"mgh3326/panewire\",\"rounds\":1,\"schema\":1,\"source\":\"panewire\",\"task\":\"3250\",\"tier\":\"T2\",\"time_to_merge_s\":3600}","origin_id":8300636185116942,"output_tokens":null,"profile":"sonnet","recorded_at":"2026-11-07T09:00:00Z","role":"impl","rounds":1,"spawn_to_merge_s":3599,"table_grade":"","task_ref":"hk3250","tier":"T2"}]}`

// failureApp is benchFakeApp with a fixed clock and an hk task read that titles
// the task with its grade marker.
func failureApp(t *testing.T, srv *httptest.Server) *App {
	t.Helper()
	a := benchFakeApp(t, srv)
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC) }
	a.TaskState = func(context.Context, string) (TaskStatus, error) {
		return TaskStatus{State: "in_progress", Title: "M4 reps (grade B, T2)"}, nil
	}
	return a
}

// seedBareJob claims and spawns a worker on task 3250 with a sonnet rung and no
// PR. extra keys go onto the job.spawned receipt.
func seedBareJob(t *testing.T, a *App, id string, extra map[string]any) {
	t.Helper()
	putEvent(t, a, id, "job.claim", map[string]any{"task_id": "3250", "t_level": "T2", "role": "worker"})
	receipt := map[string]any{
		"profile": "sonnet", "effort": "high", "model_id": "claude-sonnet-5-5",
		"harness_session": map[string]any{"kind": "claude", "id": "s1"}, "task_id": "3250",
	}
	for key, value := range extra {
		receipt[key] = value
	}
	putEvent(t, a, id, "job.spawned", receipt)
}

// seedFailureJob is seedBareJob plus a joined PR 77 (head abc123).
func seedFailureJob(t *testing.T, a *App, id string, extra map[string]any) {
	t.Helper()
	seedBareJob(t, a, id, extra)
	putEvent(t, a, id, "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/77", "head": "abc123"})
}

// prFake reports the task's one PR 77 in the given state; "merged" carries a
// merge time.
func prFake(state string) PRReader {
	return func(context.Context, string) ([]PRChange, error) {
		change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: state, Head: "abc123",
			OpenedAt: "2026-11-07T08:00:00Z"}
		if state == "merged" {
			change.MergedAt = "2026-11-07T09:00:00Z"
		}
		return []PRChange{change}, nil
	}
}

func mustFailureJob(t *testing.T, a *App, id string) Job {
	t.Helper()
	j, err := a.job(id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// (a) Each terminal state writes exactly one completed = 0 rep carrying the
// launch rung, the grade and tier from the title, and its ended reason. A job
// with no PR of its own is final at once.
func TestFailureRepWritesOneRowPerTerminalEnd(t *testing.T) {
	for _, ended := range []string{"escalated", "lost", "reaped", "superseded"} {
		t.Run(ended, func(t *testing.T) {
			fake := &hkBenchFake{}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			a := failureApp(t, srv)
			seedBareJob(t, a, "pw-job", nil)
			final, err := a.recordFailedOutcome(context.Background(), mustFailureJob(t, a, "pw-job"), "3250", ended, nil)
			if err != nil || !final {
				t.Fatalf("final=%v err=%v, want final and no error", final, err)
			}
			if len(fake.reps) != 1 {
				t.Fatalf("rows = %d, want 1", len(fake.reps))
			}
			row := fake.reps[0]
			if row["completed"] != float64(0) || row["profile"] != "sonnet" || row["effort"] != "high" ||
				row["model_id"] != "claude-sonnet-5-5" || row["task_ref"] != "hk3250" ||
				row["grade"] != "B" || row["tier"] != "T2" || row["recorded_at"] != "2026-11-07T08:00:00Z" {
				t.Fatalf("row = %v", row)
			}
			if _, ok := row["merged"]; ok {
				t.Fatalf("failure row claims merged: %v", row["merged"])
			}
			notes, _ := row["notes"].(string)
			for _, want := range []string{`"ended":"` + ended + `"`, `"harness":"claude"`, `"job":"pw-job"`, `"source":"panewire"`} {
				if !strings.Contains(notes, want) {
					t.Fatalf("notes %s missing %s", notes, want)
				}
			}
		})
	}
}

// (b) The same job reported again, even with another ended reason, keeps its
// first row.
func TestFailureRepSecondWriteForJobDoesNotDuplicate(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := failureApp(t, srv)
	seedBareJob(t, a, "pw-job", nil)
	j := mustFailureJob(t, a, "pw-job")
	ctx := context.Background()
	if _, err := a.recordFailedOutcome(ctx, j, "3250", "reaped", nil); err != nil {
		t.Fatal(err)
	}
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC) }
	if _, err := a.recordFailedOutcome(ctx, j, "3250", "reaped", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.recordFailedOutcome(ctx, j, "3250", "superseded", nil); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d, want 1", len(fake.reps))
	}
	if notes, _ := fake.reps[0]["notes"].(string); !strings.Contains(notes, `"ended":"reaped"`) {
		t.Fatalf("kept row is not the first write: %s", notes)
	}
}

// The user's rule: while the joined PR is open nothing is written; once it is
// closed unmerged, one completed = 0 row; a later merge leaves the merged path
// to write the one merged row.
func TestOpenPRWritesNothingThenClosedUnmergedWritesOneRow(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := failureApp(t, srv)
	seedFailureJob(t, a, "pw-job", nil)
	ctx := context.Background()
	j := mustFailureJob(t, a, "pw-job")

	if final, err := a.recordFailedOutcome(ctx, j, "3250", "escalated", prFake("open")); err != nil || final {
		t.Fatalf("open PR: final=%v err=%v, want not final and no error", final, err)
	}
	if len(fake.reps) != 0 {
		t.Fatalf("open PR wrote %d rows, want 0", len(fake.reps))
	}
	if final, err := a.recordFailedOutcome(ctx, j, "3250", "escalated", prFake("closed")); err != nil || !final {
		t.Fatalf("closed PR: final=%v err=%v, want final", final, err)
	}
	if len(fake.reps) != 1 || fake.reps[0]["completed"] != float64(0) {
		t.Fatalf("closed-unmerged rows = %v, want one completed=0 row", fake.reps)
	}
	if _, err := a.recordFailedOutcome(ctx, j, "3250", "escalated", prFake("closed")); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("repeat wrote a second row: %d", len(fake.reps))
	}
}

// (c) Open, then merged: nothing is written while the PR is open, and the
// merged path writes the job's one row.
func TestOpenPRThenMergedWritesOneMergedRow(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := failureApp(t, srv)
	seedFailureJob(t, a, "pw-job", nil)
	ctx := context.Background()
	if _, err := a.recordFailedOutcome(ctx, mustFailureJob(t, a, "pw-job"), "3250", "reaped", prFake("open")); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 0 {
		t.Fatalf("open PR wrote %d rows, want 0", len(fake.reps))
	}
	status := TaskStatus{State: "merged", Category: "completed", Title: "M4 reps (grade B, T2)"}
	change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: "merged", Head: "abc123",
		OpenedAt: "2026-11-07T08:00:00Z", MergedAt: "2026-11-07T09:00:00Z"}
	if err := a.recordMergedOutcome(ctx, mustFailureJob(t, a, "pw-job"), "3250", status, change); err != nil {
		t.Fatal(err)
	}
	if final, err := a.recordFailedOutcome(ctx, mustFailureJob(t, a, "pw-job"), "3250", "reaped", prFake("merged")); err != nil || !final {
		t.Fatalf("merged PR: final=%v err=%v, want final and no error", final, err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d, want 1", len(fake.reps))
	}
	if fake.reps[0]["merged"] != true || fake.reps[0]["completed"] != float64(1) {
		t.Fatalf("surviving row = %v, want the merged rep", fake.reps[0])
	}
}

// A job whose joined PR cannot be read is not final: nothing is written, and
// the sweep asks again.
func TestUnreadPRWritesNothing(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := failureApp(t, srv)
	seedFailureJob(t, a, "pw-job", nil)
	unknown := PRReader(func(context.Context, string) ([]PRChange, error) { return nil, nil })
	if final, err := a.recordFailedOutcome(context.Background(), mustFailureJob(t, a, "pw-job"), "3250", "lost", unknown); err != nil || final {
		t.Fatalf("unlisted PR: final=%v err=%v, want not final", final, err)
	}
	if len(fake.reps) != 0 {
		t.Fatalf("unknown PR state wrote %d rows", len(fake.reps))
	}
}

// (d) A catalog-experiment job writes no automatic rep, failed or merged. A job
// with another override reason still writes its failure row.
func TestCatalogExperimentJobWritesNoAutomaticRep(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := failureApp(t, srv)
	seedFailureJob(t, a, "exp-job", map[string]any{"override_reason": "catalog experiment: sonnet rung"})
	ctx := context.Background()
	if _, err := a.recordFailedOutcome(ctx, mustFailureJob(t, a, "exp-job"), "3250", "escalated", nil); err != nil {
		t.Fatal(err)
	}
	status := TaskStatus{State: "merged", Category: "completed", Title: "M4 reps (grade B, T2)"}
	change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: "merged", Head: "abc123",
		OpenedAt: "2026-11-07T08:00:00Z", MergedAt: "2026-11-07T09:00:00Z"}
	if err := a.recordMergedOutcome(ctx, mustFailureJob(t, a, "exp-job"), "3250", status, change); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 0 {
		t.Fatalf("catalog experiment wrote %d rows, want 0", len(fake.reps))
	}

	seedBareJob(t, a, "pin-job", map[string]any{"override_reason": "operator pin"})
	if _, err := a.recordFailedOutcome(ctx, mustFailureJob(t, a, "pin-job"), "3250", "escalated", nil); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("control job rows = %d, want 1", len(fake.reps))
	}
}

// (e) A merged job's rep is byte-for-byte the base's.
func TestMergedRepBodyUnchangedFromBase(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC) }
	putEvent(t, a, "pw-job", "job.claim", map[string]any{"task_id": "3250", "t_level": "T2", "role": "worker"})
	putEvent(t, a, "pw-job", "job.spawned", map[string]any{
		"profile": "sonnet", "effort": "high", "model_id": "claude-sonnet-5-5",
		"harness_session": map[string]any{"kind": "claude", "id": "s1"}, "task_id": "3250",
	})
	putEvent(t, a, "pw-job", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/77", "head": "abc123"})
	putEvent(t, a, "pw-job", "job.completed", map[string]any{})
	if err := os.WriteFile(filepath.Join(a.Jobs, "pw-job", "findings.json"),
		[]byte(`{"findings":[{"severity":"high"},{"severity":"low"},{"severity":"low"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	status := TaskStatus{State: "merged", Category: "completed", Title: "M4 reps (grade B, T2)"}
	change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: "merged", Head: "abc123",
		OpenedAt: "2026-11-07T08:00:00Z", MergedAt: "2026-11-07T09:00:00Z"}
	if err := a.recordMergedOutcome(context.Background(), mustFailureJob(t, a, "pw-job"), "3250", status, change); err != nil {
		t.Fatal(err)
	}
	if len(fake.repBodies) != 1 {
		t.Fatalf("PUT bodies = %d, want 1", len(fake.repBodies))
	}
	if fake.repBodies[0] != mergedRepBodyAtBase {
		t.Fatalf("merged rep body changed:\n got %s\nwant %s", fake.repBodies[0], mergedRepBodyAtBase)
	}
}

// An open PR prevents a sweep diagnostic. Closing the PR still does not make
// a retained pane a reaped job or publish its failure rep.
func TestSweepHoldsReapedFailureRepWhileOpenPRThenWritesOnClose(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	f := newPaneSweepFixture(t)
	f.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: f.a.Now}, now: f.a.Now}
	f.job("j", "worker", "42")
	putEvent(t, f.a, "j", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/9", "head": "head-9"})
	putEvent(t, f.a, "j", "job.revoked", map[string]any{})
	f.tasks["42"] = "in_progress"
	f.prChanges["42"] = []PRChange{{Number: 9, State: "open", Head: "head-9"}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed without ownership-bound authorization: %v", f.closed())
	}
	if len(fake.reps) != 0 {
		t.Fatalf("open PR wrote %d rows at close, want 0", len(fake.reps))
	}
	f.prChanges["42"] = []PRChange{{Number: 9, State: "closed", Head: "head-9"}}
	f.at(12)
	if len(fake.reps) != 0 {
		t.Fatalf("retained pane wrote a reaped failure row: %v", fake.reps)
	}
}

// An escalated job with an open joined PR is held by the sweep, which writes its
// row once the PR closes unmerged.
func TestSweepHoldsEscalatedJobWithOpenPRUntilPRCloses(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	f := newPaneSweepFixture(t)
	f.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: f.a.Now}, now: f.a.Now}
	f.job("e", "worker", "44")
	putEvent(t, f.a, "e", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/10", "head": "head-10"})
	putEvent(t, f.a, "e", "job.escalate", map[string]any{})
	f.tasks["44"] = "in_progress"
	f.prChanges["44"] = []PRChange{{Number: 10, State: "open", Head: "head-10"}}
	f.at(0)
	if len(fake.reps) != 0 {
		t.Fatalf("open PR wrote %d rows, want 0", len(fake.reps))
	}
	f.prChanges["44"] = []PRChange{{Number: 10, State: "closed", Head: "head-10"}}
	f.at(1)
	if len(fake.reps) != 1 {
		t.Fatalf("rows after PR closed = %d, want 1", len(fake.reps))
	}
	if notes, _ := fake.reps[0]["notes"].(string); !strings.Contains(notes, `"ended":"escalated"`) {
		t.Fatalf("notes = %s", notes)
	}
}

// A superseded job's failure row remains pending until the pane can be reaped.
func TestSweepRecordsSupersededFailureRepWithoutPR(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	f := newPaneSweepFixture(t)
	f.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: f.a.Now}, now: f.a.Now}
	f.job("j", "worker", "42")
	putEvent(t, f.a, "j", "job.revoked", map[string]any{"outcome": "superseded"})
	f.tasks["42"] = "in_progress"
	f.at(0)
	f.at(11)
	if len(fake.reps) != 0 || len(f.closed()) != 0 {
		t.Fatalf("retained pane wrote a reaped failure row or closed: rows=%v closed=%v", fake.reps, f.closed())
	}
}

// A reaped job with no PR and no completion gets one completed = 0 row; a
// reaped job whose PR merged keeps only its merged row; a completed job with no
// PR of its own writes nothing.
func TestSweepWritesRowsByOutcomeFinality(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	f := newPaneSweepFixture(t)
	f.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: f.a.Now}, now: f.a.Now}
	f.job("bare", "worker", "42")
	putEvent(t, f.a, "bare", "job.revoked", map[string]any{})
	f.job("merged", "worker", "43")
	putEvent(t, f.a, "merged", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/9", "head": "head-9"})
	putEvent(t, f.a, "merged", "job.revoked", map[string]any{})
	f.job("done", "worker", "44")
	putEvent(t, f.a, "done", "job.completed", map[string]any{})
	f.tasks["42"], f.tasks["43"], f.tasks["44"] = "in_progress", "in_progress", "in_progress"
	f.prChanges["43"] = []PRChange{{Number: 9, State: "merged", Head: "head-9",
		OpenedAt: "2026-03-01T11:00:00Z", MergedAt: "2026-03-01T12:00:00Z"}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("panes closed without ownership-bound authorization: %v", f.closed())
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d, want only the independently observed merged outcome", len(fake.reps))
	}
	row := fake.reps[0]
	if row["task_ref"] != "hk43" || row["merged"] != true {
		t.Fatalf("merged job row = %v", row)
	}
}
