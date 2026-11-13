package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// seedSpawnedRecord claims the in-memory hk record for a live fixture job and
// leaves it spawned, as a started spawn does.
func seedSpawnedRecord(t *testing.T, a *App, id string) *fakeJobStore {
	t.Helper()
	store, ok := a.JobStore.(*fakeJobStore)
	if !ok {
		t.Fatalf("test needs the in-memory job store, got %T", a.JobStore)
	}
	if e := store.Claim(context.Background(), id, "42", "test-host"); e != nil {
		t.Fatal(e)
	}
	store.mu.Lock()
	store.records[id]["state"] = "spawned"
	store.mu.Unlock()
	return store
}

// watcherCompletes runs the completion watcher on a live fixture whose pane
// signalled READY: the job and its record are completed with the report before
// the caller runs an explicit done.
func watcherCompletes(t *testing.T) (*App, string, *fakeJobStore) {
	t.Helper()
	a, _, report := watchFixture(t)
	useFakeHK(t).Handle(hkCommentAnswer)
	store := seedSpawnedRecord(t, a, "j")
	writeReport(t, report, "final report\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.completed") {
		t.Fatalf("watcher did not complete the job: %v", kinds)
	}
	return a, report, store
}

func recordResult(t *testing.T, rec map[string]any) any {
	t.Helper()
	raw, ok := rec["result"]
	if !ok || raw == nil {
		return nil
	}
	b, e := json.Marshal(raw)
	if e != nil {
		t.Fatal(e)
	}
	var v any
	if e := json.Unmarshal(b, &v); e != nil {
		t.Fatal(e)
	}
	return v
}

func countStorePuts(store *fakeJobStore) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	n := 0
	for _, c := range store.calls {
		if strings.HasPrefix(c, "put ") {
			n++
		}
	}
	return n
}

func countEvents(t *testing.T, a *App, kind string) int {
	t.Helper()
	n := 0
	for _, k := range eventKindsOf(t, a, "j") {
		if k == kind {
			n++
		}
	}
	return n
}

// hk writes `result` only in the spawned→completed PUT and refuses every other
// terminal rewrite, so a done --result that finds a record the watcher already
// completed without a result cannot attach it: it is refused as
// terminal_without_result and the record is left as it is.
func TestDoneResultOnTerminalWithoutResultIsRefused(t *testing.T) {
	a, report, store := watcherCompletes(t)
	if got := recordResult(t, store.record(t, "j")); got != nil {
		t.Fatalf("watcher completed with an unexpected result: %v", got)
	}
	findings := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, findings, `{"findings":[{"severity":"high","path":"internal/fleet/spawn.go:576"}]}`)
	out := a.Out.(*bytes.Buffer)
	out.Reset()
	e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", findings})
	if code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=terminal_without_result") {
		t.Fatalf("error=%v, want terminal_without_result", e)
	}
	if got := out.String(); strings.Contains(got, "OK") {
		t.Fatalf("output=%q", got)
	}
	if got := recordResult(t, store.record(t, "j")); got != nil {
		t.Fatalf("a result reached a terminal record: %v", got)
	}
	if n := countEvents(t, a, "job.result_attached"); n != 0 {
		t.Fatalf("job.result_attached events=%d", n)
	}
}

// Acceptance (b): an equal result already stored is confirmed once, by a
// read-back, and a repeat then writes nothing: its receipt proves the record.
func TestDoneResultEqualOnDuplicateDoesNotWrite(t *testing.T) {
	a, report, store := watcherCompletes(t)
	findings := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, findings, `{"findings":[{"severity":"high"}]}`)
	store.mu.Lock()
	store.records["j"]["result"] = map[string]any{"findings": []any{map[string]any{"severity": "high"}}}
	store.mu.Unlock()
	out := a.Out.(*bytes.Buffer)
	run := func() {
		t.Helper()
		out.Reset()
		if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", findings}); e != nil {
			t.Fatal(e)
		}
		if got := out.String(); !strings.Contains(got, "duplicate=yes") {
			t.Fatalf("output=%q", got)
		}
	}
	run()
	store.mu.Lock()
	calls := len(store.calls)
	store.mu.Unlock()
	run()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.calls) != calls {
		t.Fatalf("the repeat called the job store: %v", store.calls[calls:])
	}
}

// Acceptance (c): a different stored result is never overwritten; the call
// warns and exits 0.
func TestDoneResultConflictOnDuplicateKeepsStored(t *testing.T) {
	a, report, store := watcherCompletes(t)
	store.mu.Lock()
	store.records["j"]["result"] = map[string]any{"source": "stored"}
	store.mu.Unlock()
	findings := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, findings, `{"source":"later"}`)
	out := a.Out.(*bytes.Buffer)
	out.Reset()
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", findings}); code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=result_conflict") {
		t.Fatalf("error=%v, want strict result conflict", e)
	}
	if got := out.String(); strings.Contains(got, "OK") {
		t.Fatalf("output=%q", got)
	}
	res, ok := recordResult(t, store.record(t, "j")).(map[string]any)
	if !ok || res["source"] != "stored" {
		t.Fatalf("stored result overwritten: %v", store.record(t, "j")["result"])
	}
}

// Acceptance (d): an explicit done whose --result file does not exist fails
// and does not complete the job.
func TestDoneResultMissingFileFails(t *testing.T) {
	a := testApp(t)
	seedJob(t, a, "j", "worker")
	report := filepath.Join(a.Jobs, "j", "report.md")
	writeReport(t, report, "done\n")
	missing := filepath.Join(a.Jobs, "j", "findings.json")
	e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", missing})
	if e == nil || code(e) == 0 {
		t.Fatalf("missing --result completed the job: %v", e)
	}
	if kinds := eventKindsOf(t, a, "j"); slices.Contains(kinds, "job.completed") {
		t.Fatalf("job completed without its result: %v", kinds)
	}
}

// Acceptance (e): the watcher attaches a job-dir result file in its completion
// write, for both the reviewer (findings.json) and delegate (result.json)
// conventions.
func TestWatcherAttachesJobDirResult(t *testing.T) {
	for _, name := range []string{"findings.json", "result.json"} {
		t.Run(name, func(t *testing.T) {
			a, _, report := watchFixture(t)
			store := seedSpawnedRecord(t, a, "j")
			writeReport(t, report, "final\n")
			writeReport(t, filepath.Join(a.Jobs, "j", name), `{"answer":"filed"}`)
			putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			res, ok := recordResult(t, store.record(t, "j")).(map[string]any)
			if !ok || res["answer"] != "filed" {
				t.Fatalf("%s not attached: %v", name, store.record(t, "j")["result"])
			}
			if str(store.record(t, "j"), "state") != "completed" {
				t.Fatalf("record=%v", store.record(t, "j"))
			}
		})
	}
}
