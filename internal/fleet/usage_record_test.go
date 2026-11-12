package fleet

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// usageFailStore refuses every write that carries a usage object, so the
// terminal record still lands.
type usageFailStore struct {
	JobStore
	usagePuts int
}

func (s *usageFailStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if _, ok := fields["usage"]; ok {
		s.usagePuts++
		return errors.New("hk unreachable " + secretText)
	}
	return s.JobStore.Put(ctx, id, fields)
}

// usageJob seeds a claimed job whose receipt carries harness_session and cwd.
func usageJob(t *testing.T, a *App, session map[string]any) *fakeJobStore {
	t.Helper()
	store := a.JobStore.(*fakeJobStore)
	if err := store.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "worker")
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "cwd": testCWD, "harness_session": session})
	return store
}

func claudeSession() map[string]any {
	return map[string]any{"kind": "claude", "id": testClaudeID, "source": "herdr:claude"}
}

func doneJob(t *testing.T, a *App) {
	t.Helper()
	path := filepath.Join(a.Jobs, "j", "report.md")
	if err := os.WriteFile(path, []byte("done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.lifecycle(t.Context(), "done", []string{"j", "--report", path}); err != nil {
		t.Fatal(err)
	}
}

// reapJob reaps job j. Its worktree's HEAD reads and hk lists no open PR for
// its task, so a completed worker with a worktree is not kept for one (hk
// 3734); an unreadable worktree would keep it.
func reapJob(t *testing.T, a *App) {
	t.Helper()
	runner, ok := a.Runner.(*fakeRunner)
	if !ok {
		t.Fatalf("reapJob needs a fakeRunner, got %T", a.Runner)
	}
	base := runner.Do
	runner.Do = func(c Command) Result {
		if c.Name == "git" && len(c.Args) >= 4 && c.Args[2] == "rev-parse" && c.Args[3] == "HEAD" {
			return Result{Out: paneSweepTestHead + "\n"}
		}
		if base == nil {
			return Result{}
		}
		return base(c)
	}
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		return http.StatusOK, `{"changes":[]}`, r.Route() == "GET /v1/changes"
	})
	a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
	if err := a.hygiene(t.Context(), "reap", []string{"--job", "j", "--grace", "0s", "--apply"}); err != nil {
		t.Fatal(err)
	}
}

func usageOf(t *testing.T, rec map[string]any) map[string]any {
	t.Helper()
	usage, ok := rec["usage"].(map[string]any)
	if !ok {
		t.Fatalf("record has no usage object: %v", rec)
	}
	return usage
}

func TestCompletionWritesUsageAndModelAndReapOverwritesIt(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	store := usageJob(t, a, claudeSession())

	doneJob(t, a)
	rec := store.record(t, "j")
	if rec["state"] != "completed" || rec["model"] != "claude-sonnet-5-5" {
		t.Fatalf("record = %v", rec)
	}
	want := map[string]any{"input_tokens": int64(150), "output_tokens": int64(30), "cache_read_tokens": int64(2500), "cache_write_tokens": int64(200), "source": "claude-session-jsonl"}
	if got := usageOf(t, rec); !mapsEqual(got, want) {
		t.Fatalf("completion usage = %v, want %v", got, want)
	}

	// The harness wrote more after completion; reap reads again and replaces.
	f, err := os.OpenFile(claudeFixturePath(a.Home, testClaudeID), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(`{"type":"assistant","message":{"id":"msg_3","model":"claude-sonnet-5-5","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":0}}}` + "\n")
	if err != nil || f.Close() != nil {
		t.Fatal(err)
	}
	reapJob(t, a)
	want = map[string]any{"input_tokens": int64(160), "output_tokens": int64(35), "cache_read_tokens": int64(2600), "cache_write_tokens": int64(200), "source": "claude-session-jsonl"}
	if got := usageOf(t, store.record(t, "j")); !mapsEqual(got, want) {
		t.Fatalf("reap usage = %v, want %v", got, want)
	}
	if !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("job not reaped: %v", eventKindsOf(t, a, "j"))
	}
}

func mapsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestReapDoesNotEraseUsageWithAnEmptyRead(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	store := usageJob(t, a, claudeSession())
	doneJob(t, a)
	if err := os.Remove(claudeFixturePath(a.Home, testClaudeID)); err != nil {
		t.Fatal(err)
	}
	reapJob(t, a)
	if got := usageOf(t, store.record(t, "j")); got["input_tokens"] != int64(150) {
		t.Fatalf("reap erased the completion read: %v", got)
	}
}

func TestCompletionWithoutRecordWritesSourceOnlyUsage(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	store := usageJob(t, a, map[string]any{"kind": "cmd", "id": "unknown", "source": "unavailable"})
	doneJob(t, a)
	rec := store.record(t, "j")
	if got := usageOf(t, rec); !mapsEqual(got, map[string]any{"source": "cmd"}) {
		t.Fatalf("usage = %v", got)
	}
	if _, ok := rec["model"]; ok {
		t.Fatalf("model written without a known model: %v", rec)
	}
}

func TestCompletionWithoutHarnessSessionWritesNoUsage(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	store := a.JobStore.(*fakeJobStore)
	if err := store.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "worker")
	doneJob(t, a)
	if _, ok := store.record(t, "j")["usage"]; ok {
		t.Fatal("usage written for a receipt that names no session")
	}
}

func TestCompletionUsesTheLiveSessionReference(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","agent":"claude","agent_status":"idle","agent_session":{"agent":"claude","kind":"id","value":"` + testClaudeID + `","source":"herdr:claude"}}]}}`}
		}
		return Result{}
	}
	store := usageJob(t, a, map[string]any{"kind": "claude", "id": "unknown", "source": "unavailable"})
	doneJob(t, a)
	if got := usageOf(t, store.record(t, "j")); got["input_tokens"] != int64(150) {
		t.Fatalf("usage = %v", got)
	}
}

func TestUsageWriteFailureLeavesCompletionUnchanged(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	failing := &usageFailStore{JobStore: base}
	a.JobStore = failing
	var stderr bytes.Buffer
	a.Err = &stderr

	doneJob(t, a) // fails the test if lifecycle returns an error
	if failing.usagePuts != 1 {
		t.Fatalf("usage writes = %d", failing.usagePuts)
	}
	if rec := base.record(t, "j"); rec["state"] != "completed" || rec["usage"] != nil {
		t.Fatalf("record = %v", rec)
	}
	kinds := eventKindsOf(t, a, "j")
	if !slices.Contains(kinds, "job.completed") || slices.Contains(kinds, "job.record_failed") {
		t.Fatalf("events = %v", kinds)
	}
	relayed := false
	for _, c := range f.Calls {
		relayed = relayed || c.Name == "panewire" && slices.Contains(c.Args, "job.completed")
	}
	if !relayed {
		t.Fatal("completion relay did not run")
	}
	if !strings.Contains(stderr.String(), "job usage write failed") {
		t.Fatalf("no warning: %q", stderr.String())
	}
	if strings.Contains(stderr.String(), secretText) {
		t.Fatalf("warning carries the backend error text: %q", stderr.String())
	}

	// Reap with a failing write still reaps.
	reapJob(t, a)
	if !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") || failing.usagePuts != 2 {
		t.Fatalf("reap: usagePuts=%d events=%v", failing.usagePuts, eventKindsOf(t, a, "j"))
	}
}

// Nothing from the transcript reaches a PUT body, a log line, an event or the
// command output.
func TestUsageCarriesNoTranscriptText(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	jobs, bodies := hkJobUpdateContract(t)
	usageJob(t, a, claudeSession())
	a.JobStore = jobs
	var stderr bytes.Buffer
	a.Err = &stderr

	doneJob(t, a)
	reapJob(t, a)

	sent := bodies()
	withUsage := 0
	for _, body := range sent {
		if strings.Contains(body, `"usage":`) {
			withUsage++
			if !strings.Contains(body, `"input_tokens":150`) || !strings.Contains(body, `"source":"claude-session-jsonl"`) {
				t.Errorf("usage body = %s", body)
			}
		}
		for _, leak := range []string{secretText, a.Home, testCWD} {
			if strings.Contains(body, leak) {
				t.Errorf("PUT body carries %q: %s", leak, body)
			}
		}
	}
	if withUsage != 2 {
		t.Fatalf("usage PUTs = %d (completion + reap), bodies=%v", withUsage, sent)
	}
	for name, text := range map[string]string{"stderr": stderr.String(), "stdout": a.Out.(*bytes.Buffer).String()} {
		if strings.Contains(text, secretText) {
			t.Errorf("%s carries transcript text: %q", name, text)
		}
	}
	err := filepath.WalkDir(a.Jobs, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(path, claudeFixtureDir(a.Home)) {
			return err
		}
		if b, readErr := os.ReadFile(path); readErr == nil && bytes.Contains(b, []byte(secretText)) {
			t.Errorf("job file %s carries transcript text", filepath.Base(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func claudeFixtureDir(root string) string { return filepath.Dir(claudeFixturePath(root, "x")) }

type terminalUsageStore struct {
	JobStore
	writes []map[string]any
}

func (s *terminalUsageStore) Put(ctx context.Context, id string, fields map[string]any) error {
	rec, err := s.JobStore.Get(ctx, id)
	if err != nil {
		return err
	}
	s.writes = append(s.writes, fields)
	if state := str(rec, "state"); state == "completed" || state == "escalated" || state == "lost" {
		if _, ok := fields["usage"]; !ok || len(fields) != 1 {
			return &BackendError{Status: 409, Code: "terminal_job"}
		}
	}
	return s.JobStore.Put(ctx, id, fields)
}

func TestUsageWritesRespectTerminalJob(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	store := &terminalUsageStore{JobStore: base}
	a.JobStore = store
	doneJob(t, a)
	rec := base.record(t, "j")
	if rec["model"] != "claude-sonnet-5-5" || usageOf(t, rec)["input_tokens"] != int64(150) {
		t.Fatalf("completion record = %v", rec)
	}
	if err := os.WriteFile(claudeFixturePath(a.Home, testClaudeID), []byte(`{"type":"assistant","message":{"id":"later","model":"claude-sonnet-5-5","usage":{"input_tokens":42,"output_tokens":1}}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.writes = nil
	reapJob(t, a)
	if got := usageOf(t, base.record(t, "j"))["input_tokens"]; got != int64(42) {
		t.Fatalf("reap usage input tokens = %v", got)
	}
	if len(store.writes) != 1 || len(store.writes[0]) != 1 || store.writes[0]["usage"] == nil {
		t.Fatalf("reap writes = %v", store.writes)
	}
	store.writes = nil
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	j.Terminal = false // The local event may lag the already-terminal hk record.
	a.recordJobUsage(t.Context(), j, false)
	if len(store.writes) != 1 || len(store.writes[0]) != 1 || store.writes[0]["usage"] == nil {
		t.Fatalf("terminal completion writes = %v", store.writes)
	}
}

type modelConflictStore struct{ JobStore }

func (s modelConflictStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if _, ok := fields["model"]; ok {
		return &BackendError{Status: 409, Code: "terminal_job"}
	}
	return s.JobStore.Put(ctx, id, fields)
}

func TestModelConflictWarnsWithoutMisreportingUsage(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	a.JobStore = modelConflictStore{base}
	var stderr bytes.Buffer
	a.Err = &stderr
	doneJob(t, a)
	if usageOf(t, base.record(t, "j"))["input_tokens"] != int64(150) {
		t.Fatal("usage was not stored")
	}
	if got := stderr.String(); !strings.Contains(got, "job model write failed (hk status 409, code terminal_job)") || strings.Contains(got, "job usage write failed") {
		t.Fatalf("warning = %q", got)
	}
}

func TestModelStateReadFailureWarnsAboutRead(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	base.getErr = &BackendError{Status: 503, Code: "unavailable"}
	var stderr bytes.Buffer
	a.Err = &stderr
	doneJob(t, a)
	if got := stderr.String(); !strings.Contains(got, "job model state read failed (hk status 503, code unavailable)") || strings.Contains(got, "job model write failed") {
		t.Fatalf("warning = %q", got)
	}
	if rec := base.record(t, "j"); rec["model"] != nil || usageOf(t, rec)["input_tokens"] != int64(150) {
		t.Fatalf("record = %v", rec)
	}
}
