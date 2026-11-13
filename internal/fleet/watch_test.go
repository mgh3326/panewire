package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWatcherExpiryKeepsLiveLease(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "worker")
	seedLedger(t, a, "j")
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","agent_status":"working"}]}}`}
		}
		return original(c)
	}
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
		t.Fatalf("watch expiry released a live job: %s %v", st, pools)
	}
	if kinds := eventKindsOf(t, a, "j"); slices.Contains(kinds, "job.lost") {
		t.Fatal("watch expiry recorded loss")
	}
	_ = f
}
func TestWatcherMissingPaneReleasesThroughLedger(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	seedJob(t, a, "j", "worker")
	seedLedger(t, a, "j")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	st, pools := ledgerState(t, a, "j")
	if st != "released" || len(pools) != 0 {
		t.Fatalf("lost run not released: %s %v", st, pools)
	}
	if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.lost") {
		t.Fatalf("loss not recorded: %v", kinds)
	}
}
func TestMalformedCatalogNeverUsesFallbackModel(t *testing.T) {
	for _, body := range []string{`{"profile":"p","profile":"q"}`, `{} {}`, `{"catalog":{"stale":true},"profile":"p"}`} {
		q := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: body} }}, Binary: "quota"}
		if _, e := q.Launch(context.Background(), QuotaRequest{Profile: "p"}); code(e) != 70 {
			t.Fatalf("malformed/stale catalog accepted: %v", e)
		}
	}
}
func TestFallbackHostOrderAndLocalClosure(t *testing.T) {
	a := testApp(t)
	a.Source = &fakePlacementSource{}
	a.LoadRatio = func(context.Context) (float64, error) { return 0.1, nil }
	a.Config.Local = map[string]any{"spawn": false}
	a.Config.Hosts = map[string]map[string]any{"host-a": {}, "host-b": {}}
	a.Config.HostOrder = []string{"host-b", "host-a"}
	p, e := a.plan(context.Background(), Spawn{Tags: ""}, "", 0)
	if e != nil {
		t.Fatal(e)
	}
	var names []string
	for _, c := range p.ordered {
		names = append(names, c.name)
	}
	if strings.Join(names, ",") != "host-b,host-a" {
		t.Fatalf("%v", names)
	}
}
func TestCodexFooterRequiresLastContextRow(t *testing.T) {
	screen := "GPT-6.1 Example high /home/example/tree Context 99%"
	if !CodexFooterProof(screen, "gpt-6.1-example", "high", "/home/example/tree", "/home/example") {
		t.Fatal("valid footer refused")
	}
	if CodexFooterProof(screen+"\nOther model low /home/example/tree Context 50%", "gpt-6.1-example", "high", "/home/example/tree", "/home/example") {
		t.Fatal("stale footer accepted")
	}
}

// Codex 0.166 draws "<model> <effort> · <cwd>" with no Context column, under
// the prompt and above a shortcuts row (hk 2802).
func TestCodexFooterWithoutContextColumn(t *testing.T) {
	banner := "  directory: ~/tree\n  model: gpt-6.1-example high\n"
	footer := "› Ask Codex to do anything\n\n  GPT-6.1 Example high · ~/tree\n  ? for shortcuts      ⚠ 3 warnings · f2 to view\n"
	if !CodexFooterProof(banner+footer, "gpt-6.1-example", "high", "/home/example/tree", "/home/example") {
		t.Fatal("0.166 footer refused")
	}
	if CodexFooterProof(banner, "gpt-6.1-example", "high", "/home/example/tree", "/home/example") {
		t.Fatal("banner without footer accepted")
	}
	if CodexFooterProof(strings.Replace(banner+footer, "Example high", "Example low", 1), "gpt-6.1-example", "high", "/home/example/tree", "/home/example") {
		t.Fatal("wrong effort accepted")
	}
}

// watchFixture is a live job whose pane stays present, with its report path
// recorded in the spawn receipt, as a real spawn leaves it.
func watchFixture(t *testing.T) (*App, *fakeRunner, string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","agent_status":"working"}]}}`}
		}
		return original(c)
	}
	report := filepath.Join(a.Jobs, "j", "report.md")
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "worker"})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "report": report})
	seedLedger(t, a, "j")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	return a, f, report
}

func writeReport(t *testing.T, path, body string) {
	t.Helper()
	if e := os.WriteFile(path, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
}

// putLaneEvent writes the record `panewire emit --kind lane.event` writes.
func putLaneEvent(t *testing.T, a *App, pane, text string, at time.Time) {
	t.Helper()
	dir := filepath.Join(filepath.Dir(a.Jobs), "events-lane")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	files, _ := os.ReadDir(dir)
	path := filepath.Join(dir, fmt.Sprintf("%05d-lane.event.json", len(files)+1))
	body, e := json.Marshal(map[string]any{"type": "lane.event", "kind": "lane.event", "job_id": "", "epoch": 1, "created_at": at.UTC().Format(time.RFC3339), "owner_lane": "owner", "pane_id": pane, "event_id": fmt.Sprintf("e%d", len(files)), "text": text})
	if e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, body, 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Chtimes(path, at, at); e != nil {
		t.Fatal(e)
	}
}

func completions(t *testing.T, a *App, f *fakeRunner) (records, relays int) {
	t.Helper()
	for _, kind := range eventKindsOf(t, a, "j") {
		if kind == "job.completed" {
			records++
		}
	}
	for _, c := range f.Calls {
		if c.Name == "panewire" && slices.Contains(c.Args, "job.completed") {
			relays++
		}
	}
	return records, relays
}

// Mutation note: dropping the workerSignalledEnd condition from watch() restores
// size/mtime-only completion, and this test fails with "stable report completed
// without a marker".
func TestWatcherStableReportWithoutMarkerStaysLive(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "status: in progress\n")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if records, relays := completions(t, a, f); records != 0 || relays != 0 {
		t.Fatalf("stable report completed without a marker: records=%d relays=%d", records, relays)
	}
	if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
		t.Fatalf("stable report released the job: %s %v", st, pools)
	}
	if !strings.Contains(a.Err.(*bytes.Buffer).String(), "watch-expired: job stays live; quota retained") {
		t.Fatalf("no expiry notice: %q", a.Err)
	}
}

func TestWatcherCompletesStableReportOnTerminalSignal(t *testing.T) {
	lane := func(text string) func(*testing.T, *App) {
		return func(t *testing.T, a *App) { putLaneEvent(t, a, "w:p1", text, a.Now().Add(time.Minute)) }
	}
	for name, signal := range map[string]func(*testing.T, *App){
		"READY":       lane("READY: PR open"),
		"DONE":        lane("DONE"),
		"labelled":    lane("[agent-a] READY | PR | report"),
		"same second": func(t *testing.T, a *App) { putLaneEvent(t, a, "w:p1", "READY", a.Now()) },
	} {
		t.Run(name, func(t *testing.T) {
			a, f, report := watchFixture(t)
			writeReport(t, report, "final\n")
			signal(t, a)
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if records, relays := completions(t, a, f); records != 1 || relays != 1 {
				t.Fatalf("records=%d relays=%d, want one completion", records, relays)
			}
			if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
				t.Fatalf("completed job kept its lease: %s %v", st, pools)
			}
		})
	}
}

func TestWatcherDecisionStaysLiveUntilReady(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "[worker] ESC | choose", a.Now())
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		polls++
		if polls == 3 {
			if records, _ := completions(t, a, f); records != 0 {
				t.Fatalf("ESC completed job: %d", records)
			}
			if !slices.Contains(eventKindsOf(t, a, "j"), "job.waiting_decision") {
				t.Fatal("decision not recorded")
			}
			putLaneEvent(t, a, "w:p1", "READY | done", a.Now())
		}
		return sleep(ctx, d)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if polls < 3 {
		t.Fatalf("ESC completed before READY after %d polls", polls)
	}
	if records, _ := completions(t, a, f); records != 1 {
		t.Fatalf("completions=%d", records)
	}
}

func TestWatcherDecisionIgnoresPaneActivity(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	t.Setenv("PANEWIRE_DECISION_TIMEOUT_S", "20")
	original := f.Do
	seq := 1
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","state_change_seq":%d}]}}`, seq)}
		}
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "pane" && c.Args[1] == "read" {
			return Result{Out: fmt.Sprintf("redraw %d", seq)}
		}
		return original(c)
	}
	sleep := a.Sleep
	a.Sleep = func(ctx context.Context, d time.Duration) error { seq++; return sleep(ctx, d) }
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	kinds := eventKindsOf(t, a, "j")
	if slices.Contains(kinds, "job.decision_resumed") || !slices.Contains(kinds, "job.lost") {
		t.Fatalf("activity cleared decision or avoided timeout: %v", kinds)
	}
}

func TestWatcherDecisionRecordedReply(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	answered := false
	a.DecisionReply = func(_ context.Context, pane, lane string, since time.Time) (bool, error) {
		if pane != "w:p1" || !since.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("query %s %s %v", pane, lane, since)
		}
		return answered, nil
	}
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		polls++
		if polls == 2 {
			answered = true
		}
		if polls == 4 {
			if !slices.Contains(eventKindsOf(t, a, "j"), "job.decision_resumed") {
				t.Fatal("recorded reply did not clear wait")
			}
			putLaneEvent(t, a, "w:p1", "READY", a.Now())
		}
		return sleep(ctx, d)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := completions(t, a, f); n != 1 {
		t.Fatalf("completions=%d", n)
	}
}

func TestWatcherDecisionLaterPaneEvent(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		polls++
		if polls == 2 {
			putLaneEvent(t, a, "w:p1", "progress", a.Now())
		}
		if polls == 4 {
			if !slices.Contains(eventKindsOf(t, a, "j"), "job.decision_resumed") {
				t.Fatal("later pane event did not clear wait")
			}
			putLaneEvent(t, a, "w:p1", "READY", a.Now())
		}
		return sleep(ctx, d)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := completions(t, a, f); n != 1 {
		t.Fatalf("completions=%d", n)
	}
}

func TestWatcherDecisionSurvivesRestart(t *testing.T) {
	a, _, _ := watchFixture(t)
	t.Setenv("PANEWIRE_DECISION_TIMEOUT_S", "15")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	sleep := a.Sleep
	a.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	if err := a.watch(context.Background(), []string{"j"}); err != context.Canceled {
		t.Fatalf("first watch: %v", err)
	}
	if err := sleep(context.Background(), 20*time.Second); err != nil {
		t.Fatal(err)
	}
	a.Sleep = sleep
	if err := a.watch(context.Background(), []string{"j", "--rearmed"}); err != nil {
		t.Fatal(err)
	}
	kinds := eventKindsOf(t, a, "j")
	count := 0
	for _, kind := range kinds {
		if kind == "job.waiting_decision" {
			count++
		}
	}
	if count != 1 || !slices.Contains(kinds, "job.lost") {
		t.Fatalf("waiting events=%d kinds=%v", count, kinds)
	}
}

func TestWatcherDecisionTimeoutWhenReplyLookupFails(t *testing.T) {
	a, _, _ := watchFixture(t)
	t.Setenv("PANEWIRE_DECISION_TIMEOUT_S", "15")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	a.DecisionReply = func(context.Context, string, string, time.Time) (bool, error) {
		return false, fmt.Errorf("daemon unavailable")
	}
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		polls++
		if polls > 6 {
			return context.Canceled
		}
		return sleep(ctx, d)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatalf("watch did not time out: %v", err)
	}
	events, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.lost" && str(ev.Payload, "reason") == "decision_unanswered" {
			return
		}
	}
	t.Fatal("missing decision_unanswered loss")
}

func TestWatcherPreservesEscalation(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "stopped\n")
	putEvent(t, a, "j", "job.escalate", map[string]any{"question": "why?"})
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := completions(t, a, f); n != 0 {
		t.Fatalf("escalation became completion: %d", n)
	}
	if state, pools := ledgerState(t, a, "j"); state != "released" || len(pools) != 0 {
		t.Fatalf("escalation retained quota: state=%s pools=%v", state, pools)
	}
}

func TestRearmEscalatedWatcherReleasesQuota(t *testing.T) {
	a, _, _ := watchFixture(t)
	putEvent(t, a, "j", "job.escalate", map[string]any{"question": "why?"})
	starts := 0
	if err := a.RearmWatchers(func(id string) error { starts++; return a.watch(context.Background(), []string{id, "--rearmed"}) }, func(string) {}); err != nil {
		t.Fatal(err)
	}
	state, pools := ledgerState(t, a, "j")
	if starts != 1 || state != "released" || len(pools) != 0 {
		t.Fatalf("starts=%d state=%s pools=%v", starts, state, pools)
	}
}

func TestBriefDecisionPolicy(t *testing.T) {
	a := testApp(t)
	brief, err := a.prepareBrief(Spawn{}, Seat{}, Launch{}, "task")
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"ESC asks a question and keeps the job open", "READY or DONE", "`panewire escalate` stops with a reason", "Difficulty or size is never a reason to stop"} {
		if !strings.Contains(brief, rule) {
			t.Errorf("missing policy %q", rule)
		}
	}
}

func TestWatcherDecisionTimeout(t *testing.T) {
	a, _, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	t.Setenv("PANEWIRE_DECISION_TIMEOUT_S", "15")
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	events, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.lost" && str(ev.Payload, "reason") == "decision_unanswered" {
			return
		}
	}
	t.Fatal("missing decision_unanswered loss")
}

func TestWatcherIgnoresEventsThatAreNotThisAttemptsSignal(t *testing.T) {
	lane := func(pane, text string) func(*testing.T, *App) {
		return func(t *testing.T, a *App) { putLaneEvent(t, a, pane, text, a.Now().Add(time.Minute)) }
	}
	for name, setup := range map[string]func(*testing.T, *App){
		"another pane":    lane("w:p2", "READY"),
		"no pane":         lane("", "READY"),
		"marker mid text": lane("w:p1", "Acknowledged READY, still working"),
		"longer word":     lane("w:p1", "READYING the PR"),
		"lower case":      lane("w:p1", "ready soon"),
		"before attempt":  func(t *testing.T, a *App) { putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(-time.Hour)) },
		"before reprompt": func(t *testing.T, a *App) {
			putEvent(t, a, "j", "job.escalate", map[string]any{"question": "which?"})
			putEvent(t, a, "j", "job.reprompted", map[string]any{})
		},
		"not a lane event": func(t *testing.T, a *App) {
			dir := filepath.Join(filepath.Dir(a.Jobs), "events-lane")
			if e := os.MkdirAll(dir, 0700); e != nil {
				t.Fatal(e)
			}
			writeReport(t, filepath.Join(dir, "00001-lane.event.json"), `{"kind":"job.completed","pane_id":"w:p1","text":"READY"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, f, report := watchFixture(t)
			writeReport(t, report, "status: in progress\n")
			setup(t, a)
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if records, relays := completions(t, a, f); records != 0 || relays != 0 {
				t.Fatalf("records=%d relays=%d, want no completion", records, relays)
			}
		})
	}
}

// A report still changing after the marker waits until it settles, and a rewrite
// after the completion never produces a second one.
func TestWatcherReportRewrittenAfterMarkerCompletesOnce(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "first\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		if polls++; polls == 1 {
			writeReport(t, report, "second, longer\n")
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if polls < 2 {
		t.Fatalf("completed on an unsettled report after %d polls", polls)
	}
	writeReport(t, report, "third, after completion\n")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if records, relays := completions(t, a, f); records != 1 || relays != 1 {
		t.Fatalf("records=%d relays=%d, want one completion", records, relays)
	}
}

// A signal cannot complete a job without its report: lifecycle has nothing to
// file. The deadline then behaves as it does for any live job.
func TestWatcherSignalWithoutReportStaysLiveAtDeadline(t *testing.T) {
	a, f, _ := watchFixture(t)
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if records, relays := completions(t, a, f); records != 0 || relays != 0 {
		t.Fatalf("records=%d relays=%d, want no completion", records, relays)
	}
	if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
		t.Fatalf("deadline released a live job: %s %v", st, pools)
	}
	if !strings.Contains(a.Err.(*bytes.Buffer).String(), "watch-expired") {
		t.Fatalf("no expiry notice: %q", a.Err)
	}
}

func TestWatcherFindingsResult(t *testing.T) {
	for _, tc := range []struct {
		name, body           string
		symlink              bool
		wantResult, rejected bool
	}{
		{name: "valid", body: `{"findings":[{"severity":"high"}]}`, wantResult: true},
		{name: "absent"},
		{name: "bad JSON", body: `secret invalid`, rejected: true},
		{name: "too large", body: strings.Repeat("x", 65537), rejected: true},
		{name: "symlink", symlink: true, rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, report := watchFixture(t)
			h := newFakeHK(t)
			h.claimJob("j", float64(42), "test-host")
			h.records["j"]["state"] = "spawned"
			a.JobStore = h.client()
			writeReport(t, report, "final\n")
			path := filepath.Join(filepath.Dir(report), "findings.json")
			if tc.symlink {
				if e := os.Symlink(report, path); e != nil {
					t.Fatal(e)
				}
			} else if tc.body != "" {
				writeReport(t, path, tc.body)
			}
			putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			rec := h.record(t, "j")
			result, has := rec["result"]
			if has != tc.wantResult {
				t.Fatalf("result=%v record=%v", result, rec)
			}
			if has {
				b, _ := json.Marshal(result)
				if string(b) != tc.body {
					t.Fatalf("result=%s", b)
				}
			}
			events, e := a.events("j")
			if e != nil {
				t.Fatal(e)
			}
			count := 0
			for _, ev := range events {
				if ev.Kind == "job.result_rejected" {
					count++
					b, _ := json.Marshal(ev.Payload)
					if strings.Contains(string(b), "secret invalid") {
						t.Fatalf("content leaked: %s", b)
					}
				}
			}
			if tc.rejected && !strings.Contains(a.Err.(*bytes.Buffer).String(), "watcher result rejected") {
				t.Fatal("missing rejection warning")
			}
			if (count == 1) != tc.rejected || count > 1 {
				t.Fatalf("rejections=%d", count)
			}
			if str(rec, "state") != "completed" {
				t.Fatalf("record=%v", rec)
			}
		})
	}
}

func TestWatcherLeavesDirectDoneResultUntouched(t *testing.T) {
	a, _, report := watchFixture(t)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	useFakeHK(t).Handle(hkCommentAnswer)
	writeReport(t, report, "final\n")
	direct := filepath.Join(filepath.Dir(report), "direct.json")
	writeReport(t, direct, `{"source":"direct"}`)
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", direct}); e != nil {
		t.Fatal(e)
	}
	before := len(hkCalls(h))
	writeReport(t, filepath.Join(filepath.Dir(report), "findings.json"), `{"source":"watcher"}`)
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, "j")
	if result, ok := rec["result"].(map[string]any); !ok || result["source"] != "direct" {
		t.Fatalf("record=%v", rec)
	}
	if len(hkCalls(h)) != before {
		t.Fatalf("watcher touched job store: %v", hkCalls(h))
	}
}

// The watcher reads any worker-written findings.json, so an oversized file is
// rejected after reading at most the limit, never loaded whole (hk 2927 review).
func TestReadResultBoundsTheRead(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir()) // macOS /var is a symlink
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "findings.json")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(256 << 20); err != nil { // sparse: no disk use
		t.Fatal(err)
	}
	_ = f.Close()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = readResult(path)
	runtime.ReadMemStats(&after)
	if err == nil || !strings.Contains(err.Error(), "exceeds 64 KiB") {
		t.Fatalf("err=%v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
		t.Fatalf("readResult allocated %d bytes for an oversized file", grew)
	}
}

func TestWatcherDetachedSession(t *testing.T) {
	cmd := watchCommand("panewire", "j", false)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatal("watcher must start a new session")
	}
}

func TestWatcherRearmCatchUp(t *testing.T) {
	a, f, report := watchFixture(t)
	writeReport(t, report, "final\n")
	putLaneEvent(t, a, "w:p1", "READY | report", a.Now().Add(time.Minute))
	starts := 0
	err := a.RearmWatchers(func(id string) error { starts++; return a.watch(context.Background(), []string{id}) }, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if records, _ := completions(t, a, f); starts != 1 || records != 1 {
		t.Fatalf("starts=%d completions=%d", starts, records)
	}
}

func TestRearmedWatcherWaitsForPaneBeforeDeclaringLoss(t *testing.T) {
	for _, tc := range []struct {
		name      string
		presentAt int
		wantLost  bool
	}{
		{"never restored", 0, false},
		{"restored then lost", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			f := ledgerRunner(t, a)
			seedJob(t, a, "j", "worker")
			seedLedger(t, a, "j")
			original := f.Do
			polls := 0
			f.Do = func(c Command) Result {
				if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
					polls++
					if polls == tc.presentAt {
						return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1"}]}}`}
					}
					return Result{Out: `{"result":{"agents":[]}}`}
				}
				return original(c)
			}
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "30")
			if err := a.watch(context.Background(), []string{"j", "--rearmed"}); err != nil {
				t.Fatal(err)
			}
			state, _ := ledgerState(t, a, "j")
			lost := slices.Contains(eventKindsOf(t, a, "j"), "job.lost")
			if lost != tc.wantLost || (state == "released") != tc.wantLost {
				t.Fatalf("polls=%d lost=%v state=%s", polls, lost, state)
			}
		})
	}
}

func TestWatcherLateCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rearmed bool
	}{{"rearmed", true}, {"normal", false}} {
		t.Run(tc.name, func(t *testing.T) {
			a, f, report := watchFixture(t)
			writeReport(t, report, "final\n")
			putLaneEvent(t, a, "w:p1", "READY | report", a.Now().Add(time.Minute))
			args := []string{"j"}
			if tc.rearmed {
				args = append(args, "--rearmed")
			}
			if err := a.watch(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if records, relays := completions(t, a, f); records != 1 || relays != 1 {
				t.Fatalf("records=%d relays=%d", records, relays)
			}
			events, err := a.events("j")
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range events {
				if ev.Kind == "job.completed" && (ev.Payload["late"] == true) != tc.rearmed {
					t.Errorf("completion late=%v, want %v", ev.Payload["late"], tc.rearmed)
				}
			}
			for _, call := range f.Calls {
				if call.Name == "panewire" && slices.Contains(call.Args, "job.completed") && (slices.Contains(call.Args, "--late") != tc.rearmed || slices.Contains(call.Args, "--deliver") != tc.rearmed) {
					t.Errorf("relay args=%v, rearmed=%v", call.Args, tc.rearmed)
				}
			}
			if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
				t.Fatalf("lease: %s %v", st, pools)
			}
		})
	}
}
