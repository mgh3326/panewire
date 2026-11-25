package fleet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 6483: a loaded host reports working before its TUI draws the typed text,
// so the landing read can see an empty composer and the brief appears there a
// moment later. One settle, one composer re-read, and never landed=yes over a
// composer that still holds the brief.

type lateComposerFixture struct {
	now           time.Time
	pasted        bool
	reads         int
	returns       int
	lastScreen    string
	returnResidue []bool
	returnReads   []int
	sleeps        []time.Duration
	screen        func(*lateComposerFixture) string
	status        func(*lateComposerFixture) string
}

func lateComposerApp(t *testing.T, kind string, screen func(*lateComposerFixture) string, status func(*lateComposerFixture) string) (*App, *lateComposerFixture) {
	t.Helper()
	a := testApp(t)
	f := &lateComposerFixture{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), screen: screen, status: status}
	a.Now = func() time.Time { return f.now }
	a.Sleep = func(_ context.Context, d time.Duration) error {
		f.sleeps = append(f.sleeps, d)
		f.now = f.now.Add(d)
		return nil
	}
	a.Config.Local = map[string]any{"brief_submit_timeout_ms": int64(20000), "brief_submit_retry_ms": int64(15000)}
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		args := c.Args
		switch {
		case len(args) >= 2 && args[0] == "agent" && args[1] == "list":
			return Result{Out: fmt.Sprintf(`{"agents":[{"pane_id":"w:p1","agent_status":%q}]}`, f.status(f))}
		case len(args) >= 2 && args[0] == "pane" && args[1] == "read":
			f.reads++
			f.lastScreen = f.screen(f)
			return Result{Out: f.lastScreen}
		case len(args) >= 2 && ((args[0] == "agent" && args[1] == "prompt") || (args[0] == "pane" && args[1] == "send-text")):
			f.pasted = true
		case len(args) >= 2 && args[1] == "send-keys":
			f.returns++
			f.returnResidue = append(f.returnResidue, briefInComposer(kind, f.lastScreen, claudeTestBrief))
			f.returnReads = append(f.returnReads, f.reads)
		}
		return Result{}
	}
	return a, f
}

// workingOnceSent models herdr reporting the turn before the TUI catches up.
func workingOnceSent(f *lateComposerFixture) string {
	if !f.pasted {
		return "idle"
	}
	return "working"
}

func codexScreenEmpty() string {
	return "› Ask Codex to do anything\nContext 0%\n"
}

func codexScreenBriefInComposer() string {
	return "› " + claudeTestBrief + "\nContext 0%\n"
}

func settles(f *lateComposerFixture) int {
	n := 0
	for _, d := range f.sleeps {
		if d == briefSubmitSettle {
			n++
		}
	}
	return n
}

// The landing proof arrives on an empty composer; the brief shows up at the
// re-check. One Return submits it, and the confirmation lands only once the
// composer is clear. Without the re-check the first read would report
// landed=yes with the brief still typed in the pane.
func TestBriefLateComposerLandsOnlyAfterComposerClears(t *testing.T) {
	a, f := lateComposerApp(t, "claude", func(f *lateComposerFixture) string {
		switch {
		case f.reads <= 2:
			return claudeScreenEmpty()
		case f.reads == 3:
			return claudeScreenBriefInComposer()
		case f.returns == 0:
			return claudeScreenBriefInComposer()
		}
		return claudeScreenSubmitted()
	}, workingOnceSent)
	start := time.Now()
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("late composer refused: %v", err)
	}
	if f.returns != 1 {
		t.Fatalf("returns=%d, want the one late Return", f.returns)
	}
	if len(f.returnResidue) != 1 || !f.returnResidue[0] {
		t.Fatalf("a Return was pressed with no brief in the composer: %v", f.returnResidue)
	}
	if got := settles(f); got != 1 {
		t.Fatalf("settle sleeps=%d (%v), want exactly one %v", got, f.sleeps, briefSubmitSettle)
	}
	// The settle runs on the injected clock: 20 s of window would take 20 s of
	// wall time if the fake Sleep were not used.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("deliver took %v of wall time; the settle did not use the injected clock", elapsed)
	}
}

// The brief is gone by the re-check: nothing was left in the composer, so no
// Return may be pressed (no stray empty submit).
func TestBriefLateComposerNoReturnWhenComposerClearsAtSettle(t *testing.T) {
	a, f := lateComposerApp(t, "claude", func(*lateComposerFixture) string {
		return claudeScreenEmpty()
	}, workingOnceSent)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("clean landing refused: %v", err)
	}
	if f.returns != 0 {
		t.Fatalf("returns=%d into an empty composer", f.returns)
	}
	if got := settles(f); got != 1 {
		t.Fatalf("settle sleeps=%d (%v), want one re-check", got, f.sleeps)
	}
	if f.reads != 3 {
		t.Fatalf("reads=%d, want initial + landing + one re-check", f.reads)
	}
}

// The brief stays in the composer while herdr reports working for the whole
// window: the idle retry never fires, so one late Return rescues it, and the
// refusal still reports 76 with the composer named. A second Return would hit an
// agent that is genuinely busy.
func TestBriefLateComposerOneReturnWhileWorkingThenRefused(t *testing.T) {
	a, f := lateComposerApp(t, "claude", func(f *lateComposerFixture) string {
		if f.pasted {
			return claudeScreenBriefInComposer()
		}
		return claudeScreenEmpty()
	}, workingOnceSent)
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || !strings.Contains(err.Error(), "brief still in composer") || !strings.Contains(err.Error(), "late composer") {
		t.Fatalf("err=%v", err)
	}
	if f.returns != 1 {
		t.Fatalf("returns=%d in a working window, want exactly one", f.returns)
	}
	if f.reads < 6 {
		t.Fatalf("reads=%d: the window ended before the once-only rule was exercised", f.reads)
	}
}

// A blocked pane is waiting on a dialog that owns the keystroke. Return there
// would confirm the dialog's default (possibly approving an unseen action), so
// even a composer holding the brief gets none, and the spawn keeps today's 76.
func TestBriefLateComposerBlockedPaneGetsNoReturn(t *testing.T) {
	a, f := lateComposerApp(t, "claude", func(f *lateComposerFixture) string {
		if f.pasted {
			return claudeScreenBriefInComposer()
		}
		return claudeScreenEmpty()
	}, func(f *lateComposerFixture) string {
		if !f.pasted {
			return "idle"
		}
		return "blocked"
	})
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || !strings.Contains(err.Error(), "brief still in composer") {
		t.Fatalf("err=%v", err)
	}
	if f.returns != 0 {
		t.Fatalf("returns=%d into a blocked pane", f.returns)
	}
}

// A kind whose composer is not readable behaves as before: the landing proof is
// final, with no settle and no extra Return.
func TestBriefLateComposerSkippedForUnreadableComposers(t *testing.T) {
	a, f := lateComposerApp(t, "omp", func(*lateComposerFixture) string {
		return claudeScreenEmpty()
	}, workingOnceSent)
	if err := a.deliver(t.Context(), "omp", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("omp landing refused: %v", err)
	}
	if f.returns != 0 || settles(f) != 0 {
		t.Fatalf("returns=%d settle sleeps=%v for a kind with no composer", f.returns, f.sleeps)
	}
	if f.reads != 2 {
		t.Fatalf("reads=%d, want initial + landing only", f.reads)
	}
}

// The late Return needs the whole brief: a composer still drawing the paste is
// residue for the window, but Return there would submit truncated text.
func TestBriefLateComposerWaitsForTheWholeBrief(t *testing.T) {
	// A prefix the claude composer parser accepts as residue.
	partial := claudeBanner() + claudeTestRule + "\n❯ Your brief is the file /work/inbox/jobs/\n" + claudeTestRule + "\n"
	a, f := lateComposerApp(t, "claude", func(f *lateComposerFixture) string {
		switch {
		case f.reads <= 2:
			return claudeScreenEmpty()
		case f.reads == 3:
			return partial
		case f.returns == 0:
			return claudeScreenBriefInComposer()
		}
		return claudeScreenSubmitted()
	}, workingOnceSent)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("late composer refused: %v", err)
	}
	if f.returns != 1 {
		t.Fatalf("returns=%d, want one Return after the whole brief was drawn", f.returns)
	}
	// The Return lands on the read after the partial one, never on the partial.
	if len(f.returnReads) != 1 || f.returnReads[0] < 4 {
		t.Fatalf("the Return fired at reads %v, before the whole brief was drawn", f.returnReads)
	}
}

// The one late Return is not repeated on every poll, and the idle cadence is
// untouched: a working pane holding the brief gets exactly one.
func TestBriefLateComposerReturnIsOncePerSpawn(t *testing.T) {
	a, f := lateComposerApp(t, "codex", func(f *lateComposerFixture) string {
		if f.pasted {
			return codexScreenBriefInComposer()
		}
		return codexScreenEmpty()
	}, workingOnceSent)
	_ = a.deliver(t.Context(), "codex", "w:p1", claudeTestBrief)
	if f.returns != 1 {
		t.Fatalf("returns=%d for a busy codex pane holding the brief", f.returns)
	}
}

// Spawn level: a codex pane that reports working before its composer shows the
// brief still lands, names the late composer in its output, and records the
// re-check and the one late Return in the job.spawned receipt.
func TestSpawnReportsLateComposer(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0o700); e != nil {
		t.Fatal(e)
	}
	prompt := filepath.Join(a.Home, "brief")
	if e := os.WriteFile(prompt, []byte(strings.Repeat("Please implement and verify the requested change.\n", 20)), 0o600); e != nil {
		t.Fatal(e)
	}
	s := Spawn{CWD: cwd, Profile: "codex-sol", Prompt: prompt, Workspace: "workspace", Label: "worker-a", Job: "job-a", T: "T1", Task: "42", Role: "implementer", Host: "local", OverrideReason: "fixture", Containment: "hk:12"}
	q := &fakeQuota{LaunchValue: Launch{Profile: "codex-sol", ModelID: "example-model", Effort: "high", Grade: "A+", Gate: "default"}}
	q.GateDo = func(QuotaRequest) (string, error) { return "pool=example profile=codex-sol\n", nil }
	a.Quota = q
	useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{}}`).Handle(hkClaimAnswer)

	footer := "  >_ OpenAI Codex (v0.166.0)\n  directory: " + cwd + "\n  model: example-model high\n› Ask Codex to do anything\n\n  example-model high · " + cwd + "\n  ? for shortcuts\n"
	empty := codexScreenEmpty()

	launched, prompted := false, false
	token, pointer := "", ""
	composerReads, returns := 0, 0
	f := &fakeRunner{}
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "tab create"):
			return Result{Out: `{"result":{"root_pane":{"pane_id":"w:p1"},"tab_id":"w:t1"}}`}
		case strings.HasPrefix(args, "pane process-info"):
			if !launched {
				return Result{Out: `{"result":{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":1,"argv0":"zsh"}]}}}`}
			}
			return Result{Out: `{"result":{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"codex"}]}}}`}
		case strings.HasPrefix(args, "pane run"):
			token = c.Args[len(c.Args)-1]
			return Result{}
		case strings.HasPrefix(args, "agent start"):
			launched = true
			return Result{}
		case strings.HasPrefix(args, "agent prompt"):
			prompted = true
			pointer = c.Args[len(c.Args)-1]
			return Result{}
		case strings.HasPrefix(args, "agent send-keys"):
			returns++
			return Result{}
		case strings.HasPrefix(args, "agent list"):
			status := "idle"
			if prompted {
				status = "working"
			}
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"worker-a","agent":"codex","agent_status":%q,"cwd":%q,"state_change_seq":1}]}}`, status, cwd)}
		case strings.HasPrefix(args, "pane read"), strings.HasPrefix(args, "agent read"):
			if !launched {
				return Result{Out: token + "\n"}
			}
			if !prompted {
				return Result{Out: footer}
			}
			composerReads++
			switch {
			case composerReads == 1:
				return Result{Out: empty}
			case composerReads == 2:
				return Result{Out: "› " + pointer + "\nContext 0%\n"}
			}
			return Result{Out: "› " + pointer + "\n\n• Read the brief.\n\n› Ask Codex to do anything\n"}
		}
		return Result{}
	}
	a.Runner = f
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatalf("spawn: %v", e)
	}
	out := a.Out.(*bytes.Buffer).String()
	if !strings.Contains(out, "landed=yes") || !strings.Contains(out, "brief=late_composer") {
		t.Fatalf("spawn output does not name the late composer: %s", out)
	}
	if returns != 1 {
		t.Fatalf("returns=%d, want the one late Return", returns)
	}
	var receipt map[string]any
	for _, ev := range mustEvents(t, a, s.Job) {
		if ev.Kind == "job.spawned" {
			receipt = ev.Payload
		}
	}
	if receipt == nil {
		t.Fatal("no job.spawned receipt")
	}
	if receipt["brief_late_composer"] != true || receipt["brief_late_return"] != true {
		t.Fatalf("receipt does not record the late composer: %v", receipt)
	}
}
