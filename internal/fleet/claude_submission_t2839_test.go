package fleet

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	claudeTestBrief = "Your brief is the file /work/inbox/jobs/job/brief-delivered.md (9945 bytes, sha256 a3f67ab7fbdd). Read the whole file now, then follow it exactly; it is your only instruction."
	claudeTestRule  = "────────────────────────────────────────"
)

func claudeBanner() string {
	return " ▐▛███▛█   Claude Code v2.1.318\n▝▜██████▀  Haiku 5.5 with medium effort\n ▝▝   ▝▝   /work/project\n"
}

func claudeScreenEmpty() string {
	return claudeBanner() + claudeTestRule + "\n❯ \n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
}

// The screen of a failed seat: the pointer wrapped inside the composer.
func claudeScreenBriefInComposer() string {
	return claudeBanner() + claudeTestRule + "\n❯ Your brief is the file /work/inbox/jobs/job/brief-delivered.md (9945 bytes, sha256 a3f67ab7fbdd). Read the\n  whole file now, then follow it exactly; it is your only instruction.\n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
}

// The screen of the seat that started its turn just after the deadline: the
// brief is history above an empty composer while the status has not moved yet.
func claudeScreenSubmitted() string {
	return claudeBanner() + "❯ Your brief is the file /work/inbox/jobs/job/brief-delivered.md (9945 bytes, sha256 a3f67ab7fbdd). Read the whole\n  file now, then follow it exactly; it is your only instruction.\n· Cultivating…\n" + claudeTestRule + "\n❯\n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
}

type claudeDelivery struct {
	pastes, returns, reads int
	promptAtRead           int
	stderr                 bytes.Buffer
}

// claudeDeliveryApp scripts a pane: screen and status answer by read count.
func claudeDeliveryApp(t *testing.T, screen func(d *claudeDelivery) string, status func(d *claudeDelivery) string) (*App, *claudeDelivery) {
	t.Helper()
	a := testApp(t)
	d := &claudeDelivery{promptAtRead: -1}
	a.Err = &d.stderr
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent prompt"):
			d.pastes++
			d.promptAtRead = d.reads
		case strings.Contains(args, "send-keys"):
			d.returns++
		case strings.HasPrefix(args, "agent list"):
			return Result{Out: `{"agents":[{"pane_id":"w:p1","agent":"claude","agent_status":"` + status(d) + `"}]}`}
		case strings.HasPrefix(args, "pane read"):
			d.reads++
			return Result{Out: screen(d)}
		}
		return Result{}
	}
	return a, d
}

func idleStatus(*claudeDelivery) string { return "idle" }

// A slow host submits later than the old 30 polls: the pane stayed healthy and
// was closed by the deadline, not by any fault of its own.
func TestClaudeBriefLandsWhenSubmissionOutlastsOldWindow(t *testing.T) {
	var promptRead int
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		promptRead = d.promptAtRead
		if d.reads-promptRead < 45 {
			return claudeScreenBriefInComposer()
		}
		return claudeScreenEmpty()
	}, func(d *claudeDelivery) string {
		if d.pastes > 0 && d.reads-d.promptAtRead >= 45 {
			return "working"
		}
		return "idle"
	})
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("healthy slow pane refused: %v (returns=%d)", err, d.returns)
	}
	if d.pastes != 1 {
		t.Fatalf("brief pasted %d times", d.pastes)
	}
}

// A lost Enter must not wait 15 seconds for the only rescue.
func TestClaudeReturnRepeatsWhileComposerHoldsBrief(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		if d.returns < 3 {
			return claudeScreenBriefInComposer()
		}
		return claudeScreenSubmitted()
	}, idleStatus)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("composer rescue failed: %v", err)
	}
	if d.pastes != 1 || d.returns != 3 {
		t.Fatalf("pastes=%d returns=%d", d.pastes, d.returns)
	}
}

func TestClaudeSubmittedTurnIsProofWhileStatusLags(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		return claudeScreenSubmitted()
	}, idleStatus)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("visible turn was not proof: %v", err)
	}
	if d.returns != 0 {
		t.Fatalf("returns=%d after proof", d.returns)
	}
}

// herdr reported idle over a pane that had drawn nothing but the launch command.
// Once the TUI draws, the brief is in the composer and the retries reach it.
func TestClaudeBriefSurvivesLateDrawingTUI(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		switch {
		case d.reads <= 50:
			return "project % claude --model claude-haiku-5-5 --effort high\n"
		case d.returns == 0:
			return claudeScreenBriefInComposer()
		}
		return claudeScreenSubmitted()
	}, idleStatus)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("late TUI refused: %v", err)
	}
	if d.pastes != 1 || d.returns != 1 {
		t.Fatalf("pastes=%d returns=%d", d.pastes, d.returns)
	}
}

// hk 2841: the same load hits every harness, so the window is not Claude's alone.
func TestBriefSubmitWindowAppliesToEveryKind(t *testing.T) {
	for _, kind := range []string{"codex", "omp", "cmd", "grok"} {
		t.Run(kind, func(t *testing.T) {
			a, _ := claudeDeliveryApp(t, func(d *claudeDelivery) string { return "Working on the task\n" }, func(d *claudeDelivery) string {
				if d.reads >= 60 {
					return "working"
				}
				return "idle"
			})
			if err := a.deliver(t.Context(), kind, "w:p1", claudeTestBrief); err != nil {
				t.Fatalf("slow %s pane refused: %v", kind, err)
			}
		})
	}
}

// hk 2841: the operator sets both numbers per host.
func TestBriefSubmitWindowIsConfigurable(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		return claudeScreenBriefInComposer()
	}, idleStatus)
	a.Config.Local = map[string]any{"brief_submit_timeout_ms": int64(40000), "brief_submit_retry_ms": int64(10000)}
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); code(err) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v", err)
	}
	if polls := d.reads - d.promptAtRead; polls != 40 {
		t.Fatalf("polled %d times with a 40s window", polls)
	}
	if d.returns != 3 {
		t.Fatalf("returns=%d, want one per 10s and none on the last poll", d.returns)
	}
}

// The window is time, not polls: on a loaded host one poll takes longer than 1s.
func TestBriefSubmitWindowCountsElapsedTime(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string { return claudeScreenEmpty() }, idleStatus)
	clock := a.Now()
	a.Now = func() time.Time { return clock }
	a.Sleep = func(_ context.Context, wait time.Duration) error { clock = clock.Add(wait + 3*time.Second); return nil }
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); code(err) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v", err)
	}
	if polls := d.reads - d.promptAtRead; polls > 31 {
		t.Fatalf("polled %d times; 4s per poll fills 120s in 30", polls)
	}
}

func TestBriefSubmitConfigIsValidated(t *testing.T) {
	for name, test := range map[string]struct {
		local string
		ok    bool
	}{
		"defaults":         {"", true},
		"raised":           {"brief_submit_timeout_ms = 300000\nbrief_submit_retry_ms = 5000", true},
		"timeout too low":  {"brief_submit_timeout_ms = 1000", false},
		"timeout too high": {"brief_submit_timeout_ms = 700000", false},
		"retry too low":    {"brief_submit_retry_ms = 10", false},
		"retry not below":  {"brief_submit_timeout_ms = 30000\nbrief_submit_retry_ms = 30000", false},
		"not an integer":   {"brief_submit_timeout_ms = \"soon\"", false},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hosts.toml")
			if err := os.WriteFile(path, []byte("[local]\n"+test.local+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); (err == nil) != test.ok {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

// Return would answer a dialog's default choice.
func TestClaudeReturnNeverPressedWhileBlocked(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		return claudeScreenBriefInComposer()
	}, func(d *claudeDelivery) string {
		if d.pastes > 0 {
			return "blocked"
		}
		return "idle"
	})
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); code(err) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v", err)
	}
	if d.returns != 0 {
		t.Fatalf("returns=%d into a blocked pane", d.returns)
	}
}

func TestClaudeSubmissionStaysBoundedAndNamesTheComposer(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		return claudeScreenBriefInComposer()
	}, idleStatus)
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || !strings.Contains(err.Error(), "brief still in composer") {
		t.Fatalf("err=%v", err)
	}
	if d.pastes != 1 || d.returns > defaultBriefSubmitTimeoutMS/defaultBriefSubmitRetryMS {
		t.Fatalf("pastes=%d returns=%d", d.pastes, d.returns)
	}
}

func TestClaudeComposerParserRejectsLookalikes(t *testing.T) {
	for name, test := range map[string]struct {
		screen         string
		inBox, turnNew bool
	}{
		"empty composer":    {claudeScreenEmpty(), false, false},
		"brief in composer": {claudeScreenBriefInComposer(), true, false},
		"half drawn typing": {claudeBanner() + claudeTestRule + "\n❯ Your brief is the file /work/inbox/jobs/\n", true, false},
		"submitted turn":    {claudeScreenSubmitted(), false, true},
		"other text in box": {claudeBanner() + claudeTestRule + "\n❯ something else\n" + claudeTestRule + "\n", false, false},
		"selection dialog":  {"Do you trust this folder?\n❯ 1. Yes, proceed\n  2. No, exit\n", false, false},
		"shell only":        {"project % claude --model claude-haiku-5-5\n", false, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := briefInComposer("claude", test.screen, claudeTestBrief); got != test.inBox {
				t.Errorf("inComposer=%v", got)
			}
			if got := submittedBrief("claude", test.screen, claudeTestBrief); got != test.turnNew {
				t.Errorf("submitted=%v", got)
			}
		})
	}
}

// The rc76 hint told the operator to fix the brief file because the message
// contains the word "brief".
func TestSubmissionRefusalHintNamesTheCapture(t *testing.T) {
	hint := spawnRefusalFix(fail(exitSubmitUnconfirmed, "BRIEF_SUBMISSION_UNCONFIRMED: session=s pane=p; no working transition or submitted brief turn"))
	if strings.Contains(hint, "Pass -p") || !strings.Contains(hint, "spawn-failure-") {
		t.Fatalf("hint=%q", hint)
	}
}
