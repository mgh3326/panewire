package panewire

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 4683: Devin's free-limit banner advertises "Upgrade to Max", which is
// text, not a choice dialog. The banner is a wait-and-resume stop, so the real
// t4680 screen must be a wait decision (not choice_dialog) when its reset is
// within the task's expected time, and the resume guard must accept the screen.
func TestDevinFreeBannerWaitsForResetT4683(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	raw, err := os.ReadFile("internal/quotastop/testdata/devin_dot_cwd_screen_t4680.txt")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.LastIndex(string(raw), " ▲ Something went wrong")
	if i < 0 {
		t.Fatal("fixture has no banner")
	}
	region := string(raw[i:])

	stop, ok := classifyQuotaStop(region, now, time.UTC)
	if !ok || stop.Pool != "devin-free" {
		t.Fatalf("classify = (%+v, %v), want a devin-free stop", stop, ok)
	}
	if stop.Choice {
		t.Fatalf("the banner's \"Upgrade\" text made a choice dialog: %+v", stop)
	}
	if want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC); !stop.Reset.Equal(want) {
		t.Fatalf("reset = %v, want %v", stop.Reset, want)
	}

	policy, ok := parseQuotaPolicy("# Policy\n\n## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue (resume)\"\n")
	if !ok {
		t.Fatal("test policy did not parse")
	}
	// Started ten minutes ago with two hours expected: the 13-minute reset is
	// within the task's expected time, so the stop is waited out.
	got, reason := quotaDecisionJob(policy, true, 3, 2*time.Hour, now.Add(-10*time.Minute), 0, stop, now)
	if got != "wait" || reason != "reset_within_expected" {
		t.Fatalf("quotaDecisionJob = (%s, %s), want (wait, reset_within_expected)", got, reason)
	}
}

// The resume guard in quota_stop_runtime.go accepts a devin-free stop only when
// the screen still classifies for the pool, is not a choice, and has a located
// composer. The t4680 screen passes all three, so the resume sends its text.
func TestDevinFreeBannerPassesResumeGuardT4683(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	raw, err := os.ReadFile("internal/quotastop/testdata/devin_dot_cwd_screen_t4680.txt")
	if err != nil {
		t.Fatal(err)
	}
	screen := string(raw[strings.LastIndex(string(raw), " ▲ Something went wrong"):])
	stop, ok := quotastop.ClassifyAgent(screen, "devin", now, time.UTC)
	_, composerOK := relayComposer("devin", screen)
	if !(ok && stop.SamePool("devin-free") && !stop.Choice && composerOK) {
		t.Fatalf("resume guard refuses the banner: classified=%v stop=%+v composer=%v", ok, stop, composerOK)
	}
}
