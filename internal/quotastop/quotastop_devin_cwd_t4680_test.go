package quotastop

import (
	"os"
	"strings"
	"testing"
	"time"
)

// hk 4680: Devin's free-model limit on a real screen (evidence
// devin-dot-cwd-screen, saved as testdata/devin_dot_cwd_screen_t4680.txt). The
// stop's own rows are a banner that wraps onto a second row, the "●" working
// directory row, and a composer rule labelled with the first turn's text. The
// last banner is the 13-minute stop reset at 17:01 UTC.
func devinScreenRegionT4680(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/devin_dot_cwd_screen_t4680.txt")
	if err != nil {
		t.Fatal(err)
	}
	// The region starts at the last banner: the injected text above it is not
	// part of this test (see the open decision in the PR, hk 4680).
	i := strings.LastIndex(string(raw), " ▲ Something went wrong")
	if i < 0 {
		t.Fatal("fixture has no banner")
	}
	return string(raw[i:])
}

func TestClassifyDevinCwdRowGlyphsT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	const banner = " ▲ Something went wrong — Reached free model rate limit. Your limit will reset in 13 minutes (at 17:01 UTC). Send a message to retry\n"
	const rest = "\n────────────────────────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n────────────────────────────────────────\n  SWE-2 Medium\n"
	for name, row := range map[string]string{
		"⏺ path":          " ⏺ ~/work/hk4637-links-s2\n",
		"● path":          " ● ~/work/mwr-563-64c05d7-p1-1794674594\n",
		"● absolute path": " ● /home/mgh3326/work/x\n",
		"● at the margin": "● ~/work/x\n",
	} {
		got, ok := Classify(banner+row+rest, now, time.UTC)
		if !ok || got.Pool != "devin-free" || !got.Reset.Equal(want) {
			t.Fatalf("%s: Classify = (%+v, %v), want devin-free reset %v", name, got, ok, want)
		}
	}
}

// A mark outside the evidenced set, or prose on the path row, is agent output
// after the limit: no stop.
func TestClassifyDevinCwdRowRefusesOutputT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	const banner = " ▲ Something went wrong — Reached free model rate limit. Your limit will reset in 13 minutes (at 17:01 UTC). Send a message to retry\n"
	const rest = "\n────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n"
	for name, row := range map[string]string{
		"prose on the ● row":        " ● /tmp/x done, finished\n",
		"prose after the path":      " ● ~/work/x and more\n",
		"unlisted mark":             " ◆ ~/work/x\n",
		"output row after the path": " ● ~/work/x\nI retried and finished the task.\n",
	} {
		if got, ok := Classify(banner+row+rest, now, time.UTC); ok {
			t.Fatalf("%s: Classify = %+v, want no stop", name, got)
		}
	}
}

// The composer rule Devin labels with the first turn's text is chrome after a
// status, and the label is never read: a reset or "upgrade" inside it must not
// change the stop.
func TestClassifyDevinLabelledRuleT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	const banner = " ▲ Something went wrong — Reached free model rate limit. Your limit will reset in 13 minutes (at 17:01 UTC). Send a message to retry\n"
	const screen = "\n ● ~/work/x\n──────────── reset in 2 hours, upgrade now ─\n❭ Ask Devin to build features\n────────────────────────────────────────\n  SWE-2 Medium\n"
	got, ok := Classify(banner+screen, now, time.UTC)
	if !ok || got.Pool != "devin-free" || !got.Reset.Equal(time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)) {
		t.Fatalf("Classify = (%+v, %v), want devin-free reset 17:01 UTC", got, ok)
	}
	if got.Choice {
		t.Fatalf("the label's text changed the stop: %+v", got)
	}
	// Under a user turn the labelled rule is not the composer: the rows after
	// it stay agent output, so a banner placed after it is not a stop.
	const turn = "❭ [relay] go on\n──────────── label ─\n" + banner + "\n❭ Ask Devin to build features\n"
	if got, ok := Classify(turn, now, time.UTC); ok {
		t.Fatalf("a labelled rule under a user turn closed the turn: %+v", got)
	}
}

// The free-model banner wraps onto a second row indented with its body (the
// real screen: "Your limit will" / "reset in 13 minutes ..."), and the stop is
// the pane's last activity.
func TestClassifyDevinFreeBannerWrapsT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	const wrapped = " ▲ Something went wrong\n   Reached free model rate limit. Upgrade to Max for higher limits, or switch to a different model. Your limit will\n   reset in 13 minutes (at 17:01 UTC). Send a message to retry\n"
	const chrome = "\n ● ~/work/x\n────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n  SWE-2 Medium\n"
	got, ok := Classify(wrapped+chrome, now, time.UTC)
	if !ok || got.Pool != "devin-free" || !got.Reset.Equal(want) {
		t.Fatalf("Classify = (%+v, %v), want devin-free reset %v", got, ok, want)
	}
	if _, ok := Classify(wrapped+"I retried and finished the task.\n"+chrome, now, time.UTC); ok {
		t.Fatal("agent output after a wrapped free-model banner read as a stop")
	}
}

// The real screen from its last banner down: the wrapped banner, the ● row, the
// labelled composer rule and the prompt. The injected text above the banner is
// not in this region; the full screen stays an open decision (hk 4680).
func TestClassifyDevinRealScreenRegionT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	got, ok := Classify(devinScreenRegionT4680(t), now, time.UTC)
	if !ok {
		t.Fatalf("Classify = (%+v, false), want a devin-free stop", got)
	}
	if got.Pool != "devin-free" {
		t.Fatalf("pool = %q, want devin-free", got.Pool)
	}
	if !got.Reset.Equal(want) {
		t.Fatalf("reset = %v, want %v", got.Reset, want)
	}
}
