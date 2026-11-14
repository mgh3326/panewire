package quotastop

import (
	"os"
	"strings"
	"testing"
	"time"
)

// hk 4690 (operator decision 264): a Devin limit banner in the last block of the
// screen is a stop, whatever rows follow it inside that block. The unknown-row
// screen is the hk 4680 screen with a row no rule names ("◆ something new")
// directly under its last banner, in the banner's own block.

func devinUnknownRowScreenT4690(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/devin_unknown_row_screen.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// insertAfterLastBannerT4690 splices text directly under the last banner's
// closing sentence, inside the banner's own block (no blank row between).
func insertAfterLastBannerT4690(t *testing.T, screen, text string) string {
	t.Helper()
	const end = "Send a message to retry\n"
	i := strings.LastIndex(screen, end)
	if i < 0 {
		t.Fatal("fixture has no banner end")
	}
	i += len(end)
	return screen[:i] + text + screen[i:]
}

// Every screen read here must classify as a devin-free stop whose reset is the
// banner's own (13 minutes from 16:48 UTC, 17:01 UTC).
func TestClassifyAgentDevinBannerLastBlockT4690(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	for name, screen := range map[string]string{
		"hk 4680 screen":              devinRealScreenT4680(t),
		"hk 4680 screen, unknown row": devinUnknownRowScreenT4690(t),
	} {
		got, ok := ClassifyAgent(screen, "devin", now, time.UTC)
		if !ok || got.Pool != "devin-free" || !got.Reset.Equal(want) {
			t.Fatalf("%s: ClassifyAgent(devin) = (%+v, %v), want devin-free reset %v", name, got, ok, want)
		}
	}
}

// The hk 4644 request-rate banner, after prose and a blank row, is a devin stop
// with its own reset (8 seconds from 10:56:52 UTC, 10:57 UTC).
func TestClassifyAgentDevinRequestBannerLastBlockT4690(t *testing.T) {
	now := time.Date(2026, 11, 14, 10, 56, 52, 0, time.UTC)
	const screen = "I will fix the failing test now.\n\n" +
		" ▲ Something went wrong\n" +
		"   Reached request rate limit. Please switch to a different model or contact support. Your limit will reset in 8 seconds (at 10:57 UTC). Send a\n" +
		"   message to retry\n" +
		"\n ⏺ ~/work/hk4637-links-s2\n" +
		"────────────────────────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n────────────────────────────────────────\n  SWE-2 Medium\n"
	got, ok := ClassifyAgent(screen, "devin", now, time.UTC)
	if !ok || got.Pool != "devin" || !got.PoolFromProfile || !got.Reset.Equal(now.Add(8*time.Second)) {
		t.Fatalf("ClassifyAgent(devin) = (%+v, %v), want a devin request stop reset in 8 seconds", got, ok)
	}
}

// Rows after the banner in its own block are free, but a user turn is never
// free: a turn anywhere after the banner means the pane is no longer stopped.
// A row after a blank row that is not Devin chrome starts a block of its own,
// so the banner is no longer in the last block (hk 4690).
func TestClassifyAgentDevinBannerLastBlockRefusesT4690(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	screen := devinRealScreenT4680(t)
	for name, s := range map[string]string{
		"user turn under the banner":        insertAfterLastBannerT4690(t, screen, "❭ keep going on the fix\n"),
		"user turn after an unknown row":    insertAfterLastBannerT4690(t, screen, "◆ something new\n❭ keep going on the fix\n"),
		"unknown row in a block of its own": insertBeforeCwdRowT4680(t, screen, "◆ something new\n"),
	} {
		if got, ok := ClassifyAgent(s, "devin", now, time.UTC); ok {
			t.Fatalf("%s: ClassifyAgent = %+v, want no stop", name, got)
		}
	}
}
