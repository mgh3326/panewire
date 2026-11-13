package quotastop

import (
	"testing"
	"time"
)

// hk 4545: a paid Devin account (devin-ds41, pool devin) shows the weekly stop
// "Quota exhausted — Your weekly usage quota has been exhausted" when its paid
// window is spent. It is a quota stop of the devin pool, not of devin-free: a
// devin-free stop writes the host's free-limit record and would drop the free
// SWE-2 rungs for a paid window.
const paidWeeklyExact = "Quota exhausted — Your weekly usage quota has been exhausted"

// The paid screen's footer is not captured yet, so the prompt uses the captured
// devin footer ("SWE-2 max"): the classifier accepts only known chrome and fails
// closed on any other footer row.
// The reset wording of the paid screen is not captured yet. These cases use the
// reset grammar the shared classifier already reads (the devin free line's
// "reset in … (at …)"), so they pin the stop and its pool, not the paid wording.
func TestClassifyPaidWeeklyQuotaIsDevinPoolStop(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	reset := time.Date(2026, 11, 13, 22, 1, 0, 0, time.UTC)
	const prompt = "──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n"
	for _, tc := range []struct {
		name      string
		screen    string
		wantReset time.Time // zero: the screen names no reset
	}{
		{"exact text, idle prompt", paidWeeklyExact + "\n" + prompt, time.Time{}},
		{"exact text, reset in hours", paidWeeklyExact + ". Your limit will reset in 13 hours 9 minutes (at 22:01 UTC).\n" + prompt, reset},
		{"exact text, reset at clock", paidWeeklyExact + " (at 22:01 UTC)\n" + prompt, reset},
		{"status prefix from the live UI", "▲ Something went wrong — " + paidWeeklyExact + " · reset in 13 hours 9 minutes\n" + prompt, reset},
	} {
		got, ok := Classify(tc.screen, now, time.UTC)
		if !ok || got.Pool != "devin" {
			t.Fatalf("%s: Classify = (%+v, %v), want a devin stop (paid pool, not devin-free)", tc.name, got, ok)
		}
		if got.Choice {
			t.Fatalf("%s: the paid stop was read as a choice screen: %+v", tc.name, got)
		}
		if !got.Reset.Equal(tc.wantReset) {
			t.Fatalf("%s: reset = %v, want %v", tc.name, got.Reset, tc.wantReset)
		}
	}
}

// The paid line above an injected relay directive is found like the free line:
// the reset comes from the limit line, never from the directive's text.
func TestClassifyPaidWeeklyQuotaAboveInjectedDirective(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	reset := time.Date(2026, 11, 13, 22, 1, 0, 0, time.UTC)
	const limit = "▲ Something went wrong — " + paidWeeklyExact + " · reset in 13 hours 9 minutes (at 22:01 UTC)\n"
	const prompt = "──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n"
	screen := limit + "\n❭ [relay] reviewer asks: reset in 2 hours, upgrade at 03:00 UTC\n\n" + prompt
	got, ok := Classify(screen, now, time.UTC)
	if !ok || got.Pool != "devin" {
		t.Fatalf("Classify = (%+v, %v), want a devin stop", got, ok)
	}
	if !got.Reset.Equal(reset) {
		t.Fatalf("reset = %v, want %v (the limit line's 13 hours 9 minutes, not the directive's)", got.Reset, reset)
	}
	if got.Choice {
		t.Fatal("the directive's \"upgrade\" made the paid stop a choice screen")
	}
}

// Agent output after the paid line means the pane moved on, and a line quoted
// in a user turn is not the pane's own status: neither is a stop.
func TestClassifyPaidWeeklyQuotaRejectsAgentOutputAndQuotes(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	const prompt = "────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n"
	for name, screen := range map[string]string{
		"answer after the paid line":    paidWeeklyExact + "\nI completed the task.\n" + prompt,
		"paid line quoted in a turn":    "❭ [relay] see:\n" + paidWeeklyExact + "\n\n" + prompt,
		"paid line in a directive":      "I completed the task.\n\n❭ [relay] the pane said: " + paidWeeklyExact + "\n\n" + prompt,
		"similar but not the paid text": "Quota exhausted — Your daily usage quota has been exhausted\n" + prompt,
	} {
		if got, ok := Classify(screen, now, time.UTC); ok {
			t.Fatalf("%s: Classify = %+v, want no stop", name, got)
		}
	}
}
