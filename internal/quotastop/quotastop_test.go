package quotastop

import (
	"strings"
	"testing"
	"time"
)

// A stop line may carry a ■ or ⎿ prefix (probe-observed; no captured stop
// screen has one). Both must classify like the bare line; before the prefix
// set knew them, the line was skipped and the stop produced no decision.
func TestClassifyAcceptsTUIStopPrefixes(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 25, 0, 0, time.UTC)
	reset := time.Date(2026, 11, 12, 9, 52, 0, 0, time.UTC)
	const bare = "You've hit your usage limit. Upgrade to Pro or try again at 9:52 AM."
	for _, screen := range []string{
		bare,
		"■ " + bare,
		"⎿  " + bare,
	} {
		got, ok := Classify(screen, now, time.UTC)
		if !ok {
			t.Fatalf("Classify(%q) not a quota stop", screen)
		}
		if got.Pool != "codex" {
			t.Fatalf("Classify(%q) pool = %q, want codex", screen, got.Pool)
		}
		if !got.Reset.Equal(reset) {
			t.Fatalf("Classify(%q) reset = %v, want %v", screen, got.Reset, reset)
		}
	}
}

// Detect with the herdr rule absent is the classifier alone, with its basis.
func TestDetectWithoutHerdrRuleUsesClassifier(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 25, 0, 0, time.UTC)
	stop, basis, ok := Detect("You've hit your usage limit. Upgrade to Pro or try again at 9:52 AM.", "codex", "", now, time.UTC)
	if !ok || basis != BasisClassifier || stop.Pool != "codex" || stop.Reset.IsZero() {
		t.Fatalf("Detect = (%+v, %q, %v), want codex stop with reset, basis classifier", stop, basis, ok)
	}
	if _, basis, ok := Detect("plain shell output", "codex", "", now, time.UTC); ok || basis != BasisClassifier {
		t.Fatalf("Detect on non-stop = (%q, %v), want no stop, basis classifier", basis, ok)
	}
}

// When herdr's rule fires and the text parses, the reset comes from the text
// and the basis is herdr's.
func TestDetectHerdrRuleWithParsedReset(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 25, 0, 0, time.UTC)
	reset := time.Date(2026, 11, 12, 9, 52, 0, 0, time.UTC)
	stop, basis, ok := Detect("You've hit your usage limit. Upgrade to Pro or try again at 9:52 AM.", "codex", HerdrQuotaRule, now, time.UTC)
	if !ok || basis != BasisHerdr || !stop.Reset.Equal(reset) {
		t.Fatalf("Detect = (%+v, %q, %v), want reset %v, basis %q", stop, basis, ok, reset, BasisHerdr)
	}
}

// When herdr's rule fires but the text has no parseable limit, the stop is
// still reported: pool comes from herdr's agent kind and reset stays zero, so
// the caller hands it over as reset_unknown.
func TestDetectHerdrRuleWithUnparsedTextKeepsPoolFromAgent(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 25, 0, 0, time.UTC)
	stop, basis, ok := Detect("unrelated text", "claude", HerdrQuotaRule, now, time.UTC)
	if !ok || basis != BasisHerdr || stop.Pool != "claude" || !stop.Reset.IsZero() {
		t.Fatalf("Detect = (%+v, %q, %v), want claude stop, no reset, basis %q", stop, basis, ok, BasisHerdr)
	}
	if _, _, ok := Detect("unrelated text", "unknown-agent", HerdrQuotaRule, now, time.UTC); ok {
		t.Fatal("Detect with herdr rule and unknown agent reported a stop")
	}
}

// hk 4507: the devin free-limit text sat above an injected relay directive and
// the idle prompt, so the screen's last lines were not the limit and the stop
// went undetected. The limit is found wherever it sits, as long as only the
// directive and the empty prompt follow it.
func TestClassifyFindsFreeLimitAboveInjectedDirective(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	const limit = "▲ Something went wrong — Reached free model rate limit … Your limit will reset in 13 hours 9 minutes (at 22:01 UTC). Send a message to retry\n"
	const prompt = "──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n"
	reset := time.Date(2026, 11, 13, 22, 1, 0, 0, time.UTC)
	for name, screen := range map[string]string{
		"directive":                      "❭ [pw-x] Read the brief and review the PR\n\n" + limit + "\n❭ [relay] reviewer asks: reset in 2 hours, upgrade at 03:00 UTC\n  continued directive line\n\n" + prompt,
		"directive far above the prompt": limit + "\n❭ [relay] one\n\n❭ [relay] two\n" + strings.Repeat("\n", 40) + prompt,
		"limit alone":                    limit + "\n" + prompt,
	} {
		got, ok := Classify(screen, now, time.UTC)
		if !ok || got.Pool != "devin-free" {
			t.Fatalf("%s: Classify = (%+v, %v), want a devin-free stop", name, got, ok)
		}
		if !got.Reset.Equal(reset) {
			t.Fatalf("%s: reset = %v, want %v (the limit line's 13 hours 9 minutes, not the directive's text)", name, got.Reset, reset)
		}
		if got.Choice {
			t.Fatalf("%s: the directive's \"upgrade\" made the stop a choice screen", name)
		}
	}
}

// Agent output after the limit means the pane moved on: it is not stopped.
// Injected text and an empty prompt are not agent output; anything else is.
func TestClassifyRejectsLimitFollowedByAgentOutput(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	const limit = "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes (at 08:57 UTC)\n"
	const prompt = "──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n"
	for name, screen := range map[string]string{
		"answer after directive": limit + "\n❭ [relay] go on\n\nI picked the review up and pushed the fix.\n\n" + prompt,
		"answer after limit":     limit + "\nI completed the task.\n" + prompt,
		"directive quotes limit": "I completed the task.\n\n❭ [relay] the pane said: Reached free model rate limit\n\n" + prompt,
	} {
		if got, ok := Classify(screen, now, time.UTC); ok {
			t.Fatalf("%s: Classify = %+v, want no stop", name, got)
		}
	}
	// A second limit after other output is the newest activity.
	if _, ok := Classify(limit+"\nworking again\n\n"+limit, now, time.UTC); !ok {
		t.Fatal("a limit shown after other output was not detected")
	}
}

func TestClassifyRelativeResetAddsMinutesToHours(t *testing.T) {
	now := time.Date(2026, 11, 13, 8, 52, 0, 0, time.UTC)
	for text, want := range map[string]time.Duration{
		"Reached free model rate limit · reset in 13 hours 9 minutes":     13*time.Hour + 9*time.Minute,
		"Reached free model rate limit · reset in 2 hours and 30 minutes": 2*time.Hour + 30*time.Minute,
		"Reached free model rate limit · reset in 5 minutes":              5 * time.Minute,
		"Reached free model rate limit · resets in 29 seconds":            29 * time.Second,
	} {
		got, ok := Classify(text, now, time.UTC)
		if !ok || !got.Reset.Equal(now.Add(want)) {
			t.Fatalf("Classify(%q) = (%+v, %v), want reset %v", text, got, ok, now.Add(want))
		}
	}
}
