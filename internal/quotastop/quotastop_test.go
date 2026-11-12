package quotastop

import (
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
