package quotastop

import (
	"testing"
	"time"
)

// hk 4683: a Choice is a real choice dialog, not advertising text. Devin's limit
// banners say "Upgrade to Max" (free model, hk 4680) or name credits; those
// words never make the stop a choice, so the wait/resume path applies.
func TestDevinLimitBannerIsNotChoiceT4683(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	const prompt = "────────────────────────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n────────────────────────────────────────\n  SWE-2 Medium\n"
	for name, tc := range map[string]struct {
		screen string
		pool   string
	}{
		"free banner, wrapped, upgrade and reset": {
			screen: " ▲ Something went wrong\n   Reached free model rate limit. Upgrade to Max for higher limits, or switch to a different model. Your limit will\n   reset in 13 minutes (at 17:01 UTC). Send a message to retry\n" + "\n ● ~/work/x\n" + prompt,
			pool:   "devin-free",
		},
		"free banner, one row, upgrade": {
			screen: "▲ Something went wrong — Reached free model rate limit. Upgrade to Max · reset in 13 minutes (at 17:01 UTC). Send a message to retry\n" + prompt,
			pool:   "devin-free",
		},
		"free banner, credits advertised": {
			screen: "▲ Something went wrong — Reached free model rate limit. Buy credits for more · reset in 5 minutes\n" + prompt,
			pool:   "devin-free",
		},
		"paid weekly, upgrade wording": {
			screen: "▲ Something went wrong — Quota exhausted — Your weekly usage quota has been exhausted. Upgrade your plan · reset in 13 hours 9 minutes\n" + prompt,
			pool:   "devin",
		},
		"request rate, switch model": {
			screen: " ▲ Something went wrong\n   Reached request rate limit. Upgrade to a higher tier, or switch to a different model. Your limit will reset in 8 seconds (at 10:57 UTC). Send a\n   message to retry\n" + prompt,
			pool:   "devin",
		},
	} {
		got, ok := Classify(tc.screen, now, time.UTC)
		if !ok || got.Pool != tc.pool {
			t.Fatalf("%s: Classify = (%+v, %v), want a %s stop", name, got, ok, tc.pool)
		}
		if got.Choice {
			t.Fatalf("%s: advertising text made a choice dialog: %+v", name, got)
		}
		if got.Reset.IsZero() {
			t.Fatalf("%s: reset lost; the banner must still wait and resume: %+v", name, got)
		}
	}
}

// Claude's own limit screens keep Choice: their option wording is a dialog.
func TestClaudeLimitMenuStaysChoiceT4683(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	for name, screen := range map[string]string{
		"options menu":        "You've hit your weekly limit · resets Nov 1, 2am\nChoose an option: wait or upgrade",
		"upgrade, wait until": "You've hit your session limit · resets 9pm (UTC) · upgrade or wait until reset",
	} {
		got, ok := Classify(screen, now, time.UTC)
		if !ok || got.Pool != "claude" || !got.Choice {
			t.Fatalf("%s: Classify = (%+v, %v), want a claude choice stop", name, got, ok)
		}
	}
}
