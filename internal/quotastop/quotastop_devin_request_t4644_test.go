package quotastop

import (
	"testing"
	"time"
)

// hk 4644: Devin's request-rate-limit stop draws its message over two rows
// under a "▲ Something went wrong" header, then its working directory. The
// wrapped continuation and that status row are not later agent output: the
// stop is the pane's last activity, so it is recognised with its reset.
func TestClassifyDevinRequestRateLimit(t *testing.T) {
	now := time.Date(2026, 11, 14, 10, 56, 52, 0, time.UTC)
	reset := now.Add(8 * time.Second)
	const banner = " ▲ Something went wrong\n" +
		"   Reached request rate limit. Please switch to a different model or contact support. Your limit will reset in 8 seconds (at 10:57 UTC). Send a\n" +
		"   message to retry\n"
	const composer = " ⏺ ~/work/hk4637-links-s2\n" +
		"────────────────────────────────────────\n" +
		"❭ Ask Devin to build features, fix bugs, or work on your code\n" +
		"────────────────────────────────────────\n" +
		"  SWE-2 Medium\n"
	for name, screen := range map[string]string{
		"captured banner alone":       banner,
		"banner and idle composer":    banner + composer,
		"banner after earlier output": "I will fix the failing test now.\n\n" + banner + composer,
	} {
		got, ok := Classify(screen, now, time.UTC)
		if !ok || got.Pool != "devin" || !got.PoolFromProfile {
			t.Fatalf("%s: Classify = (%+v, %v), want a devin stop whose pool the running profile decides", name, got, ok)
		}
		if !got.Reset.Equal(reset) {
			t.Fatalf("%s: reset = %v, want %v", name, got.Reset, reset)
		}
		if got.Choice {
			t.Fatalf("%s: read as a choice screen: %+v", name, got)
		}
	}
}

// Every Devin stop sits above the working-directory row its idle composer
// prints (" ⏺ ~/work/<repo>"), so that row must be chrome for each Devin text,
// not later agent output (hk 4644).
func TestClassifyDevinStopsAboveWorktreeRow(t *testing.T) {
	now := time.Date(2026, 11, 14, 10, 56, 52, 0, time.UTC)
	const prompt = " ⏺ ~/work/hk4637-links-s2\n" +
		"────────────────────────────────────────\n" +
		"❭ Ask Devin to build features, fix bugs, or work on your code\n" +
		"────────────────────────────────────────\n" +
		"  SWE-2 Medium\n"
	for name, tc := range map[string]struct {
		limit string
		pool  string
	}{
		"free model": {
			limit: "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes (at 11:01 UTC). Send a message to retry\n",
			pool:  "devin-free",
		},
		"paid weekly": {
			limit: "▲ Something went wrong — Quota exhausted — Your weekly usage quota has been exhausted · reset in 13 hours 9 minutes\n",
			pool:  "devin",
		},
		"request rate": {
			limit: " ▲ Something went wrong\n   Reached request rate limit. Please switch to a different model or contact support. Your limit will reset in 8 seconds (at 10:57 UTC). Send a\n   message to retry\n",
			pool:  "devin",
		},
	} {
		got, ok := Classify(tc.limit+prompt, now, time.UTC)
		if !ok || got.Pool != tc.pool {
			t.Fatalf("%s: Classify = (%+v, %v), want a %s stop", name, got, ok, tc.pool)
		}
		if got.Choice {
			t.Fatalf("%s: read as a choice screen: %+v", name, got)
		}
	}
}

// Agent output after the banner still ends the stop: the continuation rule
// covers the banner body only.
func TestClassifyDevinRequestRateLimitRejectsLaterOutput(t *testing.T) {
	now := time.Date(2026, 11, 14, 10, 56, 52, 0, time.UTC)
	const banner = " ▲ Something went wrong\n" +
		"   Reached request rate limit. Please switch to a different model or contact support. Your limit will reset in 8 seconds (at 10:57 UTC). Send a\n" +
		"   message to retry\n"
	const composer = "────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n"
	for name, screen := range map[string]string{
		"answer after the banner":      banner + "I retried and finished the task.\n" + composer,
		"answer after the ⏺ row":       banner + " ⏺ ~/work/hk4637-links-s2\nI retried and finished the task.\n" + composer,
		"banner quoted in a directive": "❭ [relay] go on\n" + banner + "\n" + composer,
		"other error header":           " ▲ Something went wrong\n   connection reset by peer\n" + composer,
	} {
		if got, ok := Classify(screen, now, time.UTC); ok {
			t.Fatalf("%s: Classify = %+v, want no stop", name, got)
		}
	}
}

// A resume guard compares the stop's pool to the decision's. The screen
// cannot name the Devin rung, so both Devin pools match it.
func TestStopSamePoolForProfilePool(t *testing.T) {
	stop := Stop{Pool: "devin", PoolFromProfile: true}
	for _, pool := range []string{"devin", "devin-free"} {
		if !stop.SamePool(pool) {
			t.Fatalf("SamePool(%q) = false, want true", pool)
		}
	}
	if stop.SamePool("claude") {
		t.Fatal("SamePool(claude) = true, want false")
	}
	if named := (Stop{Pool: "claude"}); !named.SamePool("claude") || named.SamePool("devin") {
		t.Fatalf("a screen-named pool must match only itself: %+v", named)
	}
}
