package devin

import (
	"testing"
)

// oracle is what pacelever@f7a1825 makes of each fixture (devin_web.go
// parseDevin for web_*, probe screens through ParseScreen for screen_*), run
// over the very same files with `now` = 2026-11-06T12:00:00Z. The PR body
// states the same comparison.
var oracle = []struct {
	fixture, plan, errKind string
	buckets                []wantBucket
}{
	{"screen_left.txt", "", "", []wantBucket{{"1d", 60.0, "2026-11-06T15:05:00Z"}, {"7d", 30.0, "2026-11-08T16:00:00Z"}}},
	{"screen_none.txt", "", "parse_error", nil},
	{"screen_over_range.txt", "", "parse_error", nil},
	{"screen_plan.txt", "", "", []wantBucket{{"7d", 62.0, "2026-11-11T12:00:00Z"}}},
	{"screen_trust.txt", "", "", []wantBucket{{"7d", 62.0, ""}}},
	{"screen_weekly.txt", "", "", []wantBucket{{"7d", 62.0, "2026-11-12T14:00:00Z"}}},
	{"web_flat.json", "Teams", "", []wantBucket{{"1d", 25.0, "2026-11-07T00:00:00Z"}, {"7d", 55.0, "2026-11-12T16:13:20Z"}}},
	{"web_hidden_daily.json", "", "", []wantBucket{{"7d", 30.0, ""}}},
	{"web_nested.json", "", "", []wantBucket{{"1d", 40.0, "2026-11-07T00:00:00Z"}, {"7d", 70.0, "2026-11-12T16:13:20Z"}}},
	{"web_over_cap.json", "", "", []wantBucket{{"7d", 100.0, ""}}},
	{"web_unknown_shape.json", "", "parse_error", nil},
	{"web_used_limit.json", "", "", []wantBucket{{"1d", 40.0, ""}, {"7d", 77.0, ""}}},
}

func TestFixturesMatchPacelever(t *testing.T) {
	for _, want := range oracle {
		t.Run(want.fixture, func(t *testing.T) {
			var observed quotaObservation
			if want.fixture[:4] == "web_" {
				observed = parseFixtureWeb(t, fixture(t, want.fixture))
			} else {
				observed.o, observed.err = ParseScreen(fixture(t, want.fixture), testNow)
			}
			if got := kindOf(observed.err); got != want.errKind {
				t.Fatalf("error kind %q (%v), want %q", got, observed.err, want.errKind)
			}
			if want.errKind != "" {
				return
			}
			checkBuckets(t, observed.o.Buckets, want.buckets)
			if observed.o.Plan != want.plan {
				t.Fatalf("plan %q, want %q", observed.o.Plan, want.plan)
			}
		})
	}
}

func TestScreenSourceAndTrustPrompt(t *testing.T) {
	o, err := ParseScreen(fixture(t, "screen_weekly.txt"), testNow)
	if err != nil || o.Source != "cli:/usage" {
		t.Fatalf("source %q err=%v", o.Source, err)
	}
	if _, err := ParseScreen("Do you trust this folder?\r\nWeekly: 62% used\r\n", testNow); kindOf(err) != "parse_error" {
		t.Fatalf("trust prompt parsed: %v", err)
	}
}
