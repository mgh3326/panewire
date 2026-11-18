package quotastop

import (
	"os"
	"strings"
	"testing"
	"time"
)

const commandcodeLimitRow = "Error: 429 You've reached your 5-hour usage limit for your plan. Your limit resets at 2026-11-18T08:31:56.856Z. Please wait for the window to reset or upgrade your plan to continue. (type=rate_limit_error param=RATE_LIMITED)"

var commandcodeLimitReset = time.Date(2026, 11, 18, 8, 31, 56, 856000000, time.UTC)

// hk 5183: omp's Command Code window-limit row is a quota stop for the
// commandcode pool, with the reset the row names.
func TestCommandcodeLimitScreenIsAStop(t *testing.T) {
	b, err := os.ReadFile("../../testdata/quota-stop/omp-commandcode-limit-2026-11-18.txt")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 18, 4, 56, 0, 0, time.UTC)
	stop, ok := Classify(string(b), now, time.UTC)
	if !ok || stop.Pool != "commandcode" || !stop.Reset.Equal(commandcodeLimitReset) {
		t.Fatalf("Classify = (%+v, %t), want a commandcode stop resetting %v", stop, ok, commandcodeLimitReset)
	}
	if stop.Choice || !stop.CreditExhaustedAt.IsZero() {
		t.Fatalf("a window limit is neither a dialog nor a credit rejection: %+v", stop)
	}
	detected, basis, ok := Detect(string(b), "", "", DevinSessionEvidence{}, now, time.UTC)
	if !ok || basis != BasisClassifier || detected.Pool != "commandcode" || !detected.Reset.Equal(commandcodeLimitReset) {
		t.Fatalf("Detect = (%+v, %q, %t), want a classifier commandcode stop", detected, basis, ok)
	}
	if pool, ok := CreditExhausted(string(b)); ok {
		t.Fatalf("CreditExhausted = (%q, true) for a window limit", pool)
	}
}

// The pattern is omp's rendered error row: the same sentence in a final answer,
// a bare 429 or an unrelated row is not a stop, and output after the row
// cancels it.
func TestCommandcodeLimitNeedsTheErrorRow(t *testing.T) {
	now := time.Date(2026, 11, 18, 4, 56, 0, 0, time.UTC)
	wrapped := "Error: 429 You've reached your 5-hour usage limit for your plan. Your limit resets at\n2026-11-18T08:31:56.856Z. Please wait for the window to reset or upgrade your plan to\ncontinue. (type=rate_limit_error param=RATE_LIMITED)"
	for _, tc := range []struct {
		name, screen string
		ok           bool
	}{
		{"error row", commandcodeLimitRow + "\n\n❯ \n", true},
		{"error row with the retry notice", commandcodeLimitRow + "\nRetry failed after 1 attempts: Provider requested 5075005ms wait, exceeds retry.maxDelayMs\n\n❯ \n", true},
		{"status mark prefix", "⏺ " + commandcodeLimitRow + "\n\n❯ \n", true},
		{"API Error prefix", "API Error: " + strings.TrimPrefix(commandcodeLimitRow, "Error: ") + "\n\n❯ \n", true},
		{"curly apostrophe", strings.Replace(commandcodeLimitRow, "You've", "You’ve", 1) + "\n", true},
		{"weekly window", strings.Replace(commandcodeLimitRow, "5-hour", "weekly", 1) + "\n", true},
		{"wrapped over three rows", wrapped + "\n\n❯ \n", true},
		{"wrapped, then the retry notice", wrapped + "\nRetry failed after 1 attempts: Provider requested 5075005ms wait, exceeds retry.maxDelayMs\n", true},
		{"wrapped, then output", wrapped + "\nI will retry in a while.\n", false},
		{"unfinished row, then output", "Error: 429 You've reached your 5-hour usage limit for your plan.\nI will retry in a while.\n", false},
		{"quoted by a final answer", "The provider said: \"429 You've reached your 5-hour usage limit for your plan. Your limit resets at 2026-11-18T08:31:56.856Z.\"\n\n❯ \n", false},
		{"quoted mid-sentence", "I saw Error: 429 You've reached your 5-hour usage limit earlier, so I paused.\n", false},
		{"row, then an answer quoting it", commandcodeLimitRow + "\nThe row above says you've reached your 5-hour usage limit.\n", false},
		{"bare 429", "Error: 429 Too Many Requests (type=rate_limit_error)\n\n❯ \n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stop, ok := Classify(tc.screen, now, time.UTC)
			if ok != tc.ok {
				t.Fatalf("Classify(%q) ok = %t, want %t", tc.screen, ok, tc.ok)
			}
			if tc.ok && (stop.Pool != "commandcode" || !stop.Reset.Equal(commandcodeLimitReset) && !strings.Contains(tc.name, "curly")) {
				t.Fatalf("Classify(%q) = %+v, want commandcode resetting %v", tc.screen, stop, commandcodeLimitReset)
			}
		})
	}
}

// A wrapped timestamp row is joined, and a reset that does not parse still
// recognises the stop with no reset (the manager hands it over).
func TestCommandcodeLimitResetReading(t *testing.T) {
	now := time.Date(2026, 11, 18, 4, 56, 0, 0, time.UTC)
	stop, ok := Classify(strings.Replace(commandcodeLimitRow, "2026-11-18T08:31:56.856Z", "2026-13-45T99:00:00Z", 1)+"\n", now, time.UTC)
	if !ok || stop.Pool != "commandcode" || !stop.Reset.IsZero() {
		t.Fatalf("unparsable reset: Classify = (%+v, %t), want a stop with no reset", stop, ok)
	}
	stop, ok = Classify(strings.Replace(commandcodeLimitRow, "08:31:56.856Z", "17:31:56+09:00", 1)+"\n", now, time.UTC)
	want := time.Date(2026, 11, 18, 8, 31, 56, 0, time.UTC)
	if !ok || !stop.Reset.Equal(want) {
		t.Fatalf("offset reset: Classify = (%+v, %t), want reset %v", stop, ok, want)
	}
}
