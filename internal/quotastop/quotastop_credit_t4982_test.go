package quotastop

import (
	"os"
	"strings"
	"testing"
	"time"
)

// hk 4982: two stops on a spent pool were read at the pane and classified
// not_quota. The screens are the ones the CLIs wrote.
func readQuotaStopFixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../testdata/quota-stop/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// codex 0.167.0 renders the provider's own message, which spells the apostrophe
// U+2019, and its reset as a calendar date. The 2026-11-16 stop read as
// not_quota because codexSignal required the ASCII apostrophe.
func TestClassifyCodexTypographicUsageLimitT4982(t *testing.T) {
	screen := readQuotaStopFixture(t, "codex-usage-limit-2026-11-16.txt")
	kst := time.FixedZone("KST", 9*60*60)
	now := time.Date(2026, 11, 16, 20, 25, 0, 0, kst)
	stop, ok := Classify(screen, now, kst)
	if !ok || stop.Pool != "codex" {
		t.Fatalf("Classify = (%+v, %t), want a codex stop", stop, ok)
	}
	want := time.Date(2026, 11, 17, 0, 2, 0, 0, kst)
	if !stop.Reset.Equal(want) {
		t.Fatalf("reset = %v, want %v", stop.Reset, want)
	}
	// The same sentence with the ASCII apostrophe stays a stop.
	ascii := strings.ReplaceAll(screen, "’", "'")
	if stop, ok := Classify(ascii, now, kst); !ok || stop.Pool != "codex" || !stop.Reset.Equal(want) {
		t.Fatalf("Classify(ASCII) = (%+v, %t), want the same codex stop", stop, ok)
	}
}

// omp 400 credit rejection: the node's classifier had no credit branch, so the
// screen read not_quota; the harness writes its diagnostic path under the
// rejection, which the hub's activity rule counted as later output.
func TestClassifyOMPAnthropicCreditT4982(t *testing.T) {
	screen := readQuotaStopFixture(t, "omp-anthropic-credit-2026-11-17.txt")
	now := time.Date(2026, 11, 17, 0, 51, 0, 0, time.UTC)

	stop, ok := Classify(screen, now, time.UTC)
	if !ok || stop.Pool != "anthropic-api" || !stop.CreditExhaustedAt.Equal(now) {
		t.Fatalf("Classify = (%+v, %t), want an anthropic-api credit stop at %v", stop, ok, now)
	}
	stop, basis, ok := Detect(screen, "", "", DevinSessionEvidence{}, now, time.UTC)
	if !ok || basis != BasisClassifier || stop.Pool != "anthropic-api" || stop.CreditExhaustedAt.IsZero() {
		t.Fatalf("Detect = (%+v, %q, %t), want a classifier anthropic-api credit stop", stop, basis, ok)
	}
	if pool, ok := CreditExhausted(screen); !ok || pool != "anthropic-api" {
		t.Fatalf("CreditExhausted = (%q, %t), want anthropic-api", pool, ok)
	}
}

// The diagnostic-line allowance stays narrow: after the rejection, any other
// row is the answer the activity rule refuses, and an unknown screen is still
// not a stop.
func TestCreditScreenRefusesLaterOutputT4982(t *testing.T) {
	screen := readQuotaStopFixture(t, "omp-anthropic-credit-2026-11-17.txt")
	for name, tail := range map[string]string{
		"an answer after the diagnostic":   "I will retry with a smaller prompt.\n",
		"an answer without the diagnostic": "The request failed.\n",
	} {
		s := screen + tail
		if stop, ok := Classify(s, time.Now(), time.UTC); ok {
			t.Fatalf("%s: Classify = (%+v, true), want not_quota", name, stop)
		}
		if pool, ok := CreditExhausted(s); ok {
			t.Fatalf("%s: CreditExhausted = (%q, true), want not_quota", name, pool)
		}
	}
	if stop, ok := Classify("the build finished and the tests pass\n", time.Now(), time.UTC); ok {
		t.Fatalf("unknown text became a stop: %+v", stop)
	}
}
