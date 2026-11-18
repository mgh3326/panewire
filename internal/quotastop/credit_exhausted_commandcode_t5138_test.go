package quotastop

import (
	"os"
	"testing"
	"time"
)

// hk 5138: Command Code rejects a request on an account with no credits left.
// omp renders the provider's error as the last status row; the classifier must
// name the commandcode pool so the hub can mark that account, not the pool.
func TestCreditExhaustedCommandcodeScreen(t *testing.T) {
	b, err := os.ReadFile("../../testdata/quota-stop/omp-commandcode-credit-2026-11-18.txt")
	if err != nil {
		t.Fatal(err)
	}
	screen := string(b)
	now := time.Date(2026, 11, 18, 12, 40, 0, 0, time.UTC)

	pool, ok := CreditExhausted(screen)
	if !ok || pool != "commandcode" {
		t.Fatalf("CreditExhausted = (%q, %t), want commandcode", pool, ok)
	}
	stop, ok := Classify(screen, now, time.UTC)
	if !ok || stop.Pool != "commandcode" || !stop.CreditExhaustedAt.Equal(now) {
		t.Fatalf("Classify = (%+v, %t), want a commandcode credit stop at %v", stop, ok, now)
	}
	detected, basis, ok := Detect(screen, "", "", DevinSessionEvidence{}, now, time.UTC)
	if !ok || basis != BasisClassifier || detected.Pool != "commandcode" || detected.CreditExhaustedAt.IsZero() {
		t.Fatalf("Detect = (%+v, %q, %t), want a classifier commandcode credit stop", detected, basis, ok)
	}
}

// The pattern is the provider's sentence, not the HTTP status: an ordinary 400
// is not a credit rejection, and the same sentence quoted by a later answer is
// not the last agent activity.
func TestCreditExhaustedCommandcodeNeedsTheSentence(t *testing.T) {
	line := "400 You have insufficient credits to make this request. Please purchase more credits to continue using the service. (type=invalid_request_error"
	for _, tc := range []struct {
		name, screen string
		ok           bool
	}{
		{"any 400", "400 Bad Request: model not found (type=invalid_request_error)\n\n❯ \n", false},
		{"400 error body without the sentence", "⏺ API Error: 400 {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad request\"}}\n\n❯ \n", false},
		{"quoted by a later answer", line + "\nI will retry with a smaller prompt.\n", false},
		{"exact status row", "⏺ " + line + "\n\n❯ \n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := ""
			if tc.ok {
				want = "commandcode"
			}
			pool, ok := CreditExhausted(tc.screen)
			if ok != tc.ok || pool != want {
				t.Fatalf("CreditExhausted(%q) = (%q, %t), want (%q, %t)", tc.screen, pool, ok, want, tc.ok)
			}
		})
	}
}
