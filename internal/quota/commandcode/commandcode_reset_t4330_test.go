package commandcode

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// hk 4330: Command Code 7-day reset reads exactly 5 days after the reading time.
// The vendor returns windowLimits.weekly with resetAt = now + 5 days (a rolling limit),
// which when parsed into ResetsAt causes projectAtReset to see left = 5d, fraction = 2/7,
// projecting used / (2/7) = 208%. A rolling reset that moves with every reading is not
// a pace and must be marked unpaceable (ResetsAt = "").
func TestCommandcodeWeeklyResetNotPacedFromRollingWindow_t4330(t *testing.T) {
	now := time.Date(2026, 11, 12, 14, 54, 38, 537000000, time.UTC)
	// Vendor returns resetAt = now + 5 days down to the millisecond:
	resetAtMillis := now.Add(5 * 24 * time.Hour).UnixMilli()

	a := &api{replies: map[string]func() (*http.Response, error){
		"/alpha/whoami":                respond(200, fixture(t, "whoami.json")),
		"/alpha/usage/summary":         respond(200, fixture(t, "usage_summary.json")),
		"/alpha/billing/credits":       respond(200, fmt.Sprintf(`{"success":true,"data":{"windowLimits":{"weekly":{"used":20.826,"cap":35,"resetAt":%d}},"credits":{"monthlyCredits":63.48,"purchasedCredits":0,"freeCredits":0}}}`, resetAtMillis)),
		"/alpha/billing/subscriptions": respond(200, fixture(t, "subscriptions.json")),
	}}

	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil {
		t.Fatal(err)
	}

	week := bucketOf(t, observed, "7d")
	// The 7-day rolling window has no fixed reset instant; it must be marked unpaceable.
	if week.ResetsAt != "" {
		t.Fatalf("7d rolling window must be marked unpaceable (ResetsAt = %q, want empty)", week.ResetsAt)
	}
}
