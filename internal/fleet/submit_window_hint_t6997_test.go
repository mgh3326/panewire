package fleet

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// hk 6997: a submitted spawn's node budget is its attempt deadline. The
// SUBMIT_WINDOW_UNAVAILABLE refusal and its table hint must name that budget
// and the replay for a fresh attempt deadline; hub_wait is named only for a
// via = "hub" host, whose sync hop is the one path that reads it.
func TestSubmitWindowRefusalNamesAttemptDeadline(t *testing.T) {
	a, _ := claudeDeliveryApp(t, func(*claudeDelivery) string { return claudeScreenEmpty() }, idleStatus)
	// A budget of 100 ms is below the reserve, so the refusal fires before the brief.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := a.requireSubmitWindow(ctx)
	if code(err) != exitConfig || !strings.Contains(err.Error(), "SUBMIT_WINDOW_UNAVAILABLE") {
		t.Fatalf("err=%v, want the SUBMIT_WINDOW_UNAVAILABLE refusal", err)
	}
	if !strings.Contains(err.Error(), "the attempt deadline of a submitted spawn (deadline_seconds)") {
		t.Fatalf("refusal %q does not name the attempt deadline", err)
	}
	if strings.Contains(err.Error(), "raise hub_wait for this host") {
		t.Fatalf("refusal %q still tells a submitted spawn to raise hub_wait", err)
	}
	fix := spawnRefusalFix(err)
	for _, want := range []string{"attempt deadline", "deadline_seconds", "expires_at", "how long the request waited in the queue", "fresh attempt deadline", `via = "hub" host's budget is its hub_wait`} {
		if !strings.Contains(fix, want) {
			t.Fatalf("hint %q does not contain %q", fix, want)
		}
	}
	if strings.HasPrefix(fix, "Raise hub_wait") {
		t.Fatalf("hint %q leads with raising hub_wait for a submitted spawn", fix)
	}
}

// The docs/config.md hub_wait row states the same budget split as the refusal
// (docs/config.md is the contract the hint points at).
func TestHubWaitDocsRowNamesSubmittedBudget(t *testing.T) {
	b, e := os.ReadFile("../../docs/config.md")
	if e != nil {
		t.Fatal(e)
	}
	var row string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "| `hub_wait` |") {
			row = line
		}
	}
	if row == "" {
		t.Fatal("docs/config.md has no hub_wait row")
	}
	for _, want := range []string{"`panewire spawn`", "attempt deadline (`deadline_seconds`, capped at `expires_at`)", "Raise `hub_wait` only for a `via = \"hub\"` host"} {
		if !strings.Contains(row, want) {
			t.Fatalf("hub_wait row does not contain %q", want)
		}
	}
	if strings.Contains(row, "raise `hub_wait` for this host") {
		t.Fatal("hub_wait row still says to raise hub_wait for every host")
	}
}
