package fleet

import (
	"strings"
	"testing"
	"time"
)

// briefLoginScreenFixture is agy's sign-in screen: its first recorded row.
const briefLoginScreenFixture = "  Open the URL below in your browser:\n  https://example.invalid/auth\n  After authenticating, copy the code displayed in the browser and paste it below:\n  > "

// briefIdleAgyScreenFixture is an agy pane with no login and no brief in it.
const briefIdleAgyScreenFixture = "Antigravity CLI\n  > "

// A harness at its login screen gets no Return: confirmBrief refuses on the
// first poll with rc 76, without waiting out the submit window.
func TestConfirmBriefStopsOnHarnessLoginScreen(t *testing.T) {
	a, f := waitDeliveryApp(t, func(time.Duration) Result { return waitTimeout() })
	f.screen = briefLoginScreenFixture
	start := f.now
	err := a.deliver(t.Context(), "agy", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v, want rc %d", err, exitSubmitUnconfirmed)
	}
	if !strings.Contains(err.Error(), "BRIEF_SUBMISSION_UNCONFIRMED") || !strings.Contains(err.Error(), "login screen") {
		t.Fatalf("err=%v", err)
	}
	if f.waits != 1 || len(f.slices) != 1 || len(f.returns) != 0 || !f.now.Equal(start) {
		t.Fatalf("waits=%d slices=%v returns=%v elapsed=%s", f.waits, f.slices, f.returns, f.now.Sub(start))
	}
}

// An idle agy pane with no login screen still gets its Return on the retry cadence.
func TestConfirmBriefKeepsReturnsOnIdleHarnessScreen(t *testing.T) {
	var f *deliveryWaitFake
	a, fake := waitDeliveryApp(t, func(d time.Duration) Result { f.now = f.now.Add(d); return waitTimeout() })
	f = fake
	f.screen = briefIdleAgyScreenFixture
	err := a.deliver(t.Context(), "agy", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v, want rc %d", err, exitSubmitUnconfirmed)
	}
	if len(f.returns) == 0 {
		t.Fatalf("no Return sent to an idle pane; slices=%v", f.slices)
	}
}
