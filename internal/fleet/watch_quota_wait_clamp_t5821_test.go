package fleet

import (
	"testing"
	"time"
)

// The watcher's completion deadline is extended by the wait's remainder measured
// from when the ack is committed. When the ordinary deadline passes while the
// events lock is waited for, that extension would land before until, and
// QuotaWaitAckedAt refuses an ack whose deadline does not reach the wait. The
// deadline is clamped to until, so the ack is accepted (hk 5821).
func TestQuotaWaitAckClampsTheDeadlineToTheWait_t5821(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	base := time.Now().UTC().Truncate(time.Second)
	deadline := base.Add(time.Minute)
	// Adoption begins one second before the deadline; the guard runs one second after it.
	c := installHeldClock(a, deadline.Add(-time.Second))
	attempt, until := requestWait(t, a, 10*time.Minute)

	dl, held := adoptWhileLockHeld(t, a, c, deadline, deadline.Add(time.Second))
	if !dl.Equal(until) {
		t.Fatalf("the completion deadline is %s, want the wait's until %s (an extension measured from the commit fell short of it)", dl.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano))
	}
	if !held.Equal(until) {
		t.Fatalf("the held wait is %s, want %s", held.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano))
	}
	got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
	if err != nil {
		t.Fatal(err)
	}
	if got.IsZero() {
		t.Fatal("the ack was refused: its deadline does not reach the wait")
	}
}
