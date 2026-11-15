package panewire

import (
	"os"
	"testing"
	"time"
)

// hk 4644: the captured Devin request-rate-limit stop (job hk4637-links-s2,
// profile SWE-2 Medium, 2026-11-14 10:56 UTC) is a quota stop, not
// "not_quota". The message wraps across rows under the error header; the reset
// is 8 seconds, or 10:57 UTC on the capture.
func TestClassifyQuotaStopDevinRequestRateLimit(t *testing.T) {
	screen, err := os.ReadFile("testdata/quota-stop/devin-request-rate-limit.txt")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 14, 10, 56, 52, 0, time.UTC)
	reset := time.Date(2026, 11, 14, 10, 57, 0, 0, time.UTC)
	got, ok := classifyQuotaStop(string(screen), now, time.UTC)
	if !ok || got.Pool != "devin" || !got.PoolFromProfile {
		t.Fatalf("captured screen: classifyQuotaStop = (%+v, %v), want a devin stop whose pool the running profile decides", got, ok)
	}
	if !got.Reset.Equal(reset) {
		t.Fatalf("reset = %v, want %v", got.Reset, reset)
	}
	if got.Choice {
		t.Fatalf("the stop was read as a choice screen: %+v", got)
	}
}

// hk 4644, hk 4503: a short request-rate-limit reset is waited out and the
// same pane is resumed, not handed over. The stop names no rung, so the
// running SWE-2 free profile decides the pool: devin-free.
func TestQuotaStopDevinRequestRateLimitResumesAfterShortReset(t *testing.T) {
	rig := newRoleStopRigScreen(t, "implementer", time.Hour, roleStopTask, "testdata/quota-stop/devin-request-rate-limit.txt", "devin-swe2-medium")
	rig.run()
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", d, reason)
	}
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume and no hand-over", rig.sends, len(rig.announce))
	}
	var pool, reset string
	if err := rig.store.db.QueryRow(`SELECT pool,reset FROM quota_stop_decisions WHERE decision<>'' ORDER BY created LIMIT 1`).Scan(&pool, &reset); err != nil {
		t.Fatal(err)
	}
	if pool != "devin-free" {
		t.Fatalf("pool=%s, want devin-free: the running profile is a SWE-2 free rung", pool)
	}
	if want := rig.now.Add(8 * time.Second).UTC().Format(time.RFC3339); reset != want {
		t.Fatalf("reset=%s, want %s", reset, want)
	}
}
