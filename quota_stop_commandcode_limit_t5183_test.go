package panewire

import (
	"os"
	"testing"
	"time"
)

// hk 5183: omp's Command Code 5-hour limit row feeds the existing quota-stop
// manager. The case: an implementer started 13:56 KST with 6h expected, the row
// resetting at 08:31:56Z (17:31 KST). Priority 1 is urgent and is handed over;
// priority 3 waits, because the reset falls before start + expected (19:56 KST).
func TestCommandcodeLimitStopDecisionT5183(t *testing.T) {
	raw, err := os.ReadFile("testdata/quota-stop/omp-commandcode-limit-2026-11-18.txt")
	if err != nil {
		t.Fatal(err)
	}
	kst := time.FixedZone("KST", 9*60*60)
	started := time.Date(2026, 11, 18, 13, 56, 0, 0, kst)
	now := time.Date(2026, 11, 18, 16, 0, 0, 0, kst)
	stop, ok := classifyQuotaStop(string(raw), now, kst)
	if !ok || stop.Pool != "commandcode" || !stop.Reset.Equal(time.Date(2026, 11, 18, 8, 31, 56, 856000000, time.UTC)) {
		t.Fatalf("classify = (%+v, %t), want a commandcode stop resetting 08:31:56.856Z", stop, ok)
	}
	policy, ok := parseQuotaPolicy("# Policy\n\n## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue (resume)\"\n")
	if !ok {
		t.Fatal("test policy did not parse")
	}
	for _, tc := range []struct {
		priority      int
		decision, why string
	}{
		{1, "handover", "urgent"},
		{3, "wait", "reset_within_expected"},
	} {
		got, reason := quotaDecisionJob(policy, true, tc.priority, 6*time.Hour, started, 0, stop, now)
		if got != tc.decision || reason != tc.why {
			t.Errorf("priority %d: quotaDecisionJob = (%s, %s), want (%s, %s)", tc.priority, got, reason, tc.decision, tc.why)
		}
	}
}
