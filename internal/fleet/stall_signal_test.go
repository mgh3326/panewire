package fleet

import (
	"testing"
	"time"
)

func TestStallAttemptSignal(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"[worker] READY | report", true},
		{"[worker] ESC | question", true},
		{"progress: READY is pending", false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			a := testApp(t)
			seedJob(t, a, "j", "worker")
			putLaneEvent(t, a, "w:p1", tc.text, a.Now())
			_, got, err := a.StallAttemptState("j")
			if err != nil || got != tc.want {
				t.Fatalf("signal=%v err=%v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestStallAttemptSignalClearsOnDecisionResume(t *testing.T) {
	a := testApp(t)
	seedJob(t, a, "j", "worker")
	start := a.Now()
	putLaneEvent(t, a, "w:p1", "ESC | question", start)
	a.Now = func() time.Time { return start.Add(2 * time.Second) }
	putEvent(t, a, "j", "job.decision_resumed", map[string]any{"event_id": "e0"})
	if _, got, err := a.StallAttemptState("j"); err != nil || got {
		t.Fatalf("after decision resume: signal=%v err=%v", got, err)
	}
}
