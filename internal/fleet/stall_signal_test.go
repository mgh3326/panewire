package fleet

import (
	"testing"
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
			got, err := a.StallAttemptSignalled("j")
			if err != nil || got != tc.want {
				t.Fatalf("signal=%v err=%v; want %v", got, err, tc.want)
			}
		})
	}
}
