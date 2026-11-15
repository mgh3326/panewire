package panewire

import (
	"testing"
	"time"
)

func TestStallSafetyLimitConfiguration(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 30 * time.Minute},
		{[]string{"--stall-safety-limit", "42m"}, 42 * time.Minute},
	} {
		d, code, err := newDaemonForCLI(tc.args, daemonCLIDeps{})
		if err != nil || code != ExitOK || d.cfg.StallSafetyLimit != tc.want {
			t.Fatalf("args=%v daemon=%v code=%d err=%v", tc.args, d, code, err)
		}
	}
	if d, code, err := newDaemonForCLI([]string{"--stall-safety-limit", "0s"}, daemonCLIDeps{}); d != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("zero limit: daemon=%v code=%d err=%v", d, code, err)
	}
}
