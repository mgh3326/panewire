package panewire

import (
	"testing"
	"time"
)

func TestClassifyQuotaStop(t *testing.T) {
	local := time.FixedZone("local", 9*60*60)
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		screen, pool string
		reset        time.Time
		choice       bool
	}{
		{"Reached free model rate limit … reset in 29 seconds (at 00:04 UTC)", "devin-free", now.Add(29 * time.Second), false},
		{"You've hit your weekly limit · resets Nov 1, 2am\nChoose an option: wait or upgrade", "claude", time.Date(2026, 10, 31, 17, 0, 0, 0, time.UTC), true},
		{"quota exceeded; try again at 12:39 PM", "codex", time.Date(2026, 10, 29, 3, 39, 0, 0, time.UTC), false},
		{"You've hit your session limit · resets unknown", "claude", time.Time{}, false},
	}
	for _, tc := range cases {
		got, ok := classifyQuotaStop(tc.screen, now, local)
		if !ok || got.Pool != tc.pool || !got.Reset.Equal(tc.reset) || got.Choice != tc.choice {
			t.Errorf("%q: got %+v %t", tc.screen, got, ok)
		}
	}
	if _, ok := classifyQuotaStop("try running again tomorrow", now, local); ok {
		t.Fatal("unexpected classification")
	}
}
