package fleet

import (
	"context"
	"testing"
	"time"
)

// hk 3058: through the hub, a reading measured long ago but re-confirmed
// unchanged a minute ago is fresh, the same rule as the direct read (#229).
func TestHubSharedAccountAgesByConfirmedAt(t *testing.T) {
	const key = "codex:fc823b1db7f49cd4"
	for _, tc := range []struct {
		name      string
		confirmed time.Duration // before now; 0 = none
		fresh     bool
	}{
		{name: "re-confirmed a minute ago", confirmed: time.Minute, fresh: true},
		{name: "no confirmation", fresh: false},
		{name: "confirmed past the codex window", confirmed: 10 * time.Minute, fresh: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
			now := a.Now()
			q := fsQuota("host-a", key, now.Add(-20*time.Minute), 8)
			if tc.confirmed > 0 {
				q["confirmed_at"] = now.Add(-tc.confirmed)
			}
			a.State = &fakeStateReader{snap: fsSnapshot(t, now, fsPublisher("host-a", true, now.Add(-5*time.Second)), q)}
			acc, why := a.hubSharedAccount(context.Background(), key)
			if (why == "") != tc.fresh {
				t.Fatalf("why=%q acc=%+v, want fresh=%v", why, acc, tc.fresh)
			}
			if tc.fresh && (!acc.Fresh || !acc.MeasuredAt.Equal(now.Add(-tc.confirmed))) {
				t.Fatalf("acc = %+v, want fresh as of confirmed_at", acc)
			}
		})
	}
}
