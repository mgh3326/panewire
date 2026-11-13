package fleet

import (
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditObservation is an operator credit entry recorded at measured, valid
// until expires, with a used share.
func creditObservation(id int64, measured, expires time.Time, used float64) shared.Observation {
	return shared.Observation{ID: id, Kind: "manual", Window: "credit", MeasuredAt: measured, ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: used, Scope: "account"}}}
}

// TestCreditWindowOfAgreesWithCreditState: the credit row reads the entry the
// JSON credit state reads. A live state takes the unexpired entry that expires
// last, even when a newer entry has already expired; an expired state takes the
// newest recorded entry.
func TestCreditWindowOfAgreesWithCreditState(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		entries  []shared.Observation
		wantUsed float64
		wantEnd  time.Time
		state    CreditState
	}{
		{
			name: "live entry beats a newer expired entry",
			entries: []shared.Observation{
				creditObservation(1, now.Add(-2*time.Hour), now.Add(7*24*time.Hour), 10),
				creditObservation(2, now.Add(-time.Hour), now.Add(-30*time.Minute), 80),
			},
			wantUsed: 10, wantEnd: now.Add(7 * 24 * time.Hour), state: CreditLive,
		},
		{
			name: "latest expiry wins among live entries",
			entries: []shared.Observation{
				creditObservation(1, now.Add(-time.Hour), now.Add(time.Hour), 90),
				creditObservation(2, now.Add(-5*time.Hour), now.Add(2*time.Hour), 20),
			},
			wantUsed: 20, wantEnd: now.Add(2 * time.Hour), state: CreditLive,
		},
		{
			name: "expired state takes the newest recorded entry",
			entries: []shared.Observation{
				creditObservation(1, now.Add(-3*time.Hour), now.Add(-2*time.Hour), 50),
				creditObservation(2, now.Add(-time.Hour), now.Add(-30*time.Minute), 80),
			},
			wantUsed: 80, wantEnd: now.Add(-30 * time.Minute), state: CreditExpired,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			latest := shared.Latest{Manual: c.entries}
			if got := CreditStateOf(latest, now); got != c.state {
				t.Fatalf("state = %q, want %q", got, c.state)
			}
			window, ok := CreditWindowOf(latest, now)
			if !ok || window.UsedPct == nil || *window.UsedPct != c.wantUsed || !window.ExpiresAt.Equal(c.wantEnd) {
				t.Fatalf("window = %+v ok=%v, want used %v expiring %v", window, ok, c.wantUsed, c.wantEnd)
			}
		})
	}
}
