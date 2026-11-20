package fleet

import (
	"context"
	"testing"
	"time"
)

// The watcher's acknowledgement keeps the millisecond it adopted the wait at
// (hk 5803): QuotaWaitAckedAt reports that moment, not the whole second the
// ledger stores for created_at, so the manager can judge a bound inside a second.
func TestQuotaWaitAckKeepsItsMillisecond_t5803(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	base := time.Now().UTC().Truncate(time.Second)
	a.Now = func() time.Time { return base }
	attempt, until := requestWait(t, a, 10*time.Minute)

	at := base.Add(800 * time.Millisecond)
	a.Now = func() time.Time { return at }
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	a.adoptQuotaWait(context.Background(), j, viewLedger(j.Events), time.Time{}, at.Add(time.Minute))

	got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(at) {
		t.Fatalf("acknowledged at %s, want the adoption's millisecond %s", got.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano))
	}
}
