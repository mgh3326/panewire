package shared

import (
	"testing"
	"time"
)

// A recorded free-tier limit is published over the stored paid reading as it
// stands, without spending a provider measurement: the record is evidence from
// a pane, and a measurement neither learns it nor is guaranteed to finish
// before the next placement (hk 4622).
func TestFreeOnlyPublicationKeepsTheStoredReading_T4622(t *testing.T) {
	hk := newFakeHK(t)
	storedAt := testNow.Add(-time.Minute)
	hk.latest = stored(storedAt, 30)
	calls := 0
	reading, err := newFreeFlow(freeState(FreeExhausted, at(3*time.Hour))).Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&calls))
	if err != nil || calls != 0 {
		t.Fatalf("reading %+v err=%v measurements=%d, want no measurement", reading, err, calls)
	}
	if reading.Free == nil || reading.Free.State != FreeExhausted || !reading.Free.ResetsAt.Equal(*at(3 * time.Hour)) {
		t.Fatalf("published free bucket %+v", reading.Free)
	}
	if !reading.MeasuredAt.Equal(storedAt) || len(reading.Buckets) != 1 || reading.Buckets[0].UsedPct != 30 {
		t.Fatalf("the stored paid reading must be published unchanged: %+v", reading)
	}
	if len(hk.pub) != 1 {
		t.Fatalf("publications = %d, want 1", len(hk.pub))
	}
	free := hk.pub[0]["free_bucket"].(map[string]any)
	if free["state"] != FreeExhausted || free["resets_at"] != "2026-11-05T15:00:00Z" {
		t.Fatalf("published free bucket %v", free)
	}
	buckets := hk.pub[0]["buckets"].([]any)
	if len(buckets) != 1 || buckets[0].(map[string]any)["used_pct"] != float64(30) {
		t.Fatalf("published buckets %v", buckets)
	}
}

// A stored reading too old to republish still needs a measurement: the free
// record rides on a current paid reading, it does not replace one.
func TestFreeOnlyPublicationMeasuresWhenTheStoredReadingIsStale_T4622(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	calls := 0
	reading, err := newFreeFlow(freeState(FreeExhausted, at(3*time.Hour))).Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&calls))
	if err != nil || calls != 1 {
		t.Fatalf("reading %+v err=%v measurements=%d, want 1", reading, err, calls)
	}
	if reading.Free == nil || reading.Free.State != FreeExhausted {
		t.Fatalf("published free bucket %+v", reading.Free)
	}
}
