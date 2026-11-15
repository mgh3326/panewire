package panewire

import (
	"testing"
	"time"
)

// The store keeps one reading per host and account, so a delayed exhausted
// reading must not replace a newer available one for the same host: the
// supersession check reads exactly that value.
func TestFleetStateKeepsTheNewestReadingForAHost(t *testing.T) {
	hub, _ := fsHub(t, "")
	store := hub.stateStore()
	now := time.Now().UTC().Truncate(time.Second)
	fresh := FleetStateMessage{Host: "node-a", Kind: fleetStateKindQuota, Key: fsAccountKey, MeasuredAt: now, ReceivedAt: now,
		Quota: &FleetStateQuota{AccountKey: fsAccountKey, Pool: "claude", Free: &FleetStateFree{State: "available"}}}
	if !store.publish(fresh, 0) {
		t.Fatal("the first reading was not recorded")
	}
	staleReset := now.Add(time.Hour)
	stale := FleetStateMessage{Host: "node-a", Kind: fleetStateKindQuota, Key: fsAccountKey, MeasuredAt: now.Add(-time.Minute), ReceivedAt: now,
		Quota: &FleetStateQuota{AccountKey: fsAccountKey, Pool: "claude", Free: &FleetStateFree{State: "exhausted", ResetsAt: &staleReset}}}
	if store.publish(stale, 0) {
		t.Fatal("an older reading replaced a newer one for the same host")
	}
	latest, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey)
	if !ok || latest.Quota == nil || latest.Quota.Free == nil || latest.Quota.Free.State != "available" {
		t.Fatalf("stored reading = %+v, want the newer available one", latest.Quota)
	}
}
