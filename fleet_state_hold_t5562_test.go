package panewire

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

// A hold is the node's placement-only word that a provider limit stopped a pool
// band (hk 5562): the hub's cooling is set until the stop's reset, and nothing
// else happens. No reading is stored, so the hub's exhausted-pool sweep (which
// reads quota values) can never close or hand over a job because of a hold.
func TestFleetStateHoldCoolsTheBandAndStoresNoReading_T5562(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	reset := now.Add(12 * time.Minute)
	hub := &HubServer{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	raw, err := json.Marshal(FleetStatePublish{Kind: fleetStateKindHold, Key: "devin-free", MeasuredAt: now, Hold: &FleetStateHold{Until: reset}})
	if err != nil {
		t.Fatal(err)
	}
	publish, ok := decodeFleetStatePublish(raw)
	if !ok {
		t.Fatal("a hold was refused")
	}
	if !hub.publishFleetStateFromNode("node-a", publish, now) {
		t.Fatal("the hold was not taken")
	}
	snap := hub.fleetSnapshot(now)
	if got := snap.Cooling["devin-free"]; !got.Equal(reset) {
		t.Fatalf("cooling of devin-free = %v, want %v", got, reset)
	}
	if len(snap.States) != 0 {
		t.Fatalf("a hold stored a reading: %+v", snap.States)
	}
	if got := hub.fleetSnapshot(reset.Add(time.Second)).Cooling["devin-free"]; !got.IsZero() {
		t.Fatalf("the hold outlived its reset: %v", got)
	}
}

// A hold with no readable reset holds for the hub's default cooling window, and
// any pool can be held, not only the Devin free band.
func TestFleetStateHoldFromAnyPoolWithoutAResetUsesTheDefaultWindow_T5562(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hub := &HubServer{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	raw, _ := json.Marshal(FleetStatePublish{Kind: fleetStateKindHold, Key: "claude", MeasuredAt: now, Hold: &FleetStateHold{}})
	publish, ok := decodeFleetStatePublish(raw)
	if !ok || !hub.publishFleetStateFromNode("node-a", publish, now) {
		t.Fatalf("a hold with no reset was refused: %v", ok)
	}
	if got := hub.fleetSnapshot(now).Cooling["claude"]; !got.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("cooling of claude = %v, want the default 5 minutes", got)
	}
}

func TestFleetStateHoldRefusesWhatIsNotAHold_T5562(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	quota := &FleetStateQuota{AccountKey: fsAccountKey, Pool: "claude", Buckets: []FleetStateBucket{{Window: "5h"}}}
	for name, publish := range map[string]FleetStatePublish{
		"no value":          {Kind: fleetStateKindHold, Key: "claude", MeasuredAt: now},
		"reset in the past": {Kind: fleetStateKindHold, Key: "claude", MeasuredAt: now, Hold: &FleetStateHold{Until: now.Add(-time.Minute)}},
		"reset too far":     {Kind: fleetStateKindHold, Key: "claude", MeasuredAt: now, Hold: &FleetStateHold{Until: now.Add(72 * time.Hour)}},
		"bad band":          {Kind: fleetStateKindHold, Key: "no spaces here", MeasuredAt: now, Hold: &FleetStateHold{}},
		"a reading too":     {Kind: fleetStateKindHold, Key: "claude", MeasuredAt: now, Hold: &FleetStateHold{}, Quota: quota},
		"hold on a reading": {Kind: fleetStateKindQuota, Key: fsAccountKey, MeasuredAt: now, Quota: quota, Hold: &FleetStateHold{}},
	} {
		raw, _ := json.Marshal(publish)
		if _, ok := decodeFleetStatePublish(raw); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The node sends a hold for the band the pool's rungs draw on: a paid Devin stop
// holds the paid band, as the hub's cooling keys it.
func TestPublishPoolHoldQueuesAHoldForTheBand_T5562(t *testing.T) {
	client := quotaClient(t)
	reset := client.nowUTC().Add(10 * time.Minute)
	if !client.PublishPoolHold("devin", reset) {
		t.Fatal("the hold was not queued")
	}
	event := <-client.events
	if event.Kind != "state" {
		t.Fatalf("kind %q", event.Kind)
	}
	publish, ok := decodeFleetStatePublish(event.Payload)
	if !ok || publish.Kind != fleetStateKindHold || publish.Key != "devin-paid" || publish.Hold.Until.Before(reset.Add(-time.Second)) {
		t.Fatalf("published %+v (valid=%v)", publish, ok)
	}
}
