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

// A weekly window holds for days: the hub takes a reset three days away, and
// judges the horizon on its own clock, not the node's (hk 5562 review).
func TestFleetStateHoldTakesAWeeklyResetAndJudgesTheHorizonOnTheHubClock_T5562(t *testing.T) {
	received := time.Now().UTC().Truncate(time.Millisecond)
	hub := &HubServer{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	hold := func(band string, measured, until time.Time) bool {
		raw, _ := json.Marshal(FleetStatePublish{Kind: fleetStateKindHold, Key: band, MeasuredAt: measured, Hold: &FleetStateHold{Until: until}})
		publish, ok := decodeFleetStatePublish(raw)
		return ok && hub.publishFleetStateFromNode("node-a", publish, received)
	}
	week := received.Add(3 * 24 * time.Hour)
	if !hold("claude", received, week) {
		t.Fatal("a reset three days away was refused")
	}
	if got := hub.fleetSnapshot(received).Cooling["claude"]; !got.Equal(week) {
		t.Fatalf("cooling of claude = %v, want %v", got, week)
	}
	// A node whose clock runs far behind cannot stretch the horizon.
	if hold("codex", received.Add(-30*24*time.Hour), received.Add(-30*24*time.Hour+7*24*time.Hour)) != true {
		t.Fatal("a stale node clock's hold that is already past must be taken and ignored")
	}
	if got := hub.fleetSnapshot(received).Cooling["codex"]; !got.IsZero() {
		t.Fatalf("a hold already past at the hub held codex until %v", got)
	}
	if hold("codex", received, received.Add(9*24*time.Hour)) {
		t.Fatal("a reset beyond the longest provider window was taken")
	}
	if got := hub.fleetSnapshot(received).Cooling["codex"]; !got.IsZero() {
		t.Fatalf("a refused hold cooled codex until %v", got)
	}
}

// The node sends a hold for the band the pool's rungs draw on: a paid Devin stop
// holds the paid band, as the hub's cooling keys it, with an absolute deadline.
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

// A known reset already past (a banner left on an idle pane, Codex's calendar
// reset of yesterday) holds nothing: it is not an unreadable reset, which would
// start the fixed window. The same goes for a reset that rounds down to the
// moment of the observation (hk 5562 review).
func TestPublishPoolHoldSkipsAKnownResetThatIsPast_T5562(t *testing.T) {
	client := quotaClient(t)
	now := client.nowUTC()
	for name, until := range map[string]time.Time{
		"yesterday":        now.Add(-24 * time.Hour),
		"same millisecond": now.Truncate(time.Millisecond).Add(500 * time.Microsecond),
	} {
		if client.PublishPoolHold("codex", until) {
			t.Errorf("%s: a hold stands", name)
		}
	}
	if len(client.events) != 0 {
		t.Fatalf("%d messages queued for resets that are past", len(client.events))
	}
}

// A reset that could not be read holds for a fixed window from the first
// observation; a repeated observation or a retry never renews it.
func TestPublishPoolHoldUnknownResetIsFixedAndNeverRenewed_T5562(t *testing.T) {
	client := quotaClient(t)
	first := client.nowUTC()
	client.PublishPoolHold("claude", time.Time{})
	event := <-client.events
	publish, _ := decodeFleetStatePublish(event.Payload)
	deadline := publish.Hold.Until
	if d := deadline.Sub(first); d < holdUnknownResetWindow-time.Second || d > holdUnknownResetWindow+time.Second {
		t.Fatalf("unknown-reset hold lasts %v from the first observation, want %v", d, holdUnknownResetWindow)
	}
	client.PublishPoolHold("claude", time.Time{})
	client.sendPoolHolds(client.nowUTC().Add(holdResendEvery))
	for len(client.events) > 0 {
		publish, _ := decodeFleetStatePublish((<-client.events).Payload)
		if !publish.Hold.Until.Equal(deadline) {
			t.Fatalf("a repeat renewed the hold to %v, want %v", publish.Hold.Until, deadline)
		}
	}
}

// A hold the hub never got is not lost: a full queue, and a reconnect, leave it
// pending and it is sent again (hk 5562 review).
func TestPoolHoldIsSentAgainAfterAFullQueueAndAReconnect_T5562(t *testing.T) {
	client := quotaClient(t)
	client.events = make(chan hubClientEvent, 1)
	client.events <- hubClientEvent{Kind: "note"} // the queue is full
	if !client.PublishPoolHold("claude", client.nowUTC().Add(time.Hour)) {
		t.Fatal("the hold was not kept")
	}
	<-client.events // the queue drains
	now := client.nowUTC()
	client.sendPoolHolds(now.Add(holdRetryEvery))
	if len(client.events) != 1 {
		t.Fatalf("the pending hold was not sent again (%d queued)", len(client.events))
	}
	<-client.events
	client.sendPoolHolds(now.Add(2 * holdRetryEvery))
	if len(client.events) != 0 {
		t.Fatal("a delivered hold was sent again before its resend interval")
	}
	client.resyncFleetState() // a new connection: the hub may have lost it
	client.sendPoolHolds(now.Add(3 * holdRetryEvery))
	if len(client.events) != 1 {
		t.Fatal("a reconnect did not send the standing hold again")
	}
}
