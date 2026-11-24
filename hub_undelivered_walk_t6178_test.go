package panewire

// hk 6178 (design 4554 P1+P2): the stall sweep and the applied-report
// settlement read the fleet's undelivered rows with one walk and split them by
// owner lane, instead of one lane-scoped listing per lane. These tests count
// the requests handoffkeep sees, so a regression to the per-lane loop turns
// the counts from pages into lanes.

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// P1: with three lanes holding active work (one of them empty), one Sweep
// issues one walk's pages, not one listing per lane, and each lane's
// observation is the same as a lane-scoped walk would have produced.
func TestUndeliveredWalkSweepPagesOnceAcrossLanes(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-a","pane":"w1:p2"},"lane-empty":{"machine":"host-a","pane":"w1:p3"}}}`, client, newHubAlertLog(nil))
	t961ActiveNode(hub, "host-a",
		HubActiveJob{JobID: "t6178-a", OwnerLane: "lane-a"},
		HubActiveJob{JobID: "t6178-b", OwnerLane: "lane-b"},
		HubActiveJob{JobID: "t6178-e", OwnerLane: "lane-empty"},
	)
	recent := time.Now().UTC().Format(time.RFC3339Nano)
	// 250 rows interleaved across two lanes: more than one page of 200.
	for i := int64(1); i <= 250; i++ {
		lane := "lane-a"
		if i%2 == 0 {
			lane = "lane-b"
		}
		fake.seedUndelivered(t961LaneRow(i, lane, fmt.Sprintf("t6178-row-%d", i), recent, 1))
	}

	t961Sweep(hub)
	wantPages := 2 // 200 rows, then the 50-row tail
	if got := len(fake.queries("/v1/relay/events")); got != wantPages {
		t.Fatalf("undelivered GETs=%d, want %d (pages of one walk, not one listing per lane)", got, wantPages)
	}

	obs := hub.observeLanesUndelivered(context.Background(), []string{"lane-a", "lane-b", "lane-empty"}, time.Now())
	want := map[string]int{"lane-a": 125, "lane-b": 125, "lane-empty": 0}
	for lane, undelivered := range want {
		got, ok := obs[lane]
		if !ok || !got.observed || got.lane != lane || got.undelivered != undelivered {
			t.Fatalf("lane %s observation=%+v (present=%v), want observed with undelivered=%d", lane, got, ok, undelivered)
		}
	}
}

// A walk that fails leaves every lane it was asked about unobserved, so the
// caller cannot open or end an episode on it.
func TestUndeliveredWalkFailureLeavesLanesUnobserved(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, newHubAlertLog(nil))
	closeServer()

	obs := hub.observeLanesUndelivered(context.Background(), []string{"lane-a"}, time.Now())
	if got := obs["lane-a"]; got.observed {
		t.Fatalf("a failed walk observed lane-a: %+v", got)
	}
}

// P2: a node's applied report that names 64 lanes costs one undelivered
// listing, and the same rows close as the per-lane settlement closed.
func TestUndeliveredWalkAppliedReportSettlesWithOneListing(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, newHubAlertLog(nil))
	recent := time.Now().UTC().Format(time.RFC3339Nano)
	applied := make(map[string][]int64, 64)
	var closedIDs []int64
	for i := 0; i < 64; i++ {
		lane := fmt.Sprintf("t6178-lane-%02d", i)
		id := int64(1000 + i)
		row := t961LaneRow(id, lane, fmt.Sprintf("t6178-applied-%02d", i), recent, 1)
		row.Machine = "host-a"
		fake.seedUndelivered(row)
		applied[lane] = []int64{id}
		closedIDs = append(closedIDs, id)
	}
	// A row on a lane the report does not name stays open.
	open := t961LaneRow(5000, "t6178-unreported", "t6178-open", recent, 1)
	open.Machine = "host-a"
	fake.seedUndelivered(open)

	hub.settleNodeApplied(context.Background(), "host-a", applied)

	if got := len(fake.queries("/v1/relay/events")); got != 1 {
		t.Fatalf("applied report issued %d undelivered GETs, want 1 (one walk for 64 lanes)", got)
	}
	for _, id := range closedIDs {
		if row := t6178Row(fake, id); row == nil || row.DeliveredAt == "" {
			t.Fatalf("applied row %d was not closed", id)
		}
	}
	if row := t6178Row(fake, 5000); row == nil || row.DeliveredAt != "" {
		t.Fatalf("a row on an unreported lane was closed: %+v", row)
	}
}

// t6178Row returns handoffkeep's stored row with the given id, or nil.
func t6178Row(fake *fakeHandoffkeep, id int64) *handoffkeepRelayEvent {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, row := range fake.rows {
		if row.ID == id {
			return row
		}
	}
	return nil
}
