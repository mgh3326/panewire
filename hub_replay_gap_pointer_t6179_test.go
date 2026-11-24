package panewire

import (
	"strings"
	"testing"
	"time"
)

// TestReplayGapPointerListsCoveredRows pins hk 6179. The replay gap line
// (hub_replay_gap.go) names the listing that shows the rows it covers. Those
// rows are interrupt-class rows stored read at insert, so the unread listing
// (laneInboxUnread) omits them; the line must name the plain lane listing.
func TestReplayGapPointerListsCoveredRows(t *testing.T) {
	const lane = "lane-a"
	command := replayGapListCommand(lane)
	if command != "panewire inbox list --lane "+lane {
		t.Fatalf("replay gap listing = %q, want the plain lane listing", command)
	}
	if strings.Contains(command, "--unread") {
		t.Fatalf("replay gap listing %q carries --unread, which omits the covered interrupt rows", command)
	}
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		var covered []int64
		for i := range 3 {
			key := "gap-" + string(rune('a'+i))
			item, err := s.put(key, lane, "lane.event", "interrupt", quietEvent(key, "replayed"), now, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			covered = append(covered, item.ID)
		}
		listed, err := s.list(lane, false)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]bool{}
		for _, item := range listed {
			seen[item.ID] = true
		}
		for _, id := range covered {
			if !seen[id] {
				t.Errorf("%s does not list covered row %d", command, id)
			}
		}
	})
}
