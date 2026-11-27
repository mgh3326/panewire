package panewire

import "testing"

// hk 6994 #2: the notice event is hub-originated and carries the hub as its host,
// as the hub's other lane events do. Killed by: an event with no Host. The inbox
// path never reads Host, so no behavior case can fail on the base; this pins the
// field the event is built with.
func TestT6994NoticeEventIsHubOriginated(t *testing.T) {
	event := staleReplayNoticeEvent("stale-replay-2026-11-26-501", "2026-11-26", t6924Design, nil, 0)
	if event.Host != "hub" {
		t.Fatalf("notice event host=%q, want %q", event.Host, "hub")
	}
	if event.EventID != "stale-replay-2026-11-26-501" || event.OwnerLane != t6924Design {
		t.Fatalf("notice event=%+v, want the pass's event id on its design lane", event)
	}
}
