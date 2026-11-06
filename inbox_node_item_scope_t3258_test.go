package panewire

import (
	"net/http"
	"strconv"
	"testing"
)

// A node credential that names no lane reads or acks a single item by id when
// the item's own lane is routed to that node. An item on another machine's
// lane stays 401, and ack --all without a lane stays refused.
func TestNodeItemScopeWithoutLane(t *testing.T) {
	forEachHubInboxStorage(t, testNodeItemScopeWithoutLane)
}

func testNodeItemScopeWithoutLane(t *testing.T, storage hubInboxStorage) {
	h, server, _ := ackStateHub(t, storage)
	relayFrom(t, h, "node-a", "lane-a", "ev-own-ack")
	relayFrom(t, h, "node-a", "lane-a", "ev-own-read")
	relayFrom(t, h, "node-b", "lane-b", "ev-other")
	nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
	operator := pw2Caller{"operator", "", ackStateOperatorToken}
	state := func(lane, eventID string) laneInboxItem {
		t.Helper()
		items, err := h.inbox.store.list(lane, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.Event.EventID == eventID {
				return item
			}
		}
		t.Fatalf("no item %s on %s", eventID, lane)
		return laneInboxItem{}
	}
	acked := func(lane, eventID string) bool { return state(lane, eventID).AckedAt != 0 }
	read := func(lane, eventID string) bool { return state(lane, eventID).ReadAt != 0 }
	itemID := func(lane, eventID string) int64 { return state(lane, eventID).ID }
	ownAck, ownRead, other := itemID("lane-a", "ev-own-ack"), itemID("lane-a", "ev-own-read"), itemID("lane-b", "ev-other")

	// (d) ack --all with no lane is refused, and acks nothing.
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/ack", `{"all":true}`); status != http.StatusUnauthorized {
		t.Fatalf("ack --all without a lane: %d %s", status, body)
	}
	if acked("lane-a", "ev-own-ack") || acked("lane-a", "ev-own-read") {
		t.Fatal("ack --all without a lane acked an item")
	}

	// (c) an item on a lane routed to another machine: 401, not acked, not read.
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/ack", `{"id":`+strconv.FormatInt(other, 10)+`}`); status != http.StatusUnauthorized {
		t.Fatalf("ack of another machine's item: %d %s", status, body)
	}
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/"+strconv.FormatInt(other, 10)+"/read", ""); status != http.StatusUnauthorized {
		t.Fatalf("read of another machine's item: %d %s", status, body)
	}
	if acked("lane-b", "ev-other") || read("lane-b", "ev-other") {
		t.Fatal("a node touched an item on another machine's lane")
	}
	// An id that names no item looks the same as another machine's item.
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/ack", `{"id":999999}`); status != http.StatusUnauthorized {
		t.Fatalf("ack of a missing item: %d %s", status, body)
	}

	// (a) ack <id> with no lane, item on its own lane.
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/ack", `{"id":`+strconv.FormatInt(ownAck, 10)+`}`); status != http.StatusOK || body != "{\"acked\":1}\n" {
		t.Fatalf("ack own item without a lane: %d %q", status, body)
	}
	if !acked("lane-a", "ev-own-ack") || acked("lane-a", "ev-own-read") {
		t.Fatal("ack <id> did not ack exactly its item")
	}

	// (b) read <id> with no lane, item on its own lane.
	if status, body := nodeA.do(t, server, http.MethodPost, "/v1/inbox/"+strconv.FormatInt(ownRead, 10)+"/read", ""); status != http.StatusOK {
		t.Fatalf("read own item without a lane: %d %s", status, body)
	}
	if !read("lane-a", "ev-own-read") || acked("lane-a", "ev-own-read") {
		t.Fatal("read <id> did not mark exactly its item read")
	}

	// The operator is unchanged: any item, no lane.
	if status, body := operator.do(t, server, http.MethodPost, "/v1/inbox/ack", `{"id":`+strconv.FormatInt(other, 10)+`}`); status != http.StatusOK {
		t.Fatalf("operator ack: %d %s", status, body)
	}
}
