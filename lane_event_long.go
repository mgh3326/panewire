package panewire

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// A lane.event longer than laneEventTextLimit (2048 bytes) is kept whole and
// typed as a head (docs/lane-event-long.md). Only the hub's inbox row and the
// producer's event file hold the whole text; a pane line never exceeds the
// typed limit.

// hubFeatureLaneEventLong: a hub that advertises it keeps lane.event text up to
// laneEventTextCeiling from a node. An older hub refuses that text
// (decodeHubLaneEventPayload), and with it the event, so a node sends the cut
// form on a connection whose hub did not advertise this (laneEventPayloadForOldHub).
const hubFeatureLaneEventLong = "lane_event_long"

// laneEventIngressTooLong keeps the HTTP ingress contract: 2048 bytes for a
// pane route and 8192 for a sink route (docs/r24-http-ingress.md). Ingress is an
// operator producer, so it does not take the node-side ceiling.
func laneEventIngressTooLong(text string, sinkRoute bool) bool {
	return len(text) > laneEventTextLimitSink || (len(text) > laneEventTextLimit && !sinkRoute)
}

// laneEventRelayText is the text typed into a pane for a lane.event. Text that
// fits the typed limit is typed whole, as before. Longer text is typed as its
// head, then a marker and a pointer to the whole text in the lane inbox, so the
// line stays within laneEventTextLimit. Without an inbox row the marker alone
// says the line was cut.
func laneEventRelayText(event hubJobEventPayload) string {
	prefix := "(같은 내용이 두 번 보이면 재실행 금지) [event] " + event.OwnerLane + " :: "
	if len(event.Text) <= laneEventTextLimit {
		return prefix + event.Text
	}
	tail := "[truncated]"
	if event.inboxID != 0 {
		tail = fmt.Sprintf("[full text: panewire inbox read %d]", event.inboxID)
	}
	budget := max(laneEventTextLimit-len(prefix)-len(tail)-1, 0)
	return prefix + utf8PrefixWithin(event.Text, budget) + " " + tail
}

// withLaneInboxID returns e naming the lane inbox row that holds it whole. The
// live path sets inboxID in receiveInbox; a replay builds the event from its
// handoffkeep record, which has no row id, so the row is looked up by the
// event's relay key (the same key receiveInbox stored it under). Text that fits
// the typed line needs no pointer and is left alone.
func (h *HubServer) withLaneInboxID(e hubJobEventPayload) hubJobEventPayload {
	if e.inboxID != 0 || h.inbox == nil || len(e.Text) <= laneEventTextLimit {
		return e
	}
	if id, err := h.inbox.store.rowIDForKey(relayEventDedupeKey("lane.event", e)); err == nil {
		e.inboxID = id
	}
	return e
}

// rowIDForKey is the lane_inbox row stored under a relay key, or 0 when none.
func (s *laneInboxStore) rowIDForKey(key string) (int64, error) {
	ctx, done := s.op(s.life)
	defer done()
	var id int64
	where, args := s.eventKeyPredicate(key)
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM lane_inbox WHERE `+where), args...).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// laneEventPayloadForOldHub is the lane.event payload a node sends to a hub
// that did not advertise hubFeatureLaneEventLong. Text over the bound that hub
// accepts is cut to it and marked truncated. Nothing is refused and no retry
// loops: the hub keeps the cut text, and the producer's event file keeps the
// whole text. A sink event keeps 8192 bytes, as the old hub did.
func laneEventPayloadForOldHub(payload []byte, sink bool) []byte {
	var fields map[string]json.RawMessage
	var text string
	if json.Unmarshal(payload, &fields) != nil || json.Unmarshal(fields["text"], &text) != nil {
		return payload
	}
	limit := laneEventTextLimit
	if sink {
		limit = laneEventTextLimitSink
	}
	cut, truncated := truncateLaneEventTextAtLimit(text, limit)
	if !truncated {
		return payload
	}
	fields["text"], _ = json.Marshal(cut)
	fields["truncated"] = json.RawMessage("true")
	raw, err := json.Marshal(fields)
	if err != nil {
		return payload
	}
	return raw
}

// laneEventReadText is what `panewire inbox read` and `inbox wait` print for a
// lane.event: the whole stored text. The typed head is for a pane line only; the
// pointer names this command, so it must not print the head again.
func laneEventReadText(event hubJobEventPayload) string {
	return "(같은 내용이 두 번 보이면 재실행 금지) [event] " + event.OwnerLane + " :: " + event.Text
}

// inboxItemPrintText is the text `inbox read` and `inbox wait` print for an item.
func inboxItemPrintText(kind string, event hubJobEventPayload) string {
	if kind == "lane.event" {
		return laneEventReadText(event)
	}
	return relayTextForKind(kind, event)
}
