package panewire

import (
	"context"
	"encoding/json"
	"strings"
)

// A lane.event names the pane that emitted it (hk 3434), so the hub can tell a
// worker's own progress note from a design session's or the desk's on the same
// host. See docs/quiet-delivery.md "Worker results stay in a design lane's
// inbox" and docs/r21-lane-event.md.

// hubFeatureLaneEventPane: an older hub refuses a lane.event payload with an
// unknown key (decodeHubLaneEventPayload), and with it the event, so a node
// sends pane_id only on a connection whose hub advertised this.
const hubFeatureLaneEventPane = "lane_event_pane"

// stallNoticeEventIDPrefix opens the event id of a stall notice
// (stallDetectManager.sendNotification).
const stallNoticeEventIDPrefix = "stall:"

// laneEventSenderPane is the pane a lane.event names as its sender: the pane
// `panewire emit` recorded (--pane, else HERDR_PANE_ID). A node's own notice
// about a pane -- an idle-wake, a stall notice -- records the pane it is
// about, which did not send it, so it names none.
func laneEventSenderPane(job hubScannedRelayEvent) string {
	if job.NotifyType == "idle-wake" || strings.HasPrefix(job.EventID, stallNoticeEventIDPrefix) {
		return ""
	}
	return boundLaneEventPaneID(job.PaneID)
}

// laneEventsKnown reports whether this connection's hub feature state is
// known: the hub has answered (a pong) or its hub.features arrived. The hub
// queues its features at connect, before it answers, so a pong after the
// features proves the gate (hub.go handleAgent). Until then a lane.event is
// held, because relayWire would cut it for a hub that may take it whole.
func (peer *hubClientConnection) laneEventsKnown(acked bool) bool {
	return acked || peer.featuresKnown.Load()
}

// relayHeldForFeatures reports whether a relay event waits for the hub's feature
// state: a lane.event, and a final job.lost, which hubFeatureFinalLost must
// take whole. Both the scan burst and the immediate `panewire emit` path hold
// the same set (hk 5351).
func relayHeldForFeatures(event hubClientEvent) bool {
	return event.Kind == "lane.event" || event.finalLost
}

// splitRelayBurst divides a burst into the events to write now and the events
// relayHeldForFeatures holds until the hub's feature state is known.
func splitRelayBurst(events []hubClientEvent, laneKnown bool) (write, held []hubClientEvent) {
	if laneKnown {
		return events, nil
	}
	for _, event := range events {
		if relayHeldForFeatures(event) {
			held = append(held, event)
			continue
		}
		write = append(write, event)
	}
	return write, held
}

// writeRelayBurst writes the relay events scanned for this connection. Held
// lane.event records are released, so the next burst rescans them.
func (client *HubClient) writeRelayBurst(ctx context.Context, peer *hubClientConnection, laneKnown bool) error {
	write, held := splitRelayBurst(client.jobCompletionEvents(), laneKnown)
	client.releaseRelayEvents(held...)
	return client.writeRelayEvents(write, peer.relayWriter(ctx))
}

// relayWire is a relay event's wire form on this connection. The sender pane
// is added here, at write time, because the event was built before the node
// knew which hub the write would reach.
func (peer *hubClientConnection) relayWire(event hubClientEvent) any {
	if event.senderPane != "" && peer.acceptsLaneEventPane.Load() {
		event.Payload = relayPayloadOptionalString(event.Payload, "pane_id", event.senderPane)
	}
	// Gate: a hub that did not advertise hubFeatureLaneEventLong refuses text
	// over its own bound, so the cut form is sent to it (lane_event_long.go).
	if event.Kind == "lane.event" && !peer.acceptsLaneEventLong.Load() {
		event.Payload = laneEventPayloadForOldHub(event.Payload, event.sinkRecord)
	}
	// A hub without hubFeatureFinalLost takes the job.lost without its final
	// field, as an older node sends it (hk 5351).
	if event.finalLost && !peer.acceptsFinalLost.Load() {
		event.Payload = finalLostPayloadForOldHub(event.Payload)
	}
	return hubClientWireEvent(event)
}

// laneEventFromWorker reports whether a lane.event came from a worker of lane:
// the authenticated sending node's latest heartbeat lists a job in that pane,
// and the hub holds the job live (not completed, not orphaned) on that node
// with role implementer and OwnerLane == lane. The owner check makes the event lane's
// own: another design lane's worker in the same pane is not lane's. Every
// spawned implementer, reviewer and consult claims as role implementer
// (internal/fleet/spawn.go), and the hub keeps only implementer
// (normalizeHubActiveJobMetadata), so a builder is not one. An empty
// machine (a hub notice, HTTP ingress, a replay), pane (an older node) or lane
// is never a worker's.
func (h *HubServer) laneEventFromWorker(machine, pane, lane string) bool {
	if machine == "" || pane == "" || lane == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	if record == nil {
		return false
	}
	for id, active := range record.activeJobs {
		if active.Pane != pane {
			continue
		}
		if job := h.jobs[id]; job != nil && !job.Completed && !job.Orphaned && job.Node == machine && job.Pane == pane && job.Role == "implementer" && job.OwnerLane == lane {
			return true
		}
	}
	return false
}

// finalLostPayloadForOldHub is a final job.lost without its final field: the
// pre-PR payload a hub that did not advertise hubFeatureFinalLost
// decodes. An unreadable payload is sent as it is.
func finalLostPayloadForOldHub(payload json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return payload
	}
	delete(fields, "final")
	out, err := json.Marshal(fields)
	if err != nil {
		return payload
	}
	return out
}
