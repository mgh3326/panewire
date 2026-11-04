package panewire

import (
	"encoding/json"
	"fmt"
	"time"
)

// A hub message the node cannot parse used to vanish: the read loop skipped
// it, the hub's relay window then aged out, and nobody could tell why a desk
// pane never got a directive. The node now logs the drop and, for a
// relay.inject whose identity is readable, answers relay.unconfirmed so the
// hub marks the row for its one labelled replay.

const (
	// hubUnparsedWarnEvery bounds the WARN rate. A hub that keeps resending
	// one bad directive must not flood the node log.
	hubUnparsedWarnEvery = time.Minute
	// relayUnparsedReason is the fixed ack reason. It never carries message
	// text: a rejected directive may hold anything.
	relayUnparsedReason = "node_parse_rejected"
)

// handleUnparsedHubMessage is the read loop's answer to parseHubOutbound
// returning false. It logs only the message type and the reject count, never
// the payload.
func (client *HubClient) handleUnparsedHubMessage(payload []byte) {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(payload, &fields)
	var messageType string
	_ = json.Unmarshal(fields["type"], &messageType)
	if messageType == "" || len(messageType) > 64 {
		messageType = "unknown"
	}
	client.warnUnparsedHubMessage(messageType)
	if messageType != "relay.inject" {
		return
	}
	ack := relayAckPayload{Reason: relayUnparsedReason}
	if json.Unmarshal(fields["job_id"], &ack.JobID) != nil || json.Unmarshal(fields["pane"], &ack.Pane) != nil || !hubJobIDPattern.MatchString(ack.JobID) || ack.Pane == "" || len(ack.Pane) > 128 {
		return
	}
	if raw, ok := fields["event_id"]; ok && (json.Unmarshal(raw, &ack.OriginalEventID) != nil || ack.OriginalEventID < 1) {
		ack.OriginalEventID = 0
	}
	encoded, err := json.Marshal(ack)
	if err != nil {
		return
	}
	client.relayEmit(hubClientEvent{Kind: "relay.unconfirmed", Payload: encoded})
}

func (client *HubClient) warnUnparsedHubMessage(messageType string) {
	now := client.nowUTC()
	client.unparsedMu.Lock()
	client.unparsedSuppressed++
	if !client.unparsedWarnedAt.IsZero() && now.Sub(client.unparsedWarnedAt) < hubUnparsedWarnEvery {
		client.unparsedMu.Unlock()
		return
	}
	count := client.unparsedSuppressed
	client.unparsedSuppressed = 0
	client.unparsedWarnedAt = now
	client.unparsedMu.Unlock()
	client.warnMessage(fmt.Sprintf("hub message could not be parsed and was dropped: type=%s rejected=%d", messageType, count))
}
