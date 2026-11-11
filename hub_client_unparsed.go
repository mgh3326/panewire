package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// A hub message the node cannot parse used to vanish: the read loop skipped
// it, the hub's relay window then aged out, and nobody could tell why a desk
// pane never got a directive. The node now logs the drop with the reason
// class and, where the message's identity is readable, answers the hub: a
// relay.inject with relay.unconfirmed so the hub marks the row for its one
// labelled replay, and a job.spawn with a refusal so the hub's operation ends
// failed at once instead of at its request deadline.

const (
	// hubUnparsedWarnEvery bounds the WARN rate. A hub that keeps resending
	// one bad directive must not flood the node log.
	hubUnparsedWarnEvery = time.Minute
	// relayUnparsedReason is the fixed ack reason. It never carries message
	// text: a rejected directive may hold anything.
	relayUnparsedReason = "node_parse_rejected"

	// The reason classes a dropped frame is reported under. They are what
	// tells one kind of drop apart from another in the node log.
	hubUnparsedJSON            = "json"
	hubUnparsedUnknownType     = "unknown_type"
	hubUnparsedArgumentPattern = "argument_pattern"
	hubUnparsedInvalidRequest  = "invalid_request"
)

// hubOutboundRejection is why parseHubOutbound refused a frame. Type is the
// frame's own type when it could be read. Detail is the argument rule's
// sentence for an argument_pattern refusal; RequestID is the job.spawn
// request the node can still answer.
type hubOutboundRejection struct {
	Class     string
	Type      string
	Detail    string
	RequestID string
}

// handleUnparsedHubMessage is the read loop's answer to a rejected frame. It
// logs only the message type, the class and the reject count, never the
// payload.
func (client *HubClient) handleUnparsedHubMessage(ctx context.Context, peer *hubClientConnection, payload []byte, rejection hubOutboundRejection) {
	messageType := rejection.Type
	if messageType == "" || len(messageType) > 64 {
		messageType = "unknown"
	}
	class := rejection.Class
	if class == "" {
		class = hubUnparsedInvalidRequest
	}
	client.warnUnparsedHubMessage(messageType, class)
	switch messageType {
	case "relay.inject":
		client.answerUnparsedRelayInject(payload)
	case "job.spawn":
		client.answerUnparsedSpawn(ctx, peer, rejection)
	}
}

// answerUnparsedSpawn refuses a job.spawn the node cannot accept for the
// request id it names. The hub's spawn operation then ends failed with that
// reason instead of waiting out its deadline for a result that never comes.
// A frame whose request id cannot be read stays a logged drop.
func (client *HubClient) answerUnparsedSpawn(ctx context.Context, peer *hubClientConnection, rejection hubOutboundRejection) {
	if peer == nil || !validHubSpawnRequestID(rejection.RequestID) {
		return
	}
	code, detail, fix := "invalid_request", "the node could not accept this job.spawn request", "Rebuild the request with panewire spawn; the node never received this one."
	if rejection.Class == hubUnparsedArgumentPattern {
		code, detail = "invalid_args", rejection.Detail
		fix = "Correct the named flag and submit a new request; the node never received this one."
	}
	tail := hubSpawnTailWithStderr("", detail+"\nFix: "+fix)
	client.sendSpawnResult(ctx, peer, hubSpawnResult{RequestID: rejection.RequestID, RC: ExitUsage, Error: code, StdoutTail: tail}, false)
}

func (client *HubClient) answerUnparsedRelayInject(payload []byte) {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(payload, &fields)
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

func (client *HubClient) warnUnparsedHubMessage(messageType, class string) {
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
	client.warnMessage(fmt.Sprintf("hub message could not be parsed and was dropped: type=%s class=%s rejected=%d", messageType, class, count))
}
