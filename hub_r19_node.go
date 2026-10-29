package panewire

import (
	"context"
	"regexp"
)

var hubRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)

func validHubRequestID(value string) bool { return hubRequestIDPattern.MatchString(value) }

// handleHubQuota answers a legacy hub's quota.request so a mixed-version pair
// completes the round trip instead of timing out: heartbeat quota collection
// is retired and the node no longer runs any quota tool.
func (client *HubClient) handleHubQuota(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	_ = peer.write(ctx, struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Error     string `json:"error,omitempty"`
	}{Type: "quota.report", RequestID: message.RequestID, Error: "unsupported"})
}
