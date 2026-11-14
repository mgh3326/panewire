package panewire

import (
	"context"
	"time"
)

// hubCapabilitySpawnGrant is what a node advertises when it starts a
// grant-required job.spawn only after the hub's durable grant
// (docs/hub-spawn-operations.md, "Start proof").
const hubCapabilitySpawnGrant = "spawn_grant_v1"

// awaitSpawnGrant asks the hub for the start grant of a grant-required frame
// and waits for the answer until the frame's start_by. Only true lets the
// spawn claim its journal row; a refusal, silence or a lost connection means
// this attempt never starts, and the hub may fence it. ctx is the admission
// context, so the wait also ends at the request deadline.
func (client *HubClient) awaitSpawnGrant(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) bool {
	startBy := time.UnixMilli(message.StartByMS)
	if !time.Now().Before(startBy) {
		return false
	}
	answer := make(chan bool, 1)
	client.spawnGrantMu.Lock()
	if client.spawnGrants == nil {
		client.spawnGrants = map[string]chan bool{}
	}
	client.spawnGrants[message.RequestID] = answer
	client.spawnGrantMu.Unlock()
	defer func() {
		client.spawnGrantMu.Lock()
		if client.spawnGrants[message.RequestID] == answer {
			delete(client.spawnGrants, message.RequestID)
		}
		client.spawnGrantMu.Unlock()
	}()
	waitCtx, cancel := context.WithDeadline(ctx, startBy)
	defer cancel()
	if peer.write(waitCtx, struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
	}{"job.spawn.claim", message.RequestID}) != nil {
		return false
	}
	select {
	case granted := <-answer:
		// select picks at random when the answer and the deadline are both
		// ready: a grant read at or after start_by is not taken.
		return granted && time.Now().Before(startBy)
	case <-waitCtx.Done():
		return false
	case <-peer.closed:
		return false
	}
}

// deliverSpawnGrant hands a hub answer to the spawn waiting for it. An answer
// nobody waits for (late, or for a frame already refused) is dropped: the
// spawn it named has already given up and will not start.
func (client *HubClient) deliverSpawnGrant(requestID string, granted bool) {
	client.spawnGrantMu.Lock()
	answer := client.spawnGrants[requestID]
	client.spawnGrantMu.Unlock()
	if answer == nil {
		return
	}
	select {
	case answer <- granted:
	default:
	}
}
