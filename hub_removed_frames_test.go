package panewire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// The frames of the removed burst, node-wake and held-relay edit features are
// not hub work any more. A node that still receives one warns and drops it
// without a reply, and the frame behind it is handled in order and not lost.
func TestRemovedInboundFramesAreIgnored(t *testing.T) {
	var warnings warningLog
	hub := &spawnDropHub{frames: [][]byte{
		[]byte(`{"type":"burst","machine":"node-b","phase":"up","emitted_at":"2026-11-13T00:00:00Z","wake_mac":"02:1a:2b:3c:4d:5e"}`),
		[]byte(`{"type":"burst.holds","holds_active":true}`),
		[]byte(`{"type":"wake","machine":"node-b","emitted_at":"2026-11-13T00:00:00Z"}`),
		[]byte(`{"type":"relay.edit","event_id":7,"text":"after"}`),
		[]byte(`{"type":"relay.cancel","event_id":7}`),
		[]byte(`{"type":"job.assigned","job_id":"job-b","epoch":3}`),
	}}
	server := hub.serve(t)
	client := inboundDispatchClient(t, t14WSURL(server.URL, ""), &warnings)
	var handledMu sync.Mutex
	var handled []string
	client.inboundHandled = func(message hubOutboundMessage) {
		handledMu.Lock()
		defer handledMu.Unlock()
		handled = append(handled, message.Type)
	}

	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.serve(ctx, connection)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	eventually(t, 5*time.Second, "the frame after the removed ones is handled", func() bool {
		handledMu.Lock()
		defer handledMu.Unlock()
		return len(handled) >= 1
	})
	handledMu.Lock()
	defer handledMu.Unlock()
	if len(handled) != 1 || handled[0] != "job.assigned" {
		t.Fatalf("handled %v, want only [job.assigned]: the removed frames must not reach a handler", handled)
	}
}
