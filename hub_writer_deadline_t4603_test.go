package panewire

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

// A caller deadline that expires while a frame is already being written must
// return to the caller without closing the node link. The peer does not read
// until after the deadline, so the first frame is mid-write when the caller
// gives up; once the peer reads, that frame and the next one must arrive.
func TestHubWriterCallerDeadlineMidWriteKeepsLink(t *testing.T) {
	agent, peer := hubWriterPipe(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := agent.writeJSONContext(ctx, map[string]string{"op": "first"}); err == nil {
		t.Fatal("write with no reader returned success before the caller deadline")
	}

	readCtx, cancelRead := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelRead()
	var first map[string]string
	if err := wsjson.Read(readCtx, peer, &first); err != nil {
		t.Fatalf("frame abandoned by its caller was cut short; the link dropped: %v", err)
	}
	if first["op"] != "first" {
		t.Fatalf("first frame = %v, want op=first", first)
	}

	// The pipe is unbuffered, so the second write completes only while the
	// peer reads: it runs beside the read, not before it.
	written := make(chan error, 1)
	go func() { written <- agent.writeJSONContext(t.Context(), map[string]string{"op": "second"}) }()
	var second map[string]string
	if err := wsjson.Read(readCtx, peer, &second); err != nil {
		t.Fatalf("second frame did not arrive: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("write after the abandoned frame failed: %v", err)
	}
	if second["op"] != "second" {
		t.Fatalf("second frame = %v, want op=second", second)
	}
}
