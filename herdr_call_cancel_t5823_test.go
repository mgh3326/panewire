package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A reply herdr has already answered is not discarded when the caller cancels
// its context and the call has no deadline. The fake herdr answers pane.send_text
// after 400ms of simulated latency; the context is cancelled at 100ms. Call keeps
// reading to the end of the reply and returns it. Fails on 240538b7, where the
// cancel set an expired deadline on the socket and Call returned an i/o timeout.
func TestHerdrCallCancelKeepsDeliveredReply(t *testing.T) {
	client, err := NewHerdrClient(startDelayedReplyHerdr(t, 400*time.Millisecond))
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	raw, err := client.Call(ctx, "pane.send_text", map[string]any{"pane_id": "w1:p1", "text": "hi"})
	if err != nil {
		t.Fatalf("Call cancelled at 100ms with no deadline = %v; want the reply herdr sent at 400ms", err)
	}
	var got struct {
		Accepted bool `json:"accepted"`
	}
	if err := json.Unmarshal(raw, &got); err != nil || !got.Accepted {
		t.Fatalf("Call result = %s (%v), want the delivered reply {\"accepted\":true}", raw, err)
	}
}

// startDelayedReplyHerdr is a herdr socket double that answers every request
// after delay. It returns the socket path; the listener closes at cleanup.
func startDelayedReplyHerdr(t *testing.T, delay time.Duration) string {
	t.Helper()
	// unix sockets cap sun_path near 104 bytes, so the socket lives in short /tmp.
	dir, err := os.MkdirTemp("/tmp", "t5823")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "h.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil {
					return
				}
				var req struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(line, &req) != nil {
					return
				}
				// Simulated latency: herdr answers only after delay.
				time.Sleep(delay)
				raw, err := json.Marshal(map[string]any{"id": req.ID, "result": map[string]any{"accepted": true}})
				if err != nil {
					return
				}
				_, _ = conn.Write(append(raw, '\n'))
			}()
		}
	}()
	return listener.Addr().String()
}
