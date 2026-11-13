package panewire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// t4375: a hub frame whose handler blocks on the node journal must not delay
// the frames behind it. The read loop once ran relay.persisted inline, so a
// held Store lock kept a job.spawn unread until its deadline had passed
// (rc=124 deadline_not_started on every landing).

// inboundDispatchClient is spawnDropClient plus a spawn journal, so a
// job.spawn can be claimed and answered without any spawn policy.
func inboundDispatchClient(t *testing.T, url string, warnings *warningLog) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: url, MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond,
		JobsInboxRoot:       t.TempDir(),
		SpawnJournalDir:     t.TempDir(),
		Warn:                warnings.add,
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func relayPersistedFrame(jobID string, eventID int64) []byte {
	return []byte(fmt.Sprintf(`{"type":"relay.persisted","job_id":%q,"kind":"job.completed","epoch":1,"report_path":"a.md","reason":"done","event_id":%d}`, jobID, eventID))
}

func spawnFrame(requestID string, deadline time.Time) []byte {
	return []byte(fmt.Sprintf(`{"type":"job.spawn","request_id":%q,"cwd_key":"project","brief":{"doc":"briefs/job-a.md"},"args":["--job","job-a"],"wait_seconds":1,"deadline_ms":%d,"operator_authorized":true}`, requestID, deadline.UnixMilli()))
}

// A relay.persisted whose Store lock stays held must not keep the job.spawn
// behind it from reaching handleHubSpawn inside the bound: the dispatcher
// blocks, the reader does not.
func TestHubInboundBlockedHandlerDoesNotDelaySpawn(t *testing.T) {
	store := NewMemoryStore(t)
	var warnings warningLog
	hub := &spawnDropHub{frames: [][]byte{
		relayPersistedFrame("job-blocked", 11),
		spawnFrame("11111111-2222-4333-8333-444444444444", time.Now().Add(-time.Minute)),
	}}
	server := hub.serve(t)
	client := inboundDispatchClient(t, t14WSURL(server.URL, ""), &warnings)
	client.SetRelayOutbox(store)
	// Hold the store-wide lock for the whole assertion: recordRelayPersisted
	// cannot retire the row, so on the pre-fix read loop the job.spawn behind
	// it stays unread past every bound. The dispatch worker blocks instead.
	store.mu.Lock()
	defer store.mu.Unlock()

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

	eventually(t, 5*time.Second, "job.spawn answered while relay.persisted blocks on the store lock", func() bool {
		return hub.resultCount() >= 1
	})
	result := hub.firstResult(t)
	if result["request_id"] != "11111111-2222-4333-8333-444444444444" {
		t.Fatalf("spawn result answers a different request: %v", result)
	}
}

// The ordered dispatcher preserves the wire's receive order for the frames it
// owns, including across a head frame that blocks.
func TestHubInboundDispatchPreservesReceiveOrder(t *testing.T) {
	store := NewMemoryStore(t)
	var warnings warningLog
	hub := &spawnDropHub{frames: [][]byte{
		relayPersistedFrame("job-first", 21),
		[]byte(`{"type":"job.assigned","job_id":"job-b","epoch":3}`),
		[]byte(`{"type":"failover","machine":"node-b","phase":"up","emitted_at":"2026-11-13T00:00:00Z"}`),
		relayPersistedFrame("job-last", 22),
	}}
	server := hub.serve(t)
	client := inboundDispatchClient(t, t14WSURL(server.URL, ""), &warnings)
	client.SetRelayOutbox(store)
	var handledMu sync.Mutex
	var handled []string
	client.inboundHandled = func(message hubOutboundMessage) {
		handledMu.Lock()
		defer handledMu.Unlock()
		handled = append(handled, message.Type)
	}
	// Block the head relay.persisted until the rest of the batch is queued
	// behind it: the assertion then proves order, not coincidence.
	store.mu.Lock()

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

	// The sleep is the window that proves the head frame is blocked: nothing
	// is handled until the lock is released.
	time.Sleep(300 * time.Millisecond)
	handledMu.Lock()
	if len(handled) != 0 {
		t.Fatalf("handled %v while the head frame should still block on the store lock", handled)
	}
	handledMu.Unlock()
	store.mu.Unlock()

	eventually(t, 5*time.Second, "queued inbound frames handled", func() bool {
		handledMu.Lock()
		defer handledMu.Unlock()
		return len(handled) >= 4
	})
	handledMu.Lock()
	defer handledMu.Unlock()
	want := []string{"relay.persisted", "job.assigned", "failover", "relay.persisted"}
	if fmt.Sprint(handled[:4]) != fmt.Sprint(want) {
		t.Fatalf("dispatch order %v, want wire order %v", handled[:4], want)
	}
}

// A redial ends the session through the same queue, so every frame the hub
// sent ahead of it is applied before serveConnection reports errHubRedial -
// the drain contract docs/hub-drain.md gives a restarting hub.
func TestHubInboundRedialAppliesQueuedFramesFirst(t *testing.T) {
	var warnings warningLog
	hub := &spawnDropHub{frames: [][]byte{
		[]byte(`{"type":"job.assigned","job_id":"job-c","epoch":4}`),
		[]byte(`{"type":"redial"}`),
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
	done := make(chan error, 1)
	go func() {
		_, err := client.serveConnection(ctx, connection)
		done <- err
	}()
	t.Cleanup(cancel)

	select {
	case err := <-done:
		if !errors.Is(err, errHubRedial) {
			t.Fatalf("serveConnection returned %v, want errHubRedial", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end on redial")
	}
	handledMu.Lock()
	defer handledMu.Unlock()
	if fmt.Sprint(handled) != "[job.assigned]" {
		t.Fatalf("frames applied before redial: %v, want [job.assigned]", handled)
	}
	if epoch := client.assignedJobs["job-c"]; epoch != 4 {
		t.Fatalf("job.assigned was dropped at session end: epoch=%d", epoch)
	}
}

// closingHub answers the registration ping, sends every queued frame, then
// drops the connection: the reader ends on a read error with the frames still
// owed to the inbound worker.
type closingHub struct{ frames [][]byte }

func (hub *closingHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		sent := false
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), connection, &message) != nil {
				return
			}
			if message["type"] != "ping" || sent {
				continue
			}
			_ = wsjson.Write(request.Context(), connection, map[string]any{"type": "pong"})
			sent = true
			for _, frame := range hub.frames {
				if connection.Write(request.Context(), websocket.MessageText, frame) != nil {
					return
				}
			}
			return
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// A read error ends the session with queued frames still owed to the
// dispatcher. The drain must apply all of them: a job.revoked whose ack
// write fails on the dead connection must not drop the frames behind it.
func TestHubInboundDrainAppliesQueuePastFailedAck(t *testing.T) {
	var warnings warningLog
	hub := &closingHub{frames: [][]byte{
		[]byte(`{"type":"job.revoked","job_id":"job-r","epoch":2}`),
		[]byte(`{"type":"job.assigned","job_id":"job-d","epoch":6}`),
	}}
	server := hub.serve(t)
	jobsRoot := t.TempDir()
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond,
		JobsInboxRoot:       jobsRoot,
		SpawnJournalDir:     t.TempDir(),
		Warn:                warnings.add,
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := client.serveConnection(ctx, connection)
		done <- err
	}()
	t.Cleanup(cancel)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end when the hub dropped the connection")
	}

	eventually(t, 5*time.Second, "drained frames applied after the session ended", func() bool {
		client.assignmentMu.Lock()
		defer client.assignmentMu.Unlock()
		return client.assignedJobs["job-d"] == 6
	})
	eventsDir := filepath.Join(jobsRoot, "jobs", "job-r", "events")
	entries, err := os.ReadDir(eventsDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("job.revoked local write missing after drain: entries=%v err=%v", entries, err)
	}
}
