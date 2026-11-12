package panewire

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
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
		[]byte(`{"type":"burst.holds","holds_active":true}`),
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
	want := []string{"relay.persisted", "job.assigned", "burst.holds", "relay.persisted"}
	if fmt.Sprint(handled[:4]) != fmt.Sprint(want) {
		t.Fatalf("dispatch order %v, want wire order %v", handled[:4], want)
	}
}
