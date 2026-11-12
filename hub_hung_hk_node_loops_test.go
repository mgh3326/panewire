package panewire

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

type t3432BlockedJobStore struct {
	started atomic.Bool
	release chan struct{}
}

func (s *t3432BlockedJobStore) Claim(context.Context, string, string, string) error { return nil }
func (s *t3432BlockedJobStore) Put(context.Context, string, map[string]any) error   { return nil }
func (s *t3432BlockedJobStore) Get(ctx context.Context, _ string) (map[string]any, error) {
	s.started.Store(true)
	select {
	case <-s.release:
		return map[string]any{"task_id": 3432}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A hung job-record read for a design-lane completion must not stop the node
// websocket from answering its next message.
func TestHungHKCompletionLookupDoesNotBlockNodeReadLoop(t *testing.T) {
	_, hk, closeHK := newFakeHandoffkeep(t)
	t.Cleanup(closeHK)
	h, _ := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)
	h.mu.Lock()
	h.nodes["host-a"].agent = nil
	h.mu.Unlock()
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)

	jobs := &t3432BlockedJobStore{release: make(chan struct{})}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	defer close(jobs.release)

	connection, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+"/v1/agent", &websocket.DialOptions{HTTPHeader: http.Header{
		"X-Panewire-Machine-ID": {"host-a"},
		"Authorization":         {"Bearer node"},
	}})
	if err != nil {
		t.Fatalf("agent dial response=%v err=%v", response, err)
	}
	t.Cleanup(func() { _ = connection.Close(websocket.StatusNormalClosure, "") })
	write := func(value any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := wsjson.Write(ctx, connection, value); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string]any{"type": "hello", "machine_id": "host-a", "version": "test"})
	event := hubJobEventPayload{JobID: "hk3432-blocked", Epoch: 1, AgentLabel: "worker", OwnerLane: t3426Design, EventID: "completion-3432", ReportPath: "/jobs/hk3432-blocked/report.md"}
	write(hubClientWireEvent(hubClientEvent{Kind: "job.completed", Payload: hubJobCompletionPayloadForJob(HubActiveJob{JobID: event.JobID, Epoch: event.Epoch, AgentLabel: event.AgentLabel, OwnerLane: event.OwnerLane, ReportPath: event.ReportPath}, event.EventID, false)}))

	eventually(t, time.Second, "hk job-record lookup started", func() bool { return jobs.started.Load() })
	write(map[string]any{"type": "ping"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		var message struct {
			Type string `json:"type"`
		}
		if err := wsjson.Read(ctx, connection, &message); err != nil {
			t.Fatalf("node read loop did not answer while hk was hung: %v", err)
		}
		if message.Type == "pong" {
			break
		}
	}
}

func TestHungHKUnconfirmedEscalationDoesNotBlockNodeReadLoop(t *testing.T) {
	fake, hk, closeHK := newFakeHandoffkeep(t)
	t.Cleanup(closeHK)
	rowRead, release := make(chan struct{}), make(chan struct{})
	var rowReadOnce sync.Once
	fake.mu.Lock()
	fake.observeQuery = func(method, path, query string) {
		if method == http.MethodGet && path == "/v1/relay/events" && query == "after_id=99&limit=1" {
			rowReadOnce.Do(func() { close(rowRead) })
			<-release
		}
	}
	fake.mu.Unlock()
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 100, Kind: "lane.event", JobID: laneEventTransportID("worker-3404", "drop-3432"), OwnerLane: "worker-3404", EventID: "drop-3432"})
	h, _ := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)
	h.mu.Lock()
	h.nodes["host-a"].agent = nil
	h.mu.Unlock()
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	defer close(release)

	connection, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+"/v1/agent", &websocket.DialOptions{HTTPHeader: http.Header{
		"X-Panewire-Machine-ID": {"host-a"},
		"Authorization":         {"Bearer node"},
	}})
	if err != nil {
		t.Fatalf("agent dial response=%v err=%v", response, err)
	}
	t.Cleanup(func() { _ = connection.Close(websocket.StatusNormalClosure, "") })
	write := func(value any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := wsjson.Write(ctx, connection, value); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string]any{"type": "hello", "machine_id": "host-a", "version": "test"})
	dropped, _ := json.Marshal(map[string]any{
		"job_id": laneEventTransportID("worker-3404", "drop-3432"), "pane": "w1:worker",
		"lane": "worker-3404", "original_event_id": 100, "reason": relayDroppedMaybeInPane,
	})
	write(hubClientWireEvent(hubClientEvent{Kind: "relay.dropped", Payload: dropped}))
	select {
	case <-rowRead:
	case <-time.After(time.Second):
		t.Fatal("hk relay-row read did not start")
	}
	write(map[string]any{"type": "ping"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		var message struct {
			Type string `json:"type"`
		}
		if err := wsjson.Read(ctx, connection, &message); err != nil {
			t.Fatalf("node read loop did not answer while hk was hung: %v", err)
		}
		if message.Type == "pong" {
			break
		}
	}
}

func TestNodeRelayQueueKeepsCompletionBeforeEscalation(t *testing.T) {
	_, hk, closeHK := newFakeHandoffkeep(t)
	t.Cleanup(closeHK)
	h, _ := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)
	h.mu.Lock()
	h.nodes["host-a"].agent = nil
	h.mu.Unlock()
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)

	jobs := &t3432BlockedJobStore{release: make(chan struct{})}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(jobs.release) }) }
	defer release()
	connection, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+"/v1/agent", &websocket.DialOptions{HTTPHeader: http.Header{
		"X-Panewire-Machine-ID": {"host-a"},
		"Authorization":         {"Bearer node"},
	}})
	if err != nil {
		t.Fatalf("agent dial response=%v err=%v", response, err)
	}
	t.Cleanup(func() { _ = connection.Close(websocket.StatusNormalClosure, "") })
	write := func(value any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := wsjson.Write(ctx, connection, value); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string]any{"type": "hello", "machine_id": "host-a", "version": "test"})
	completion := hubJobEventPayload{JobID: "hk3432-ordered", Epoch: 1, AgentLabel: "worker-3432", OwnerLane: t3426Design, EventID: "completion-ordered", ReportPath: "/jobs/hk3432-ordered/report.md"}
	write(hubClientWireEvent(hubClientEvent{Kind: "job.completed", Payload: hubJobCompletionPayloadForJob(HubActiveJob{JobID: completion.JobID, Epoch: completion.Epoch, AgentLabel: completion.AgentLabel, OwnerLane: completion.OwnerLane, ReportPath: completion.ReportPath}, completion.EventID, false)}))
	eventually(t, time.Second, "hk job-record lookup started", func() bool { return jobs.started.Load() })
	escalation, _ := json.Marshal(map[string]any{
		"job_id": completion.JobID, "epoch": completion.Epoch, "agent_label": "worker-3432",
		"owner_lane": "worker-3404", "event_id": "escalation-ordered", "reason": "needs_decision",
		"question": "Which action should be taken?", "report_path": completion.ReportPath,
	})
	write(hubClientWireEvent(hubClientEvent{Kind: "job.escalate", Payload: escalation}))
	write(map[string]any{"type": "ping"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		var message struct {
			Type string `json:"type"`
		}
		if err := wsjson.Read(ctx, connection, &message); err != nil {
			t.Fatalf("node read loop did not answer while hk was hung: %v", err)
		}
		if message.Type == "pong" {
			break
		}
	}
	items, err := h.inbox.store.list(t3426Design, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Kind == "job.escalate" && item.Event.EventID == "escalation-ordered" {
			t.Fatal("job.escalate was persisted while the preceding completion was blocked on hk")
		}
	}

	release()
	var completionID, escalationID int64
	eventually(t, time.Second, "ordered completion and escalation stored", func() bool {
		items, err := h.inbox.store.list(t3426Design, false)
		if err != nil {
			return false
		}
		for _, item := range items {
			switch {
			case item.Kind == "job.completed" && item.Event.JobID == completion.JobID:
				completionID = item.ID
			case item.Kind == "job.escalate" && item.Event.EventID == "escalation-ordered":
				escalationID = item.ID
			}
		}
		return completionID != 0 && escalationID != 0
	})
	if completionID >= escalationID {
		t.Fatalf("completion inbox id=%d, escalation inbox id=%d; want completion first", completionID, escalationID)
	}
}

func TestNodeRelayQueueAcceptsTransientSender(t *testing.T) {
	_, hk, closeHK := newFakeHandoffkeep(t)
	t.Cleanup(closeHK)
	h, _ := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)
	agent := &hubAgent{machineID: "host-a", credential: "node", transient: true}
	lanePayload := mustJSON(map[string]any{"type": "event", "kind": "lane.event", "payload": map[string]any{
		"owner_lane": t3426Design, "event_id": "transient-lane-3432", "text": "transient lane event", "epoch": 1,
	}})
	if handled, enqueued := h.queueNodeRelayMessage("host-a", "fixture", agent, lanePayload); !handled || !enqueued {
		t.Fatalf("queue transient lane event handled=%v enqueued=%v", handled, enqueued)
	}
	event := hubJobEventPayload{JobID: "hk3432-transient", Epoch: 1, AgentLabel: "worker-3432", OwnerLane: t3426Design, EventID: "transient-3432", ReportPath: "/jobs/hk3432-transient/report.md"}
	job := HubActiveJob{JobID: event.JobID, Epoch: event.Epoch, AgentLabel: event.AgentLabel, OwnerLane: event.OwnerLane, ReportPath: event.ReportPath}
	payload := mustJSON(hubClientWireEvent(hubClientEvent{Kind: "job.completed", Payload: hubJobCompletionPayloadForJob(job, event.EventID, false)}))
	if handled, enqueued := h.queueNodeRelayMessage("host-a", "fixture", agent, payload); !handled || !enqueued {
		t.Fatalf("queue transient event handled=%v enqueued=%v", handled, enqueued)
	}
	eventually(t, time.Second, "transient lane event and completion relayed", func() bool {
		items, err := h.inbox.store.list(t3426Design, false)
		if err != nil {
			return false
		}
		lane, completion := false, false
		for _, item := range items {
			if item.Kind == "lane.event" && item.Event.EventID == "transient-lane-3432" {
				lane = true
			}
			if item.Kind == "job.completed" && item.Event.EventID == event.EventID {
				completion = true
			}
		}
		return lane && completion
	})
}

var _ fleet.JobStore = (*t3432BlockedJobStore)(nil)
