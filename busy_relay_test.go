package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// r27FakeHerdr is an explicit command seam.  It reads the committed captured
// shapes, so tests exercise the production parser without ever invoking a
// workstation's herdr binary.
type r27FakeHerdr struct {
	t          *testing.T
	getStatus  string
	waitStatus string
	waitGate   chan struct{}
	started    chan struct{}
	mu         sync.Mutex
	calls      [][]string
}

func TestR27HeldProjectionEditCancelAPIs(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node"}})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.relayHeld[301] = hubRelayHeldProjection{ID: 301, Lane: "lane-a", Pane: "fixture-pane", Machine: "host-a", Preview: "before", HeldSince: "2026-01-01T00:00:00Z", DeliverPolicy: "idle", JobID: "relay-job-301"}
	request := httptest.NewRequest(http.MethodGet, "/v1/relay/held?lane=lane-a", nil)
	request.Header.Set("Authorization", "Bearer op")
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	var listed struct {
		Held []hubRelayHeldProjection `json:"held"`
	}
	if writer.Code != http.StatusOK || json.Unmarshal(writer.Body.Bytes(), &listed) != nil || len(listed.Held) != 1 || listed.Held[0].Preview != "before" {
		t.Fatalf("list status=%d body=%s held=%+v", writer.Code, writer.Body.String(), listed.Held)
	}
	patchRequest := httptest.NewRequest(http.MethodPatch, "/v1/relay/events/301", bytes.NewBufferString(`{"text":"after"}`))
	patchRequest.Header.Set("Authorization", "Bearer op")
	patchWriter := httptest.NewRecorder()
	hub.Handler().ServeHTTP(patchWriter, patchRequest)
	select {
	case control := <-agent.relays:
		if patchWriter.Code != http.StatusOK || control.Type != "relay.edit" || control.EventID != 301 || control.Text != "after" {
			t.Fatalf("patch=%d control=%+v", patchWriter.Code, control)
		}
	default:
		t.Fatal("PATCH did not relay edit to owning node")
	}
	deleteRequest := httptest.NewRequest(http.MethodDelete, "/v1/relay/events/301", nil)
	deleteRequest.Header.Set("Authorization", "Bearer op")
	deleteWriter := httptest.NewRecorder()
	hub.Handler().ServeHTTP(deleteWriter, deleteRequest)
	select {
	case control := <-agent.relays:
		if deleteWriter.Code != http.StatusNoContent || control.Type != "relay.cancel" || control.EventID != 301 {
			t.Fatalf("delete=%d control=%+v", deleteWriter.Code, control)
		}
	default:
		t.Fatal("DELETE did not relay cancellation to owning node")
	}
	alreadyDelivered := httptest.NewRequest(http.MethodPatch, "/v1/relay/events/301", bytes.NewBufferString(`{"text":"too late"}`))
	alreadyDelivered.Header.Set("Authorization", "Bearer op")
	alreadyDeliveredWriter := httptest.NewRecorder()
	hub.Handler().ServeHTTP(alreadyDeliveredWriter, alreadyDelivered)
	if alreadyDeliveredWriter.Code != http.StatusConflict {
		t.Fatalf("PATCH after delivery status=%d, want 409", alreadyDeliveredWriter.Code)
	}
	for _, method := range []string{http.MethodGet, http.MethodPatch, http.MethodDelete} {
		path := "/v1/relay/held"
		if method != http.MethodGet {
			path = "/v1/relay/events/301"
		}
		req := httptest.NewRequest(method, path, nil)
		res := httptest.NewRecorder()
		hub.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusUnauthorized {
			t.Fatalf("method=%s unauthenticated status=%d", method, res.Code)
		}
	}
}

func (fake *r27FakeHerdr) run(ctx context.Context, args ...string) ([]byte, error) {
	fake.mu.Lock()
	fake.calls = append(fake.calls, append([]string(nil), args...))
	fake.mu.Unlock()
	if len(args) >= 2 && args[0] == "agent" && args[1] == "get" {
		return r27Fixture(fake.t, "agent-get-"+fake.getStatus+".json"), nil
	}
	if len(args) >= 2 && args[0] == "agent" && args[1] == "wait" {
		select {
		case fake.started <- struct{}{}:
		default:
		}
		if fake.waitGate != nil {
			select {
			case <-fake.waitGate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if fake.waitStatus == "timeout" {
			return r27Fixture(fake.t, "agent-wait-timeout.json"), errors.New("timeout")
		}
		return r27Fixture(fake.t, "agent-wait-"+fake.waitStatus+".json"), nil
	}
	fake.t.Fatalf("unexpected fake herdr argv=%q", args)
	return nil, errors.New("unreachable")
}

func r27Fixture(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(filepath.Join("testdata", "herdr", name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func (fake *r27FakeHerdr) count(command string) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	n := 0
	for _, call := range fake.calls {
		if len(call) > 1 && call[0] == "agent" && call[1] == command {
			n++
		}
	}
	return n
}

func (fake *r27FakeHerdr) lastWait() []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for index := len(fake.calls) - 1; index >= 0; index-- {
		if len(fake.calls[index]) > 1 && fake.calls[index][1] == "wait" {
			return append([]string(nil), fake.calls[index]...)
		}
	}
	return nil
}

func r27Node(t *testing.T, store *Store, fake *r27FakeHerdr) (*HubClient, *[]string, chan hubClientEvent) {
	t.Helper()
	if fake == nil {
		t.Fatal("R27 isolation guard: fake herdr command seam is required")
	}
	prompts := new([]string)
	events := make(chan hubClientEvent, 32)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInject: func(_ context.Context, pane, text string) bool {
		*prompts = append(*prompts, pane+"\x00"+text)
		return true
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().restore(t.Context())
	return client, prompts, events
}

func r27Directive(id int64, text, policy string) hubOutboundMessage {
	return hubOutboundMessage{Type: "relay.inject", Kind: "lane.event", JobID: "relay-job-" + strconv.FormatInt(id, 10), Pane: "fixture-pane", Lane: "lane-a", EventID: id, Text: text, DeliverPolicy: policy}
}

func r27Await(t *testing.T, events <-chan hubClientEvent, kind string) hubClientEvent {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func r27WaitStarted(t *testing.T, fake *r27FakeHerdr) {
	t.Helper()
	select {
	case <-fake.started:
	case <-time.After(time.Second):
		t.Fatal("agent wait was not armed")
	}
}

func TestR27BusyRelaySixPathsUseFixtureHerdr(t *testing.T) {
	r27GuardInbox(t)
	t.Run("immediate idle", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(101, "now please", "idle"))
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "now please") {
			t.Fatalf("prompts=%q", got)
		}
		r27Await(t, events, "relay.released")
		r27Await(t, events, "relay.delivered")
		if fake.count("get") != 1 || fake.count("wait") != 0 {
			t.Fatalf("get=%d wait=%d", fake.count("get"), fake.count("wait"))
		}
	})

	t.Run("held then idle release", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(102, "after work", "max_wait=7"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		close(gate)
		r27Await(t, events, "relay.released")
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "after work") {
			t.Fatalf("prompts=%q", got)
		}
		wait := fake.lastWait()
		want := []string{"agent", "wait", "fixture-pane", "--until", "idle", "--until", "done", "--timeout", "7000"}
		if strings.Join(wait, "|") != strings.Join(want, "|") {
			t.Fatalf("wait argv=%q want=%q", wait, want)
		}
		if fake.count("get") != 1 {
			t.Fatalf("agent get polled %d times", fake.count("get"))
		}
	})

	t.Run("timeout forces delivery", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, getStatus: "blocked", waitStatus: "timeout", started: make(chan struct{}, 2)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(103, "deadline note", "max_wait=1"))
		r27Await(t, events, "relay.held")
		r27Await(t, events, "relay.released")
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "[대기 만료 0분] deadline note") {
			t.Fatalf("prompts=%q", got)
		}
	})

	t.Run("cancelled held never prompts", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(104, "discard", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		if !client.relayBusyManager().cancel(t.Context(), 104) {
			t.Fatal("cancel did not remove held relay")
		}
		r27Await(t, events, "relay.cancelled")
		if len(*prompts) != 0 {
			t.Fatalf("cancelled relay prompted=%q", *prompts)
		}
	})

	t.Run("edited held releases edited text", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "done", waitGate: gate, started: make(chan struct{}, 3)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(105, "before", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		if !client.relayBusyManager().edit(t.Context(), 105, "after") {
			t.Fatal("edit rejected held relay")
		}
		close(gate)
		released := r27Await(t, events, "relay.released")
		if !strings.Contains(string(released.Payload), `"final_text":"after"`) || !strings.Contains(string(released.Payload), `"edited":true`) {
			t.Fatalf("released=%s", released.Payload)
		}
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "after") {
			t.Fatalf("prompts=%q", got)
		}
	})

	t.Run("batch preserves receive order", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 4)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(106, "first", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(107, "second", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		close(gate)
		r27Await(t, events, "relay.batched")
		var delivered []hubClientEvent
		released := 0
		deadline := time.After(time.Second)
		for released+len(delivered) < 4 {
			select {
			case event := <-events:
				switch event.Kind {
				case "relay.released":
					released++
				case "relay.delivered":
					delivered = append(delivered, event)
				}
			case <-deadline:
				t.Fatalf("batch released=%d delivered=%d", released, len(delivered))
			}
		}
		if released != 2 || len(delivered) != 2 || !strings.Contains(string(delivered[0].Payload), `"original_event_id":106`) || !strings.Contains(string(delivered[1].Payload), `"original_event_id":107`) {
			t.Fatalf("batch released=%d deliveries=%+v", released, delivered)
		}
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "1) first 2) second") {
			t.Fatalf("prompts=%q", got)
		}
	})
}

func TestR27RestoreDedupesHubReplayAndNeverPromptsCancelledPane(t *testing.T) {
	r27GuardInbox(t)
	store := NewMemoryStore(t)
	defer store.Close()
	firstGate := make(chan struct{})
	firstFake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: firstGate, started: make(chan struct{}, 2)}
	first, _, firstEvents := r27Node(t, store, firstFake)
	firstContext, stopFirst := context.WithCancel(t.Context())
	first.relayBusyManager().offer(firstContext, r27Directive(201, "only once", "idle"))
	r27Await(t, firstEvents, "relay.held")
	r27WaitStarted(t, firstFake)
	stopFirst()

	secondGate := make(chan struct{})
	secondFake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "done", waitGate: secondGate, started: make(chan struct{}, 3)}
	second, prompts, events := r27Node(t, store, secondFake)
	second.relayBusyManager().resume(t.Context())
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, secondFake)
	// The replay names the same (lane,event_id); it re-reports but adds no row.
	second.relayBusyManager().offer(t.Context(), r27Directive(201, "only once", "idle"))
	r27Await(t, events, "relay.held")
	close(secondGate)
	r27Await(t, events, "relay.released")
	if len(*prompts) != 1 {
		t.Fatalf("replayed relay prompt count=%d, want 1", len(*prompts))
	}
	getsBeforeSentinel := secondFake.count("get")
	second.relayBusyManager().offer(t.Context(), hubOutboundMessage{Type: "relay.inject", JobID: "relay-job-202", Pane: relayCancelledPane, Lane: "lane-a", EventID: 202, Text: "must not run", DeliverPolicy: "now"})
	if len(*prompts) != 1 || secondFake.count("get") != getsBeforeSentinel {
		t.Fatalf("cancel sentinel ran prompt/get: prompts=%d get=%d", len(*prompts), secondFake.count("get"))
	}
	inserted, err := store.InsertRelayHeld(t.Context(), relayHeld{Pane: relayCancelledPane, Lane: "lane-a", EventID: 203, JobID: "relay-job-203", Text: "must stay inert", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second})
	if err != nil || !inserted {
		t.Fatalf("seed cancelled pane inserted=%t err=%v", inserted, err)
	}
	second.relayBusyManager().restore(t.Context())
	if restored, err := store.RelayHeldAll(t.Context()); err != nil || len(restored) != 0 {
		t.Fatalf("cancel sentinel restored=%+v err=%v", restored, err)
	}
}

func TestR27BatchByteLimitKeepsOversizedItemIntact(t *testing.T) {
	now := time.Now()
	items := []relayHeld{{Text: strings.Repeat("a", 5000), HeldSince: now}, {Text: strings.Repeat("b", 5000), HeldSince: now}}
	groups := relayBatchGroups(items, false, now)
	if len(groups) != 2 || len(groups[0]) != 1 || len(groups[1]) != 1 {
		t.Fatalf("8KB groups=%v", groups)
	}
	big := relayHeld{Text: strings.Repeat("x", relayBatchTextLimit+1), HeldSince: now}
	groups = relayBatchGroups([]relayHeld{big}, false, now)
	if len(groups) != 1 || len(groups[0]) != 1 || groups[0][0].Text != big.Text {
		t.Fatalf("oversized item was split or truncated: %#v", groups)
	}
	store := NewMemoryStore(t)
	defer store.Close()
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
	client, prompts, events := r27Node(t, store, fake)
	big.Pane, big.Lane, big.JobID, big.EventID = "fixture-pane", "lane-a", "relay-job-401", 401
	wire, err := json.Marshal(hubRelayInjectEvent{Type: "relay.inject", Kind: "lane.event", JobID: big.JobID, Pane: big.Pane, Lane: big.Lane, EventID: big.EventID, Text: big.Text, Deliver: "idle"})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := parseHubOutbound(wire); !valid {
		t.Fatalf("oversized single-item directive rejected: %d bytes", len(wire))
	}
	client.relayBusyManager().deliver(t.Context(), []relayHeld{big}, false)
	if len(*prompts) != 1 || !strings.Contains((*prompts)[0], big.Text) {
		t.Fatalf("oversized item was not injected whole: prompts=%q", *prompts)
	}
	released := r27Await(t, events, "relay.released")
	delivered := r27Await(t, events, "relay.delivered")
	if _, valid := decodeRelayReleasedPayload(released.Payload); !valid {
		t.Fatalf("oversized release rejected by hub parser: %s", released.Payload)
	}
	if _, valid := decodeRelayAckPayload(delivered.Payload); !valid {
		t.Fatalf("oversized delivery rejected by hub parser: %s", delivered.Payload)
	}
}

func TestR27IsolationAndInboxGuards(t *testing.T) {
	// This deliberately fails if a test forgets the explicit command seam.
	if (&HubClient{relayInject: func(context.Context, string, string) bool { return true }}).relayRunner() != nil {
		t.Fatal("R27 isolation guard: fixture client would contact real herdr")
	}
}

func r27GuardInbox(t *testing.T) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "work", "herdr-inbox", "jobs")
	before, err := os.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		after, err := os.ReadDir(path)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if len(before) != len(after) {
			t.Fatalf("real inbox was changed: before=%d after=%d", len(before), len(after))
		}
	})
}
