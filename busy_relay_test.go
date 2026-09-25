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
	getOutput  []byte
	getErr     error
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
	if held := hub.relayHeld[301]; held.Preview != "after" {
		t.Fatalf("PATCH preview=%q, want edited text", held.Preview)
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
	getOutput, getErr, getStatus := fake.getOutput, fake.getErr, fake.getStatus
	fake.mu.Unlock()
	if len(args) >= 2 && args[0] == "agent" && args[1] == "get" {
		output := getOutput
		if output == nil {
			output = r27Fixture(fake.t, "agent-get-"+getStatus+".json")
		}
		return r27RewritePane(output, args[2]), getErr
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

// r27RewritePane stamps the queried pane into a captured agent_info reply —
// real herdr always answers for the pane it was asked about, while the
// committed fixtures all carry w1:p1. Error envelopes carry no pane and pass
// through untouched.
func r27RewritePane(raw []byte, pane string) []byte {
	var doc map[string]json.RawMessage
	if json.Unmarshal(raw, &doc) != nil {
		return raw
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(doc["result"], &result) != nil {
		return raw
	}
	var agent map[string]json.RawMessage
	if json.Unmarshal(result["agent"], &agent) != nil || agent == nil {
		return raw
	}
	encoded, _ := json.Marshal(pane)
	agent["pane_id"] = encoded
	agentDoc, _ := json.Marshal(agent)
	result["agent"] = agentDoc
	resultDoc, _ := json.Marshal(result)
	doc["result"] = resultDoc
	out, err := json.Marshal(doc)
	if err != nil {
		return raw
	}
	return out
}

// r27SetGet swaps the reply the next agent get returns, under the fake's lock
// so a release-gate probe running on the wait goroutine sees a consistent
// answer.
func (fake *r27FakeHerdr) r27SetGet(output []byte, err error) {
	fake.mu.Lock()
	fake.getOutput, fake.getErr = output, err
	fake.mu.Unlock()
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
		// One get decides the hold; the release gate runs exactly one more to
		// re-verify the lease occupant before injecting.
		if fake.count("get") != 2 {
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
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "[대기 만료 0분]") || !strings.Contains(got[0], "deadline note") {
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
		if got := *prompts; len(got) != 1 ||
			!strings.Contains(got[0], relayNonce(relayHeld{EventID: 106})+" first") ||
			!strings.Contains(got[0], relayNonce(relayHeld{EventID: 107})+" second") {
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

func TestR27FailOpenAgentGetPathsUseFixtureHerdr(t *testing.T) {
	r27GuardInbox(t)
	tests := []struct {
		name   string
		output func(*testing.T) []byte
		err    error
	}{
		{name: "unknown status", output: func(t *testing.T) []byte { return r27Fixture(t, "agent-get-unknown.json") }},
		{name: "exit status one", output: func(t *testing.T) []byte { return r27Fixture(t, "agent-get-working.json") }, err: errors.New("exit status 1")},
		{name: "agent not found", output: func(t *testing.T) []byte { return r27Fixture(t, "agent-get-not-found.json") }, err: errors.New("exit status 1")},
		{name: "malformed JSON", output: func(*testing.T) []byte { return []byte("not json") }},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &r27FakeHerdr{t: t, getOutput: test.output(t), getErr: test.err, waitStatus: "idle", started: make(chan struct{}, 1)}
			store := NewMemoryStore(t)
			defer store.Close()
			client, prompts, _ := r27Node(t, store, fake)
			client.relayBusyManager().offer(t.Context(), r27Directive(int64(130+index), "fail open", "idle"))
			if len(*prompts) != 1 || fake.count("wait") != 0 {
				t.Fatalf("prompts=%q wait=%d", *prompts, fake.count("wait"))
			}
		})
	}
}

func TestR27ExplicitReceiveSequenceOrdersHeldRows(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, _, _ := r27Node(t, store, fake)
	second := r27Directive(141, "second", "idle")
	second.RecvSeq = 2
	first := r27Directive(140, "first", "idle")
	first.RecvSeq = 1
	client.relayBusyManager().offer(t.Context(), second)
	client.relayBusyManager().offer(t.Context(), first)
	items, err := store.RelayHeldForPane(t.Context(), "fixture-pane")
	if err != nil || len(items) != 2 || items[0].EventID != 140 || items[0].RecvSeq != 1 || items[1].EventID != 141 || items[1].RecvSeq != 2 {
		t.Fatalf("held=%+v err=%v", items, err)
	}
	close(gate)
}

func TestR27CancelledConfirmationBounded(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	for id := int64(1); id <= relayCancelledMaxEntries+1; id++ {
		hub.rememberRelayCancelledLocked(id)
	}
	length := len(hub.relayCancelled)
	_, oldestPresent := hub.relayCancelled[1]
	_, newestPresent := hub.relayCancelled[relayCancelledMaxEntries+1]
	hub.mu.Unlock()
	if length != relayCancelledMaxEntries || oldestPresent || !newestPresent {
		t.Fatalf("cancelled length=%d oldest=%t newest=%t", length, oldestPresent, newestPresent)
	}
}

func TestR27HubIngestsBusyRelayNodeEvents(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node"}})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 4)}
	hub.subscribers[subscriber] = struct{}{}
	send := func(kind string, value any) {
		t.Helper()
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(struct {
			Type    string          `json:"type"`
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}{Type: "event", Kind: kind, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		if _, valid := parseHubInbound(wire); !valid {
			t.Fatalf("parseHubInbound rejected %s", wire)
		}
		hub.handleAgentMessage("host-a", "fixture", agent, wire)
	}
	held := relayHeldPayload{EventID: 501, JobID: "relay-job-501", Pane: "fixture-pane", Lane: "lane-a", Reason: "working", Preview: "waiting", HeldSince: "2026-01-01T00:00:00Z", DeliverPolicy: "idle"}
	send("relay.held", held)
	if projection, exists := hub.relayHeld[501]; !exists || projection.Preview != "waiting" {
		t.Fatalf("held projection=%+v exists=%t", projection, exists)
	}
	send("relay.released", relayReleasedPayload{JobID: held.JobID, Pane: held.Pane, Lane: held.Lane, FinalText: "final text", Edited: true, OriginalEventID: held.EventID})
	if _, exists := hub.relayHeld[501]; exists {
		t.Fatal("released relay remained held")
	}
	send("relay.cancelled", relayCancelledPayload{OriginalEventID: 502})
	send("relay.batched", relayBatchedPayload{Pane: "fixture-pane", Lane: "lane-a", EventIDs: []int64{503, 504}})
	seen := make(map[string]json.RawMessage, 4)
	for range 4 {
		message := <-subscriber.messages
		if message.event == nil {
			t.Fatalf("non-event broadcast=%+v", message)
		}
		seen[message.event.Kind] = message.event.Payload
	}
	released, valid := decodeRelayReleasedPayload(seen["relay.released"])
	if !valid || released.FinalText != "final text" || !released.Edited || released.OriginalEventID != held.EventID || len(seen) != 4 {
		t.Fatalf("released=%+v valid=%t broadcasts=%v", released, valid, seen)
	}
}

func TestR27IsolationAndInboxGuards(t *testing.T) {
	// This deliberately fails if a test forgets the explicit command seam.
	if (&HubClient{relayInject: func(context.Context, string, string) bool { return true }}).relayRunner() != nil {
		t.Fatal("R27 isolation guard: fixture client would contact real herdr")
	}
}

// r449Directive is r27Directive with the route coordinates made explicit: a
// route change is exactly an inject naming the same lane on a different pane.
func r449Directive(id int64, lane, pane, text, policy string) hubOutboundMessage {
	return hubOutboundMessage{Type: "relay.inject", Kind: "lane.event", JobID: "relay-job-" + strconv.FormatInt(id, 10), Pane: pane, Lane: lane, EventID: id, Text: text, DeliverPolicy: policy}
}

// r449OccupantGet builds an agent_info reply from the committed fixture with
// occupant fields rewritten, so tests can hold a lease on one instance and
// then answer the release probe with another. The pane stamp still comes from
// r27RewritePane when the fake serves it.
func r449OccupantGet(t *testing.T, status string, mutate func(agent map[string]json.RawMessage)) []byte {
	t.Helper()
	raw := r27Fixture(t, "agent-get-"+status+".json")
	var doc map[string]json.RawMessage
	if json.Unmarshal(raw, &doc) != nil {
		t.Fatalf("fixture %s not json", status)
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(doc["result"], &result) != nil {
		t.Fatalf("fixture %s has no result", status)
	}
	var agent map[string]json.RawMessage
	if json.Unmarshal(result["agent"], &agent) != nil || agent == nil {
		t.Fatalf("fixture %s has no agent", status)
	}
	mutate(agent)
	agentDoc, _ := json.Marshal(agent)
	result["agent"] = agentDoc
	resultDoc, _ := json.Marshal(result)
	doc["result"] = resultDoc
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func r449SetSession(session string) func(agent map[string]json.RawMessage) {
	return func(agent map[string]json.RawMessage) {
		encoded, _ := json.Marshal(map[string]string{"agent": "claude", "kind": "id", "source": "herdr:claude", "value": session})
		agent["agent_session"] = encoded
	}
}

// r449DroppedReason awaits a relay.dropped event and decodes it through the
// same hub-side parser the wire path uses, so the reason contract is checked
// end to end rather than against a string the node happens to emit.
func r449DroppedReason(t *testing.T, events <-chan hubClientEvent) relayDroppedPayload {
	t.Helper()
	event := r27Await(t, events, "relay.dropped")
	dropped, valid := decodeRelayDroppedPayload(event.Payload)
	if !valid {
		t.Fatalf("dropped payload rejected by hub parser: %s", event.Payload)
	}
	return dropped
}

func TestR449ReusedPaneNewOccupantNeverReceivesHeldText(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	client.relayBusyManager().offer(t.Context(), r27Directive(601, "to the first agent", "idle"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	held, err := store.RelayHeldForPane(t.Context(), "fixture-pane")
	if err != nil || len(held) != 1 || held[0].Lease == "" {
		t.Fatalf("held rows=%+v err=%v want one leased row", held, err)
	}
	// Same pane id, same registered name, same terminal — but a new agent
	// session now occupies it. Pane-string equality alone would deliver.
	fake.r27SetGet(r449OccupantGet(t, "idle", func(agent map[string]json.RawMessage) {
		r449SetSession("different-session-value")(agent)
	}), nil)
	close(gate)
	dropped := r449DroppedReason(t, events)
	if dropped.Reason != "pane_occupant_changed" || dropped.OriginalEventID != 601 {
		t.Fatalf("dropped=%+v", dropped)
	}
	if got := *prompts; len(got) != 0 {
		t.Fatalf("reused pane prompted=%q", got)
	}
	if held, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(held) != 0 {
		t.Fatalf("expired lease rows=%+v err=%v", held, err)
	}
}

func TestR449LaneRerouteExpiresStaleHeld(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	client.relayBusyManager().offer(t.Context(), r449Directive(602, "lane-a", "fixture-pane", "bound to old pane", "idle"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	// The hub re-resolved lane-a to a different pane and injected there —
	// that directive is the node's newest route observation for the lane.
	client.relayBusyManager().offer(t.Context(), r449Directive(603, "lane-a", "fixture-pane-2", "fresh text", "now"))
	close(gate)
	dropped := r449DroppedReason(t, events)
	if dropped.Reason != "lane_rerouted" || dropped.OriginalEventID != 602 {
		t.Fatalf("dropped=%+v", dropped)
	}
	got := *prompts
	if len(got) != 1 || got[0] != "fixture-pane-2\x00"+task687Text(603, "fresh text") {
		t.Fatalf("prompts=%q want only the rerouted pane's own delivery", got)
	}
	if held, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(held) != 0 {
		t.Fatalf("stale rows=%+v err=%v", held, err)
	}
}

func TestR449UnchangedRouteAndOccupantDelivers(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	client.relayBusyManager().offer(t.Context(), r27Directive(604, "still mine", "idle"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	held, err := store.RelayHeldForPane(t.Context(), "fixture-pane")
	if err != nil || len(held) != 1 || held[0].Lease == "" {
		t.Fatalf("held rows=%+v err=%v want one leased row", held, err)
	}
	close(gate)
	r27Await(t, events, "relay.released")
	r27Await(t, events, "relay.delivered")
	if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "still mine") {
		t.Fatalf("prompts=%q", got)
	}
}

func TestR449DeadPaneOccupantGone(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	client.relayBusyManager().offer(t.Context(), r27Directive(605, "to a dying pane", "idle"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	// herdr's definitive absence answer — the pane no longer hosts an agent.
	fake.r27SetGet(r27Fixture(t, "agent-get-not-found.json"), errors.New("exit status 1"))
	close(gate)
	dropped := r449DroppedReason(t, events)
	if dropped.Reason != "pane_occupant_gone" || dropped.OriginalEventID != 605 {
		t.Fatalf("dropped=%+v", dropped)
	}
	if got := *prompts; len(got) != 0 {
		t.Fatalf("dead pane prompted=%q", got)
	}
}

func TestR449UnverifiableProbeRearmsRatherThanExpires(t *testing.T) {
	r27GuardInbox(t)
	rearmCycle := func(t *testing.T, eventID int64, getOutput []byte, getErr error) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 8)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(eventID, "probe keeps failing", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		fake.r27SetGet(getOutput, getErr)
		close(gate)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "inject_failed_max_attempts" || dropped.OriginalEventID != eventID {
			t.Fatalf("dropped=%+v want bounded inject retry exhaustion", dropped)
		}
		if got := *prompts; len(got) != 0 {
			t.Fatalf("unverifiable pane prompted=%q", got)
		}
	}
	// A dead socket is not stale evidence: the lease gate must not expire on
	// it. It runs the bounded rearm instead, which is why the row ends as an
	// inject-failure drop and never as a lease verdict.
	t.Run("dead socket rearms", func(t *testing.T) {
		rearmCycle(t, 606, []byte("dial unix /missing/herdr.sock: no such file"), errors.New("exit status 1"))
	})
	// Valid JSON that is not an agent_info is equally unverifiable — it must
	// not be mistaken for an occupant and expired as pane_occupant_changed.
	t.Run("agentless json rearms", func(t *testing.T) {
		rearmCycle(t, 607, []byte("{}"), nil)
	})
}

func TestR449PreLeaseRowsFallBackToNameMembership(t *testing.T) {
	r27GuardInbox(t)
	seed := func(t *testing.T, lane string, eventID int64) (*HubClient, *[]string, chan hubClientEvent, *Store) {
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		client, prompts, events := r27Node(t, store, fake)
		inserted, err := store.InsertRelayHeld(t.Context(), relayHeld{Pane: "fixture-pane", Lane: lane, EventID: eventID, JobID: "relay-job-" + strconv.FormatInt(eventID, 10), Text: "legacy row", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second})
		if err != nil || !inserted {
			t.Fatalf("seed inserted=%t err=%v", inserted, err)
		}
		return client, prompts, events, store
	}

	t.Run("name match still delivers", func(t *testing.T) {
		client, prompts, events, store := seed(t, "lane-a", 610)
		defer store.Close()
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		r27Await(t, events, "relay.delivered")
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "legacy row") {
			t.Fatalf("prompts=%q", got)
		}
	})

	t.Run("name mismatch expires unverifiable", func(t *testing.T) {
		client, prompts, events, store := seed(t, "lane-b", 611)
		defer store.Close()
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "lease_unverifiable" {
			t.Fatalf("dropped=%+v", dropped)
		}
		if got := *prompts; len(got) != 0 {
			t.Fatalf("prompts=%q", got)
		}
	})

	t.Run("unnamed occupant expires unverifiable", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, waitStatus: "idle", started: make(chan struct{}, 1)}
		fake.r27SetGet(r449OccupantGet(t, "idle", func(agent map[string]json.RawMessage) {
			delete(agent, "name")
		}), nil)
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		inserted, err := store.InsertRelayHeld(t.Context(), relayHeld{Pane: "fixture-pane", Lane: "lane-a", EventID: 612, JobID: "relay-job-612", Text: "legacy row", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second})
		if err != nil || !inserted {
			t.Fatalf("seed inserted=%t err=%v", inserted, err)
		}
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "lease_unverifiable" {
			t.Fatalf("dropped=%+v", dropped)
		}
		if got := *prompts; len(got) != 0 {
			t.Fatalf("prompts=%q", got)
		}
	})

	// The held row this issue exists for (real id16936) is a pre-#449 row and
	// therefore carries no lease. This is the dead-pane leg of that shape: an
	// empty lease is not "nothing to compare" — a definitively absent occupant
	// still expires it fail-closed.
	t.Run("dead pane expires leaseless row", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, waitStatus: "idle", started: make(chan struct{}, 1)}
		fake.r27SetGet(r27Fixture(t, "agent-get-not-found.json"), errors.New("exit status 1"))
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		inserted, err := store.InsertRelayHeld(t.Context(), relayHeld{Pane: "fixture-pane", Lane: "lane-a", EventID: 613, JobID: "relay-job-613", Text: "legacy row", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second})
		if err != nil || !inserted {
			t.Fatalf("seed inserted=%t err=%v", inserted, err)
		}
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "pane_occupant_gone" {
			t.Fatalf("dropped=%+v", dropped)
		}
		if got := *prompts; len(got) != 0 {
			t.Fatalf("prompts=%q", got)
		}
	})
}

func TestR449LegalExtremes(t *testing.T) {
	r27GuardInbox(t)
	t.Run("zero held rows probes nothing", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, _ := r27Node(t, store, fake)
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 0 || fake.count("get") != 0 || len(*prompts) != 0 {
			t.Fatalf("rows=%d get=%d prompts=%d err=%v", len(rows), fake.count("get"), len(*prompts), err)
		}
	})

	t.Run("route moved twice expires on newest observation", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 3)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r449Directive(620, "lane-a", "fixture-pane", "oldest binding", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		client.relayBusyManager().offer(t.Context(), r449Directive(621, "lane-a", "fixture-pane-2", "hop one", "now"))
		client.relayBusyManager().offer(t.Context(), r449Directive(622, "lane-a", "fixture-pane-3", "hop two", "now"))
		close(gate)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "lane_rerouted" || dropped.OriginalEventID != 620 {
			t.Fatalf("dropped=%+v", dropped)
		}
		got := *prompts
		if len(got) != 2 || got[0] != "fixture-pane-2\x00"+task687Text(621, "hop one") || got[1] != "fixture-pane-3\x00"+task687Text(622, "hop two") {
			t.Fatalf("prompts=%q want only the two live-route deliveries", got)
		}
	})

	t.Run("route back to same pane still checks occupant", func(t *testing.T) {
		gate := make(chan struct{})
		fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 4)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().offer(t.Context(), r27Directive(623, "first occupant's text", "idle"))
		r27Await(t, events, "relay.held")
		r27WaitStarted(t, fake)
		// Route happens to resolve to the same pane id again — but a new
		// session occupies it now, and the second row leases to that session.
		fake.r27SetGet(r449OccupantGet(t, "working", r449SetSession("second-session")), nil)
		client.relayBusyManager().offer(t.Context(), r27Directive(624, "second occupant's text", "idle"))
		r27Await(t, events, "relay.held")
		fake.r27SetGet(r449OccupantGet(t, "idle", r449SetSession("second-session")), nil)
		close(gate)
		dropped := r449DroppedReason(t, events)
		if dropped.Reason != "pane_occupant_changed" || dropped.OriginalEventID != 623 {
			t.Fatalf("dropped=%+v", dropped)
		}
		r27Await(t, events, "relay.delivered")
		got := *prompts
		if len(got) != 1 || !strings.Contains(got[0], "second occupant's text") || strings.Contains(got[0], "first occupant") {
			t.Fatalf("prompts=%q want only the live occupant's delivery", got)
		}
	})

	t.Run("unregistered lane falls back to lease check", func(t *testing.T) {
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		client, prompts, events := r27Node(t, store, fake)
		// No inject for this lane was ever observed, so there is no route
		// evidence; the row must stand on its occupant lease alone.
		occupant := r449OccupantGet(t, "idle", func(agent map[string]json.RawMessage) {})
		parsed, ok := parseRelayAgentOccupant(r27RewritePane(occupant, "fixture-pane"))
		if !ok {
			t.Fatal("fixture occupant did not parse")
		}
		inserted, err := store.InsertRelayHeld(t.Context(), relayHeld{Pane: "fixture-pane", Lane: "lane-ghost", EventID: 630, JobID: "relay-job-630", Text: "no route observation", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second, Lease: parsed.key()})
		if err != nil || !inserted {
			t.Fatalf("seed inserted=%t err=%v", inserted, err)
		}
		client.relayBusyManager().release(t.Context(), "fixture-pane", false)
		r27Await(t, events, "relay.delivered")
		if got := *prompts; len(got) != 1 || !strings.Contains(got[0], "no route observation") {
			t.Fatalf("prompts=%q", got)
		}
	})
}

func TestR449HubDroppedClearsProjectionAndPendingAck(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node", "host-b": "node"}})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	key := relayPendingKey(640, "relay-job-640")
	hub.mu.Lock()
	hub.relayHeld[640] = hubRelayHeldProjection{ID: 640, Lane: "lane-a", Pane: "fixture-pane", Machine: "host-a", JobID: "relay-job-640"}
	hub.r19a.relayPending[key] = relayPending{machine: "host-a", pane: "fixture-pane", eventID: 640, kind: "lane.event", held: true}
	hub.mu.Unlock()
	payload, err := json.Marshal(relayDroppedPayload{JobID: "relay-job-640", Pane: "fixture-pane", Lane: "lane-a", OriginalEventID: 640, Reason: "lane_rerouted"})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: "relay.dropped", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := parseHubInbound(wire); !valid {
		t.Fatalf("parseHubInbound rejected %s", wire)
	}
	// A drop from a different machine — or naming a different pane — must not
	// retire another node's projection or ack window.
	for _, sender := range []string{"host-b", "host-a"} {
		forged := wire
		if sender == "host-a" {
			forgedPayload, _ := json.Marshal(relayDroppedPayload{JobID: "relay-job-640", Pane: "fixture-pane-9", Lane: "lane-a", OriginalEventID: 640, Reason: "lane_rerouted"})
			forged, _ = json.Marshal(struct {
				Type    string          `json:"type"`
				Kind    string          `json:"kind"`
				Payload json.RawMessage `json:"payload"`
			}{Type: "event", Kind: "relay.dropped", Payload: forgedPayload})
		}
		hub.handleAgentMessage(sender, "fixture", agent, forged)
		hub.mu.Lock()
		_, heldExists := hub.relayHeld[640]
		_, pendingExists := hub.r19a.relayPending[key]
		hub.mu.Unlock()
		if !heldExists || !pendingExists {
			t.Fatalf("forged drop from %s cleared held=%t pending=%t", sender, heldExists, pendingExists)
		}
	}
	hub.handleAgentMessage("host-a", "fixture", agent, wire)
	hub.mu.Lock()
	_, heldExists := hub.relayHeld[640]
	_, pendingExists := hub.r19a.relayPending[key]
	hub.mu.Unlock()
	if heldExists || pendingExists {
		t.Fatalf("drop left held=%t pending=%t", heldExists, pendingExists)
	}
}

// A reroute observed between the release filter and the inject itself must
// still fail closed: deliver() re-checks each item's lane immediately before
// prompting, which also covers the "now" path that never enters the filter.
func TestR449DeliverRechecksRouteBeforeInject(t *testing.T) {
	r27GuardInbox(t)
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	item := relayHeld{Pane: "fixture-pane", Lane: "lane-a", EventID: 650, JobID: "relay-job-650", Text: "raced text", HeldSince: time.Now(), DeliverPolicy: "idle", MaxWait: time.Second}
	inserted, err := store.InsertRelayHeld(t.Context(), item)
	if err != nil || !inserted {
		t.Fatalf("seed inserted=%t err=%v", inserted, err)
	}
	manager.noteLaneRoute("lane-a", "fixture-pane-2")
	manager.deliver(t.Context(), []relayHeld{item}, false)
	dropped := r449DroppedReason(t, events)
	if dropped.Reason != "lane_rerouted" || dropped.OriginalEventID != 650 {
		t.Fatalf("dropped=%+v", dropped)
	}
	if got := *prompts; len(got) != 0 {
		t.Fatalf("rerouted item prompted=%q", got)
	}
	if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 0 {
		t.Fatalf("rows=%+v err=%v", rows, err)
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
