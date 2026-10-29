package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func r24IngressBody(eventID, text string) map[string]any {
	return map[string]any{"kind": "lane.event", "lane": "lane-a", "event_id": eventID, "text": text, "label": "operator-web"}
}

func r24PostIngress(t *testing.T, hub *HubServer, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/relay/events", strings.NewReader(string(encoded)))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func r24DecodeIngressResponse(t *testing.T, writer *httptest.ResponseRecorder) hubRelayIngressResponse {
	t.Helper()
	var response hubRelayIngressResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("response=%q err=%v", writer.Body.String(), err)
	}
	return response
}

func r24DecodeDuplicate(t *testing.T, writer *httptest.ResponseRecorder) struct {
	Error string `json:"error"`
	ID    int64  `json:"id"`
} {
	t.Helper()
	var response struct {
		Error string `json:"error"`
		ID    int64  `json:"id"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("response=%q err=%v", writer.Body.String(), err)
	}
	return response
}

func r24FirstPostBody(t *testing.T, fake *fakeHandoffkeep) map[string]any {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, call := range fake.calls {
		if call.Method == http.MethodPost && call.Path == "/v1/relay/events" {
			return call.Body
		}
	}
	t.Fatal("handoffkeep POST was not recorded")
	return nil
}

// AC1: the HTTP source writes first, then queues precisely the established
// lane-event directive. It has no producer node and therefore no persisted ACK.
func TestR24HTTPIngressRouted(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)

	writer := r24PostIngress(t, hub, "op", r24IngressBody("event-1", "approval requested"))
	if writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	response := r24DecodeIngressResponse(t, writer)
	if response.ID < 1 || response.EventID != "event-1" || response.Lane != "lane-a" || !response.Routed || response.Machine != "host-a" {
		t.Fatalf("response=%+v", response)
	}
	body := r24FirstPostBody(t, fake)
	if body["kind"] != "lane.event" || body["owner_lane"] != "lane-a" || body["reason"] != "http_ingress:operator-web" || body["machine"] != "hub" || body["pane_id"] != "" || body["event_id"] != "event-1" || body["text"] != "approval requested" {
		t.Fatalf("handoffkeep body=%v", body)
	}
	select {
	case directive := <-destination.relays:
		want := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: approval requested"
		if directive.Type != "relay.inject" || directive.Kind != "lane.event" || directive.Pane != "w1:p1" || directive.Text != want {
			t.Fatalf("directive=%+v want=%q", directive, want)
		}
	default:
		t.Fatal("HTTP ingress did not queue relay.inject")
	}
	if persisted := drainPersisted(destination); len(persisted) != 0 {
		t.Fatalf("HTTP ingress sent producer acknowledgement=%+v", persisted)
	}
	if events := seen("relay.http_ingress"); len(events) != 1 || !strings.Contains(string(events[0]), `"label":"operator-web"`) || !strings.Contains(string(events[0]), `"lane":"lane-a"`) || !strings.Contains(string(events[0]), `"event_id":"event-1"`) || !strings.Contains(string(events[0]), `"routed":true`) {
		t.Fatalf("http ingress events=%q", events)
	}
}

// AC2: an unregistered destination is durable, visible, and replayable after
// the target node's next hello, without a second HTTP request.
func TestR24HTTPIngressUnroutedReplaysOnHello(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{}}`, client, nil)
	seen := r20t5Subscribe(t, hub)

	writer := r24PostIngress(t, hub, "op", r24IngressBody("event-2", "decision needed"))
	response := r24DecodeIngressResponse(t, writer)
	if writer.Code != http.StatusCreated || response.ID < 1 || response.Routed || response.Machine != "" || drainRelays(&hubAgent{}) != 0 {
		t.Fatalf("status=%d response=%+v", writer.Code, response)
	}
	if fake.rowCount() != 1 || len(seen("relay.unrouted")) != 1 {
		t.Fatalf("rows=%d unrouted=%d", fake.rowCount(), len(seen("relay.unrouted")))
	}
	if err := os.WriteFile(hub.reportRelayPath, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.connect("host-a", "r24", "fixture", destination, true)
	select {
	case directive := <-destination.relays:
		if directive.Text != "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: decision needed" {
			t.Fatalf("directive=%+v", directive)
		}
	case <-time.After(time.Second):
		t.Fatal("node hello did not replay the durable HTTP ingress row")
	}
	hub.replayUndeliveredLaneEvents(context.Background())
	if injections := drainRelays(destination); injections != 0 {
		t.Fatalf("replay injected %d duplicate events", injections)
	}
}

func TestR24HTTPIngressRejectsInvalidPayloads(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)
	cases := []struct {
		name string
		body any
	}{
		{name: "kind", body: map[string]any{"kind": "job.completed", "lane": "lane-a", "event_id": "event-3", "text": "payload", "label": "operator-web"}},
		{name: "missing event_id", body: map[string]any{"kind": "lane.event", "lane": "lane-a", "text": "payload", "label": "operator-web"}},
		{name: "unknown field", body: map[string]any{"kind": "lane.event", "lane": "lane-a", "event_id": "event-3", "text": "payload", "label": "operator-web", "extra": true}},
		{name: "control text", body: map[string]any{"kind": "lane.event", "lane": "lane-a", "event_id": "event-3", "text": "bad\ntext", "label": "operator-web"}},
		{name: "invalid lane", body: map[string]any{"kind": "lane.event", "lane": "Lane A", "event_id": "event-3", "text": "payload", "label": "operator-web"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			writer := r24PostIngress(t, hub, "op", test.body)
			if writer.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
		})
	}
	if fake.count(http.MethodPost, "/v1/relay/events") != 0 || drainRelays(destination) != 0 || len(seen("relay.http_ingress")) != 0 {
		t.Fatalf("posts=%d injections=%d ingress events=%d", fake.count(http.MethodPost, "/v1/relay/events"), drainRelays(destination), len(seen("relay.http_ingress")))
	}
}

func TestR24HTTPIngressRejectsRouteSizedTextWithoutTruncating(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	paneHub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	paneDestination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	paneHub.nodes["host-a"] = &hubNodeRecord{agent: paneDestination}
	seen := r20t5Subscribe(t, paneHub)
	tooLong := strings.Repeat("x", laneEventTextLimit+1)
	if writer := r24PostIngress(t, paneHub, "op", r24IngressBody("event-4", tooLong)); writer.Code != http.StatusBadRequest {
		t.Fatalf("pane status=%d body=%s", writer.Code, writer.Body.String())
	}
	if fake.count(http.MethodPost, "/v1/relay/events") != 0 || drainRelays(paneDestination) != 0 || len(seen("relay.rejected")) != 1 {
		t.Fatalf("pane posts=%d injections=%d rejected=%d", fake.count(http.MethodPost, "/v1/relay/events"), drainRelays(paneDestination), len(seen("relay.rejected")))
	}

	sinkHub := r20Hub(t, `{"lanes":{"lane-a":{"sink":true}}}`, client, nil)
	if writer := r24PostIngress(t, sinkHub, "op", r24IngressBody("event-5", tooLong)); writer.Code != http.StatusCreated {
		t.Fatalf("sink 2049 status=%d body=%s", writer.Code, writer.Body.String())
	}
	// encoding/json writes '<' as a six-byte \u003c escape. The HTTP body cap
	// must admit this valid 8192-byte sink text before route-sized validation.
	if writer := r24PostIngress(t, sinkHub, "op", r24IngressBody("event-5-escaped", strings.Repeat("<", laneEventTextLimitSink))); writer.Code != http.StatusCreated {
		t.Fatalf("escaped sink 8192 status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := r24PostIngress(t, sinkHub, "op", r24IngressBody("event-6", strings.Repeat("x", laneEventTextLimitSink+1))); writer.Code != http.StatusBadRequest {
		t.Fatalf("sink 8193 status=%d body=%s", writer.Code, writer.Body.String())
	}
}

func TestR24HTTPIngressRequiresOperatorToken(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)
	for _, token := range []string{"", "wrong"} {
		writer := r24PostIngress(t, hub, token, r24IngressBody("event-7", "payload"))
		if writer.Code != http.StatusUnauthorized {
			t.Fatalf("token=%q status=%d body=%s", token, writer.Code, writer.Body.String())
		}
	}
	if fake.count(http.MethodPost, "/v1/relay/events") != 0 || drainRelays(destination) != 0 || len(seen("relay.http_ingress")) != 0 {
		t.Fatalf("posts=%d injections=%d ingress=%d", fake.count(http.MethodPost, "/v1/relay/events"), drainRelays(destination), len(seen("relay.http_ingress")))
	}
}

func TestR24HTTPIngressSameHubDuplicateReturnsDurableID(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)
	first := r24PostIngress(t, hub, "op", r24IngressBody("event-8", "payload"))
	firstResponse := r24DecodeIngressResponse(t, first)
	secondBody := r24IngressBody("event-8", "changed payload")
	// Dedupe is only (lane,event_id), not the producer label or body. A
	// different valid producer label exposes an accidental fallback to the old
	// job dedupe key, which included reason.
	secondBody["label"] = "host-a"
	second := r24PostIngress(t, hub, "op", secondBody)
	duplicate := r24DecodeDuplicate(t, second)
	if first.Code != http.StatusCreated || second.Code != http.StatusConflict || duplicate.Error != "duplicate_event_id" || duplicate.ID != firstResponse.ID {
		t.Fatalf("first=%d response=%+v second=%d duplicate=%+v", first.Code, firstResponse, second.Code, duplicate)
	}
	if fake.rowCount() != 1 || fake.count(http.MethodPost, "/v1/relay/events") != 1 || drainRelays(destination) != 1 || len(seen("relay.http_ingress")) != 1 {
		t.Fatalf("rows=%d posts=%d injections=%d ingress=%d", fake.rowCount(), fake.count(http.MethodPost, "/v1/relay/events"), drainRelays(destination), len(seen("relay.http_ingress")))
	}
}

func TestR24HTTPIngressConcurrentDuplicateReturnsZeroBeforePersistedID(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	started, release := make(chan struct{}), make(chan struct{})
	fake.mu.Lock()
	fake.observe = func(method, path string) {
		if method == http.MethodPost && path == "/v1/relay/events" {
			started <- struct{}{}
			<-release
		}
	}
	fake.mu.Unlock()
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		body, _ := json.Marshal(r24IngressBody("event-8-race", "payload"))
		request := httptest.NewRequest(http.MethodPost, "/v1/relay/events", strings.NewReader(string(body)))
		request.Header.Set("Authorization", "Bearer op")
		writer := httptest.NewRecorder()
		hub.Handler().ServeHTTP(writer, request)
		firstDone <- writer
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach persistence")
	}
	second := r24PostIngress(t, hub, "op", r24IngressBody("event-8-race", "payload"))
	duplicate := r24DecodeDuplicate(t, second)
	if second.Code != http.StatusConflict || duplicate.Error != "duplicate_event_id" || duplicate.ID != 0 || fake.count(http.MethodPost, "/v1/relay/events") != 1 {
		t.Fatalf("status=%d duplicate=%+v posts=%d", second.Code, duplicate, fake.count(http.MethodPost, "/v1/relay/events"))
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusCreated {
			t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not complete after persistence released")
	}
}

func TestR24HTTPIngressRestartDuplicateReleasesReplayClaim(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 101, Kind: "lane.event", JobID: laneEventTransportID("lane-a", "event-9"), Epoch: 1, OwnerLane: "lane-a", EventID: "event-9", Text: "payload", Reason: "http_ingress:operator-web", Attempts: 1})
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	seen := r20t5Subscribe(t, hub)
	writer := r24PostIngress(t, hub, "op", r24IngressBody("event-9", "payload"))
	duplicate := r24DecodeDuplicate(t, writer)
	if writer.Code != http.StatusConflict || duplicate.ID != 101 || fake.count(http.MethodPost, "/v1/relay/events") != 1 || len(seen("relay.http_ingress")) != 0 {
		t.Fatalf("status=%d duplicate=%+v posts=%d ingress=%d", writer.Code, duplicate, fake.count(http.MethodPost, "/v1/relay/events"), len(seen("relay.http_ingress")))
	}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.connect("host-a", "r24", "fixture", destination, true)
	select {
	case <-destination.relays:
	case <-time.After(time.Second):
		t.Fatal("restart duplicate left the durable row blocked from replay")
	}
}

func TestR24HTTPIngressPersistenceFailuresDoNotInject(t *testing.T) {
	t.Run("handoffkeep error", func(t *testing.T) {
		fake, client, closeServer := newFakeHandoffkeep(t)
		defer closeServer()
		fake.status = http.StatusInternalServerError
		hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
		destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
		hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
		seen := r20t5Subscribe(t, hub)
		writer := r24PostIngress(t, hub, "op", r24IngressBody("event-10", "payload"))
		injections, unpersisted, ingress := drainRelays(destination), len(seen("relay.unpersisted")), len(seen("relay.http_ingress"))
		if writer.Code != http.StatusBadGateway || injections != 0 || unpersisted != 1 || ingress != 0 {
			t.Fatalf("status=%d injections=%d unpersisted=%d ingress=%d", writer.Code, injections, unpersisted, ingress)
		}
	})
	t.Run("no handoffkeep", func(t *testing.T) {
		hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
		destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
		hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
		seen := r20t5Subscribe(t, hub)
		writer := r24PostIngress(t, hub, "op", r24IngressBody("event-11", "payload"))
		injections, unpersisted, ingress := drainRelays(destination), len(seen("relay.unpersisted")), len(seen("relay.http_ingress"))
		if writer.Code != http.StatusBadGateway || injections != 0 || unpersisted != 1 || ingress != 0 {
			t.Fatalf("status=%d injections=%d unpersisted=%d ingress=%d", writer.Code, injections, unpersisted, ingress)
		}
	})
}

func TestR24HTTPIngressSinkMarksDeliveredWithoutAckTimer(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"sink":true}}}`, client, nil)
	writer := r24PostIngress(t, hub, "op", r24IngressBody("event-12", "payload"))
	response := r24DecodeIngressResponse(t, writer)
	if writer.Code != http.StatusCreated || !response.Routed || response.Machine != "sink" {
		t.Fatalf("status=%d response=%+v", writer.Code, response)
	}
	want := []string{"POST /v1/relay/events", "POST /v1/relay/events/101/delivered"}
	if got := fake.sequence(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("handoffkeep calls=%v want=%v", got, want)
	}
	hub.mu.Lock()
	pending, timeouts := len(hub.r19a.relayPending), len(hub.r19a.relayTimeouts)
	hub.mu.Unlock()
	if pending != 0 || timeouts != 0 {
		t.Fatalf("sink started ack state pending=%d timeouts=%d", pending, timeouts)
	}
}

func TestR24HTTPIngressDeliveredRowDoesNotReplay(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	writer := r24PostIngress(t, hub, "op", r24IngressBody("event-13", "payload"))
	if writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if injections := drainRelays(destination); injections != 1 {
		t.Fatalf("initial injections=%d", injections)
	}
	pending := hub.acknowledgeRelayPendingForTest(t, "host-a", relayAckPayload{JobID: laneEventTransportID("lane-a", "event-13"), Pane: "w1:p1"})
	hub.markRelayEventDelivered(pending)
	hub.replayUndeliveredLaneEvents(context.Background())
	if injections := drainRelays(destination); injections != 0 {
		t.Fatalf("delivered HTTP row replayed %d times", injections)
	}
}
