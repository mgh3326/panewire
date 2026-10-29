package panewire

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The sink fixture verifies that hub shutdown leaves ordinary relays intact.
type t1009HungSink struct {
	fake      *fakeHandoffkeep
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	posts     atomic.Int64
	cancelled atomic.Int64
}

func (s *t1009HungSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/relay/events" {
		s.fake.ServeHTTP(w, r)
		return
	}
	s.posts.Add(1)
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	s.startOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-r.Context().Done():
		s.cancelled.Add(1)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	s.fake.ServeHTTP(w, r)
}

// An ordinary lane-event relay keeps context.Background(), so hub Close must
// not cancel one in flight and must not reach one started after Close.
func TestT1009CloseNeverCancelsOrdinaryRelay(t *testing.T) {
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	sink := &t1009HungSink{fake: fake, started: make(chan struct{}), release: make(chan struct{})}
	server := httptest.NewServer(sink)
	t.Cleanup(server.Close)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(sink.release) }) })
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, io.Discard)
	ingress := func(eventID string) hubJobEventPayload {
		return hubJobEventPayload{JobID: laneEventTransportID("lane-a", eventID), Epoch: 1, OwnerLane: "lane-a", EventID: eventID, Text: "ordinary lane event", Host: "hub", Label: "t1009"}
	}

	result := make(chan relayLaneEventResult, 1)
	go func() { result <- hub.relayLaneEvent(ingress("t1009-ordinary"), nil) }()
	select {
	case <-sink.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the ordinary relay never reached the sink")
	}
	if err := hub.Close(); err != nil {
		t.Fatalf("Close=%v", err)
	}
	releaseOnce.Do(func() { close(sink.release) })
	select {
	case res := <-result:
		if res.PersistFailed || res.ID == 0 {
			t.Fatalf("Close broke the in-flight ordinary relay: %+v", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ordinary relay never finished")
	}
	if got := sink.cancelled.Load(); got != 0 {
		t.Fatalf("Close cancelled the in-flight ordinary relay (%d cancelled POSTs)", got)
	}
	// A relay that starts after Close must still persist on its own context.
	res := hub.relayLaneEvent(ingress("t1009-ordinary-2"), nil)
	if res.PersistFailed || res.ID == 0 {
		t.Fatalf("an ordinary relay started after Close was not persisted: %+v", res)
	}
}

// AC5/NICE-1: pin the held.Lane == obs.lane term in laneHeldCountLocked. A
// held projection entry whose lane no longer matches the durable row's owner
// lane — a lane re-pointed between inject and sweep leaves exactly that
// divergence — must not be credited to this lane even though its durable id
// sits inside the observation's counted set. Dropping the term turns this
// RED: the foreign held row inflates held_count to 1.
func TestT1009LaneStallHeldCountSkipsHeldRowsFromOtherLanes(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logBuf bytes.Buffer
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, &logBuf)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t1009-held", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(61, "lane-a", "t1009-held", old, 1))
	hub.mu.Lock()
	// The durable row is lane-a's, but the held projection still names the
	// lane the pane reported before the lane was re-pointed.
	hub.relayHeld[61] = hubRelayHeldProjection{ID: 61, Lane: "lane-b", Pane: "w1:p9", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t1009-held"}
	hub.mu.Unlock()
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub) // first dampened observation
	t961Sweep(hub) // second: the episode activates
	stalled := t961StallBroadcasts(events)
	if len(stalled) != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", len(stalled))
	}
	payload := stalled[0]
	if payload["undelivered_count"] != float64(1) || payload["held_count"] != float64(0) {
		t.Fatalf("relay.lane_stalled payload=%v, want undelivered_count=1 held_count=0 (the held projection belongs to lane-b)", payload)
	}
	line := ""
	for _, candidate := range strings.Split(strings.TrimRight(logBuf.String(), "\n"), "\n") {
		if strings.Contains(candidate, "relay lane stalled") {
			line = candidate
		}
	}
	if line == "" || !strings.Contains(line, "held_count=0") || !strings.Contains(line, "undelivered_count=1") {
		t.Fatalf("the Warn line lacks held_count=0:\n%s", logBuf.String())
	}
}
