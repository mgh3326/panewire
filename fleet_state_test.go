package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	fsOperatorToken = "fs-fixture-operator-token-not-for-logs"
	fsNodeAToken    = "fs-fixture-node-a-token-not-for-logs"
	fsNodeBToken    = "fs-fixture-node-b-token-not-for-logs"
	fsAccountKey    = "claude:0123456789abcdef"
)

func fsServiceToken(client string) string { return "fs-fixture-" + client + "-token-not-for-logs" }

func fsHub(t *testing.T, policyPath string) (*HubServer, *httptest.Server) {
	t.Helper()
	tokens := map[string]string{"operator": fsOperatorToken, "node-a": fsNodeAToken, "node-b": fsNodeBToken, "svc-state": fsServiceToken("svc-state"), "svc-other": fsServiceToken("svc-other")}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:              tokens,
		Scopes:              map[string]string{"svc-state": "state.read", "svc-other": "nodes.agents.read"},
		PlacementPolicyPath: policyPath,
		StaleAfter:          30 * time.Second,
		KeepaliveInterval:   10 * time.Second,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	t.Cleanup(func() { _ = hub.Close() })
	return hub, server
}

// fsConnectNode opens the node websocket, says hello and sends one heartbeat,
// then waits until the hub has announced the node as a live publisher.
func fsConnectNode(t *testing.T, hub *HubServer, server *httptest.Server, machine, token string) *websocket.Conn {
	t.Helper()
	node := r6DialAgent(t, server.URL, machine, token)
	t.Cleanup(func() { _ = node.CloseNow() })
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": machine, "version": "fs-test", "accepting": true})
	r6Write(t, node, map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive"}})
	eventually(t, 2*time.Second, machine+" publisher", func() bool {
		message, ok := fsLatest(hub, machine, fleetStateKindPublisher, "")
		return ok && message.Publisher.Connected
	})
	return node
}

func fsLatest(hub *HubServer, host, kind, key string) (FleetStateMessage, bool) {
	hub.fleetState.mu.Lock()
	defer hub.fleetState.mu.Unlock()
	message, ok := hub.fleetState.latest[fleetStateKey{host, kind, key}]
	return message, ok
}

func fsQuotaPublish(host string, measured time.Time, used float64) map[string]any {
	payload := map[string]any{
		"kind": "quota", "key": fsAccountKey, "measured_at": measured.Format(time.RFC3339),
		"quota": map[string]any{"account_key": fsAccountKey, "pool": "claude", "source": "usage-api", "buckets": []map[string]any{{"window": "5h", "used_pct": used, "resets_at": "2026-11-01T05:00:00Z", "scope": "account"}}},
	}
	if host != "" {
		payload["host"] = host
	}
	return map[string]any{"type": "event", "kind": "state", "payload": payload}
}

func fsSpawnPublish(phase, job, profile, spawnHost string, at time.Time) map[string]any {
	spawn := map[string]any{"phase": phase, "job": job, "profile": profile, "at": at.Format(time.RFC3339)}
	if spawnHost != "" {
		spawn["host"] = spawnHost
	}
	return map[string]any{"type": "event", "kind": "state", "payload": map[string]any{"kind": "spawn", "key": job, "measured_at": at.Format(time.RFC3339), "spawn": spawn}}
}

func fsGet(t *testing.T, server *httptest.Server, path string, header http.Header) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range header {
		request.Header[name] = values
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, body
}

func fsOperator() http.Header {
	return http.Header{"Authorization": {"Bearer " + fsOperatorToken}}
}

func fsAs(machine, token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}, "X-Panewire-Machine-Id": {machine}}
}

func fsSnapshot(t *testing.T, server *httptest.Server) fleetStateSnapshot {
	t.Helper()
	status, body := fsGet(t, server, "/v1/state", fsOperator())
	if status != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", status, body)
	}
	var snapshot fleetStateSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type fsEvent struct {
	id, name string
	message  FleetStateMessage
}

// fsStream opens /v1/state/stream and delivers parsed events until the test
// or cancel ends it.
func fsStream(t *testing.T, server *httptest.Server, header http.Header) (<-chan fsEvent, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/state/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range header {
		request.Header[name] = values
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream status=%d type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	events := make(chan fsEvent, 4096)
	go func() {
		defer response.Body.Close()
		defer close(events)
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 64<<10), 1<<20)
		var current fsEvent
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case line == "":
				if current.name != "" {
					events <- current
				}
				current = fsEvent{}
			case strings.HasPrefix(line, "id: "):
				current.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				current.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: ") && current.name == "state":
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &current.message); err != nil {
					current.name = "invalid"
				}
			}
		}
	}()
	t.Cleanup(cancel)
	return events, cancel
}

// fsNext returns the next event the filter accepts, skipping the others.
func fsNext(t *testing.T, events <-chan fsEvent, label string, accept func(fsEvent) bool) fsEvent {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, open := <-events:
			if !open {
				t.Fatalf("stream closed waiting for %s", label)
			}
			if accept(event) {
				return event
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s", label)
		}
	}
}

// Acceptance 1: a node token publishes only its own host; operator and
// service tokens read; a node token cannot read.
func TestFleetStateNodePublishesOnlyItsOwnHost(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	measured := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)

	before := hub.unknownMessageCount()
	r6Write(t, nodeA, fsQuotaPublish("node-b", measured, 10))
	r6Write(t, nodeA, fsSpawnPublish("start", "job-x", "builder-opus", "node-b", measured))
	eventually(t, 2*time.Second, "refusals counted", func() bool { return hub.unknownMessageCount() == before+2 })
	if _, ok := fsLatest(hub, "node-b", fleetStateKindQuota, fsAccountKey); ok {
		t.Fatal("node-a published a quota value for node-b")
	}
	if _, ok := fsLatest(hub, "node-b", fleetStateKindSpawn, "job-x"); ok {
		t.Fatal("node-a published a spawn value for node-b")
	}

	r6Write(t, nodeA, fsQuotaPublish("", measured, 10))
	r6Write(t, nodeA, fsQuotaPublish("node-a", measured.Add(time.Second), 11))
	eventually(t, 2*time.Second, "own quota stored", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey)
		return ok && *message.Quota.Buckets[0].UsedPct == 11
	})

	for _, read := range []struct {
		name   string
		header http.Header
		want   int
	}{
		{"operator", fsOperator(), http.StatusOK},
		{"service with state.read", fsAs("svc-state", fsServiceToken("svc-state")), http.StatusOK},
		{"service without state.read", fsAs("svc-other", fsServiceToken("svc-other")), http.StatusForbidden},
		{"node token", fsAs("node-a", fsNodeAToken), http.StatusUnauthorized},
		{"no token", http.Header{}, http.StatusUnauthorized},
	} {
		for _, path := range []string{"/v1/state", "/v1/state/stream"} {
			if read.want == http.StatusOK && path == "/v1/state/stream" {
				events, cancel := fsStream(t, server, read.header)
				event := fsNext(t, events, read.name+" stream", func(e fsEvent) bool { return e.message.Kind == fleetStateKindQuota })
				if event.message.Host != "node-a" {
					t.Fatalf("%s stream quota host=%q", read.name, event.message.Host)
				}
				cancel()
				continue
			}
			if status, body := fsGet(t, server, path, read.header); status != read.want {
				t.Fatalf("%s GET %s status=%d want %d body=%s", read.name, path, status, read.want, body)
			}
		}
	}
	snapshot := fsSnapshot(t, server)
	for _, message := range snapshot.States {
		if message.Kind == fleetStateKindQuota && message.Host != "node-a" {
			t.Fatalf("snapshot quota host=%q", message.Host)
		}
	}
}

// Acceptance 1: a reader that reconnects with Last-Event-ID receives exactly
// what it missed while the ring still covers it, and a reset otherwise.
func TestFleetStateStreamResumesWithLastEventID(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	events, cancel := fsStream(t, server, fsOperator())
	r6Write(t, nodeA, fsSpawnPublish("start", "job-0", "builder-opus", "", base))
	seen := fsNext(t, events, "job-0", func(e fsEvent) bool { return e.message.Key == "job-0" })
	cancel()

	for i := 1; i <= 5; i++ {
		r6Write(t, nodeA, fsSpawnPublish("start", fmt.Sprintf("job-%d", i), "builder-opus", "", base.Add(time.Duration(i)*time.Second)))
	}
	eventually(t, 2*time.Second, "five more spawns", func() bool { _, ok := fsLatest(hub, "node-a", fleetStateKindSpawn, "job-5"); return ok })

	resumed, _ := fsStream(t, server, http.Header{"Authorization": {"Bearer " + fsOperatorToken}, "Last-Event-ID": {seen.id}})
	for i := 1; i <= 5; i++ {
		event := fsNext(t, resumed, fmt.Sprintf("missed job-%d", i), func(fsEvent) bool { return true })
		if event.name != "state" || event.message.Key != fmt.Sprintf("job-%d", i) || event.id != event.message.ID {
			t.Fatalf("missed event %d = %+v", i, event)
		}
	}

	// The query parameter resumes the same way (EventSource cannot set
	// headers on its first request).
	status, _ := fsGet(t, server, "/v1/state?x=1", fsOperator())
	if status != http.StatusOK {
		t.Fatalf("snapshot status=%d", status)
	}

	// An id from another epoch, or older than the ring, gets a reset and the
	// full snapshot instead of a silent gap.
	for _, stale := range []string{"otherepoch-1", hub.fleetState.epoch + "-0"} {
		if stale == hub.fleetState.epoch+"-0" {
			hub.fleetState.mu.Lock()
			for i := 0; i < fleetStateRingSize+10; i++ {
				hub.fleetState.seq++
				hub.fleetState.ring = append(hub.fleetState.ring, FleetStateMessage{Seq: hub.fleetState.seq})
			}
			hub.fleetState.ring = hub.fleetState.ring[len(hub.fleetState.ring)-fleetStateRingSize:]
			hub.fleetState.mu.Unlock()
		}
		reset, _ := fsStream(t, server, http.Header{"Authorization": {"Bearer " + fsOperatorToken}, "Last-Event-ID": {stale}})
		if first := fsNext(t, reset, "reset", func(fsEvent) bool { return true }); first.name != "reset" {
			t.Fatalf("Last-Event-ID %q: first event %q, want reset", stale, first.name)
		}
		fsNext(t, reset, "snapshot after reset", func(e fsEvent) bool { return e.message.Key == "job-5" })
	}
}

func TestFleetStateRingReplayIsBounded(t *testing.T) {
	store := newFleetStateStore(time.Unix(1, 0))
	now := time.Unix(100, 0)
	for i := 0; i < 3*fleetStateRingSize; i++ {
		store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindSpawn, Key: fmt.Sprintf("job-%d", i%10), ReceivedAt: now.Add(time.Duration(i) * time.Second), Spawn: &FleetStateSpawn{Phase: "start", Job: "x", At: now.Add(time.Duration(i) * time.Second)}}, 0)
	}
	if len(store.ring) >= 2*fleetStateRingSize {
		t.Fatalf("ring grew to %d", len(store.ring))
	}
	recent := fmt.Sprintf("%s-%d", store.epoch, store.seq-10)
	_, replay, reset, ok := store.subscribe(recent)
	if !ok || reset || len(replay) != 10 || replay[0].Seq != store.seq-9 {
		t.Fatalf("recent resume reset=%v replay=%d", reset, len(replay))
	}
	_, replay, reset, ok = store.subscribe(fmt.Sprintf("%s-%d", store.epoch, store.seq-fleetStateRingSize-1))
	if !ok || !reset || len(replay) != 10 {
		t.Fatalf("expired resume reset=%v replay=%d (want reset and the 10-key snapshot)", reset, len(replay))
	}
	for i := 2; i < fleetStateMaxReaders; i++ {
		if _, _, _, ok := store.subscribe(""); !ok {
			t.Fatalf("reader %d refused", i)
		}
	}
	if _, _, _, ok := store.subscribe(""); ok {
		t.Fatal("reader beyond the cap admitted")
	}
}

func TestFleetStateSlowReaderIsCutOff(t *testing.T) {
	store := newFleetStateStore(time.Unix(1, 0))
	reader, _, _, _ := store.subscribe("")
	now := time.Unix(100, 0)
	for i := 0; i <= fleetStateReaderBuffer; i++ {
		store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindSpawn, Key: fmt.Sprintf("job-%d", i), ReceivedAt: now, Spawn: &FleetStateSpawn{Phase: "start", Job: "x", At: now}}, 0)
	}
	select {
	case <-reader.overflow:
	default:
		t.Fatal("a reader a full buffer behind was not cut off")
	}
	store.unsubscribe(reader) // must not double-close
}

func TestFleetStateHostKeysAreBounded(t *testing.T) {
	store := newFleetStateStore(time.Unix(1, 0))
	now := time.Unix(100, 0)
	store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindSpawn, Key: "ended", ReceivedAt: now, Spawn: &FleetStateSpawn{Phase: "end", Job: "ended", At: now}}, 0)
	for i := 0; i < fleetStateSpawnKeysPerHost; i++ {
		at := now.Add(time.Duration(i+1) * time.Second)
		store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindSpawn, Key: fmt.Sprintf("job-%d", i), ReceivedAt: at, Spawn: &FleetStateSpawn{Phase: "start", Job: "x", At: at}}, 0)
	}
	if len(store.latest) != fleetStateSpawnKeysPerHost {
		t.Fatalf("latest holds %d spawn keys", len(store.latest))
	}
	if _, kept := store.latest[fleetStateKey{"node-a", fleetStateKindSpawn, "ended"}]; kept {
		t.Fatal("the ended spawn was kept over a running one")
	}
	store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindSpawn, Key: "job-0", ReceivedAt: now.Add(time.Hour), Spawn: &FleetStateSpawn{Phase: "end", Job: "job-0", At: now.Add(time.Hour)}}, 0)
	store.expire(now.Add(3 * time.Hour))
	if _, kept := store.latest[fleetStateKey{"node-a", fleetStateKindSpawn, "job-0"}]; kept {
		t.Fatal("an ended spawn outlived its retention")
	}
}

// Acceptance 4: spawn start and end appear on the stream with job, profile
// and host.
func TestFleetStateSpawnEventsOnStream(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	events, _ := fsStream(t, server, fsOperator())
	started := time.Now().UTC().Truncate(time.Second)

	r6Write(t, nodeA, fsSpawnPublish("start", "job-sse", "builder-opus", "", started))
	start := fsNext(t, events, "spawn start", func(e fsEvent) bool { return e.message.Kind == fleetStateKindSpawn })
	r6Write(t, nodeA, fsSpawnPublish("end", "job-sse", "builder-opus", "node-a", started.Add(time.Minute)))
	end := fsNext(t, events, "spawn end", func(e fsEvent) bool { return e.message.Kind == fleetStateKindSpawn })
	for phase, event := range map[string]fsEvent{"start": start, "end": end} {
		spawn := event.message.Spawn
		if spawn == nil || spawn.Phase != phase || spawn.Job != "job-sse" || spawn.Profile != "builder-opus" || spawn.Host != "node-a" || event.message.Host != "node-a" || event.message.Key != "job-sse" {
			t.Fatalf("%s event = %+v spawn=%+v", phase, event.message, spawn)
		}
	}
	if end.message.Seq <= start.message.Seq {
		t.Fatalf("end seq %d not after start seq %d", end.message.Seq, start.message.Seq)
	}
}

// The publisher heartbeat distinguishes an unchanged value from a dead
// publisher: disconnect is announced at once and the quota value stays.
func TestFleetStatePublisherHeartbeat(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	r6Write(t, nodeA, fsQuotaPublish("", time.Now().UTC().Add(-20*time.Minute).Truncate(time.Second), 10))
	eventually(t, 2*time.Second, "quota stored", func() bool { _, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey); return ok })
	events, _ := fsStream(t, server, fsOperator())
	_ = nodeA.Close(websocket.StatusNormalClosure, "")
	dead := fsNext(t, events, "publisher disconnect", func(e fsEvent) bool {
		return e.message.Kind == fleetStateKindPublisher && e.message.Seq > 0 && !e.message.Publisher.Connected
	})
	if dead.message.Host != "node-a" || dead.message.Publisher.LastSeen.IsZero() {
		t.Fatalf("publisher event = %+v", dead.message)
	}
	if _, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey); !ok {
		t.Fatal("the quota value was dropped with its publisher; consumers decide by the publisher state")
	}
}

func TestFleetStateLoadMirrorsPlacement(t *testing.T) {
	policyPath := t.TempDir() + "/placement.json"
	writeTestFile(t, policyPath, `{"local_machine":"node-a","spill_targets":["node-b"],"max_active_jobs":5,"load_ratio":0.5,"capacity_weight":{"node-a":2}}`)
	hub, server := fsHub(t, policyPath)
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Now().UTC()
	hub.publishFleetLoad(t.Context(), now)
	hub.mu.Lock()
	policy := hub.placementPolicy
	hub.mu.Unlock()
	placement := hub.makePlacement(policy, placementMetrics{}, "hub-only", now)
	for _, candidate := range placement.Candidates {
		message, ok := fsLatest(hub, candidate.Machine, fleetStateKindLoad, "")
		if !ok {
			t.Fatalf("no load value for %s", candidate.Machine)
		}
		if message.Load.Score != candidate.Score || message.Load.Connected != candidate.Connected || message.Load.Reason != candidate.Reason {
			t.Fatalf("%s load %+v differs from placement %+v", candidate.Machine, message.Load, candidate)
		}
	}
	a, _ := fsLatest(hub, "node-a", fleetStateKindLoad, "")
	b, _ := fsLatest(hub, "node-b", fleetStateKindLoad, "")
	if a.Load.CapacityWeight != 2 || !a.Load.Connected || !a.Load.Accepting || b.Load.Connected || b.Load.CapacityWeight != 1 {
		t.Fatalf("load a=%+v b=%+v", a.Load, b.Load)
	}
	// An unchanged value is not re-emitted before the republish bound.
	hub.publishFleetLoad(t.Context(), now.Add(time.Second))
	if again, _ := fsLatest(hub, "node-b", fleetStateKindLoad, ""); again.Seq != b.Seq {
		t.Fatalf("unchanged load re-emitted after 1s (seq %d -> %d)", b.Seq, again.Seq)
	}
	hub.publishFleetLoad(t.Context(), now.Add(fleetStateLoadRepublish))
	if again, _ := fsLatest(hub, "node-b", fleetStateKindLoad, ""); again.Seq == b.Seq || !again.MeasuredAt.Equal(now.Add(fleetStateLoadRepublish)) {
		t.Fatalf("unchanged load not republished after %s: %+v", fleetStateLoadRepublish, again)
	}
}

func TestFleetStatePublishDecodeRefusesMalformed(t *testing.T) {
	valid := `{"kind":"quota","key":"claude:0123456789abcdef","measured_at":"2026-11-01T00:00:00Z","quota":{"account_key":"claude:0123456789abcdef","pool":"claude","buckets":[{"window":"5h","used_pct":10}]}}`
	if _, ok := decodeFleetStatePublish([]byte(valid)); !ok {
		t.Fatal("valid quota refused")
	}
	for name, raw := range map[string]string{
		"unknown field":   strings.Replace(valid, `"kind"`, `"token":"x","kind"`, 1),
		"key mismatch":    strings.Replace(valid, `"account_key":"claude:0123456789abcdef"`, `"account_key":"claude:fedcba9876543210"`, 1),
		"pool mismatch":   strings.Replace(valid, `"pool":"claude"`, `"pool":"codex"`, 1),
		"used_pct > 100":  strings.Replace(valid, `"used_pct":10`, `"used_pct":101`, 1),
		"no buckets":      strings.Replace(valid, `[{"window":"5h","used_pct":10}]`, `[]`, 1),
		"load from node":  strings.Replace(valid, `"kind":"quota"`, `"kind":"load"`, 1),
		"no measured_at":  strings.Replace(valid, `"measured_at":"2026-11-01T00:00:00Z",`, ``, 1),
		"free-text scope": strings.Replace(valid, `"used_pct":10`, `"used_pct":10,"scope":"usage HTTP 429 for user"`, 1),
		"oversized":       strings.Replace(valid, `"5h"`, `"`+strings.Repeat("a", fleetStateMaxPublishBytes)+`"`, 1),
		"spawn bad phase": `{"kind":"spawn","key":"job-a","measured_at":"2026-11-01T00:00:00Z","spawn":{"phase":"maybe","job":"job-a","at":"2026-11-01T00:00:00Z"}}`,
		"spawn key/job":   `{"kind":"spawn","key":"job-a","measured_at":"2026-11-01T00:00:00Z","spawn":{"phase":"start","job":"job-b","at":"2026-11-01T00:00:00Z"}}`,
	} {
		if _, ok := decodeFleetStatePublish([]byte(raw)); ok {
			t.Errorf("%s accepted", name)
		}
	}
}

func (h *HubServer) unknownMessageCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unknownMessages
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Production reaches publishFleetLoad only through Sweep.
func TestFleetStateSweepPublishesLoad(t *testing.T) {
	policyPath := t.TempDir() + "/placement.json"
	writeTestFile(t, policyPath, `{"local_machine":"node-a","spill_targets":["node-b"],"max_active_jobs":5,"load_ratio":0.5}`)
	hub, server := fsHub(t, policyPath)
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.Sweep()
	eventually(t, 2*time.Second, "load from sweep", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindLoad, "")
		return ok && message.Load.Connected
	})
}
