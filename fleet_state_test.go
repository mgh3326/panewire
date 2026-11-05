package panewire

import (
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
		if status, body := fsGet(t, server, "/v1/state", read.header); status != read.want {
			t.Fatalf("%s GET /v1/state status=%d want %d body=%s", read.name, status, read.want, body)
		}
	}
	snapshot := fsSnapshot(t, server)
	for _, message := range snapshot.States {
		if message.Kind == fleetStateKindQuota && message.Host != "node-a" {
			t.Fatalf("snapshot quota host=%q", message.Host)
		}
	}
}

// The publisher heartbeat distinguishes an unchanged value from a dead
// publisher: disconnect is announced at once and the quota value stays.
func TestFleetStatePublisherHeartbeat(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	r6Write(t, nodeA, fsQuotaPublish("", time.Now().UTC().Add(-20*time.Minute).Truncate(time.Second), 10))
	eventually(t, 2*time.Second, "quota stored", func() bool { _, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey); return ok })
	_ = nodeA.Close(websocket.StatusNormalClosure, "")
	eventually(t, 2*time.Second, "publisher disconnect", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindPublisher, "")
		return ok && !message.Publisher.Connected && !message.Publisher.LastSeen.IsZero()
	})
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
		raw, _ := json.Marshal(message.Load)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if len(fields["signals"]) == 0 {
			t.Fatalf("%s: computed signals missing from fleet state: %s", candidate.Machine, raw)
		}
		if message.Load.Score != candidate.Score || message.Load.Connected != candidate.Connected || message.Load.Reason != candidate.Reason {
			t.Fatalf("%s load %+v differs from placement %+v", candidate.Machine, message.Load, candidate)
		}
	}
	a, _ := fsLatest(hub, "node-a", fleetStateKindLoad, "")
	b, _ := fsLatest(hub, "node-b", fleetStateKindLoad, "")
	if !a.Load.Connected || !a.Load.Accepting || b.Load.Connected {
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

// Review #141: a hub not built by NewHubServer must not panic on the
// heartbeat, state, sweep or load paths.
func TestFleetStateHubWithoutConstructorDoesNotPanic(t *testing.T) {
	now := time.Now().UTC()
	hub := &HubServer{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), nodes: map[string]*hubNodeRecord{"node-a": {machineID: "node-a", state: "connected", lastHeartbeat: now}}}
	hub.publishFleetPublisher("node-a", now)
	quota := 10.0
	if !hub.publishFleetStateFromNode("node-a", FleetStatePublish{Kind: fleetStateKindQuota, Key: fsAccountKey, MeasuredAt: now, Quota: &FleetStateQuota{AccountKey: fsAccountKey, Pool: "claude", Buckets: []FleetStateBucket{{Window: "5h", UsedPct: &quota}}}}, now) {
		t.Fatal("state publish refused")
	}
	hub.sweepFleetState(now)
	hub.publishFleetLoad(context.Background(), now)
	if _, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey); !ok {
		t.Fatal("quota not stored on a lazily created store")
	}
}

// Review #141: one node cannot flood the hub.
func TestFleetStateNodePublishBudget(t *testing.T) {
	store := newFleetStateStore()
	now := time.Unix(100, 0)
	for i := 0; i < fleetStateNodeBurst; i++ {
		if !store.allowNode("node-a", now) {
			t.Fatalf("publish %d refused inside the burst", i)
		}
	}
	if store.allowNode("node-a", now) {
		t.Fatal("publish beyond the burst admitted")
	}
	if !store.allowNode("node-b", now) {
		t.Fatal("another node's budget was spent by node-a")
	}
	if !store.allowNode("node-a", now.Add(time.Second)) {
		t.Fatal("budget did not refill")
	}
}

func TestFleetStateNodeOverBudgetIsDropped(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	base := time.Now().UTC().Truncate(time.Second)
	before := hub.unknownMessageCount()
	for i := 0; i < fleetStateNodeBurst+40; i++ {
		r6Write(t, nodeA, fsSpawnPublish("start", fmt.Sprintf("job-%d", i), "builder-opus", "", base.Add(time.Duration(i)*time.Second)))
	}
	eventually(t, 3*time.Second, "over-budget publishes counted", func() bool { return hub.unknownMessageCount() >= before+20 })
}

// Review #141: a dead publisher is announced once, not every beat.
func TestFleetStateDeadPublisherAnnouncedOnce(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	_ = nodeA.Close(websocket.StatusNormalClosure, "")
	eventually(t, 2*time.Second, "disconnect announced", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindPublisher, "")
		return ok && !message.Publisher.Connected
	})
	dead, _ := fsLatest(hub, "node-a", fleetStateKindPublisher, "")
	for _, later := range []time.Duration{time.Minute, time.Hour, 24 * time.Hour} {
		hub.publishFleetPublisher("node-a", time.Now().UTC().Add(later))
	}
	if again, _ := fsLatest(hub, "node-a", fleetStateKindPublisher, ""); again.Seq != dead.Seq {
		t.Fatalf("connected:false re-emitted (seq %d -> %d)", dead.Seq, again.Seq)
	}
}

// Review #141: accepting comes from placement's acceptance, not the reason
// text, and usable is placement's own verdict.
func TestFleetStateLoadAcceptingFromPlacement(t *testing.T) {
	policyPath := t.TempDir() + "/placement.json"
	writeTestFile(t, policyPath, `{"local_machine":"node-a","spill_targets":["node-b"],"max_active_jobs":5,"load_ratio":0.5}`)
	hub, server := fsHub(t, policyPath)
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	nodeB := r6DialAgent(t, server.URL, "node-b", fsNodeBToken)
	t.Cleanup(func() { _ = nodeB.CloseNow() })
	r6Write(t, nodeB, map[string]any{"type": "hello", "machine_id": "node-b", "version": "fs-test", "accepting": false})
	r6Write(t, nodeB, map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive"}})
	eventually(t, 2*time.Second, "node-b publisher", func() bool { _, ok := fsLatest(hub, "node-b", fleetStateKindPublisher, ""); return ok })
	hub.publishFleetLoad(t.Context(), time.Now().UTC())
	a, _ := fsLatest(hub, "node-a", fleetStateKindLoad, "")
	b, _ := fsLatest(hub, "node-b", fleetStateKindLoad, "")
	if !a.Load.Accepting || !a.Load.Usable || b.Load.Accepting || b.Load.Usable || !b.Load.Connected {
		t.Fatalf("a=%+v b=%+v", a.Load, b.Load)
	}
}

// hk 2649: the hub state stream had no reader and is gone; the snapshot stays.
func TestFleetStateStreamRouteRemoved(t *testing.T) {
	_, server := fsHub(t, "")
	if status, _ := fsGet(t, server, "/v1/state/stream", fsOperator()); status != http.StatusNotFound {
		t.Fatalf("GET /v1/state/stream status=%d, want 404", status)
	}
	if status, _ := fsGet(t, server, "/v1/state", fsOperator()); status != http.StatusOK {
		t.Fatalf("GET /v1/state status=%d, want 200", status)
	}
}

// hk 2649: an old node still publishes spawn values. They stay valid, so they
// are not counted as unknown messages, and nothing stores them.
func TestFleetStateOldNodeSpawnPublishAcceptedAndDropped(t *testing.T) {
	hub, server := fsHub(t, "")
	nodeA := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	measured := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	before := hub.unknownMessageCount()
	r6Write(t, nodeA, fsSpawnPublish("start", "job-old", "builder-opus", "", measured))
	r6Write(t, nodeA, fsSpawnPublish("start", "job-other", "builder-opus", "node-b", measured)) // still refused
	r6Write(t, nodeA, fsQuotaPublish("", measured, 10))                                         // ordered after both
	eventually(t, 2*time.Second, "publishes handled", func() bool {
		_, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey)
		return ok
	})
	if got := hub.unknownMessageCount(); got != before+1 {
		t.Fatalf("unknown messages grew by %d, want 1 (the other-host spawn)", got-before)
	}
	for _, message := range fsSnapshot(t, server).States {
		if message.Kind == fleetStateKindSpawn {
			t.Fatalf("a spawn value was stored: %+v", message)
		}
	}
}

// Quota keys stay bounded per host, the oldest dropped first.
func TestFleetStateQuotaKeysAreBounded(t *testing.T) {
	store := newFleetStateStore()
	now := time.Unix(100, 0)
	for i := 0; i <= fleetStateQuotaKeysPerHost; i++ {
		store.publish(FleetStateMessage{Host: "node-a", Kind: fleetStateKindQuota, Key: fmt.Sprintf("claude:%016x", i), ReceivedAt: now.Add(time.Duration(i) * time.Second)}, 0)
	}
	if len(store.latest) != fleetStateQuotaKeysPerHost {
		t.Fatalf("latest holds %d quota keys", len(store.latest))
	}
	if _, kept := store.latest[fleetStateKey{"node-a", fleetStateKindQuota, fmt.Sprintf("claude:%016x", 0)}]; kept {
		t.Fatal("the oldest quota key was kept")
	}
}

func TestFleetStateLoadCarriesPrometheusScoreTerms(t *testing.T) {
	prom := placementPromServer(t, .23, 1, nil)
	t.Cleanup(prom.Close)
	hub := placementHub(t, prom.URL)
	t.Cleanup(func() { _ = hub.Close() })
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	now := time.Now().UTC()
	metrics, err := hub.fetchPlacementMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	hub.reloadPlacementPolicyLocked()
	policy := hub.placementPolicy
	hub.mu.Unlock()
	candidates := hub.placementCandidates(policy, metrics, "prometheus", now)
	hub.publishFleetLoad(t.Context(), now)
	for _, candidate := range candidates {
		message, ok := fsLatest(hub, candidate.Machine, fleetStateKindLoad, "")
		if !ok || message.Load.Signals == nil || message.Load.Signals.Terms == nil {
			t.Fatalf("%s missing computed signals: %+v", candidate.Machine, message.Load)
		}
		got, want := message.Load.Signals, candidate.Signals
		if *got.Terms != *want.Terms || got.Power != want.Power || got.BridgedJobs != want.BridgedJobs {
			t.Fatalf("%s: published %+v, computed %+v", candidate.Machine, got, want)
		}
	}
}
