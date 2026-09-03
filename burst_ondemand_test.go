package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func r16Hub(t *testing.T, now *time.Time) *HubServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "burst.json")
	policy := BurstPolicy{SourceMachine: "source", SwapGB: 1, Load5: 1, Consecutive: 1, WakeVia: "wake", WakeMAC: "02:00:00:00:00:16", TargetMachine: "desktop", IdleMinutes: 1, CooldownMinutes: 0}
	if err := os.WriteFile(path, []byte(formatBurstPolicy(policy)), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "source": r6NodeAToken, "wake": r6NodeBToken, "desktop": "desktop-r16-fixture-token"}, BurstPolicyPath: path, Now: func() time.Time { return *now }, KeepaliveInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func TestBurstR16RequestWakeUpLeaseAndHoldSuppression(t *testing.T) {
	now := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	hub := r16Hub(t, &now)
	wake := &hubAgent{bursts: make(chan hubBurstEvent, 1)}
	hub.connect("wake", "r16", "fixture", wake, false)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	body := []byte(`{"target":"desktop","hold":"30m","reason":"pg-backup-secondary"}`)
	done := make(chan *http.Response, 1)
	go func() {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/burst/request", bytes.NewReader(body))
		request.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
		response, _ := server.Client().Do(request)
		done <- response
	}()
	select {
	case event := <-wake.bursts:
		if event.Phase != "up" || event.Machine != "desktop" || event.WakeMAC != "02:00:00:00:00:16" {
			t.Fatalf("unexpected wake event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not reuse wake-via burst event")
	}
	target := &hubAgent{holds: make(chan hubBurstHoldsEvent, 2)}
	hub.connect("desktop", "r16", "fixture", target, false)
	var response *http.Response
	select {
	case response = <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not await target up")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("request status=%d", response.StatusCode)
	}
	var lease hubBurstHold
	if err := json.NewDecoder(response.Body).Decode(&lease); err != nil || lease.Status != "active" || lease.Target != "desktop" || lease.ID == "" {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
	// Mutant ①: removing holdsActiveLocked from observeBurstLocked makes this RED.
	now = now.Add(time.Minute)
	hub.mu.Lock()
	events := hub.observeBurstLocked(now, "desktop", HubHostLoad{WorkerProcs: 0})
	hub.mu.Unlock()
	if len(events) != 0 {
		t.Fatalf("active hold permitted idle poweroff: %+v", events)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/burst/holds", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
	holds, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer holds.Body.Close()
	if holds.StatusCode != http.StatusOK {
		t.Fatalf("holds status=%d", holds.StatusCode)
	}
}

func TestBurstR16TTLAuthAndTimeoutCLI(t *testing.T) {
	now := time.Date(2026, 9, 4, 2, 0, 0, 0, time.UTC)
	hub := r16Hub(t, &now)
	// Mutant ②: removing sweepBurstHoldsLocked leaves this lease active.
	hub.holds["hold-fixture"] = &hubBurstHold{ID: "hold-fixture", Target: "desktop", Reason: "test", ExpiresAt: now.Add(time.Second), Status: "active"}
	now = now.Add(time.Second)
	hub.mu.Lock()
	active := hub.holdsActiveLocked("desktop", now)
	status := hub.holds["hold-fixture"].Status
	hub.mu.Unlock()
	if active || status != "expired" {
		t.Fatalf("TTL lease remained active: active=%t status=%s", active, status)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	hub.connect("wake", "r16", "fixture", &hubAgent{bursts: make(chan hubBurstEvent, 1)}, false)
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/burst/holds", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+r6NodeAToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	// Mutant ③: removing authorizeOperator makes this RED.
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("node token accepted: %d", response.StatusCode)
	}
	env := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := runBurstCLIWithDeps([]string{"request", "--hub-url", server.URL, "--hub-token-env", env, "--target", "desktop", "--hold", "1m", "--reason", "timeout", "--timeout", "20ms"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if code != ExitTimeout || !bytes.Contains(out.Bytes(), []byte("target_timeout")) {
		t.Fatalf("timeout code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func TestBurstR16ClientAdoptsAssignedEpochAndHoldResponse(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.claimed.json"), []byte(`{"type":"job.claimed","agent_label":"wrk-a","epoch":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: root, assignedJobs: map[string]uint64{"job-a": 2}, completedJobs: make(map[string]uint64), burstSeen: make(map[string]time.Time)}
	client.burstHoldsActive = true
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(context.Background()).Payload)
	if !ok || !heartbeat.HoldsActive || len(heartbeat.ActiveJobs) != 1 || heartbeat.ActiveJobs[0].Epoch != 2 {
		t.Fatalf("node did not adopt directives: %+v ok=%t", heartbeat, ok)
	}
	if parsed, ok := parseHubOutbound([]byte(`{"type":"job.assigned","job_id":"job-a","epoch":2}`)); !ok || parsed.Epoch != 2 {
		t.Fatalf("assigned parse=%+v ok=%t", parsed, ok)
	}
	if parsed, ok := parseHubOutbound([]byte(`{"type":"burst.holds","holds_active":true}`)); !ok || !parsed.HoldsActive {
		t.Fatalf("holds parse=%+v ok=%t", parsed, ok)
	}
	_ = context.Background()
}
