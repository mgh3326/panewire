package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// grantPeer is a hub stand-in for the node side of the start proof. answer
// decides each job.spawn.claim: "grant", "deny" or "silent". A decided claim
// is handed to the node as its read loop would hand a job.spawn.grant.
func grantPeer(t *testing.T, client *HubClient, answer string) (*hubClientConnection, <-chan hubSpawnResult, *atomic.Int32) {
	t.Helper()
	results := make(chan hubSpawnResult, 10)
	var claims atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			var raw json.RawMessage
			if wsjson.Read(r.Context(), conn, &raw) != nil {
				return
			}
			if id, ok := parseHubSpawnClaim(raw); ok {
				claims.Add(1)
				if answer != "silent" {
					client.deliverSpawnGrant(id, answer == "grant")
				}
				continue
			}
			var result hubSpawnResult
			if json.Unmarshal(raw, &result) == nil {
				results <- result
			}
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &hubClientConnection{connection: conn}, results, &claims
}

func grantNode(t *testing.T) (*HubClient, *atomic.Int32) {
	t.Helper()
	var runs atomic.Int32
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write}
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		runs.Add(1)
		return t14EngineOK()
	}
	return client, &runs
}

// A grant-required frame starts only with the hub's grant. A refusal or
// silence until start_by journals the attempt as never started, and a replay
// of the same frame keeps that answer even when the hub would now grant it.
func TestNodeSpawnStartsOnlyWithHubGrant_t4612(t *testing.T) {
	for _, tc := range []struct {
		answer string
		want   int32
	}{{"grant", 1}, {"deny", 0}, {"silent", 0}} {
		t.Run(tc.answer, func(t *testing.T) {
			client, runs := grantNode(t)
			peer, results, claims := grantPeer(t, client, tc.answer)
			message := admissionMessage(461201, true)
			// The grant case gets a long start_by so a slow runner cannot
			// turn it into silence; silence needs only a short one.
			startBy := 5 * time.Second
			if tc.answer == "silent" {
				startBy = 300 * time.Millisecond
			}
			message.StartByMS = time.Now().Add(startBy).UnixMilli()
			client.handleHubSpawn(t.Context(), peer, message)
			result := nextSpawnResult(t, results)
			if claims.Load() != 1 || runs.Load() != tc.want {
				t.Fatalf("claims=%d engine runs=%d, want 1 and %d", claims.Load(), runs.Load(), tc.want)
			}
			if tc.want == 1 {
				if result.RC != 0 || admissionJournalStatus(t, client, message.RequestID) != "completed" {
					t.Fatalf("granted spawn result=%+v", result)
				}
				return
			}
			if result.RC != 124 || result.Error != "deadline_not_started" || result.Stage != "claim" || !result.Retryable {
				t.Fatalf("ungranted spawn result=%+v, want deadline_not_started", result)
			}
			if status := admissionJournalStatus(t, client, message.RequestID); status != "completed" {
				t.Fatalf("ungranted journal status=%q, want completed", status)
			}
			replayPeer, replayResults, replayClaims := grantPeer(t, client, "grant")
			message.StartByMS = time.Now().Add(time.Second).UnixMilli()
			client.handleHubSpawn(t.Context(), replayPeer, message)
			replay := nextSpawnResult(t, replayResults)
			if runs.Load() != 0 || replay.Error != "deadline_not_started" || replayClaims.Load() != 0 {
				t.Fatalf("replay ran=%d result=%+v claims=%d; a refused attempt must stay unstarted", runs.Load(), replay, replayClaims.Load())
			}
		})
	}
}

// A frame read after its start_by never asks for a grant and never starts.
func TestNodeSpawnPastStartByIsNotStarted_t4612(t *testing.T) {
	client, runs := grantNode(t)
	peer, results, claims := grantPeer(t, client, "grant")
	message := admissionMessage(461202, true)
	message.StartByMS = time.Now().Add(-time.Second).UnixMilli()
	client.handleHubSpawn(t.Context(), peer, message)
	result := nextSpawnResult(t, results)
	if runs.Load() != 0 || claims.Load() != 0 || result.Error != "deadline_not_started" {
		t.Fatalf("late frame ran=%d claims=%d result=%+v", runs.Load(), claims.Load(), result)
	}
}

// A frame from a hub that sends no start_by keeps today's behavior: no
// grant request, the spawn runs.
func TestNodeSpawnWithoutStartByNeedsNoGrant_t4612(t *testing.T) {
	client, runs := grantNode(t)
	peer, results, claims := grantPeer(t, client, "deny")
	client.handleHubSpawn(t.Context(), peer, admissionMessage(461203, true))
	if result := nextSpawnResult(t, results); result.RC != 0 || runs.Load() != 1 || claims.Load() != 0 {
		t.Fatalf("legacy frame result=%+v runs=%d claims=%d", result, runs.Load(), claims.Load())
	}
}

// The wire: a node advertises the capability, accepts start_by_ms on
// job.spawn and reads job.spawn.grant strictly.
func TestNodeSpawnGrantWire_t4612(t *testing.T) {
	found := false
	for _, capability := range hubSpawnCapabilities {
		found = found || capability == hubCapabilitySpawnGrant
	}
	if !found {
		t.Fatalf("hello capabilities %v lack %s", hubSpawnCapabilities, hubCapabilitySpawnGrant)
	}
	frame := `{"type":"job.spawn","request_id":"` + t14RequestID(1) + `","cwd_key":"repo-a","brief":{"inline":"b"},"args":["--job","job-a"],"wait_seconds":10,"deadline_ms":1700000000000,"start_by_ms":1700000000000,"operator_authorized":true}`
	if message, ok := parseHubOutbound([]byte(frame)); !ok || message.StartByMS != 1700000000000 {
		t.Fatalf("job.spawn with start_by_ms: ok=%v start_by=%d", ok, message.StartByMS)
	}
	grant := `{"type":"job.spawn.grant","request_id":"` + t14RequestID(1) + `","granted":true}`
	if message, ok := parseHubOutbound([]byte(grant)); !ok || !message.Granted || message.RequestID != t14RequestID(1) {
		t.Fatalf("job.spawn.grant: ok=%v %+v", ok, message)
	}
	if _, ok := parseHubOutbound([]byte(`{"type":"job.spawn.grant","request_id":"` + t14RequestID(1) + `","granted":true,"x":1}`)); ok {
		t.Fatal("job.spawn.grant with an extra field was accepted")
	}
}
