package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Each request state, served as the only answer, ends the wait the way the
// design says: landed returns, refusals and ended requests exit with their
// documented code, and every state still in flight keeps the wait open (the
// short context proves it did not return).
func TestWaitSpawnRequestStateTable(t *testing.T) {
	landed := map[string]any{"rc": 0, "host": "node-b", "profile": "sonnet", "pane": "w:p9"}
	for _, test := range []struct {
		state  string
		result map[string]any
		code   int  // 0: the wait returns without error
		waits  bool // the wait is still open when the context ends
	}{
		{state: "succeeded", result: landed},
		{state: "landed", result: landed},
		{state: "failed", result: map[string]any{"rc": 81, "reason": "no_host"}, code: 81},
		{state: "retryable", result: map[string]any{"rc": 2, "reason": "cwd_unmapped"}, code: 2},
		{state: "refused", result: map[string]any{"rc": 77, "reason": "brief_missing"}, code: 77},
		{state: "refused", result: map[string]any{"reason": "task_closed"}, code: exitPlacement},
		{state: "given_up", result: map[string]any{"reason": "expired"}, code: exitWaitLost},
		{state: "cancelled", result: map[string]any{"reason": "owner"}, code: exitWaitLost},
		{state: "deadline_exceeded", result: map[string]any{"rc": 124}, code: exitStateConflict},
		{state: "accepted", waits: true},
		{state: "waiting", waits: true},
		{state: "pending", waits: true},
		{state: "parked", waits: true},
		{state: "bound", waits: true},
		{state: "dispatching", waits: true},
		{state: "launching", waits: true},
		{state: "reconciling", waits: true},
	} {
		t.Run(test.state+"/"+firstNonempty(strings.Join(mapKeys(test.result), ","), "bare"), func(t *testing.T) {
			c, _ := spawnStatusSequence(t, map[string]any{"status": test.state, "result": test.result})
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
			defer cancel()
			_, err := c.WaitSpawn(ctx, statusTestOperation, SpawnWaitOptions{})
			switch {
			case test.waits:
				if err == nil || ctx.Err() == nil {
					t.Fatalf("state %s ended the wait early: %v", test.state, err)
				}
			case test.code == 0:
				if err != nil {
					t.Fatalf("state %s: %v", test.state, err)
				}
			default:
				if ErrorCode(err) != test.code {
					t.Fatalf("state %s: code=%d err=%v, want %d", test.state, ErrorCode(err), err, test.code)
				}
			}
		})
	}
}

func mapKeys(m map[string]any) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// A state no client knows keeps the wait going, says so once, and never
// turns into success or failure; a later landed still ends it.
func TestWaitSpawnUnknownStateKeepsWaiting(t *testing.T) {
	c, reads := spawnStatusSequence(t,
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "landed", "result": map[string]any{"host": "node-b", "pane": "w:p9"}})
	var notice bytes.Buffer
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Notice: &notice})
	if err != nil || attempt.Pane != "w:p9" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
	if got, want := notice.String(), "unknown state warming_up: still waiting\n"; got != want {
		t.Fatalf("notice=%q want exactly once %q", got, want)
	}
	if reads.Load() != 4 {
		t.Fatalf("reads=%d", reads.Load())
	}
}

// An unknown state that never resolves ends only at the caller's own bound,
// with the timeout code, not a failure of the request.
func TestWaitSpawnUnknownStateEndsOnlyAtTheWaitBound(t *testing.T) {
	c, _ := spawnStatusSequence(t, map[string]any{"status": "warming_up"})
	_, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond})
	if ErrorCode(err) != exitWaitTimeout {
		t.Fatalf("code=%d err=%v", ErrorCode(err), err)
	}
}

// A parked request prints its reason and next check once per change, and a
// wait that ends while it is still parked has its own exit code.
func TestWaitSpawnParkedPastTheBound(t *testing.T) {
	c, _ := spawnStatusSequence(t,
		map[string]any{"status": "parked", "reason": "quota", "next_check_at": "2026-11-15T10:00:00Z"},
		map[string]any{"status": "parked", "reason": "quota", "next_check_at": "2026-11-15T10:00:00Z"},
		map[string]any{"status": "parked", "reason": "pool_cap", "next_check_at": "2026-11-15T10:05:00Z"})
	var notice bytes.Buffer
	_, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond, Notice: &notice})
	if ErrorCode(err) != exitWaitParked || !strings.Contains(err.Error(), statusTestOperation) {
		t.Fatalf("code=%d err=%v", ErrorCode(err), err)
	}
	want := "wait request=" + statusTestOperation + " state=parked reason=quota next_check_at=2026-11-15T10:00:00Z\n" +
		"wait request=" + statusTestOperation + " state=parked reason=pool_cap next_check_at=2026-11-15T10:05:00Z\n"
	if notice.String() != want {
		t.Fatalf("notice=%q want %q", notice.String(), want)
	}
}

// A request that is not parked ends at the bound with the existing timeout
// code, and a request that lands in time is not cut off by a bound.
func TestWaitSpawnBoundOnlyEndsAnOpenRequest(t *testing.T) {
	c, _ := spawnStatusSequence(t, map[string]any{"status": "pending"})
	if _, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond}); ErrorCode(err) != exitWaitTimeout {
		t.Fatalf("pending past the bound: %v", err)
	}
	c, _ = spawnStatusSequence(t, map[string]any{"status": "pending"}, map[string]any{"status": "landed", "result": map[string]any{"host": "node-b"}})
	if _, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: time.Minute}); err != nil {
		t.Fatalf("landed inside the bound: %v", err)
	}
}

// Only a terminal state with no pane lets a job directory go as "the hub
// operation failed".
func TestSpawnNeverLanded(t *testing.T) {
	for state, want := range map[string]bool{
		"failed": true, "refused": true, "given_up": true, "cancelled": true,
		"succeeded": false, "landed": false, "retryable": false, "deadline_exceeded": false, "reconciling": false,
		"parked": false, "pending": false, "unknown": false,
	} {
		if got := spawnNeverLanded(state); got != want {
			t.Errorf("spawnNeverLanded(%q)=%v want %v", state, got, want)
		}
	}
}

// The wait bound also covers the status read, its retries and the failure
// sleeps: a hub outage cannot hold a short --wait-timeout open (production
// retry policy, so an unbounded read would run for a minute), and a request
// that answers "landed" only after the bound is not treated as success.
func TestWaitSpawnBoundCoversReadsAndOutages(t *testing.T) {
	serve := func(t *testing.T, handler http.HandlerFunc) hubHTTP {
		t.Helper()
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: time.Millisecond}
	}
	bound := SpawnWaitOptions{Timeout: 300 * time.Millisecond}
	for name, test := range map[string]struct {
		handler http.HandlerFunc
		code    int
	}{
		"outage from the start": {func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", 503) }, exitWaitTimeout},
		"parked then outage": {func() http.HandlerFunc {
			var reads atomic.Int32
			return func(w http.ResponseWriter, r *http.Request) {
				if reads.Add(1) == 1 {
					json.NewEncoder(w).Encode(map[string]any{"status": "parked", "reason": "quota"})
					return
				}
				http.Error(w, "down", 503)
			}
		}(), exitWaitParked},
		"landed only after the bound": {func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(2 * time.Second):
			case <-r.Context().Done():
			}
			json.NewEncoder(w).Encode(map[string]any{"status": "landed"})
		}, exitWaitTimeout},
	} {
		t.Run(name, func(t *testing.T) {
			c := serve(t, test.handler)
			start := time.Now()
			_, err := c.WaitSpawn(t.Context(), statusTestOperation, bound)
			if ErrorCode(err) != test.code {
				t.Fatalf("code=%d err=%v, want %d", ErrorCode(err), err, test.code)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("wait ran %v past a %v bound", took, bound.Timeout)
			}
		})
	}
}

// hk 4660: a waiting request says once what it waits for and when its time
// limit ends; a given_up request names its reason and expiry in the error.
func TestWaitSpawnWaitingAndGivenUpShowExpiry(t *testing.T) {
	c, _ := spawnStatusSequence(t,
		map[string]any{"status": "waiting", "expires_at": "2026-11-15T16:00:00Z", "start_condition": map[string]any{"status": "waiting", "reasons": []string{"task:9000=merged"}}},
		map[string]any{"status": "waiting", "expires_at": "2026-11-15T16:00:00Z", "start_condition": map[string]any{"status": "waiting", "reasons": []string{"task:9000=merged"}}},
		map[string]any{"status": "given_up", "expires_at": "2026-11-15T16:00:00Z", "result": map[string]any{"reason": "expires_at passed"}})
	var notice bytes.Buffer
	_, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Notice: &notice})
	if ErrorCode(err) != exitWaitLost || !strings.Contains(err.Error(), "given_up") || !strings.Contains(err.Error(), "expires_at=2026-11-15T16:00:00Z") || !strings.Contains(err.Error(), "expires_at passed") {
		t.Fatalf("code=%d err=%v", ErrorCode(err), err)
	}
	want := "wait request=" + statusTestOperation + " state=waiting expires_at=2026-11-15T16:00:00Z waiting_for=task:9000=merged\n"
	if notice.String() != want {
		t.Fatalf("notice=%q want exactly once %q", notice.String(), want)
	}
}

func TestParseSpawnExpires(t *testing.T) {
	for _, test := range []struct {
		value   string
		seconds int
		bad     bool
	}{
		{"90m", 5400, false}, {"6h", 21600, false}, {"24h", 86400, false}, {"1m", 60, false},
		{"24h1s", 0, true}, {"59s", 0, true}, {"0s", 0, true}, {"-1h", 0, true}, {"soon", 0, true}, {"1500ms", 0, true}, {"", 0, true},
	} {
		got, err := parseSpawnExpires(test.value)
		if (err != nil) != test.bad || got != test.seconds {
			t.Errorf("parseSpawnExpires(%q) = %d, %v; want %d bad=%v", test.value, got, err, test.seconds, test.bad)
		}
	}
	if _, err := parseSpawn([]string{"--expires", "25h"}); err == nil || ErrorCode(err) != exitUsage {
		t.Fatalf("--expires 25h must be a usage error, got %v", err)
	}
	if SpawnExpiresDefault("implementer") != 6*time.Hour || SpawnExpiresDefault("reviewer") != time.Hour || SpawnExpiresDefault("consult") != time.Hour {
		t.Fatal("role defaults changed")
	}
	if SpawnExpiresFor("reviewer", 7200) != 2*time.Hour || SpawnExpiresFor("reviewer", 0) != time.Hour {
		t.Fatal("an explicit limit overrides the role default")
	}
	// A wire value that would wrap into range as a Duration is out of range.
	if ValidSpawnExpiresSeconds(18446744134) || ValidSpawnExpiresSeconds(int(^uint(0)>>1)) {
		t.Fatal("an overflowing expires_seconds passed validation")
	}
	if !ValidSpawnExpiresSeconds(0) || ValidSpawnExpiresSeconds(86401) || ValidSpawnExpiresSeconds(59) || ValidSpawnExpiresSeconds(-1) {
		t.Fatal("ValidSpawnExpiresSeconds bounds")
	}
}

// hk 4660: a hub that does not advertise spawn_expires_v1 would drop
// expires_seconds and run the request under the default limit, so the CLI
// refuses --expires instead of submitting.
func TestSubmitWithExpiresNeedsHubFeature(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
		accepted bool
	}{
		{"hub without the feature", []string{"start_conditions_v1"}, false},
		{"hub with the feature", []string{"start_conditions_v1", "spawn_expires_v1"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var posted atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/spawns/features":
					json.NewEncoder(w).Encode(map[string]any{"features": test.features})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/spawns":
					var request Submission
					if json.NewDecoder(r.Body).Decode(&request) != nil || request.ExpiresSeconds != 5400 {
						t.Errorf("expires_seconds not sent: %+v", request)
					}
					posted.Add(1)
					w.WriteHeader(http.StatusAccepted)
					json.NewEncoder(w).Encode(map[string]any{"job_id": "test-job", "request_id": request.RequestID})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
			request := Submission{RequestID: "d676220b-e763-4af1-aefd-000000000001", ExpiresSeconds: 5400}
			_, err := c.Submit(t.Context(), request)
			if test.accepted != (err == nil) || test.accepted != (posted.Load() == 1) {
				t.Fatalf("accepted=%v err=%v posted=%d", test.accepted, err, posted.Load())
			}
			if !test.accepted && (ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "--expires")) {
				t.Fatalf("refusal: code=%d err=%v", ErrorCode(err), err)
			}
		})
	}
	// A request without --expires never asks for the feature.
	var asked atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/spawns/features" {
			asked.Add(1)
		}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{"job_id": "test-job", "request_id": "d676220b-e763-4af1-aefd-000000000001"})
	}))
	t.Cleanup(server.Close)
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
	if _, err := c.Submit(t.Context(), Submission{RequestID: "d676220b-e763-4af1-aefd-000000000001"}); err != nil || asked.Load() != 0 {
		t.Fatalf("err=%v feature reads=%d", err, asked.Load())
	}
}
