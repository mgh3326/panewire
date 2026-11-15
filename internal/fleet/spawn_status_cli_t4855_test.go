package fleet

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// spawnStatusHub is a fake hub that records every request it receives and
// answers one spawn-status body.
type spawnStatusHub struct {
	mu       sync.Mutex
	requests []string
	answer   map[string]any
}

func (h *spawnStatusHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests = append(h.requests, r.Method+" "+r.URL.Path)
	h.mu.Unlock()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(h.answer)
}

func (h *spawnStatusHub) seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.requests...)
}

func spawnStatusApp(t *testing.T, hub *hubHTTP) (*App, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	home := t.TempDir()
	return &App{Out: out, Err: io.Discard, Hub: hub, Home: home, Jobs: filepath.Join(home, "jobs"), Bins: map[string]string{}, Now: time.Now, Sleep: sleepContext}, out
}

func spawnStatusFixture(t *testing.T, h *spawnStatusHub) (*App, *bytes.Buffer) {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	hub := &hubHTTP{base: server.URL, token: "fixture", client: server.Client(), retry: hubretry.Policy{Budget: -1}}
	return spawnStatusApp(t, hub)
}

// TestSpawnStatusReadsEachStateOnce is the closed contract of `spawn status`:
// one GET, no POST, the state's exit code and its fields on one line, for
// every state spawn_request_state.go classifies.
func TestSpawnStatusReadsEachStateOnce(t *testing.T) {
	req := statusTestOperation
	prefix := "request=" + req + " "
	for _, test := range []struct {
		name   string
		answer map[string]any
		want   int
		line   string
	}{
		{
			name:   "succeeded",
			answer: map[string]any{"status": "succeeded", "request_id": req, "job_id": "worker-a", "expires_at": "2026-11-16T12:00:00Z", "result": map[string]any{"rc": 0, "host": "node-b"}},
			want:   0,
			line:   prefix + "state=succeeded expires_at=2026-11-16T12:00:00Z job=worker-a",
		},
		{
			name:   "landed",
			answer: map[string]any{"status": "landed", "job_id": "worker-b"},
			want:   0,
			line:   prefix + "state=landed job=worker-b",
		},
		{
			name:   "failed",
			answer: map[string]any{"status": "failed", "job_id": "worker-c", "result": map[string]any{"rc": 81, "stage": "placement", "reason": "no_eligible_host"}},
			want:   81,
			line:   prefix + "state=failed reason=no_eligible_host rc=81 stage=placement job=worker-c",
		},
		{
			name:   "retryable",
			answer: map[string]any{"status": "retryable", "job_id": "worker-d", "result": map[string]any{"rc": 81, "stage": "launch", "reason": "agent not in foreground"}},
			want:   81,
			line:   prefix + "state=retryable reason=agent not in foreground rc=81 stage=launch job=worker-d",
		},
		{
			name:   "refused",
			answer: map[string]any{"status": "refused", "job_id": "worker-e", "result": map[string]any{"rc": 77, "stage": "admission", "reason": "seat_refused"}},
			want:   77,
			line:   prefix + "state=refused reason=seat_refused rc=77 stage=admission job=worker-e",
		},
		{
			name:   "failed without a recorded rc",
			answer: map[string]any{"status": "failed", "job_id": "worker-f", "result": map[string]any{"stage": "launch"}},
			want:   81,
			line:   prefix + "state=failed stage=launch job=worker-f",
		},
		{
			name:   "cancelled",
			answer: map[string]any{"status": "cancelled", "job_id": "worker-g", "reason": "start condition failed"},
			want:   84,
			line:   prefix + "state=cancelled reason=start condition failed job=worker-g",
		},
		{
			name:   "given_up",
			answer: map[string]any{"status": "given_up", "job_id": "worker-h", "expires_at": "2026-11-16T12:00:00Z", "result": map[string]any{"reason": "time limit passed"}},
			want:   84,
			line:   prefix + "state=given_up reason=time limit passed expires_at=2026-11-16T12:00:00Z job=worker-h",
		},
		{
			name:   "deadline_exceeded",
			answer: map[string]any{"status": "deadline_exceeded", "job_id": "worker-i", "result": map[string]any{"stage": "launch"}},
			want:   75,
			line:   prefix + "state=deadline_exceeded stage=launch job=worker-i",
		},
		{
			name:   "parked",
			answer: map[string]any{"status": "parked", "job_id": "worker-j", "reason": "no_eligible_host", "next_check_at": "2026-11-16T12:00:30Z"},
			want:   91,
			line:   prefix + "state=parked reason=no_eligible_host next_check_at=2026-11-16T12:00:30Z job=worker-j",
		},
		{
			name:   "accepted",
			answer: map[string]any{"status": "accepted", "job_id": "worker-k"},
			want:   85,
			line:   prefix + "state=accepted job=worker-k",
		},
		{
			name:   "dispatching",
			answer: map[string]any{"status": "dispatching", "job_id": "worker-l"},
			want:   85,
			line:   prefix + "state=dispatching job=worker-l",
		},
		{
			name:   "unknown state",
			answer: map[string]any{"status": "queued", "job_id": "worker-m"},
			want:   85,
			line:   prefix + "state=queued job=worker-m",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &spawnStatusHub{answer: test.answer}
			app, out := spawnStatusFixture(t, fake)
			err := app.Run(t.Context(), []string{"spawn", "status", req})
			if got := ErrorCode(err); got != test.want {
				t.Fatalf("exit=%d, want %d (err=%v)", got, test.want, err)
			}
			if got := out.String(); got != test.line+"\n" {
				t.Fatalf("line=%q, want %q", got, test.line+"\n")
			}
			if got, want := fake.seen(), []string{"GET /v1/spawns/" + req}; len(got) != 1 || got[0] != want[0] {
				t.Fatalf("hub requests=%v, want exactly %v", got, want)
			}
		})
	}
}

func TestSpawnStatusJSON(t *testing.T) {
	req := statusTestOperation
	fake := &spawnStatusHub{answer: map[string]any{
		"status": "failed", "request_id": req, "job_id": "worker-a",
		"expires_at": "2026-11-16T12:00:00Z",
		"result":     map[string]any{"rc": 81, "stage": "placement", "reason": "no_eligible_host"},
	}}
	app, out := spawnStatusFixture(t, fake)
	if err := app.Run(t.Context(), []string{"spawn", "status", req, "--json"}); ErrorCode(err) != 81 {
		t.Fatalf("exit=%d, want 81 (err=%v)", ErrorCode(err), err)
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatalf("output is not one JSON record: %v (%q)", err, out.String())
	}
	want := map[string]any{
		"request": req, "state": "failed", "reason": "no_eligible_host",
		"expires_at": "2026-11-16T12:00:00Z", "rc": float64(81),
		"stage": "placement", "job": "worker-a",
	}
	if len(record) != len(want) {
		t.Fatalf("record=%v, want exactly %v", record, want)
	}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("%s=%v, want %v (record=%v)", key, record[key], value, record)
		}
	}
}

func TestSpawnStatusUsageRefusalsSendNoRequest(t *testing.T) {
	fake := &spawnStatusHub{answer: map[string]any{"status": "succeeded"}}
	app, out := spawnStatusFixture(t, fake)
	for _, args := range [][]string{
		{"spawn", "status"},
		{"spawn", "status", "not an id"},
		{"spawn", "status", statusTestOperation, "extra"},
	} {
		if err := app.Run(t.Context(), args); ErrorCode(err) != 2 {
			t.Fatalf("%v: exit=%d, want 2 (err=%v)", args, ErrorCode(err), err)
		}
	}
	if got := fake.seen(); len(got) != 0 {
		t.Fatalf("usage refusals reached the hub: %v", got)
	}
	if out.Len() != 0 {
		t.Fatalf("usage refusals printed a status line: %q", out.String())
	}
}

// A transport failure reads exactly as the one-shot reader SpawnStatus gave
// before the command existed: same error class, same message.
func TestSpawnStatusTransportErrorMatchesSpawnStatus(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	closed := server.URL
	server.Close()
	hub := &hubHTTP{base: closed, token: "fixture", client: &http.Client{}, retry: hubretry.Policy{Budget: -1}}
	app, _ := spawnStatusApp(t, hub)
	err := app.Run(t.Context(), []string{"spawn", "status", statusTestOperation})
	if err == nil || ErrorCode(err) == 0 {
		t.Fatalf("transport failure read as %v", err)
	}
	_, directErr := hub.SpawnStatus(t.Context(), statusTestOperation)
	if directErr == nil {
		t.Fatal("SpawnStatus accepted an unreachable hub")
	}
	if ErrorCode(err) != ErrorCode(directErr) || err.Error() != directErr.Error() {
		t.Fatalf("spawn status error class changed: command=%d %q, SpawnStatus=%d %q", ErrorCode(err), err, ErrorCode(directErr), directErr)
	}
}
