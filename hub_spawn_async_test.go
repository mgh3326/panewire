package panewire

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/getsentry/sentry-go"
)

type spawnSentryTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (*spawnSentryTransport) Configure(sentry.ClientOptions) {}
func (*spawnSentryTransport) Close()                         {}
func (*spawnSentryTransport) Flush(time.Duration) bool       { return true }
func (s *spawnSentryTransport) SendEvent(e *sentry.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func TestAsyncSpawnLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "failure", "timeout", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			fake, hk, stop := newFakeHandoffkeep(t)
			defer stop()
			h, server := t14Hub(t)
			t.Cleanup(func() { _ = h.Close() })
			h.handoffkeep = hk
			transport := &spawnSentryTransport{}
			client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://test@example.invalid/1", EnableTracing: true, TracesSampleRate: 0, Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			h.spawnTelemetry = sentry.NewHub(client, sentry.NewScope())
			node := t14Node(t, h, server, true)
			h.mu.Lock()
			h.nodes["machine-a"].capabilities["spawn_async"] = true
			h.mu.Unlock()
			body := t14SpawnBody(t14RequestID(701))
			body["async"], body["job_id"], body["owner_lane"], body["deadline_seconds"] = true, "job-a", "lane-a", 1
			status, _, raw := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
			if status != http.StatusAccepted || !strings.Contains(string(raw), `"status":"accepted"`) {
				t.Fatalf("%d %s", status, raw)
			}
			var directive map[string]any
			if err := wsjson.Read(t.Context(), node, &directive); err != nil {
				t.Fatal(err)
			}
			if directive["type"] != "job.spawn" || directive["deadline_seconds"] != float64(1) {
				t.Fatal(directive)
			}
			body["request_id"] = t14RequestID(702)
			status, _, raw = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
			if status != http.StatusAccepted || !strings.Contains(string(raw), t14RequestID(701)) {
				t.Fatalf("duplicate: %d %s", status, raw)
			}
			h.mu.Lock()
			count := len(h.spawnRecords)
			h.mu.Unlock()
			if count != 1 {
				t.Fatalf("executions=%d", count)
			}
			if mode == "success" || mode == "failure" {
				rc, reason := 0, ""
				if mode == "failure" {
					rc, reason = 76, "spawn_failed"
				}
				result := map[string]any{"type": "job.spawn.result", "request_id": t14RequestID(701), "rc": rc, "job_id": "job-a", "pane": "w1:p9", "stdout_tail": "OK pane=w1:p9 job=job-a model=fixture\n", "stage_ms": map[string]int64{"claim": 1, "placement": 2, "checkout": 3, "launch": 4, "landing": 5}, "stage": "landing"}
				if reason != "" {
					result["error"] = reason
				}
				if err := wsjson.Write(t.Context(), node, result); err != nil {
					t.Fatal(err)
				}
			} else if mode == "disconnect" {
				_ = node.CloseNow()
			}
			deadline := time.Now().Add(4 * time.Second)
			for fake.rowCount() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 5)
			}
			if fake.rowCount() != 1 {
				t.Fatal("missing terminal lane event")
			}
			payload, _ := json.Marshal(r24FirstPostBody(t, fake))
			want := "SPAWN_FAILED job-a"
			if mode == "success" {
				want = "SPAWNED job-a host=machine-a pane=w1:p9 model=fixture"
			}
			if !strings.Contains(string(payload), want) || mode == "timeout" && !strings.Contains(string(payload), "reason=timeout") || mode == "failure" && !strings.Contains(string(payload), "rc=76") {
				t.Fatal(string(payload))
			}
			// Late replies cannot produce a second terminal event or transaction.
			h.resolveSpawn("machine-a", hubSpawnResult{RequestID: t14RequestID(701), RC: 99})
			transport.mu.Lock()
			events := append([]*sentry.Event(nil), transport.events...)
			transport.mu.Unlock()
			transactions, failures := 0, 0
			for _, event := range events {
				if event.Type == "transaction" {
					transactions++
					if event.Transaction != "runtug.spawn" || event.Tags["job"] != "job-a" {
						t.Fatalf("transaction=%+v", event)
					}
					if mode == "success" && len(event.Spans) != 5 {
						t.Fatalf("spans=%d", len(event.Spans))
					}
				} else {
					failures++
					if len(event.Fingerprint) != 3 {
						t.Fatal(event.Fingerprint)
					}
				}
			}
			if transactions != 1 || (mode == "success" && failures != 0) || (mode != "success" && failures != 1) {
				t.Fatalf("transactions=%d failures=%d", transactions, failures)
			}
		})
	}
}

func TestSpawnTimingsReply(t *testing.T) {
	output := &hubSpawnOutput{}
	_, _ = output.Write([]byte("RUNTUG_SPAWN_TIMINGS {\"stage_ms\":{\"claim\":12,\"launch\":34},\"stage\":\"landing\"}\nOK pane=w1:p9 job=job-a\n"))
	if output.stageMS["claim"] != 12 || output.stage != "landing" || output.jobID != "job-a" {
		t.Fatalf("%+v", output)
	}
	if validSpawnStages(map[string]int64{"unknown": 1}) || validSpawnStages(map[string]int64{"claim": -1}) {
		t.Fatal("invalid timing accepted")
	}
}
