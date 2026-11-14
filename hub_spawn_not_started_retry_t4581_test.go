package panewire

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

type retryOnlyOtherHost struct{ retrySource }

// The test starts the deadline only after observing the first node frame.
type triggeredDeadlineContext struct{ context.Context }

func (c triggeredDeadlineContext) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

func (s retryOnlyOtherHost) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{{HostID: "machine-b", AccountKey: "p:0000000000000001", LastSeen: s.now}}, nil
}

// A not-started attestation cannot create an hk host claim. If an earlier
// attempt did claim the job, however, the attestation cannot erase that pin.
func TestNotStartedReplayRespectsPriorHostClaim_t4581(t *testing.T) {
	for _, priorClaim := range []bool{false, true} {
		name := "no earlier claim can choose another host"
		if priorClaim {
			name = "earlier claim keeps its host"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			retryHK(t)
			config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
			h, err := NewHubServer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			request := durableSpawnRequest()
			var task hubSpawnTask
			if priorClaim {
				task = claimedHarnessRefusal(t, h, request, hubSpawnResult{RC: 70, Error: "spawn_failed", Stage: "launch", Retryable: true})
				if task.Status != "retryable" {
					t.Fatalf("prior claimed refusal status=%s", task.Status)
				}
				task, err = h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now())
			} else {
				task, _, err = h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			}
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			desktop := t14NamedNode(t, h, server, "desktop", true, true)
			other := t14NamedNode(t, h, server, "machine-b", true, true)
			deadline := time.Now().Add(30 * time.Second)
			if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET deadline_ms=? WHERE request_id=?`, deadline.UnixMilli(), request.RequestID); err != nil {
				t.Fatal(err)
			}
			task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			nodeRequest := boundaryTestNode(task, 458950, "desktop")
			nodeRequest.DeadlineMS = task.Deadline.UnixMilli()
			nodeRequest.Args = []string{"--job", "test-job", "-m", "sonnet", "-l", "test-job", "--role", "implementer"}
			task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, nodeRequest, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			safety, stopSafety := context.WithTimeout(t.Context(), 10*time.Second)
			defer stopSafety()
			waiting, triggerDeadline := context.WithCancel(safety)
			defer triggerDeadline()
			ctx := triggeredDeadlineContext{Context: waiting}
			done := make(chan error, 1)
			go func() {
				_, err := (&directSpawnTransport{h: h, task: task, probeWait: 2 * time.Second}).dispatch(ctx, nodeRequest)
				done <- err
			}()
			first := t14ReadNodeSpawn(t, desktop)
			triggerDeadline()
			probe := t14ReadNodeSpawn(t, desktop)
			if probe.RequestID != first.RequestID {
				t.Fatalf("deadline probe changed node request: %s then %s", first.RequestID, probe.RequestID)
			}
			if err := wsjson.Write(t.Context(), desktop, map[string]any{"type": "job.spawn.result", "request_id": probe.RequestID, "rc": 124, "job_id": "", "pane": "", "stdout_tail": "", "error": "deadline_not_started", "stage": "claim", "retryable": true}); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatalf("not-started result: %v", err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "retryable" || saved.Outcome.Reason != "deadline_not_started" {
				t.Fatalf("not-started outcome: status=%s reason=%s err=%v", saved.Status, saved.Outcome.Reason, err)
			}
			h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
				app := retryDispatchApp(t, out, errout)
				app.Config.Hosts["machine-b"] = map[string]any{}
				app.Config.HostOrder = []string{"desktop", "machine-b"}
				if priorClaim {
					app.Source = claimedHarnessSource{retrySource{now: app.Now()}}
				} else {
					app.Source = retryOnlyOtherHost{retrySource{now: app.Now()}}
				}
				return app, nil
			}
			h.startSpawnCoordinator(t.Context())
			status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
			if status != http.StatusAccepted {
				t.Fatalf("saved request replay returned HTTP %d", status)
			}
			if priorClaim {
				t14ReadNodeSpawn(t, desktop)
			} else {
				t14ReadNodeSpawn(t, other)
			}
			resumed, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || resumed.Attempt != task.Attempt+1 || resumed.Node == nil {
				t.Fatalf("replayed attempt=%d node-present=%v err=%v", resumed.Attempt, resumed.Node != nil, err)
			}
			wantHost := "machine-b"
			if priorClaim {
				wantHost = "desktop"
			}
			if resumed.Node.Machine != wantHost {
				t.Fatalf("replay host=%s, want %s", resumed.Node.Machine, wantHost)
			}
		})
	}
}
