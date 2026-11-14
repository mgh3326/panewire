package panewire

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// claimedHarnessSource gives both nodes the same usable account, so a host
// change would be a real placement choice rather than a missing fixture.
type claimedHarnessSource struct{ retrySource }

func (s claimedHarnessSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{
		{HostID: "desktop", AccountKey: "p:0000000000000001", LastSeen: s.now},
		{HostID: "machine-b", AccountKey: "p:0000000000000001", LastSeen: s.now},
	}, nil
}

type claimedHarnessQuota struct{ retryQuota }

func (claimedHarnessQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", ModelID: "codex-model", Pool: "p", Gate: "default"},
		{Profile: "sonnet", Effort: "high", Grade: "B", ModelID: "claude-model", Pool: "p", Gate: "default"},
	}, nil
}

func (claimedHarnessQuota) Launch(_ context.Context, request fleet.QuotaRequest) (fleet.Launch, error) {
	return fleet.Launch{Profile: request.Profile, ModelID: "model", Effort: "high", Grade: "B", Pool: "p", Gate: "default"}, nil
}

func claimedHarnessApp(t *testing.T, alternate bool) func(io.Writer, io.Writer) (*fleet.App, error) {
	t.Helper()
	return func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Source = claimedHarnessSource{retrySource{now: app.Now()}}
		if alternate {
			app.Quota = claimedHarnessQuota{}
		}
		return app, nil
	}
}

// claimedHarnessRefusal completes one real bound send without starting the
// coordinator. The caller can therefore restart after the refusal commits and
// before placement sees it, a window that otherwise races the worker.
func claimedHarnessRefusal(t *testing.T, h *HubServer, request fleet.Submission, result hubSpawnResult) hubSpawnTask {
	t.Helper()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 458901, "desktop")
	node.Args = []string{"--job", "test-job", "-m", "sonnet", "-l", "test-job", "--role", "implementer"}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	return dispatchClaimedHarnessRefusal(t, h, task, result)
}

func dispatchClaimedHarnessRefusal(t *testing.T, h *HubServer, task hubSpawnTask, result hubSpawnResult) hubSpawnTask {
	t.Helper()
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	connection := t14NamedNode(t, h, server, "desktop", true, true)
	defer connection.CloseNow()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(boundedContext(t), *task.Node)
	}()
	message := t14ReadNodeSpawn(t, connection)
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "job.spawn.result", "request_id": message.RequestID, "rc": result.RC, "job_id": "", "pane": "", "stdout_tail": result.StdoutTail, "error": result.Error, "stage": result.Stage, "retryable": result.Retryable}); err != nil {
		t.Fatal(err)
	}
	<-done
	task, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestClaimedHarnessRefusalKeepsBindingOnStoreFailure_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_claimed_unbind BEFORE UPDATE ON spawn_operations WHEN NEW.status='accepted' BEGIN SELECT RAISE(FAIL, 'injected unbind failure'); END`); err != nil {
		t.Fatal(err)
	}
	refusal := hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch", Retryable: true}
	task := claimedHarnessRefusal(t, h, durableSpawnRequest(), refusal)
	if task.Status != "dispatching" || task.Node == nil || task.Node.Machine != "desktop" {
		t.Fatalf("failed pin write released ownership: status=%s node=%+v", task.Status, task.Node)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_claimed_unbind`); err != nil {
		t.Fatal(err)
	}
	recovered := dispatchClaimedHarnessRefusal(t, h, task, refusal)
	if recovered.Status != "accepted" || recovered.Node != nil {
		t.Fatalf("replayed refusal did not commit after repair: status=%s node=%+v", recovered.Status, recovered.Node)
	}
}

func TestClaimedHarnessRetrySurvivesHubRestart_t4581(t *testing.T) {
	for _, tc := range []struct {
		name    string
		host    string
		profile string
		result  hubSpawnResult
		retry   bool
	}{
		{"launch proof", "auto", "auto", hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch", Retryable: true}, true},
		{"login screen on pinned host", "desktop", "auto", hubSpawnResult{RC: 88, Error: "spawn_failed", Stage: "landing", StdoutTail: t4614AuthTail, Retryable: true}, true},
		{"cleanup unconfirmed", "auto", "auto", hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch"}, false},
		{"explicit profile", "auto", "sonnet", hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch", Retryable: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			retryHK(t)
			config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
			h, err := NewHubServer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			request := durableSpawnRequest()
			for i := 0; i+1 < len(request.Args); i++ {
				if request.Args[i] == "--host" {
					request.Args[i+1] = tc.host
				}
				if request.Args[i] == "-m" {
					request.Args[i+1] = tc.profile
				}
			}
			if tc.host != "auto" || tc.profile != "auto" {
				request.Args = append(request.Args, "--containment", "hk:12", "--override-reason", "test")
			}
			task := claimedHarnessRefusal(t, h, request, tc.result)
			if !tc.retry {
				if task.Node == nil || task.Node.Machine != "desktop" || task.Status == "accepted" {
					t.Fatalf("ineligible refusal started another harness: status=%s node=%+v", task.Status, task.Node)
				}
				return
			}
			if task.Status != "accepted" || task.Node != nil {
				t.Fatalf("cleanup-confirmed harness refusal did not queue an alternate: status=%s node=%+v", task.Status, task.Node)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			h, err = NewHubServer(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			h.spawnNewApp = claimedHarnessApp(t, true)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14NamedNode(t, h, server, "desktop", true, true)
			t14NamedNode(t, h, server, "machine-b", true, true)
			h.startSpawnCoordinator(t.Context())
			next := t14ReadNodeSpawn(t, node)
			if profile := hubSpawnArg(next.Args, "-m"); profile != "codex-sol" || next.RequestID == t14RequestID(458901) {
				t.Fatalf("alternate on claimed host = %s, profile %q", next.RequestID, profile)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Attempt != task.Attempt || saved.Node == nil || saved.Node.Machine != "desktop" {
				t.Fatalf("claimed host retry lost its attempt or host: attempt=%d host=%s err=%v", saved.Attempt, spawnTaskHost(saved), err)
			}
			if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": next.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "launch_proof_failed", "stage": "launch", "retryable": true}); err != nil {
				t.Fatal(err)
			}
			eventually(t, 3*time.Second, "both failed harnesses to exhaust the claimed host", func() bool {
				saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
				return err == nil && saved.Status == "retryable"
			})
			if saved.Outcome.Host != "desktop" || saved.Outcome.RC != 70 || saved.Outcome.Reason != "launch_proof_failed" {
				t.Fatalf("exhausting both harnesses lost the last node refusal: %+v", saved.Outcome)
			}
		})
	}
}

func TestClaimedHostStaysPinnedWhenAlternativesRunOut_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	request := durableSpawnRequest()
	task := claimedHarnessRefusal(t, h, request, hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch", Retryable: true})
	if task.Status != "accepted" {
		t.Fatalf("refusal did not queue an alternate: status=%s", task.Status)
	}
	foreign := boundaryTestNode(task, 458902, "machine-b")
	if _, err := h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, foreign, time.Now()); err == nil {
		t.Fatal("the claimed job was bound to another host")
	}
	h.spawnNewApp = claimedHarnessApp(t, false)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14NamedNode(t, h, server, "desktop", true, true)
	t14NamedNode(t, h, server, "machine-b", true, true)
	h.startSpawnCoordinator(t.Context())
	eventually(t, 3*time.Second, "no alternate harness to finish on the claimed host", func() bool {
		task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && task.Status == "retryable"
	})
	if task.Outcome.Host != "desktop" || task.Outcome.RC != 70 || task.Outcome.Reason != "launch_proof_failed" {
		t.Fatalf("exhausted alternate lost the original node refusal: %+v", task.Outcome)
	}
	task, err = h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_ = node.CloseNow()
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	h, err = NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = claimedHarnessApp(t, true)
	server = httptest.NewServer(h.Handler())
	defer server.Close()
	node = t14NamedNode(t, h, server, "desktop", true, true)
	t14NamedNode(t, h, server, "machine-b", true, true)
	h.startSpawnCoordinator(t.Context())
	next := t14ReadNodeSpawn(t, node)
	if task.Attempt != 2 {
		t.Fatalf("new attempt = %d, want 2", task.Attempt)
	}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": next.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "launch_proof_failed", "stage": "placement", "retryable": true}); err != nil {
		t.Fatal(err)
	}
	alternate := t14ReadNodeSpawn(t, node)
	if hubSpawnArg(alternate.Args, "-m") == hubSpawnArg(next.Args, "-m") {
		t.Fatalf("pre-claim refusal on a previously claimed host reused a harness: %v then %v", next.Args, alternate.Args)
	}
	if err := wsjson.Write(t.Context(), node, nodeRefusal(alternate.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, "a pre-claim refusal after the prior claim to finish on that host", func() bool {
		task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && task.Status == "retryable" && task.Attempt == 2
	})
	if task.Outcome.Host != "desktop" {
		t.Fatalf("later refusal lost the host: %+v", task.Outcome)
	}
	if _, err = h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = node.CloseNow()
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	h, err = NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = claimedHarnessApp(t, false)
	server = httptest.NewServer(h.Handler())
	defer server.Close()
	node = t14NamedNode(t, h, server, "desktop", true, true)
	t14NamedNode(t, h, server, "machine-b", true, true)
	h.startSpawnCoordinator(t.Context())
	last := t14ReadNodeSpawn(t, node)
	if hubSpawnArg(last.Args, "--job") != "test-job" {
		t.Fatalf("post-restart retry did not stay with the claimed job: %v", last.Args)
	}
}
