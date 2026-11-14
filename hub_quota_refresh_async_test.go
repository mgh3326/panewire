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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/fleetevent"
)

type staleRefreshQuota struct{ retryQuota }

type secondPoolRefreshQuota struct{ retryQuota }

func (secondPoolRefreshQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{{Profile: "codex-sol", Effort: "high", Grade: "B", ModelID: "m", Pool: "codex", Gate: "default"}}, nil
}

type secondPoolRefreshSource struct {
	now  time.Time
	host string
}

func (secondPoolRefreshSource) Hosts(context.Context, string) ([]fleet.HostView, error) {
	return nil, nil
}
func (s secondPoolRefreshSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{{HostID: s.host, AccountKey: "codex:0000000000000001", LastSeen: s.now}}, nil
}
func (s secondPoolRefreshSource) SharedAccount(_ context.Context, key string) (fleet.SharedAccount, error) {
	return fleet.SharedAccount{Key: key, Observed: true, MeasuredAt: s.now.Add(-time.Hour)}, nil
}

func (staleRefreshQuota) Launch(ctx context.Context, request fleet.QuotaRequest) (fleet.Launch, error) {
	launch, err := (retryQuota{}).Launch(ctx, request)
	launch.Pool = "claude"
	return launch, err
}

func (staleRefreshQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "B", ModelID: "m", Pool: "claude", Gate: "default"}}, nil
}

func (staleRefreshQuota) Gate(_ context.Context, request fleet.QuotaRequest) (string, error) {
	raw, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "provider_id": "claude", "account_key": request.AccountKey, "profile": request.Profile, "ok": true, "exit_code": 0})
	return string(raw), nil
}

type staleRefreshSource struct {
	now      time.Time
	second   bool
	hosts    []string
	fresh    *atomic.Bool
	freshKey string
}

// changingRefreshSource models a shared account becoming stale between the
// selection snapshot and one host's admission, then fresh at the next host.
type changingRefreshSource struct {
	now   time.Time
	reads atomic.Int64
}

func (*changingRefreshSource) Hosts(context.Context, string) ([]fleet.HostView, error) {
	return nil, nil
}

func (s *changingRefreshSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{
		{HostID: "desktop", AccountKey: "claude:0000000000000001", LastSeen: s.now},
		{HostID: "machine-b", AccountKey: "claude:0000000000000001", LastSeen: s.now},
	}, nil
}

func (s *changingRefreshSource) SharedAccount(_ context.Context, key string) (fleet.SharedAccount, error) {
	if s.reads.Add(1) == 3 {
		return fleet.SharedAccount{Key: key, Observed: true, MeasuredAt: s.now.Add(-time.Hour)}, nil
	}
	zero := 0.0
	return fleet.SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: s.now, Quota: &fleet.PoolView{Status: "ok", Class: "spend", Blocking: &zero}}, nil
}

func (staleRefreshSource) Hosts(context.Context, string) ([]fleet.HostView, error) { return nil, nil }
func (s staleRefreshSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	hosts := s.hosts
	if len(hosts) == 0 {
		hosts = []string{"desktop"}
		if s.second {
			hosts = append(hosts, "machine-b")
		}
	}
	var records []fleet.PresenceRecord
	for i, host := range hosts {
		records = append(records, fleet.PresenceRecord{HostID: host, AccountKey: fmt.Sprintf("claude:%016x", i+1), LastSeen: s.now})
	}
	return records, nil
}
func (s staleRefreshSource) SharedAccount(_ context.Context, key string) (fleet.SharedAccount, error) {
	if s.fresh != nil && s.fresh.Load() && (s.freshKey == "" || s.freshKey == key) {
		zero := 0.0
		return fleet.SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: s.now, Quota: &fleet.PoolView{Status: "ok", Class: "spend", Blocking: &zero}}, nil
	}
	return fleet.SharedAccount{Key: key, Observed: true, MeasuredAt: s.now.Add(-time.Hour)}, nil
}

func TestHubQuotaRefreshHeldWriterLeavesPlacementPending(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.executeSpawnTask(ctx, task)
	}()
	finished := waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !finished {
		t.Fatal("placement waited for the node writer instead of leaving the accepted request pending")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("stale quota should leave an unbound accepted request: status=%s node=%v err=%v", saved.Status, saved.Node, err)
	}
}

func TestHubQuotaRefreshPendingDoesNotRecordRefusal(t *testing.T) {
	stateDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	var mu sync.Mutex
	var events []fleetevent.Event
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.StateDir = stateDir
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		app.EventSink = func(event fleetevent.Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
		}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	finished := waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !finished {
		t.Fatal("placement waited for the node writer instead of remaining pending")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("request did not remain accepted and unbound: status=%s err=%v", saved.Status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, event := range events {
		if (event.Kind == "spawn" || event.Kind == "placement") && event.Outcome == "refused" {
			t.Errorf("pending request recorded a %s refusal", event.Kind)
		}
	}
	if _, err := os.Stat(filepath.Join(stateDir, "placement-attempts", "test-job")); !os.IsNotExist(err) {
		t.Errorf("pending request recorded a placement attempt: stat error=%v", err)
	}
}

func TestHubQuotaRefreshPreservesAttemptBeforePending(t *testing.T) {
	stateDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	node := t14NamedNode(t, h, server, "machine-b", true, true)
	var source *changingRefreshSource
	var mu sync.Mutex
	var events []fleetevent.Event
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.StateDir = stateDir
		app.Config.Hosts = map[string]map[string]any{"desktop": {}, "machine-b": {}}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Quota = staleRefreshQuota{}
		source = &changingRefreshSource{now: app.Now()}
		app.Source = source
		app.EventSink = func(event fleetevent.Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
		}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	readCtx, stopRead := context.WithTimeout(ctx, 2*time.Second)
	defer stopRead()
	var message map[string]json.RawMessage
	if err := wsjson.Read(readCtx, node, &message); err != nil {
		cancel()
		<-done
		t.Fatalf("fresh holder received no spawn after stale holder: reads=%d err=%v", source.reads.Load(), err)
	}
	var kind, requestID string
	_ = json.Unmarshal(message["type"], &kind)
	_ = json.Unmarshal(message["request_id"], &requestID)
	if kind != "job.spawn" {
		t.Fatalf("fresh holder received %q, want job.spawn", kind)
	}
	if err := wsjson.Write(ctx, node, nodeRefusal(requestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("request must remain accepted after the host refusal: status=%s err=%v", saved.Status, err)
	}
	spool, err := os.ReadFile(filepath.Join(stateDir, "placements", "test-job.1.json"))
	if err != nil {
		t.Fatalf("contacted host's failed placement attempt was not retained: %v", err)
	}
	var attempt struct {
		Record struct {
			Outcome    string `json:"outcome"`
			Reason     string `json:"reason"`
			Candidates []struct {
				Host   string `json:"host"`
				Reason string `json:"reason"`
			} `json:"candidates"`
		} `json:"record"`
	}
	if err := json.Unmarshal(spool, &attempt); err != nil {
		t.Fatal(err)
	}
	if attempt.Record.Outcome != "failed" {
		t.Errorf("pending attempt outcome=%q, want failed", attempt.Record.Outcome)
	}
	if attempt.Record.Reason != "quota_refresh_pending" {
		t.Errorf("pending attempt reason=%q, want quota_refresh_pending", attempt.Record.Reason)
	}
	foundHost := false
	for _, candidate := range attempt.Record.Candidates {
		if candidate.Host == "machine-b" && candidate.Reason != "" {
			foundHost = true
		}
	}
	if !foundHost {
		t.Error("failed placement attempt omitted the contacted host")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, event := range events {
		if (event.Kind == "spawn" || event.Kind == "placement") && event.Outcome == "refused" {
			t.Errorf("pending request recorded a %s refusal", event.Kind)
		}
	}
}

// A hung first account must not prevent the later account's node from even
// receiving its refresh. Both candidates go through the real selection path.
func TestHubQuotaRefreshHungFirstDoesNotSuppressLaterAccount(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	healthy := t14NamedNode(t, h, server, "machine-b", true, true)
	var fresh atomic.Bool
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now(), second: true, fresh: &fresh, freshKey: "claude:0000000000000002"}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	release := sync.OnceFunc(agent.writeMu.Unlock)
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	commands := make(chan map[string]json.RawMessage, 1)
	go func() {
		var command map[string]json.RawMessage
		if wsjson.Read(ctx, healthy, &command) == nil {
			commands <- command
		}
	}()
	var command map[string]json.RawMessage
	received := waitUntil(time.Second, func() bool {
		select {
		case command = <-commands:
			return true
		default:
			return false
		}
	})
	if !received {
		cancel()
		release()
		<-done
		t.Fatal("the later account received no refresh while the first node writer was held")
	}
	if string(command["type"]) != `"node.command"` || string(command["op"]) != `"quota.refresh"` {
		t.Fatalf("later node received %v, want quota.refresh", command)
	}
	<-done
	// The coordinator has retried and parked this accepted row before the
	// healthy node replies. Its completion must wake placement again.
	h.startSpawnCoordinator(ctx)
	eventually(t, time.Second, "accepted request reconsidered", func() bool {
		saved, err := h.inbox.store.spawnTask(ctx, request.RequestID)
		return err == nil && !saved.LastRun.IsZero()
	})
	fresh.Store(true)
	nodeCommandReplyTo(t, healthy, command, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	placed := make(chan map[string]json.RawMessage, 1)
	go func() {
		var message map[string]json.RawMessage
		if wsjson.Read(ctx, healthy, &message) == nil {
			placed <- message
		}
	}()
	var message map[string]json.RawMessage
	if !waitUntil(5*time.Second, func() bool {
		select {
		case message = <-placed:
			return true
		default:
			return false
		}
	}) || string(message["type"]) != `"job.spawn"` {
		saved, _ := h.inbox.store.spawnTask(ctx, request.RequestID)
		t.Fatalf("healthy refresh did not wake placement: message=%v status=%s reason=%s", message, saved.Status, saved.Outcome.Reason)
	}
}

func TestHubQuotaRefreshWriteFailureLeavesRequestPending(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(t.Context(), task) }()
	registered := waitUntil(time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) != 0
	})
	_ = agent.conn.CloseNow()
	agent.writeMu.Unlock()
	<-done
	if !registered {
		t.Fatal("refresh did not reach the held writer")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("failed refresh write ended the accepted request: status=%s node=%v err=%v", saved.Status, saved.Node, err)
	}
}

func TestHubQuotaRefreshReportsFailureWhilePending(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	logs := &controlPlaneLogCapture{}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"), Logger: slog.New(logs)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(t.Context(), task) }()
	command := nodeCommandRead(t, node)
	nodeCommandReplyTo(t, node, command, map[string]any{"error": "refresh_failed"})
	<-done
	var record slog.Record
	if !waitUntil(time.Second, func() bool {
		var found bool
		record, found = logs.find("hub quota refresh failed")
		return found
	}) {
		t.Fatal("failed quota refresh did not report its cause")
	}
	attrs := controlPlaneRecordAttrs(record)
	if attrs["machine"] != "desktop" || attrs["pool"] != "claude" || attrs["reason"] != "refresh_failed" {
		t.Fatalf("quota refresh failure lacked its machine, pool or reason: %v", attrs)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("failed refresh must leave the request pending: status=%s err=%v", saved.Status, err)
	}
	// A later placement pass retries the same failing key. Wait for a third
	// command to prove the second result was processed before checking logs.
	h.startSpawnCoordinator(t.Context())
	readCtx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var retry map[string]json.RawMessage
	if err := wsjson.Read(readCtx, node, &retry); err != nil {
		t.Fatalf("retry was not sent: %v", err)
	}
	nodeCommandReplyTo(t, node, retry, map[string]any{"error": "refresh_failed"})
	var next map[string]json.RawMessage
	if err := wsjson.Read(readCtx, node, &next); err != nil {
		t.Fatalf("next retry was not sent: %v", err)
	}
	logs.mu.Lock()
	count := 0
	for _, event := range logs.records {
		if event.Message == "hub quota refresh failed" {
			count++
		}
	}
	logs.mu.Unlock()
	if count != 1 {
		t.Fatalf("identical refresh failures wrote %d warnings, want one", count)
	}
}

func TestHubQuotaRefreshFairAdmissionAndDeduplication(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	hosts := []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"}
	tokens := map[string]string{"operator": t14OperatorToken}
	for _, host := range hosts {
		tokens[host] = "node-token"
	}
	h, err := NewHubServer(HubServerConfig{Tokens: tokens, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	type observed struct {
		host    string
		message map[string]json.RawMessage
	}
	observedCommands := make(chan observed, 20)
	connections := make(map[string]*websocket.Conn)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, host := range hosts {
		conn := t14NamedNode(t, h, server, host, true, true)
		connections[host] = conn
		go func(host string, conn *websocket.Conn) {
			for {
				var message map[string]json.RawMessage
				if wsjson.Read(ctx, conn, &message) != nil {
					return
				}
				if string(message["type"]) == `"node.command"` {
					observedCommands <- observed{host, message}
				}
			}
		}(host, conn)
	}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts = map[string]map[string]any{}
		for _, host := range hosts {
			app.Config.Hosts[host] = map[string]any{}
		}
		app.Config.HostOrder = hosts
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now(), hosts: hosts}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	var first []observed
	gotFour := waitUntil(time.Second, func() bool {
		select {
		case item := <-observedCommands:
			first = append(first, item)
		default:
		}
		return len(first) == 4
	})
	if !gotFour {
		cancel()
		<-done
		t.Fatalf("only %d different nodes received refreshes while earlier nodes hung; want 4", len(first))
	}
	<-done
	seen := map[string]bool{}
	for _, item := range first {
		if seen[item.host] || string(item.message["op"]) != `"quota.refresh"` {
			t.Fatalf("duplicate or wrong refresh in initial admission: %+v", item)
		}
		seen[item.host] = true
	}
	// Repeated placement passes for the same accepted request must coalesce.
	for range 3 {
		h.executeSpawnTask(ctx, task)
	}
	// Observe a quiet window to prove that neither saturation nor duplicates
	// allow a fifth command before an active refresh completes.
	var extra observed
	if waitUntil(150*time.Millisecond, func() bool {
		select {
		case extra = <-observedCommands:
			return true
		default:
			return false
		}
	}) {
		t.Fatalf("refresh exceeded its bound or duplicated a key: %+v", extra)
	}
	nodeCommandReplyTo(t, connections[first[0].host], first[0].message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	var next observed
	if !waitUntil(time.Second, func() bool {
		select {
		case next = <-observedCommands:
			return true
		default:
			return false
		}
	}) || seen[next.host] {
		t.Fatalf("queued node did not receive the freed slot: next=%+v admitted=%v", next, seen)
	}

	// Queue a different pool on a still-busy node. Once all other nodes have
	// finished, spare global slots must not admit a second refresh there.
	busy := first[1]
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts = map[string]map[string]any{}
		for _, host := range hosts {
			app.Config.Hosts[host] = map[string]any{}
		}
		app.Config.HostOrder = hosts
		app.Quota = secondPoolRefreshQuota{}
		app.Source = secondPoolRefreshSource{now: app.Now(), host: busy.host}
		return app, nil
	}
	h.executeSpawnTask(ctx, task)
	nodeCommandReplyTo(t, connections[first[2].host], first[2].message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	var last observed
	if !waitUntil(time.Second, func() bool {
		select {
		case last = <-observedCommands:
			return true
		default:
			return false
		}
	}) || seen[last.host] || last.host == next.host {
		t.Fatalf("the final queued node did not progress: next=%+v", last)
	}
	for _, item := range []observed{first[3], next, last} {
		nodeCommandReplyTo(t, connections[item.host], item.message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	}
	eventually(t, time.Second, "other nodes finished their refreshes", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) == 1
	})
	// This quiet window proves that a free global slot cannot bypass the
	// still-busy node's per-node admission limit.
	if waitUntil(150*time.Millisecond, func() bool {
		select {
		case extra = <-observedCommands:
			return true
		default:
			return false
		}
	}) {
		t.Fatalf("a second pool ran while its node was busy: %+v", extra)
	}
	nodeCommandReplyTo(t, connections[busy.host], busy.message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	if !waitUntil(time.Second, func() bool {
		select {
		case extra = <-observedCommands:
			return true
		default:
			return false
		}
	}) || extra.host != busy.host || string(extra.message["args"]) != `{"pool":"codex"}` {
		t.Fatalf("the second pool did not run after its node became free: %+v", extra)
	}
}

func TestHubQuotaRefreshDeadlineCoversWriterAdmission(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Quota = map[string]any{"measure_timeout_s": int64(1)}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	registered := waitUntil(time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) > 0
	})
	expired := registered && waitUntil(2*time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) == 0
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !registered || !expired {
		t.Fatalf("writer admission outlived the measurement deadline: registered=%v expired=%v", registered, expired)
	}
	// After expiry, a released writer must not transmit the abandoned command.
	readCtx, stopRead := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer stopRead()
	var message map[string]any
	if err := wsjson.Read(readCtx, node, &message); err == nil {
		t.Fatalf("expired refresh was sent after writer release: %v", message)
	}
}

// Saturating the placement workers with stale requests must not stop a later
// request whose account already has a confirmed reading.
func TestHubQuotaRefreshDoesNotBlockUnrelatedSubmission(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/"):
			_, _ = fmt.Fprintf(w, `{"id":%s,"title":"work (grade B, T1)","state":"ready","refs":{}}`, strings.TrimPrefix(r.URL.Path, "/v1/tasks/"))
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			_, _ = io.WriteString(w, `{"document":{},"changed":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not_found"}`)
		}
	}))
	t.Cleanup(hk.Close)
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "fixture-token")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	healthy := t14NamedNode(t, h, server, "machine-b", true, true)
	var fresh atomic.Bool
	fresh.Store(true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now(), second: true, fresh: &fresh, freshKey: "claude:0000000000000002"}
		return app, nil
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	release := sync.OnceFunc(agent.writeMu.Unlock)
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.startSpawnCoordinator(ctx)
	makeRequest := func(index int, excluded string) fleet.Submission {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(5000 + index)
		request.Args = append([]string(nil), request.Args...)
		for i, arg := range request.Args {
			switch arg {
			case "-l", "--job":
				request.Args[i+1] = fmt.Sprintf("job-%d", index)
			case "--task":
				request.Args[i+1] = fmt.Sprint(42 + index)
			}
		}
		request.Args = append(request.Args, "--exclude-host", excluded)
		return request
	}
	requests := make([]fleet.Submission, hubSpawnWorkerCount)
	for i := range requests {
		requests[i] = makeRequest(i, "machine-b")
		status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, requests[i])
		if status != http.StatusAccepted {
			t.Fatalf("stale request %d was not accepted: status=%d body=%s", i, status, body)
		}
	}
	eventually(t, 3*time.Second, "all stale requests picked up", func() bool {
		for _, request := range requests {
			saved, err := h.inbox.store.spawnTask(ctx, request.RequestID)
			if err != nil || saved.LastRun.IsZero() {
				return false
			}
		}
		return true
	})
	unrelated := makeRequest(len(requests), "desktop")
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, unrelated)
	if status != http.StatusAccepted {
		t.Fatalf("unrelated request was not accepted: status=%d body=%s", status, body)
	}
	messages := make(chan map[string]json.RawMessage, 1)
	go func() {
		var message map[string]json.RawMessage
		if wsjson.Read(ctx, healthy, &message) == nil {
			messages <- message
		}
	}()
	var message map[string]json.RawMessage
	if !waitUntil(2*time.Second, func() bool {
		select {
		case message = <-messages:
			return true
		default:
			return false
		}
	}) || string(message["type"]) != `"job.spawn"` {
		t.Fatalf("unrelated request could not place while stale workers waited: message=%v", message)
	}
}

func TestHubQuotaRefreshPinnedProfileDoesNotWaitForWriter(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	request.Args = append([]string(nil), request.Args...)
	for i, arg := range request.Args {
		if arg == "-m" {
			request.Args[i+1] = "sonnet"
		}
	}
	request.Args = append(request.Args, "--operator-request", "op-1", "--override-reason", "fixture")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	finished := waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !finished {
		t.Fatal("pinned-profile placement waited for the node writer")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("pinned-profile stale quota ended the request: status=%s node=%v err=%v", saved.Status, saved.Node, err)
	}
	h.mu.Lock()
	_, counted := h.placementCounts[task.Job]
	h.mu.Unlock()
	if counted {
		t.Fatal("an unbound request awaiting quota still counts against a node")
	}
}

func TestHubQuotaRefreshResponseDeadlineKeepsRequestAccepted(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Quota = map[string]any{"measure_timeout_s": int64(1)}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(t.Context(), task) }()
	command := nodeCommandRead(t, node)
	if string(command["op"]) != `"quota.refresh"` {
		t.Fatalf("unexpected node command: %v", command)
	}
	// The node intentionally never replies; wait for its pending command to
	// disappear rather than treating elapsed time as proof of cancellation.
	eventually(t, 2*time.Second, "refresh response deadline", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) == 0
	})
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("response timeout ended the accepted request: status=%s node=%v err=%v", saved.Status, saved.Node, err)
	}
}

type refreshRequiredGate struct{ staleRefreshQuota }

func (refreshRequiredGate) Gate(context.Context, fleet.QuotaRequest) (string, error) {
	return "", &fleet.ExitError{Code: 4, Message: "shared observation is stale at admission"}
}

func TestHubQuotaRefreshWhenAdmissionNeedsNewerObservation(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	var fresh atomic.Bool
	fresh.Store(true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = refreshRequiredGate{}
		app.Source = staleRefreshSource{now: app.Now(), fresh: &fresh}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	finished := waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !finished {
		t.Fatal("admission's newer-observation refresh blocked placement on the node writer")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("newer-observation refresh ended the request: status=%s node=%v err=%v", saved.Status, saved.Node, err)
	}
}

func TestHubQuotaRefreshQueuedDeadlinePreventsLateSend(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	hosts := []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"}
	tokens := map[string]string{"operator": t14OperatorToken}
	for _, host := range hosts {
		tokens[host] = "node-token"
	}
	h, err := NewHubServer(HubServerConfig{Tokens: tokens, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	type observed struct {
		host    string
		message map[string]json.RawMessage
	}
	messages := make(chan observed, 10)
	connections := make(map[string]*websocket.Conn)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, host := range hosts {
		conn := t14NamedNode(t, h, server, host, true, true)
		connections[host] = conn
		go func(host string, conn *websocket.Conn) {
			var message map[string]json.RawMessage
			if wsjson.Read(ctx, conn, &message) == nil {
				messages <- observed{host, message}
			}
		}(host, conn)
	}
	var shortDeadline atomic.Bool
	var reverseOrder atomic.Bool
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts = map[string]map[string]any{}
		for _, host := range hosts {
			app.Config.Hosts[host] = map[string]any{}
		}
		app.Config.HostOrder = hosts
		app.Quota = staleRefreshQuota{}
		present := hosts[:4]
		if shortDeadline.Load() {
			present = hosts[4:]
			app.Config.Quota = map[string]any{"measure_timeout_s": int64(1)}
			if reverseOrder.Load() {
				app.Config.HostOrder = []string{hosts[0], hosts[1], hosts[2], hosts[3], hosts[5], hosts[4]}
			}
		}
		app.Source = staleRefreshSource{now: app.Now(), hosts: present}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	var admitted []observed
	if !waitUntil(time.Second, func() bool {
		select {
		case message := <-messages:
			admitted = append(admitted, message)
		default:
		}
		return len(admitted) == 4
	}) {
		cancel()
		<-done
		t.Fatalf("only %d of four independent refreshes were admitted", len(admitted))
	}
	<-done
	shortDeadline.Store(true)
	h.executeSpawnTask(ctx, task)
	reverseOrder.Store(true)
	coordinatorCtx, stopCoordinator := context.WithCancel(ctx)
	defer stopCoordinator()
	h.startSpawnCoordinator(coordinatorCtx)
	var firstPass time.Time
	eventually(t, time.Second, "initial pending placement pass", func() bool {
		saved, err := h.inbox.store.spawnTask(ctx, request.RequestID)
		if err != nil || saved.LastRun.IsZero() {
			return false
		}
		firstPass = saved.LastRun
		return true
	})
	if !waitUntil(1500*time.Millisecond, func() bool {
		saved, err := h.inbox.store.spawnTask(ctx, request.RequestID)
		return err == nil && saved.LastRun.After(firstPass)
	}) {
		t.Fatal("queued deadline did not wake placement while all refresh slots were occupied")
	}
	eventually(t, time.Second, "re-placement pass completed", h.drain.idle)
	stopCoordinator()
	h.spawnWorkers.Wait()
	// The re-placement pass offered the two expired keys in reverse order;
	// the older queued key must still receive the next slot.
	nodeCommandReplyTo(t, connections[admitted[0].host], admitted[0].message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	var next observed
	if !waitUntil(time.Second, func() bool {
		select {
		case next = <-messages:
			return true
		default:
			return false
		}
	}) || next.host != "node-e" {
		t.Fatalf("queue lost its order across expired attempts: got=%+v want=node-e", next)
	}
	// Hold every slot beyond the queued key's one-second budget: no queued
	// command may run while all slots are occupied.
	var unexpected observed
	if waitUntil(1200*time.Millisecond, func() bool {
		select {
		case unexpected = <-messages:
			return true
		default:
			return false
		}
	}) {
		t.Fatalf("queued refresh ran before a slot was free: %+v", unexpected)
	}
	nodeCommandReplyTo(t, connections[admitted[1].host], admitted[1].message, map[string]any{"result": map[string]any{"outcome": "refreshed"}})
	// This window proves that releasing a slot cannot send an expired key.
	if waitUntil(200*time.Millisecond, func() bool {
		select {
		case unexpected = <-messages:
			return true
		default:
			return false
		}
	}) {
		t.Fatalf("expired queued refresh was sent after a slot opened: %+v", unexpected)
	}
}

func TestHubQuotaRefreshLargeTimeoutDoesNotOverflow(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Quota = map[string]any{"measure_timeout_s": int64(9223372036854775807)}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now()}
		return app, nil
	}
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	agent := h.nodes["desktop"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, task) }()
	registered := waitUntil(time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.nodeCommands) != 0
	})
	finished := waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	cancel()
	agent.writeMu.Unlock()
	<-done
	if !registered || !finished {
		t.Fatalf("large timeout must not expire immediately or block placement: registered=%v placement_returned=%v", registered, finished)
	}
}
