package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/sentinel"
	"github.com/mgh3326/panewire/stage2/adapters/supabase"
)

func TestR5HeartbeatUpsertE2EAndCadence(t *testing.T) {
	fixture := newR5SupabaseFixture(map[string]string{"fixture-access-a": "fixture-a"})
	server := httptest.NewServer(fixture)
	defer server.Close()
	settings, err := sentinel.ParseConfig([]byte(`{
  "version": "r5-fixture",
  "nodes": {"fixture-a": {"checks": [
    {"name": "service", "argv": ["service-check"], "timeout": "20ms"},
    {"name": "disk", "argv": ["disk-check"], "timeout": "20ms"}
  ]}}
}`), "fixture-a")
	if err != nil {
		t.Fatal(err)
	}
	adapter := r5SupabaseAdapter(t, server, "fixture-access-a")
	service, err := sentinel.NewService(sentinel.ServiceConfig{
		MachineID: "fixture-a", Settings: settings, Remote: adapter,
		Execute: func(_ context.Context, argv []string) error {
			if argv[0] == "disk-check" {
				return errors.New("fixture command output must not be retained")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Sentinel: SentinelConfig{Enabled: true, Runner: service, HeartbeatInterval: 10 * time.Millisecond}})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		daemon.sentinelLoop(ctx)
	}()
	r5Eventually(t, "two heartbeat upserts", func() bool { return fixture.HeartbeatPosts() >= 2 })
	cancel()
	<-done
	heartbeat, found := fixture.Heartbeat("fixture-a")
	if !found || heartbeat.Version != "r5-fixture" || heartbeat.Checks["service"] != sentinel.CheckOK || heartbeat.Checks["disk"] != sentinel.CheckFail {
		t.Fatalf("heartbeat=%+v found=%v", heartbeat, found)
	}
	if !fixture.OnlyClosedCheckVocabulary() {
		t.Fatal("heartbeat contained a value outside the closed check vocabulary")
	}
}

func TestR5EvaluateFreshStaleAndFailedChecks(t *testing.T) {
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	settings := r5TwoNodeSettings(t, 1)
	for _, scenario := range []struct {
		name       string
		heartbeat  sentinel.Heartbeat
		wantAlerts int
		wantReason string
	}{
		{name: "fresh", heartbeat: sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now, Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"}, wantAlerts: 0},
		{name: "stale", heartbeat: sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now.Add(-301 * time.Second), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"}, wantAlerts: 1, wantReason: "stale"},
		{name: "failed check", heartbeat: sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now, Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckFail}, Version: "r5-fixture"}, wantAlerts: 1, wantReason: "checks_fail"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			remote := &r5MemoryRemote{heartbeats: map[string]sentinel.Heartbeat{scenario.heartbeat.MachineID: scenario.heartbeat}}
			notifier := &r5Notifier{}
			service := r5Service(t, "fixture-a", settings, remote, notifier, func() time.Time { return now })
			if err := service.Evaluate(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := notifier.Count(); got != scenario.wantAlerts {
				t.Fatalf("notifications=%d, want %d", got, scenario.wantAlerts)
			}
			if scenario.wantReason != "" && notifier.Alert(0).Reason != scenario.wantReason {
				t.Fatalf("reason=%q, want %q", notifier.Alert(0).Reason, scenario.wantReason)
			}
		})
	}
}

func TestR5TelegramEnvModeAndNoSecretLeak(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "telegram.env")
	secretToken := "fixture-bot-token-not-for-logs"
	secretChat := "fixture-chat-id-not-for-logs"
	if err := os.WriteFile(envPath, []byte("TG_BOT_TOKEN="+secretToken+"\nTG_CHAT_ID="+secretChat+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSentinelTelegramEnv(envPath); err == nil {
		t.Fatal("non-0600 Telegram env was accepted")
	}
	if err := os.Chmod(envPath, 0600); err != nil {
		t.Fatal(err)
	}
	env, err := loadSentinelTelegramEnv(envPath)
	if err != nil {
		t.Fatal(err)
	}
	var received struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/bot"+secretToken+"/sendMessage" || json.NewDecoder(request.Body).Decode(&received) != nil {
			http.Error(writer, "fixture failure", http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	notifier, err := newTelegramNotifier(env, sentinelNotifierDeps{HTTPClient: server.Client(), BaseURL: server.URL, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := notifier.Send(t.Context(), sentinel.Alert{MachineID: "fixture-b", Reason: "stale", LastSeen: time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckFail}}); err != nil {
		t.Fatal(err)
	}
	if received.ChatID != secretChat || !strings.Contains(received.Text, "machine: fixture-b") || strings.Contains(received.Text, secretToken) || strings.Contains(received.Text, secretChat) {
		t.Fatalf("Telegram message was malformed or leaked a secret: %+v", received)
	}

	var logs bytes.Buffer
	remote := &r5MemoryRemote{heartbeats: map[string]sentinel.Heartbeat{
		"fixture-b": {MachineID: "fixture-b", SeenAt: time.Now().Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"},
	}}
	failing, err := sentinel.NewService(sentinel.ServiceConfig{
		MachineID: "fixture-a", Settings: r5TwoNodeSettings(t, 1), Remote: remote,
		Notifier: r5FailingNotifier{err: errors.New(secretToken + secretChat)},
		Warn:     func(message string) { logs.WriteString(message) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := failing.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), secretToken) || strings.Contains(logs.String(), secretChat) || !strings.Contains(logs.String(), "sentinel Telegram notification failed") {
		t.Fatalf("warning exposed a secret or was absent: %q", logs.String())
	}
}

func TestR5SentinelStatusAndIndependentGate(t *testing.T) {
	fixture := newR5SupabaseFixture(map[string]string{"fixture-access-a": "fixture-a"})
	fixture.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-a", SeenAt: time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC), Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"})
	fixture.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: time.Date(2026, 8, 31, 0, 59, 0, 0, time.UTC), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"})
	server := httptest.NewServer(fixture)
	defer server.Close()
	root := t.TempDir()
	clientEnv := filepath.Join(root, "client.env")
	if err := writeClientCredentialEnv(clientEnv, clientCredentialEnv{URL: server.URL, MachineID: "fixture-a", AccessToken: "fixture-access-a", RefreshToken: "fixture-refresh-a", PublishableKey: "fixture-public"}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runSentinelCLI([]string{"status", "--stage2-client-env", clientEnv}, &stdout, &stderr, sentinelCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true, Now: func() time.Time { return time.Date(2026, 8, 31, 1, 1, 0, 0, time.UTC) }}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("status code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "MACHINE") || !strings.Contains(stdout.String(), "fixture-b") || strings.Contains(stdout.String(), "fixture-access-a") {
		t.Fatalf("status output=%q", stdout.String())
	}
	configPath := filepath.Join(root, "sentinel.json")
	if err := os.WriteFile(configPath, []byte(`{"nodes":{"fixture-a":{"checks":[]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	daemon, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(root, "daemon.sock"), "--sentinel", "--stage2-client-env", clientEnv, "--sentinel-config", configPath}, daemonCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon == nil {
		t.Fatalf("sentinel-only daemon=%v code=%d err=%v", daemon, code, err)
	}
	if !daemon.cfg.Sentinel.Enabled || daemon.cfg.Sentinel.Watch || daemon.cfg.Stage2.Enabled {
		t.Fatalf("sentinel-only gate configuration=%+v", daemon.cfg)
	}
	if daemon, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(root, "default.sock")}, daemonCLIDeps{}); err != nil || code != ExitOK || daemon == nil {
		t.Fatalf("default-off daemon=%v code=%d err=%v", daemon, code, err)
	} else if daemon.cfg.Sentinel.Enabled {
		t.Fatalf("default-off sentinel was enabled: %+v", daemon.cfg.Sentinel)
	}
	if daemon, code, err := newDaemonForCLI([]string{"--sentinel-watch"}, daemonCLIDeps{}); daemon != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("watch-without-gate daemon=%v code=%d err=%v", daemon, code, err)
	}
}

func TestR5ProvisionArtifactsAreIncrementalAndOffline(t *testing.T) {
	schema, err := os.ReadFile("provision/sentinel.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS panewire.sentinel_heartbeats", "CREATE TABLE IF NOT EXISTS panewire.sentinel_alerts",
		"panewire_sentinel_heartbeats_authenticated_read", "panewire_sentinel_claim_alert", "panewire_sentinel_mark_alert_delivered",
	} {
		if !strings.Contains(string(schema), required) {
			t.Fatalf("sentinel schema is missing %q", required)
		}
	}
	script, err := os.ReadFile("scripts/verify-sentinel-provision-local.sh")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(script), "provision/sentinel.sql") != 2 || !strings.Contains(string(script), "docker image inspect postgres:16-alpine") {
		t.Fatalf("local provision verifier must apply sentinel.sql twice without a Docker pull: %q", script)
	}
}

func TestR5SingleFiringUsesSharedClaim(t *testing.T) {
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	remote := &r5MemoryRemote{heartbeats: map[string]sentinel.Heartbeat{
		"fixture-a": {MachineID: "fixture-a", SeenAt: now, Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"},
		"fixture-b": {MachineID: "fixture-b", SeenAt: now.Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"},
		"fixture-c": {MachineID: "fixture-c", SeenAt: now, Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"},
	}}
	settings := r5Settings(t, now, 1)
	notifier := &r5Notifier{}
	first := r5Service(t, "fixture-a", settings, remote, notifier, func() time.Time { return now })
	second := r5Service(t, "fixture-c", settings, remote, notifier, func() time.Time { return now })

	var group sync.WaitGroup
	for _, service := range []*sentinel.Service{first, second} {
		group.Add(1)
		go func(service *sentinel.Service) {
			defer group.Done()
			if err := service.Evaluate(t.Context()); err != nil {
				t.Errorf("evaluate: %v", err)
			}
		}(service)
	}
	group.Wait()
	if got := notifier.Count(); got != 1 {
		t.Fatalf("notifications=%d, want exactly one shared claim winner", got)
	}
}

func TestR5TwoDaemonHTTPClaimRaceSendsOneNotification(t *testing.T) {
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	fixture := newR5SupabaseFixture(map[string]string{
		"fixture-access-a": "fixture-a",
		"fixture-access-c": "fixture-c",
	})
	fixture.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-a", SeenAt: now, Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"})
	fixture.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now.Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"})
	fixture.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-c", SeenAt: now, Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"})
	server := httptest.NewServer(fixture)
	defer server.Close()
	settings := r5Settings(t, now, 1)
	notifier := &r5Notifier{}
	first, err := sentinel.NewService(sentinel.ServiceConfig{MachineID: "fixture-a", Settings: settings, Remote: r5SupabaseAdapter(t, server, "fixture-access-a"), Notifier: notifier, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	second, err := sentinel.NewService(sentinel.ServiceConfig{MachineID: "fixture-c", Settings: settings, Remote: r5SupabaseAdapter(t, server, "fixture-access-c"), Notifier: notifier, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for _, service := range []*sentinel.Service{first, second} {
		group.Add(1)
		go func(service *sentinel.Service) {
			defer group.Done()
			if err := service.Evaluate(t.Context()); err != nil {
				t.Errorf("evaluate: %v", err)
			}
		}(service)
	}
	group.Wait()
	if got := notifier.Count(); got != 1 {
		t.Fatalf("two daemon fixture notifications=%d, want 1", got)
	}
}

func TestR5TelegramFailureRetriesAfterClaimTTL(t *testing.T) {
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	clock := now
	remote := &r5LeaseRemote{now: func() time.Time { return clock }, heartbeats: map[string]sentinel.Heartbeat{
		"fixture-b": {MachineID: "fixture-b", SeenAt: now.Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"},
	}}
	settings := r5TwoNodeSettings(t, 1)
	settings.ClaimTTL = 5 * time.Second
	notifier := &r5RetryNotifier{failures: 1}
	service := r5Service(t, "fixture-a", settings, remote, notifier, func() time.Time { return clock })
	if err := service.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := notifier.Count(); got != 1 {
		t.Fatalf("initial Telegram attempt count=%d, want 1", got)
	}
	clock = clock.Add(6 * time.Second)
	if err := service.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := notifier.Count(); got != 2 || !remote.Delivered() {
		t.Fatalf("retry attempts=%d delivered=%v", got, remote.Delivered())
	}
}

func TestR5RecoveryAndFlapRequireConsecutiveObservations(t *testing.T) {
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	remote := &r5MemoryRemote{heartbeats: map[string]sentinel.Heartbeat{
		"fixture-b": {MachineID: "fixture-b", SeenAt: now.Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"},
		"fixture-c": {MachineID: "fixture-c", SeenAt: now, Checks: map[string]sentinel.CheckStatus{}, Version: "r5-fixture"},
	}}
	notifier := &r5Notifier{}
	service := r5Service(t, "fixture-a", r5Settings(t, now, 2), remote, notifier, func() time.Time { return now })

	if err := service.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	remote.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now, Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"})
	if err := service.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := notifier.Count(); got != 0 {
		t.Fatalf("flap emitted %d notifications before two consecutive observations", got)
	}

	remote.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now.Add(-6 * time.Minute), Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"})
	for range 2 {
		if err := service.Evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := notifier.Count(); got != 1 {
		t.Fatalf("incident notifications=%d, want 1", got)
	}

	remote.SetHeartbeat(sentinel.Heartbeat{MachineID: "fixture-b", SeenAt: now, Checks: map[string]sentinel.CheckStatus{"service": sentinel.CheckOK}, Version: "r5-fixture"})
	for range 2 {
		if err := service.Evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := notifier.Count(); got != 2 {
		t.Fatalf("incident plus recovery notifications=%d, want 2", got)
	}
	for range 3 {
		if err := service.Evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := notifier.Count(); got != 2 {
		t.Fatalf("recovery repeated: notifications=%d", got)
	}
}

func r5Settings(t *testing.T, now time.Time, consecutive int) sentinel.Settings {
	t.Helper()
	settings, err := sentinel.ParseConfig([]byte(`{
  "version": "r5-fixture",
  "consecutive_observations": 2,
  "alert_window": "15m",
  "claim_ttl": "30s",
  "nodes": {
    "fixture-a": {"checks": []},
    "fixture-b": {"threshold": "300s", "checks": []},
    "fixture-c": {"checks": []}
  }
}`), "fixture-a")
	if err != nil {
		t.Fatal(err)
	}
	settings.ConsecutiveObservations = consecutive
	return settings
}

func r5Service(t *testing.T, machine string, settings sentinel.Settings, remote sentinel.Remote, notifier sentinel.Notifier, now func() time.Time) *sentinel.Service {
	t.Helper()
	service, err := sentinel.NewService(sentinel.ServiceConfig{
		MachineID: machine,
		Settings:  settings,
		Remote:    remote,
		Notifier:  notifier,
		Now:       now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type r5MemoryRemote struct {
	mu         sync.Mutex
	heartbeats map[string]sentinel.Heartbeat
	claims     map[string]bool
}

func (r *r5MemoryRemote) UpsertHeartbeat(context.Context, sentinel.Heartbeat) error { return nil }

func (r *r5MemoryRemote) ListHeartbeats(context.Context) ([]sentinel.Heartbeat, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]sentinel.Heartbeat, 0, len(r.heartbeats))
	for _, heartbeat := range r.heartbeats {
		result = append(result, heartbeat)
	}
	return result, nil
}

func (r *r5MemoryRemote) ClaimAlert(_ context.Context, claim sentinel.AlertClaim) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claims == nil {
		r.claims = make(map[string]bool)
	}
	key := claim.IncidentKey + "|" + claim.AlertWindow.UTC().Format(time.RFC3339Nano)
	if r.claims[key] {
		return false, nil
	}
	r.claims[key] = true
	return true, nil
}

func (r *r5MemoryRemote) MarkAlertDelivered(context.Context, sentinel.AlertClaim) error { return nil }

func (r *r5MemoryRemote) SetHeartbeat(heartbeat sentinel.Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeats[heartbeat.MachineID] = heartbeat
}

type r5Notifier struct {
	mu     sync.Mutex
	alerts []sentinel.Alert
}

func (n *r5Notifier) Send(_ context.Context, alert sentinel.Alert) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alerts = append(n.alerts, alert)
	return nil
}

func (n *r5Notifier) Count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.alerts)
}

func (n *r5Notifier) Alert(index int) sentinel.Alert {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.alerts[index]
}

type r5FailingNotifier struct{ err error }

func (n r5FailingNotifier) Send(context.Context, sentinel.Alert) error { return n.err }

func r5TwoNodeSettings(t *testing.T, consecutive int) sentinel.Settings {
	t.Helper()
	settings, err := sentinel.ParseConfig([]byte(`{
  "version": "r5-fixture",
  "consecutive_observations": 1,
  "alert_window": "15m",
  "claim_ttl": "30s",
  "nodes": {
    "fixture-a": {"checks": []},
    "fixture-b": {"threshold": "300s", "checks": []}
  }
}`), "fixture-a")
	if err != nil {
		t.Fatal(err)
	}
	settings.ConsecutiveObservations = consecutive
	return settings
}

func r5Eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

type r5SupabaseFixture struct {
	mu             sync.Mutex
	identities     map[string]string
	heartbeats     map[string]sentinel.Heartbeat
	heartbeatPosts int
	closedChecks   bool
	claims         map[string]r5FixtureClaim
}

type r5FixtureClaim struct {
	claimant  string
	expires   time.Time
	delivered bool
}

func newR5SupabaseFixture(identities map[string]string) *r5SupabaseFixture {
	return &r5SupabaseFixture{
		identities: identities, heartbeats: make(map[string]sentinel.Heartbeat), claims: make(map[string]r5FixtureClaim), closedChecks: true,
	}
}

func (f *r5SupabaseFixture) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	defer f.mu.Unlock()
	machine := f.identities[strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")]
	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/rest/v1/sentinel_heartbeats":
		var rows []struct {
			MachineID string                          `json:"machine_id"`
			SeenAt    time.Time                       `json:"seen_at"`
			Checks    map[string]sentinel.CheckStatus `json:"checks_json"`
			Version   string                          `json:"version"`
		}
		if machine == "" || !strings.Contains(request.Header.Get("Prefer"), "resolution=merge-duplicates") || json.NewDecoder(request.Body).Decode(&rows) != nil || len(rows) != 1 || rows[0].MachineID != machine {
			http.Error(writer, "denied", http.StatusForbidden)
			return
		}
		heartbeat := sentinel.Heartbeat{MachineID: rows[0].MachineID, SeenAt: rows[0].SeenAt, Checks: rows[0].Checks, Version: rows[0].Version}
		if sentinel.ValidateHeartbeat(heartbeat) != nil {
			f.closedChecks = false
			http.Error(writer, "invalid", http.StatusBadRequest)
			return
		}
		f.heartbeats[heartbeat.MachineID] = heartbeat
		f.heartbeatPosts++
		writer.WriteHeader(http.StatusNoContent)
	case request.Method == http.MethodGet && request.URL.Path == "/rest/v1/sentinel_heartbeats":
		if machine == "" {
			http.Error(writer, "denied", http.StatusUnauthorized)
			return
		}
		rows := make([]map[string]any, 0, len(f.heartbeats))
		for _, heartbeat := range f.heartbeats {
			rows = append(rows, map[string]any{"machine_id": heartbeat.MachineID, "seen_at": heartbeat.SeenAt, "checks_json": heartbeat.Checks, "version": heartbeat.Version})
		}
		_ = json.NewEncoder(writer).Encode(rows)
	case request.Method == http.MethodPost && request.URL.Path == "/rest/v1/rpc/panewire_sentinel_claim_alert":
		var input struct {
			IncidentKey string    `json:"p_incident_key"`
			AlertWindow time.Time `json:"p_alert_window"`
			TTL         int64     `json:"p_claim_ttl_seconds"`
		}
		if machine == "" || json.NewDecoder(request.Body).Decode(&input) != nil || input.IncidentKey == "" || input.AlertWindow.IsZero() || input.TTL < 5 {
			http.Error(writer, "denied", http.StatusForbidden)
			return
		}
		key := input.IncidentKey + "|" + input.AlertWindow.UTC().Format(time.RFC3339Nano)
		claim, exists := f.claims[key]
		claimed := false
		if !exists || (!claim.delivered && !claim.expires.After(time.Now())) {
			f.claims[key] = r5FixtureClaim{claimant: machine, expires: time.Now().Add(time.Duration(input.TTL) * time.Second)}
			claimed = true
		}
		_ = json.NewEncoder(writer).Encode([]map[string]bool{{"claimed": claimed}})
	case request.Method == http.MethodPost && request.URL.Path == "/rest/v1/rpc/panewire_sentinel_mark_alert_delivered":
		var input struct {
			IncidentKey string    `json:"p_incident_key"`
			AlertWindow time.Time `json:"p_alert_window"`
		}
		if machine == "" || json.NewDecoder(request.Body).Decode(&input) != nil {
			http.Error(writer, "denied", http.StatusForbidden)
			return
		}
		key := input.IncidentKey + "|" + input.AlertWindow.UTC().Format(time.RFC3339Nano)
		claim, exists := f.claims[key]
		delivered := exists && claim.claimant == machine && !claim.delivered && claim.expires.After(time.Now())
		if delivered {
			claim.delivered = true
			f.claims[key] = claim
		}
		_ = json.NewEncoder(writer).Encode([]map[string]bool{{"delivered": delivered}})
	default:
		http.NotFound(writer, request)
	}
}

func (f *r5SupabaseFixture) SetHeartbeat(heartbeat sentinel.Heartbeat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats[heartbeat.MachineID] = heartbeat
}

func (f *r5SupabaseFixture) Heartbeat(machineID string) (sentinel.Heartbeat, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	heartbeat, found := f.heartbeats[machineID]
	return heartbeat, found
}

func (f *r5SupabaseFixture) HeartbeatPosts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heartbeatPosts
}

func (f *r5SupabaseFixture) OnlyClosedCheckVocabulary() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closedChecks
}

func r5SupabaseAdapter(t *testing.T, server *httptest.Server, token string) *supabase.Adapter {
	t.Helper()
	adapter, err := supabase.New(supabase.Config{
		BaseURL: server.URL, AccessToken: token, RefreshToken: "fixture-refresh", APIKey: "fixture-public", HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

type r5LeaseRemote struct {
	mu         sync.Mutex
	now        func() time.Time
	heartbeats map[string]sentinel.Heartbeat
	claims     map[string]r5FixtureClaim
}

func (r *r5LeaseRemote) UpsertHeartbeat(context.Context, sentinel.Heartbeat) error { return nil }

func (r *r5LeaseRemote) ListHeartbeats(context.Context) ([]sentinel.Heartbeat, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]sentinel.Heartbeat, 0, len(r.heartbeats))
	for _, heartbeat := range r.heartbeats {
		rows = append(rows, heartbeat)
	}
	return rows, nil
}

func (r *r5LeaseRemote) ClaimAlert(_ context.Context, claim sentinel.AlertClaim) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claims == nil {
		r.claims = make(map[string]r5FixtureClaim)
	}
	key := claim.IncidentKey + "|" + claim.AlertWindow.UTC().Format(time.RFC3339Nano)
	existing, found := r.claims[key]
	if found && (existing.delivered || existing.expires.After(r.now())) {
		return false, nil
	}
	r.claims[key] = r5FixtureClaim{claimant: "fixture-a", expires: r.now().Add(claim.TTL)}
	return true, nil
}

func (r *r5LeaseRemote) MarkAlertDelivered(_ context.Context, claim sentinel.AlertClaim) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := claim.IncidentKey + "|" + claim.AlertWindow.UTC().Format(time.RFC3339Nano)
	existing, found := r.claims[key]
	if !found || !existing.expires.After(r.now()) {
		return errors.New("lease is unavailable")
	}
	existing.delivered = true
	r.claims[key] = existing
	return nil
}

func (r *r5LeaseRemote) Delivered() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, claim := range r.claims {
		if claim.delivered {
			return true
		}
	}
	return false
}

type r5RetryNotifier struct {
	mu       sync.Mutex
	failures int
	calls    int
}

func (n *r5RetryNotifier) Send(context.Context, sentinel.Alert) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls++
	if n.failures > 0 {
		n.failures--
		return errors.New("fixture Telegram failure")
	}
	return nil
}

func (n *r5RetryNotifier) Count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls
}
