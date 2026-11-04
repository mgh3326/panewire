package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const readinessHerdrStatus = `pi: not installed (/Users/someone/.pi/agent/extensions/herdr-agent-state.ts)
omp: current (v10) (/Users/someone/.omp/agent/extensions/herdr-omp-agent-state.ts)
claude: current (v11) (/Users/someone/.claude/hooks/herdr-agent-state.sh)
codex: outdated (v8) (/Users/someone/.codex/herdr-agent-state.sh)
devin: not installed (/Users/someone/.config/devin/herdr-agent-state.sh)
antigravity-cli: current (v4) (/Users/someone/.gemini/config/hooks/herdr-agent-state.sh)
`

// readinessFakes answers the probe from a table: which binaries exist and what
// they print. The outputs carry text a report must never repeat.
func readinessFakes(installed map[string]string) nodeReadinessDeps {
	return nodeReadinessDeps{
		lookPath: func(name string) (string, error) {
			if _, ok := installed[name]; ok {
				return "/opt/fake/bin/" + name, nil
			}
			return "", errors.New("not found")
		},
		run: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name == "herdr" {
				return []byte(readinessHerdrStatus), nil
			}
			base := filepath.Base(name)
			out, ok := installed[base]
			if !ok {
				return nil, errors.New("not found")
			}
			return []byte(out), nil
		},
		listDir:     func(string) []string { return nil },
		envReadable: func(string) bool { return true },
	}
}

func readinessClient(t *testing.T, spawnJSON string, deps nodeReadinessDeps) *HubClient {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spawn.json")
	if spawnJSON != "" {
		if err := os.WriteFile(path, []byte(spawnJSON), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	return &HubClient{spawnConfigPath: path, readinessDeps: deps, now: func() time.Time { return now }, assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}}
}

func TestCollectReadinessFromFakes(t *testing.T) {
	workRoot := t.TempDir()
	client := readinessClient(t, `{"cwd_map":{"panewire":"/srv/secret-place/panewire","other":"/srv/other"},"workspace":"workers","herdr_session":"main","enabled":true,"work_root":"`+workRoot+`","handoffkeep_env":"/home/someone/secret.env"}`, readinessFakes(map[string]string{
		"claude": "2.1.5 (Claude Code) token=SECRET-TOKEN\nthe screen reads: trust this folder?",
		"codex":  "codex-cli 0.166.0",
		"go":     "go version go1.26.1 darwin/arm64",
		"bun":    "1.2.3",
	}))
	client.noteQuotaAccount("claude", "claude:0123456789abcdef")
	client.noteQuotaAccount("codex", "") // pacelever refresh failed
	client.readiness.dialogs = map[string]dialogMark{"codex": {marker: fleet.DialogCodexHooksReview, at: client.nowUTC().Add(-time.Minute)}}
	report := client.collectReadiness(t.Context())

	claude, codex, omp := report.Harnesses["claude"], report.Harnesses["codex"], report.Harnesses["omp"]
	if !claude.Installed || claude.Version != "2.1.5" || !claude.HerdrIntegration || claude.Login != fleet.ReadinessLoginPresent || claude.Account != "claude:0123456789abcdef" || claude.Dialog != "" {
		t.Errorf("claude=%+v", claude)
	}
	if !codex.Installed || codex.Version != "0.166.0" || codex.HerdrIntegration || codex.Login != fleet.ReadinessLoginMissing || codex.Dialog != fleet.DialogCodexHooksReview {
		t.Errorf("codex=%+v", codex)
	}
	if omp.Installed || omp.Login != fleet.ReadinessLoginUnknown || !report.Harnesses["cmd"].Installed {
		t.Errorf("omp=%+v cmd=%+v", omp, report.Harnesses["cmd"])
	}
	if report.Toolchains["go"] != "1.26.1" || report.Toolchains["bun"] != "1.2.3" || len(report.Toolchains) != 2 {
		t.Errorf("toolchains=%v", report.Toolchains)
	}
	if strings.Join(report.Repos, ",") != "other,panewire" || !report.Brief.HandoffkeepEnv {
		t.Errorf("repos=%v brief=%+v", report.Repos, report.Brief)
	}
	if report.Config != (fleet.ReadinessConfig{Source: fleet.ReadinessConfigFile, Workspace: "workers", HerdrSession: fleet.ReadinessConfigCustom, WorkRoot: true}) {
		t.Errorf("config=%+v", report.Config)
	}
	for _, kind := range fleet.ReadinessKinds {
		if _, ok := report.Harnesses[kind]; !ok {
			t.Errorf("kind %s missing from the report", kind)
		}
	}

	// What the node reads must not travel: no credential, no path, no file
	// contents, no screen text.
	encoded := readinessJSON(report)
	for _, leak := range []string{"SECRET", "secret", "screen", "trust this", "/srv", "/home", "/Users", "someone", workRoot, "hooks/herdr", "token="} {
		if strings.Contains(encoded, leak) {
			t.Errorf("the report carries %q: %s", leak, encoded)
		}
	}
	if sanitized, _ := json.Marshal(sanitizeReadiness(*report)); string(sanitized) != encoded {
		t.Errorf("the hub would change a well-formed report:\nnode: %s\nhub:  %s", encoded, sanitized)
	}
}

func TestCollectReadinessBriefNeedsAReadableEnv(t *testing.T) {
	deps := readinessFakes(nil)
	deps.envReadable = func(string) bool { return false }
	client := readinessClient(t, `{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true,"handoffkeep_env":"/x/env"}`, deps)
	if report := client.collectReadiness(t.Context()); report.Brief.HandoffkeepEnv {
		t.Fatal("an unreadable env reported as readable")
	}
	client = readinessClient(t, `{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true}`, readinessFakes(nil))
	if report := client.collectReadiness(t.Context()); report.Brief.HandoffkeepEnv {
		t.Fatal("no handoffkeep_env reported as readable")
	}
}

func TestCollectReadinessWithARefusedSpawnConfigOffersNothing(t *testing.T) {
	client := readinessClient(t, `{"cwd_map":{"a":"/x"},"workspace":"w","herdr_session":"s","enabled":false}`, readinessFakes(nil))
	report := client.collectReadiness(t.Context())
	if len(report.Repos) != 0 || report.Brief.HandoffkeepEnv || report.Config.Source != fleet.ReadinessConfigFile {
		t.Fatalf("report=%+v", report)
	}
}

// spawn.json is optional at its default path: the node runs on defaults and
// reports the effective config.
func TestSpawnConfigOptionalAtTheDefaultPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	workRoot := filepath.Join(home, "work")
	if err := os.MkdirAll(filepath.Join(workRoot, "panewire"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(workRoot, "not a repo key"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(workRoot, "linked")); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: defaultHubSpawnConfigPath(""), spawnConfigDefault: true, readinessDeps: readinessFakes(nil)}
	config, defaults, err := client.resolveHubSpawnConfig()
	if err != nil || !defaults || config.Workspace != "workers" || config.HERDRSession != "default" || config.WorkRoot != workRoot || config.HandoffkeepEnv != filepath.Join(home, ".config", "handoffkeep", "config.env") || !config.Enabled {
		t.Fatalf("config=%+v defaults=%t err=%v", config, defaults, err)
	}
	client.readinessDeps.listDir = func(dir string) []string {
		entries, _ := os.ReadDir(dir)
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		return names
	}
	report := client.collectReadiness(t.Context())
	if report.Config.Source != fleet.ReadinessConfigDefault || !report.Config.WorkRoot || strings.Join(report.Repos, ",") != "panewire" {
		t.Fatalf("report=%+v", report)
	}
	if strings.Contains(readinessJSON(report), home) {
		t.Fatalf("the report carries the home path: %s", readinessJSON(report))
	}
	// A node on defaults finds a repo under its work root; a symlink or a
	// missing directory is unmapped.
	for key, want := range map[string]bool{"panewire": true, "missing": false, "linked": false, "Bad Key": false} {
		message := hubOutboundMessage{RequestID: t14RequestID(200), CWDKey: key, BriefInline: "b", WaitSeconds: 1}
		client.spawnEngine = func(_ context.Context, cwd, _, _, _ string, _ []string) (*hubSpawnOutput, fleet.Execution) {
			if cwd != filepath.Join(workRoot, "panewire") {
				t.Errorf("engine cwd=%s", cwd)
			}
			return &hubSpawnOutput{}, fleet.Execution{}
		}
		result := client.runHubSpawn(t.Context(), message)
		if unmapped := result.Error == "cwd_unmapped"; unmapped == want {
			t.Errorf("%s: result=%+v", key, result)
		}
	}
	// An explicitly configured path that is missing stays a refusal.
	explicit := &HubClient{spawnConfigPath: filepath.Join(t.TempDir(), "missing.json")}
	if _, _, err := explicit.resolveHubSpawnConfig(); err == nil || !strings.Contains(err.Error(), "config_missing") {
		t.Fatalf("explicit missing path: %v", err)
	}
	// A present but invalid file is never replaced by defaults.
	bad := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(bad, []byte(`{"enabled":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := &HubClient{spawnConfigPath: bad, spawnConfigDefault: true}
	if _, defaults, err := invalid.resolveHubSpawnConfig(); err == nil || defaults {
		t.Fatalf("invalid file: defaults=%t err=%v", defaults, err)
	}
}

func heartbeatReadiness(t *testing.T, client *HubClient) *fleet.Readiness {
	t.Helper()
	var payload struct {
		Readiness *fleet.Readiness `json:"readiness"`
	}
	if err := json.Unmarshal(client.heartbeatEvent(t.Context()).Payload, &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Readiness
}

func readinessHeartbeatClient(t *testing.T, collector func(context.Context) *fleet.Readiness) (*HubClient, *hubClientConnection, *time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	client := &HubClient{
		jobsInboxRoot: t.TempDir(), assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{},
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("unavailable") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("unavailable") },
		readinessCollector:  collector,
	}
	client.now = func() time.Time { return now }
	peer := &hubClientConnection{}
	client.signalsPeer.Store(peer)
	return client, peer, &now
}

// The handshake: readiness goes only to a hub that advertised it, is probed in
// the background, is not re-sent while unchanged, and is re-sent on a new
// connection and after the resend interval.
func TestReadinessHeartbeatHandshakeAndCadence(t *testing.T) {
	var probes atomic.Int32
	collector := func(context.Context) *fleet.Readiness {
		probes.Add(1)
		return &fleet.Readiness{CheckedAt: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC), Harnesses: map[string]fleet.HarnessReadiness{}}
	}
	client, peer, now := readinessHeartbeatClient(t, collector)
	if heartbeatReadiness(t, client) != nil || probes.Load() != 0 {
		t.Fatal("readiness sent (or probed) before the hub advertised it")
	}
	peer.acceptsReadiness.Store(true)
	if heartbeatReadiness(t, client) != nil {
		t.Fatal("the first heartbeat waited for a probe instead of sending nothing")
	}
	eventually(t, 5*time.Second, "background probe", func() bool {
		client.readiness.mu.Lock()
		defer client.readiness.mu.Unlock()
		return client.readiness.cached != nil
	})
	if heartbeatReadiness(t, client) == nil {
		t.Fatal("the cached report was not sent")
	}
	if heartbeatReadiness(t, client) != nil {
		t.Fatal("an unchanged report was re-sent at once")
	}
	*now = now.Add(nodeReadinessResend)
	if heartbeatReadiness(t, client) == nil {
		t.Fatal("not re-sent after the resend interval")
	}
	if probes.Load() != 1 {
		t.Fatalf("probes=%d: the slow interval has not passed", probes.Load())
	}
	// A new connection starts over.
	next := &hubClientConnection{}
	next.acceptsReadiness.Store(true)
	client.signalsPeer.Store(next)
	if heartbeatReadiness(t, client) == nil {
		t.Fatal("a new connection was not sent the report")
	}
	// A hub that never advertised gets nothing, whatever was sent before.
	old := &hubClientConnection{}
	client.signalsPeer.Store(old)
	if heartbeatReadiness(t, client) != nil {
		t.Fatal("sent readiness to a connection that did not advertise it")
	}
}

// Refresh: on the slow interval, and after a failed launch; never after a
// successful one that changed nothing.
func TestReadinessRefreshesOnIntervalAndAfterAFailedLaunch(t *testing.T) {
	var probes atomic.Int32
	collector := func(context.Context) *fleet.Readiness {
		probes.Add(1)
		return &fleet.Readiness{CheckedAt: time.Now(), Harnesses: map[string]fleet.HarnessReadiness{}}
	}
	client, peer, now := readinessHeartbeatClient(t, collector)
	peer.acceptsReadiness.Store(true)
	waitProbes := func(want int32) {
		t.Helper()
		eventually(t, 5*time.Second, "probe count", func() bool {
			client.readiness.mu.Lock()
			defer client.readiness.mu.Unlock()
			return probes.Load() == want && !client.readiness.refreshing
		})
	}
	heartbeatReadiness(t, client)
	waitProbes(1)
	heartbeatReadiness(t, client)
	*now = now.Add(time.Minute)
	heartbeatReadiness(t, client)
	if probes.Load() != 1 {
		t.Fatalf("refreshed before the slow interval: %d", probes.Load())
	}
	client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", "opus"}}, hubSpawnResult{RC: 0})
	heartbeatReadiness(t, client)
	if probes.Load() != 1 {
		t.Fatalf("a successful launch forced a refresh: %d", probes.Load())
	}
	client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", "opus"}}, hubSpawnResult{RC: 2, Error: "cwd_unmapped"})
	heartbeatReadiness(t, client)
	waitProbes(2)
	*now = now.Add(nodeReadinessInterval)
	heartbeatReadiness(t, client)
	waitProbes(3)
}

func TestLaunchResultsTeachTheDialogState(t *testing.T) {
	client := &HubClient{}
	launch := func(profile string, rc int, tail string) {
		client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", profile}}, hubSpawnResult{RC: rc, StdoutTail: tail})
	}
	dialog := func(kind string) string {
		client.readiness.mu.Lock()
		defer client.readiness.mu.Unlock()
		return client.readiness.dialogs[kind].marker
	}
	for _, tc := range []struct{ profile, kind, tail, want string }{
		{"builder-sol-low", "codex", "stderr: HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen", fleet.DialogCodexHooksReview},
		{"builder-sol-low", "codex", "stderr: WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for /x", fleet.DialogCodexTrust},
		{"omp-opus", "omp", "stderr: OMP_SETUP_WIZARD: omp is in its setup wizard", fleet.DialogOmpSetupWizard},
		{"opus", "claude", "stderr: WORKSPACE_TRUST_REQUIRED: trust prompt", fleet.DialogClaudeTrust},
	} {
		launch(tc.profile, 90, tc.tail)
		if got := dialog(tc.kind); got != tc.want {
			t.Errorf("%s: dialog=%q want %q", tc.kind, got, tc.want)
		}
		launch(tc.profile, 0, "")
		if got := dialog(tc.kind); got != "" {
			t.Errorf("%s: a launch that got past it left %q", tc.kind, got)
		}
	}
	// A marker for one kind does not set another's dialog, and an unrelated
	// failure keeps the last known state.
	launch("omp-opus", 90, "WORKSPACE_TRUST_REQUIRED")
	launch("opus", 90, "HOOKS_REVIEW_REQUIRED")
	launch("builder-sol-low", 90, "HOOKS_REVIEW_REQUIRED")
	launch("builder-sol-low", 1, "spawn_failed for another reason")
	if dialog("omp") != "" || dialog("claude") != "" || dialog("codex") != fleet.DialogCodexHooksReview {
		t.Fatalf("claude=%q codex=%q", dialog("claude"), dialog("codex"))
	}
}

// An old hub never advertises readiness, so a new node's heartbeat stays inside
// the vocabulary that hub decodes.
func TestNewNodeHeartbeatWithoutAdvertisementHasNoReadiness(t *testing.T) {
	client, _, _ := readinessHeartbeatClient(t, func(context.Context) *fleet.Readiness {
		return &fleet.Readiness{Harnesses: map[string]fleet.HarnessReadiness{}}
	})
	if strings.Contains(string(client.heartbeatEvent(t.Context()).Payload), "readiness") {
		t.Fatal("readiness sent without an advertisement")
	}
}

// End to end over a real socket: the hub advertises, the node reports, and
// the hub publishes the report.
func TestReadinessHandshakeEndToEnd(t *testing.T) {
	hub, server := fsHub(t, "")
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.readinessCollector = func(context.Context) *fleet.Readiness {
			report := hubReadinessReport()
			return &report
		}
	})
	eventually(t, 10*time.Second, "readiness reached the fleet state", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindReadiness, "")
		return ok && message.Readiness != nil && !message.Readiness.Unknown && message.Readiness.Readiness.Harnesses["claude"].Installed
	})
}

func TestHubFeaturesAdvertiseReadiness(t *testing.T) {
	encoded, err := json.Marshal(hubFeaturesEvent{Type: "hub.features", Features: []string{hubFeatureHostSignals, hubFeatureReadiness}})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"type":"hub.features","features":["host_signals","readiness"]}` {
		t.Fatalf("encoded=%s", encoded)
	}
	// A hub that advertises only host_signals leaves readiness off.
	message, ok := parseHubOutbound([]byte(`{"type":"hub.features","features":["host_signals"]}`))
	if !ok {
		t.Fatal("parse")
	}
	peer := &hubClientConnection{}
	peer.acceptsReadiness.Store(strings.Contains(strings.Join(message.Features, ","), hubFeatureReadiness))
	if peer.acceptsReadiness.Load() {
		t.Fatal("readiness accepted from a hub that did not advertise it")
	}
}

// A pair the dialog dropped is never launched again, so the dialog must expire
// on its own: after the TTL the report stops carrying it and the next launch
// can find out whether the host owner dealt with it.
func TestReportedDialogExpires(t *testing.T) {
	client := readinessClient(t, `{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true}`, readinessFakes(map[string]string{"codex": "0.166.0"}))
	client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", "builder-sol-low"}}, hubSpawnResult{RC: 90, StdoutTail: "HOOKS_REVIEW_REQUIRED"})
	now := client.nowUTC()
	if got := client.collectReadiness(t.Context()).Harnesses["codex"].Dialog; got != fleet.DialogCodexHooksReview {
		t.Fatalf("dialog=%q right after the launch", got)
	}
	client.now = func() time.Time { return now.Add(nodeDialogTTL - time.Second) }
	if got := client.collectReadiness(t.Context()).Harnesses["codex"].Dialog; got == "" {
		t.Fatal("dialog expired early")
	}
	client.now = func() time.Time { return now.Add(nodeDialogTTL) }
	if got := client.collectReadiness(t.Context()).Harnesses["codex"].Dialog; got != "" {
		t.Fatalf("dialog=%q past the TTL", got)
	}
	// Stopping at it again reports it for another TTL.
	client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", "builder-sol-low"}}, hubSpawnResult{RC: 90, StdoutTail: "HOOKS_REVIEW_REQUIRED"})
	if got := client.collectReadiness(t.Context()).Harnesses["codex"].Dialog; got != fleet.DialogCodexHooksReview {
		t.Fatalf("dialog=%q after stopping at it again", got)
	}
}

// A node on defaults still gets the startup guard: the spawn journal is
// validated, and a retired runtug_path in a present file still refuses.
func TestSpawnStartupGuardRunsOnDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	blocked := filepath.Join(t.TempDir(), "state-is-a-file")
	if err := os.WriteFile(blocked, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: defaultHubSpawnConfigPath(""), spawnConfigDefault: true, spawnJournalDir: filepath.Join(blocked, "spawn")}
	if err := client.spawnStartupError(); err == nil || !strings.Contains(err.Error(), "spawn journal unavailable") {
		t.Fatalf("a node on defaults skipped the journal check: %v", err)
	}
	client.spawnJournalDir = filepath.Join(t.TempDir(), "spawn")
	if err := client.spawnStartupError(); err != nil {
		t.Fatalf("a usable journal refused: %v", err)
	}
	client.releaseSpawnJournal()
}
