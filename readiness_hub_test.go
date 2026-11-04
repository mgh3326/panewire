package panewire

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func hubReadinessReport() fleet.Readiness {
	return fleet.Readiness{
		PanewireVersion: "v1.2.3",
		CheckedAt:       time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
		Harnesses: map[string]fleet.HarnessReadiness{
			"claude": {Installed: true, Version: "2.1.0", HerdrIntegration: true, Login: fleet.ReadinessLoginPresent, Account: "claude:0123456789abcdef"},
			"codex":  {Installed: false, Login: fleet.ReadinessLoginUnknown},
		},
		Repos:      []string{"panewire"},
		Brief:      fleet.ReadinessBrief{HandoffkeepEnv: true},
		Toolchains: map[string]string{"go": "1.26.0"},
		Config:     fleet.ReadinessConfig{Source: fleet.ReadinessConfigDefault, Workspace: "workers", HerdrSession: "default", WorkRoot: true},
	}
}

// A report round-trips through the heartbeat, and a bad one costs the report,
// never the heartbeat.
func TestReadinessHeartbeatDecode(t *testing.T) {
	report := hubReadinessReport()
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Readiness: &report})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(payload)
	if !ok || heartbeat.Readiness == nil || !heartbeat.Readiness.Harnesses["claude"].Installed || heartbeat.Readiness.Repos[0] != "panewire" {
		t.Fatalf("heartbeat=%+v ok=%t", heartbeat, ok)
	}
	for name, raw := range map[string]string{"not an object": `"x"`, "null": `null`, "no harnesses": `{"repos":["a"]}`, "wrong type": `{"harnesses":3}`} {
		heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","readiness":` + raw + `}`))
		if !ok || heartbeat.Readiness != nil {
			t.Errorf("%s: ok=%t readiness=%+v", name, ok, heartbeat.Readiness)
		}
	}
	oversized := `{"harnesses":{},"repos":["` + strings.Repeat("a", hubReadinessMaxBytes) + `"]}`
	if heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","readiness":` + oversized + `}`)); !ok || heartbeat.Readiness != nil {
		t.Errorf("oversized: ok=%t readiness=%+v", ok, heartbeat.Readiness)
	}
}

// The version forms a build stamps survive the hub; they come from
// dist-version.sh and cli.go's injected version.
func TestReadinessKeepsBuildVersionForms(t *testing.T) {
	for _, version := range []string{"dev", "6a44c39", "6a44c39-dirty", "v1.2.3", "v1.2.3-4-gabc1234-dirty", "panewire-r10", "panewire-r10-3-gabc1234"} {
		report := hubReadinessReport()
		report.PanewireVersion = version
		if got := sanitizeReadiness(report).PanewireVersion; got != version {
			t.Errorf("version %q became %q", version, got)
		}
	}
	for _, version := range []string{"sk-secret", "token=abc", "/usr/bin/panewire", "ghp_0123456789abcdef", "v1"} {
		report := hubReadinessReport()
		report.PanewireVersion = version
		if got := sanitizeReadiness(report).PanewireVersion; got != "" {
			t.Errorf("version %q survived as %q", version, got)
		}
	}
}

// Free text cannot reach the fleet state: only the closed vocabulary survives.
func TestReadinessSanitizeKeepsOnlyClosedData(t *testing.T) {
	report := hubReadinessReport()
	report.PanewireVersion = "/Users/someone/bin/panewire"
	report.Harnesses["claude"] = fleet.HarnessReadiness{Installed: true, Version: "token=abc 2.1", Login: "sk-secret", Account: "sk-live-0123456789", Dialog: "the screen said: trust this folder?"}
	report.Harnesses["rogue"] = fleet.HarnessReadiness{Installed: true}
	report.Repos = []string{"panewire", "/etc/passwd", "Has Space"}
	report.Toolchains = map[string]string{"go": "1.26.0", "rm": "1", "node": "/usr/bin/node"}
	report.Config.Workspace = "ws; rm -rf /"
	report.Config.HerdrSession = "sk-secret-session"
	report.Toolchains["bun"] = "sk-secret"
	clean := sanitizeReadiness(report)
	encoded, _ := json.Marshal(clean)
	for _, leak := range []string{"/Users", "token=", "sk-", "screen said", "rogue", "/etc", "Has Space", "rm -rf", "/usr/bin", "secret"} {
		if strings.Contains(string(encoded), leak) {
			t.Errorf("%q survived: %s", leak, encoded)
		}
	}
	if clean.Config.Workspace != fleet.ReadinessConfigCustom || clean.Config.HerdrSession != fleet.ReadinessConfigCustom || clean.PanewireVersion != "" {
		t.Errorf("config=%+v version=%q", clean.Config, clean.PanewireVersion)
	}
	h := clean.Harnesses["claude"]
	if !h.Installed || h.Login != fleet.ReadinessLoginUnknown || h.Account != "" || h.Dialog != "" || len(clean.Repos) != 1 || clean.Toolchains["go"] != "1.26.0" || len(clean.Toolchains) != 1 {
		t.Fatalf("clean=%s", encoded)
	}
}

func readinessState(t *testing.T, hub *HubServer, machine string) fleet.ReadinessState {
	t.Helper()
	message, ok := fsLatest(hub, machine, fleetStateKindReadiness, "")
	if !ok || message.Readiness == nil {
		t.Fatalf("no readiness state for %s", machine)
	}
	return *message.Readiness
}

// A node that reports is published with its report; one that never did is
// published readiness_unknown, which placement reads as today's behaviour.
func TestReadinessPublishedToFleetState(t *testing.T) {
	hub, server := fsHub(t, "")
	oldNode := fsConnectNode(t, hub, server, "node-b", fsNodeBToken)
	_ = oldNode
	eventually(t, 2*time.Second, "old node unknown", func() bool {
		hub.publishFleetReadiness("node-b", hub.now())
		message, ok := fsLatest(hub, "node-b", fleetStateKindReadiness, "")
		return ok && message.Readiness.Unknown && message.Readiness.Readiness == nil
	})
	node := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	report := hubReadinessReport()
	r6Write(t, node, map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "readiness": report}})
	eventually(t, 2*time.Second, "report published", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindReadiness, "")
		return ok && !message.Readiness.Unknown && message.Readiness.Readiness != nil && message.Readiness.Readiness.Harnesses["claude"].Installed
	})
	state := readinessState(t, hub, "node-a")
	if state.Readiness.Harnesses["codex"].Installed || state.BackoffUntil != nil {
		t.Fatalf("state=%+v", state)
	}
	// The report survives a reconnect until the node sends a new one.
	hub.connect("node-a", "v", "addr", &hubAgent{}, true)
	hub.publishFleetReadiness("node-a", hub.now().Add(time.Minute))
	if state := readinessState(t, hub, "node-a"); state.Unknown {
		t.Fatalf("reconnect forgot the last report: %+v", state)
	}
	// And the snapshot the engine reads carries it.
	status, body := fsGet(t, server, "/v1/state", fsOperator())
	if status != 200 || !strings.Contains(string(body), `"kind":"readiness"`) || !strings.Contains(string(body), `"readiness_unknown":true`) {
		t.Fatalf("status=%d body=%s", status, body)
	}
}

func backoffPolicyFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placement.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLaunchBackoffAppliesAndClears(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	until := func() map[string]time.Time { return readinessState(t, hub, "node-a").BackoffUntil }

	hub.observeLaunchOutcome("node-a", "opus", 2, "cwd_unmapped", now)
	if got := until(); len(got) != 0 {
		t.Fatalf("one failure started a backoff: %v", got)
	}
	// A success between failures resets the count: failures must be consecutive.
	hub.observeLaunchOutcome("node-a", "opus", 0, "", now)
	hub.observeLaunchOutcome("node-a", "opus", 2, "cwd_unmapped", now)
	if got := until(); len(got) != 0 {
		t.Fatalf("a failure after a success started a backoff: %v", got)
	}
	hub.observeLaunchOutcome("node-a", "sonnet", 1, "spawn_failed", now.Add(time.Second))
	got := until()
	if want := now.Add(time.Second).Add(30 * time.Minute); !got["claude"].Equal(want) || len(got) != 1 {
		t.Fatalf("until=%v want claude at %v", got, want)
	}
	// Another kind on the same host, and the same kind on another host, are free.
	hub.observeLaunchOutcome("node-a", "builder-sol-low", 1, "spawn_failed", now)
	if got := until(); len(got) != 1 {
		t.Fatalf("one codex failure reached the backoff: %v", got)
	}
	// A busy checkout is not a launch failure.
	hub.observeLaunchOutcome("node-a", "builder-sol-low", 81, "checkout_refused:busy", now)
	hub.observeLaunchOutcome("node-a", "builder-sol-low", 81, "checkout_refused:busy", now)
	if got := until(); len(got) != 1 {
		t.Fatalf("busy counted: %v", got)
	}
	// A profile with no kind is ignored.
	hub.observeLaunchOutcome("node-a", "no-such-profile", 1, "x", now)
	hub.observeLaunchOutcome("node-a", "no-such-profile", 1, "x", now)
	if got := until(); len(got) != 1 {
		t.Fatalf("unknown profile counted: %v", got)
	}
	// A success clears it.
	hub.observeLaunchOutcome("node-a", "opus", 0, "", now.Add(2*time.Second))
	if got := until(); len(got) != 0 {
		t.Fatalf("success did not clear: %v", got)
	}
	// Expiry: past the window the state no longer lists it.
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	hub.publishFleetReadiness("node-a", now.Add(31*time.Minute))
	if got := until(); len(got) != 0 {
		t.Fatalf("expired backoff still listed: %v", got)
	}
}

// One failure of each of two kinds on a host is two kinds with one failure, not
// one kind with two: the count is per (host, kind).
func TestLaunchBackoffCountsPerKind(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	fsConnectNode(t, hub, server, "node-b", fsNodeBToken)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	hub.observeLaunchOutcome("node-a", "builder-sol-low", 1, "spawn_failed", now)
	hub.observeLaunchOutcome("node-b", "opus", 1, "spawn_failed", now)
	for _, machine := range []string{"node-a", "node-b"} {
		if got := readinessState(t, hub, machine).BackoffUntil; len(got) != 0 {
			t.Fatalf("%s backed off after one failure per kind: %v", machine, got)
		}
	}
	hub.observeLaunchOutcome("node-a", "builder-sol-medium", 1, "spawn_failed", now)
	got := readinessState(t, hub, "node-a").BackoffUntil
	if _, codex := got["codex"]; !codex || len(got) != 1 {
		t.Fatalf("until=%v want codex only", got)
	}
}

// The two failures must fall inside the backoff window: an old one does not
// combine with a new one.
func TestLaunchBackoffFailuresAgeOut(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now.Add(31*time.Minute))
	if got := readinessState(t, hub, "node-a").BackoffUntil; len(got) != 0 {
		t.Fatalf("a failure 31 minutes old combined with a new one: %v", got)
	}
	// The new one starts a fresh window; a second inside it backs off.
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now.Add(40*time.Minute))
	if got := readinessState(t, hub, "node-a").BackoffUntil; len(got) != 1 {
		t.Fatalf("two failures inside the window did not back off: %v", got)
	}
}

func TestLaunchBackoffConstantsComeFromPlacementJSON(t *testing.T) {
	hub, server := fsHub(t, backoffPolicyFile(t, `{"local_machine":"node-a","spill_targets":[],"max_active_jobs":5,"launch_failure_threshold":3,"launch_backoff_minutes":10}`))
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	if got := readinessState(t, hub, "node-a").BackoffUntil; len(got) != 0 {
		t.Fatalf("two failures backed off under a threshold of 3: %v", got)
	}
	hub.observeLaunchOutcome("node-a", "opus", 1, "spawn_failed", now)
	if got := readinessState(t, hub, "node-a").BackoffUntil["claude"]; !got.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("until=%v", got)
	}
}

func TestPlacementPolicyRejectsBadBackoff(t *testing.T) {
	for _, body := range []string{`"launch_failure_threshold":-1`, `"launch_failure_threshold":101`, `"launch_backoff_minutes":-5`, `"launch_backoff_minutes":100000`} {
		if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","spill_targets":[],"max_active_jobs":5,` + body + `}`)); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
	policy := DefaultPlacementPolicy()
	if policy.launchFailureThreshold() != 2 || policy.launchBackoff() != 30*time.Minute {
		t.Fatalf("defaults: %d %v", policy.launchFailureThreshold(), policy.launchBackoff())
	}
}

// A report the node stopped resending expires: the pair is not judged on a
// stale verdict, and the resend that follows brings the report back.
func TestReadinessReportExpiresWithoutResend(t *testing.T) {
	hub, server := fsHub(t, "")
	node := fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	r6Write(t, node, map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "readiness": hubReadinessReport()}})
	eventually(t, 2*time.Second, "report published", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindReadiness, "")
		return ok && !message.Readiness.Unknown
	})
	hub.mu.Lock()
	received := hub.nodes["node-a"].readinessAt
	hub.mu.Unlock()
	hub.publishFleetReadiness("node-a", received.Add(hubReadinessExpiry-time.Second))
	if state := readinessState(t, hub, "node-a"); state.Unknown {
		t.Fatalf("a report inside the expiry read unknown: %+v", state)
	}
	hub.publishFleetReadiness("node-a", received.Add(hubReadinessExpiry+time.Second))
	if state := readinessState(t, hub, "node-a"); !state.Unknown || state.Readiness != nil {
		t.Fatalf("an expired report was published as fresh: %+v", state)
	}
}

// POST /v1/spawn is a launch path too: its failed answers count toward the
// backoff, and a success clears it.
func TestLaunchBackoffCountsSynchronousSpawnAnswers(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	spawnBody := func(number int) map[string]any {
		body := t14SpawnBody(t14RequestID(number))
		body["args"] = []string{"-m", "codex-sol", "-w", "worker", "-l", "label-a", "--t", "T1", "--job", "job-a", "--owner", "lane-a"}
		return body
	}
	for number := 1; number <= 2; number++ {
		body := spawnBody(number)
		response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
		message := t14ReadNodeSpawn(t, node)
		t14WriteSpawnResult(t, node, message.RequestID, 1, "", "", "", "spawn_failed")
		if result := t14Result(t, response); result.status != http.StatusOK {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
	}
	if _, backed := readinessState(t, hub, "machine-a").BackoffUntil["codex"]; !backed {
		t.Fatal("two failed POST /v1/spawn answers did not back the pair off")
	}
	body := spawnBody(3)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	t14WriteSpawnResult(t, node, message.RequestID, 0, "job-a", "pane-a", "", "")
	t14Result(t, response)
	if got := readinessState(t, hub, "machine-a").BackoffUntil; len(got) != 0 {
		t.Fatalf("a success did not clear the backoff: %v", got)
	}
}
