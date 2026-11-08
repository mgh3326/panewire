package panewire

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/hkcred"
)

// hk 3570 review: a node running on defaults (no spawn.json) resolves its hk
// credential exactly as hkcred.Load does — the environment pair first, then
// the panewire-owned hk-svc.env with the svc key only. HANDOFFKEEP_TOKEN is
// read only from a file the operator named (spawn.json handoffkeep_env, a hub
// flag), and the default-or-explicit choice travels with the config, never
// guessed from the path.

// svcNodeHK records every request's route and bearer and answers a brief.
type svcNodeHK struct {
	mu   sync.Mutex
	seen []string
}

func newSvcNodeHK(t *testing.T) (*svcNodeHK, string) {
	t.Helper()
	state := &svcNodeHK{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		state.seen = append(state.seen, r.URL.EscapedPath()+" "+r.Header.Get("Authorization"))
		state.mu.Unlock()
		_, _ = io.WriteString(w, `{"key":"briefs/job-3570/brief.md","body":"brief text"}`)
	}))
	t.Cleanup(server.Close)
	return state, server.URL
}

func (s *svcNodeHK) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func defaultNodeClient() *HubClient {
	return &HubClient{spawnConfigPath: defaultHubSpawnConfigPath(""), spawnConfigDefault: true, readinessDeps: readinessFakes(nil)}
}

// runDefaultNodeBriefSpawn drives a real spawn with a brief document on a
// node running on defaults; the engine is a fake.
func runDefaultNodeBriefSpawn(t *testing.T, home string) hubSpawnResult {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "work", "repo-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := defaultNodeClient()
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		return t14EngineOK()
	}
	return client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(3570), CWDKey: "repo-a", BriefDoc: "briefs/job-3570/brief.md", WaitSeconds: 1})
}

func TestHKSvcNodeDefaultRefusesLegacyOnlyFile(t *testing.T) {
	home := isolateRootSvcCred(t)
	hk, url := newSvcNodeHK(t)
	writeRootSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN="+svcCredRootLegacy+"\n")

	_, err := defaultNodeClient().nodeHandoffkeepEnv()
	var unconfigured *hkcred.UnconfiguredError
	if !errors.As(err, &unconfigured) {
		t.Fatalf("node default accepted a legacy-only hk-svc.env: err=%v", err)
	}
	if strings.Contains(err.Error(), svcCredRootLegacy) || !strings.Contains(err.Error(), hkcred.TokenKey) {
		t.Fatalf("refusal must name the svc key and no value: %q", err)
	}

	deps := readinessFakes(nil)
	deps.envReadable = nil
	client := defaultNodeClient()
	client.readinessDeps = deps
	if report := client.collectReadiness(t.Context()); report.Brief.HandoffkeepEnv {
		t.Fatal("readiness offers brief documents from a legacy-only default file")
	}

	result := runDefaultNodeBriefSpawn(t, home)
	if result.RC == 0 || result.Error != "brief_doc_unavailable" {
		t.Fatalf("spawn on a legacy-only default file: %+v", result)
	}
	if got := hk.requests(); len(got) != 0 {
		t.Fatalf("hk was called with the legacy key: %q", got)
	}
}

// The default follows hkcred.Load's whole order: the environment pair wins
// over (and works without) the file.
func TestHKSvcNodeDefaultUsesTheEnvironmentPair(t *testing.T) {
	home := isolateRootSvcCred(t)
	hk, url := newSvcNodeHK(t)
	writeRootSvcCredFile(t, filepath.Join(home, ".config", "handoffkeep", "config.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN=cli-config-3570\n")
	t.Setenv(hkcred.URLKey, url)
	t.Setenv(hkcred.TokenKey, svcCredRootToken)
	t.Setenv("HANDOFFKEEP_TOKEN", svcCredRootLegacy)

	env, err := defaultNodeClient().nodeHandoffkeepEnv()
	if err != nil || env.Token != svcCredRootToken || env.URL != url {
		t.Fatalf("node default ignored the environment pair: token is svc=%t err=%v", env.Token == svcCredRootToken, err)
	}

	deps := readinessFakes(nil)
	deps.envReadable = nil
	client := defaultNodeClient()
	client.readinessDeps = deps
	if report := client.collectReadiness(t.Context()); !report.Brief.HandoffkeepEnv {
		t.Fatal("readiness hides brief documents from a node configured by environment")
	}

	result := runDefaultNodeBriefSpawn(t, home)
	if result.RC != 0 {
		t.Fatalf("spawn with the environment pair: %+v", result)
	}
	if got := hk.requests(); len(got) != 1 || got[0] != "/v1/documents/briefs/job-3570/brief.md Bearer "+svcCredRootToken {
		t.Fatalf("hk saw %q, want one svc-token brief fetch", got)
	}
}

// An explicit spawn.json handoffkeep_env keeps the legacy fallback — even
// when it names the very path the default would use.
func TestHKSvcExplicitSpawnJSONFileAcceptsLegacyKey(t *testing.T) {
	home := isolateRootSvcCred(t)
	_, url := newSvcNodeHK(t)
	defaultPath := filepath.Join(home, ".config", "panewire", "hk-svc.env")
	elsewhere := filepath.Join(t.TempDir(), "node-hk.env")
	for _, path := range []string{elsewhere, defaultPath} {
		writeRootSvcCredFile(t, path, "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN="+svcCredRootLegacy+"\n")
		spawnJSON := filepath.Join(t.TempDir(), "spawn.json")
		if err := os.WriteFile(spawnJSON, []byte(`{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true,"handoffkeep_env":"`+path+`"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		client := &HubClient{spawnConfigPath: spawnJSON}
		env, err := client.nodeHandoffkeepEnv()
		if err != nil || env.Token != svcCredRootLegacy {
			t.Fatalf("explicit file %s refused its legacy key: err=%v", filepath.Base(path), err)
		}
	}
}

// fsRunNode is the shared node fixture: a spawn.json at the HOME the test
// process was handed (a developer's real one, say) is never read through it.
func TestFsRunNodeNeverReadsTheLaunchHomeSpawnConfig(t *testing.T) {
	launchHome := t.TempDir()
	planted := filepath.Join(launchHome, ".config", "panewire", "spawn.json")
	if err := os.MkdirAll(filepath.Dir(planted), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planted, []byte(`{"cwd_map":{"planted":"/planted"},"workspace":"planted-workspace","herdr_session":"planted","enabled":true,"handoffkeep_env":"/planted/hk.env"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", launchHome)

	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	var client *HubClient
	fsRunNode(t, server.URL, func(c *HubClient) { client = c })
	if strings.HasPrefix(client.spawnConfigPath, launchHome+string(filepath.Separator)) {
		t.Fatalf("fixture node reads spawn.json under the launch HOME: %s", client.spawnConfigPath)
	}
	config, _, err := client.resolveHubSpawnConfig()
	if err == nil && (config.Workspace == "planted-workspace" || strings.HasPrefix(config.HandoffkeepEnv, "/planted")) {
		t.Fatalf("fixture node read the planted spawn.json: %+v", config)
	}
}
