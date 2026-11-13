package panewire

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// hk 3570: the node's default hk credential is panewire's own svc-panewire
// file, never the hk CLI's config.env; an explicit file (hub flag, spawn.json
// handoffkeep_env) accepts HANDOFFKEEP_TOKEN_SVC_PANEWIRE
// first and keeps HANDOFFKEEP_TOKEN as its fallback.

const (
	svcCredRootToken  = "svc-panewire-root-3570"
	svcCredRootLegacy = "legacy-root-3570"
)

func isolateRootSvcCred(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, key := range []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN", "HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "HANDOFFKEEP_CONFIG", "PANEWIRE_HK_ENV"} {
		t.Setenv(key, "")
	}
	return home
}

func writeRootSvcCredFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestHKSvcNodeDefaultPathIsPanewireFile(t *testing.T) {
	home := isolateRootSvcCred(t)
	client := &HubClient{spawnConfigPath: defaultHubSpawnConfigPath(""), spawnConfigDefault: true, readinessDeps: readinessFakes(nil)}
	config, defaults, err := client.resolveHubSpawnConfig()
	if err != nil || !defaults {
		t.Fatalf("defaults=%t err=%v", defaults, err)
	}
	if want := filepath.Join(home, ".config", "panewire", "hk-svc.env"); config.HandoffkeepEnv != want {
		t.Fatalf("node default handoffkeep_env=%q, want %q", config.HandoffkeepEnv, want)
	}

	override := filepath.Join(t.TempDir(), "node-hk.env")
	t.Setenv("PANEWIRE_HK_ENV", override)
	config, _, err = client.resolveHubSpawnConfig()
	if err != nil || config.HandoffkeepEnv != override {
		t.Fatalf("PANEWIRE_HK_ENV ignored: handoffkeep_env=%q err=%v", config.HandoffkeepEnv, err)
	}
}

// The node default reaches hk with the svc token even when the hk CLI's
// config.env sits next to it with another token.
func TestHKSvcNodeDefaultBriefFetchSendsSvcToken(t *testing.T) {
	home := isolateRootSvcCred(t)
	const key = "briefs/job-3570/brief.md"
	var mu sync.Mutex
	var bearers []string
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		bearers = append(bearers, r.URL.EscapedPath()+" "+r.Header.Get("Authorization"))
		mu.Unlock()
		_, _ = io.WriteString(w, `{"key":"`+key+`","body":"brief text"}`)
	}))
	defer hk.Close()
	writeRootSvcCredFile(t, filepath.Join(home, ".config", "handoffkeep", "config.env"), "HANDOFFKEEP_URL="+hk.URL+"\nHANDOFFKEEP_TOKEN="+svcCredRootLegacy+"\n")
	writeRootSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+hk.URL+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE="+svcCredRootToken+"\n")

	client := &HubClient{spawnConfigPath: defaultHubSpawnConfigPath(""), spawnConfigDefault: true, readinessDeps: readinessFakes(nil)}
	config, _, err := client.resolveHubSpawnConfig()
	if err != nil {
		t.Fatal(err)
	}
	text, err := client.fetchNodeSpawnBrief(t.Context(), config, key)
	if err != nil || text != "brief text" {
		t.Fatalf("brief fetch: text=%q err=%v", text, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bearers) != 1 || bearers[0] != "/v1/documents/"+key+" Bearer "+svcCredRootToken {
		t.Fatalf("hk saw %q, want one svc-token request to the document route", bearers)
	}
}

// An explicit file (hub --handoffkeep-env/--chat-env, spawn.json
// handoffkeep_env) prefers the svc key and still accepts today's files.
func TestHKSvcExplicitFileAcceptsServiceKeyFirst(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name, body, want string
	}{
		{"svc only", "HANDOFFKEEP_TOKEN_SVC_PANEWIRE=" + svcCredRootToken + "\n", svcCredRootToken},
		{"svc wins over legacy", "HANDOFFKEEP_TOKEN=" + svcCredRootLegacy + "\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE=" + svcCredRootToken + "\n", svcCredRootToken},
		{"legacy fallback", "HANDOFFKEEP_TOKEN=" + svcCredRootLegacy + "\n", svcCredRootLegacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "-")+".env")
			writeRootSvcCredFile(t, path, "HANDOFFKEEP_URL=https://hk.example\n"+tc.body)
			env, err := loadHubHandoffkeepEnv(path)
			if err != nil {
				t.Fatalf("explicit file refused: %v", err)
			}
			request, _ := http.NewRequest(http.MethodGet, "https://hk.example/v1/relay/events", nil)
			env.setAuth(request)
			if got := request.Header.Get("Authorization"); got != "Bearer "+tc.want {
				t.Fatalf("Authorization=%q, want Bearer %s", got, tc.want)
			}
		})
	}
}
