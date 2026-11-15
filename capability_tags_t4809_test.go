package panewire

// hk 4809 (decision 308): a host's capabilities are a configured value, and the
// hub's own hosts config is the one place they are declared. A submitter on
// another machine sends --tags and the hub's placement picks the host that
// declares them; the submitter's config plays no part, so nothing is copied
// per placing host. blender is a capability name; no licence is looked up.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// capabilitySource reports the configured hosts' login evidence. The hub's own
// placement reads caps from the config; account evidence is what lets it admit
// a seat on the host that declares them.
type capabilitySource struct{ retrySource }

func (s capabilitySource) Presence(ctx context.Context) ([]fleet.PresenceRecord, error) {
	records, err := s.retrySource.Presence(ctx)
	for _, host := range []string{"plain", "unity-box", "blender-box"} {
		records = append(records, fleet.PresenceRecord{HostID: host, AccountKey: "p:0000000000000001", LastSeen: s.now})
	}
	return records, err
}

// capabilityHosts is the hub process's hosts.toml: every host's declaration,
// once. The submitter below is machine-a, which is not in this file.
const capabilityHosts = `[local]
machine_id = "hub-host"
spawn = false

[hosts.plain]
tags = ["linux"]

[hosts.unity-box]
caps = ["unity"]

[hosts.blender-box]
caps = ["blender"]
`

func capabilityHub(t *testing.T) (*HubServer, *planJobs) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(capabilityHosts), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	planHK(t)
	jobs := &planJobs{}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		cfg, err := fleet.LoadConfig(path)
		if err != nil {
			return nil, err
		}
		app := retryDispatchApp(t, out, errout)
		app.Config = cfg
		app.Source = capabilitySource{app.Source.(retrySource)}
		app.JobStore = jobs
		return app, nil
	}
	return h, jobs
}

// planTags asks the hub to plan durableSpawnRequest with tags added. The
// request comes from machine-a, which no host table names.
func planTags(t *testing.T, h *HubServer, tags string) *httptest.ResponseRecorder {
	t.Helper()
	request := durableSpawnRequest()
	request.OriginMachine = "machine-a"
	request.Args = append(append([]string{}, request.Args...), "--tags", tags)
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/spawns/plan", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	return w
}

func TestHubPlacementReadsCapabilitiesFromTheHubConfig_t4809(t *testing.T) {
	h, _ := capabilityHub(t)
	for tags, want := range map[string]string{
		"unity":   "unity-box",
		"blender": "blender-box",
	} {
		t.Run(tags, func(t *testing.T) {
			w := planTags(t, h, tags)
			if w.Code != http.StatusOK {
				t.Fatalf("plan --tags %s: %d %s", tags, w.Code, w.Body)
			}
			var plan fleet.SpawnPlan
			if err := json.Unmarshal(w.Body.Bytes(), &plan); err != nil {
				t.Fatalf("plan body=%s err=%v", w.Body, err)
			}
			if plan.Host != want {
				t.Fatalf("plan --tags %s chose %q, want %q", tags, plan.Host, want)
			}
		})
	}
}

func TestHubPlacementRefusalNamesTheUndeclaredTag_t4809(t *testing.T) {
	h, _ := capabilityHub(t)
	w := planTags(t, h, "xcode")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("plan --tags xcode=%d %s, want 400", w.Code, w.Body)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["reason"] != "no_eligible_host" || !strings.Contains(body["fix"], "no host declares cap xcode") {
		t.Fatalf("refusal=%v, want no_eligible_host naming cap xcode", body)
	}
}
