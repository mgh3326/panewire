package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHubUIAccessAndDataSchema(t *testing.T) {
	now := time.Date(2026, 9, 2, 5, 0, 0, 0, time.UTC)
	policyPath := filepath.Join(t.TempDir(), "burst.json")
	policy := BurstPolicy{SourceMachine: "node-a", SwapGB: 8, Load5: 6, Consecutive: 3, WakeVia: "node-a", WakeMAC: "02:1a:2b:3c:4d:5e", TargetMachine: "node-b", IdleMinutes: 30, CooldownMinutes: 20}
	if err := os.WriteFile(policyPath, []byte(formatBurstPolicy(policy)), 0600); err != nil {
		t.Fatal(err)
	}
	operatorSecret := "r13-operator-secret-must-never-reach-browser"
	nodeSecret := "r13-node-secret-must-never-reach-browser"
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": operatorSecret, "node-a": nodeSecret, "node-b": "r13-node-b-secret"},
		Now:    func() time.Time { return now }, BurstPolicyPath: policyPath, UIAllowCFOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.connect("node-a", "r13-test", "127.0.0.1:4567", nil, true)
	hub.recordUIEvent("burst", "up", "node-b", now)

	// This is the authorization mutant: removing authorizeUI from either
	// handler changes this assertion from 404 to 200.
	denied := httptest.NewRequest(http.MethodGet, "/ui", nil)
	denied.RemoteAddr = "198.51.100.25:4444"
	deniedResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(deniedResponse, denied)
	if deniedResponse.Code != http.StatusNotFound {
		t.Fatalf("non-CF non-loopback UI status=%d, want 404", deniedResponse.Code)
	}
	deniedData := httptest.NewRequest(http.MethodGet, "/ui/data.json", nil)
	deniedData.RemoteAddr = "198.51.100.25:4444"
	deniedDataResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(deniedDataResponse, deniedData)
	if deniedDataResponse.Code != http.StatusNotFound {
		t.Fatalf("non-CF non-loopback UI data status=%d, want 404", deniedDataResponse.Code)
	}
	loopback := httptest.NewRequest(http.MethodGet, "/ui", nil)
	loopback.RemoteAddr = "127.0.0.1:4444"
	loopbackResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(loopbackResponse, loopback)
	if loopbackResponse.Code != http.StatusOK {
		t.Fatalf("loopback UI status=%d, want 200", loopbackResponse.Code)
	}
	// This tunnel-shaped request must not inherit loopback access. Removing the
	// Cf-Ray identity check makes this authorization mutant return 200.
	cloudflareWithoutIdentity := httptest.NewRequest(http.MethodGet, "/ui", nil)
	cloudflareWithoutIdentity.RemoteAddr = "127.0.0.1:4444"
	cloudflareWithoutIdentity.Header.Set("Cf-Ray", "fixture-ray")
	cloudflareWithoutIdentityResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(cloudflareWithoutIdentityResponse, cloudflareWithoutIdentity)
	if cloudflareWithoutIdentityResponse.Code != http.StatusNotFound {
		t.Fatalf("Cloudflare request without Access identity status=%d, want 404", cloudflareWithoutIdentityResponse.Code)
	}

	page := httptest.NewRequest(http.MethodGet, "/ui", nil)
	page.RemoteAddr = "198.51.100.25:4444"
	page.Header.Set("Cf-Ray", "fixture-ray")
	page.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
	pageResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(pageResponse, page)
	if pageResponse.Code != http.StatusOK || !strings.Contains(pageResponse.Body.String(), "<th>Machine</th>") {
		t.Fatalf("CF UI page status=%d body=%q", pageResponse.Code, pageResponse.Body.String())
	}
	for _, secret := range []string{operatorSecret, nodeSecret, "HUB_TOKEN_operator=" + operatorSecret} {
		if strings.Contains(pageResponse.Body.String(), secret) {
			t.Fatal("UI HTML exposed auth-file secret")
		}
	}

	dataRequest := httptest.NewRequest(http.MethodGet, "/ui/data.json", nil)
	dataRequest.RemoteAddr = "198.51.100.25:4444"
	dataRequest.Header.Set("Cf-Ray", "fixture-ray")
	dataRequest.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
	dataResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(dataResponse, dataRequest)
	if dataResponse.Code != http.StatusOK {
		t.Fatalf("CF UI data status=%d body=%q", dataResponse.Code, dataResponse.Body.String())
	}
	for _, secret := range []string{operatorSecret, nodeSecret, "HUB_TOKEN_operator=" + operatorSecret} {
		if strings.Contains(dataResponse.Body.String(), secret) {
			t.Fatal("UI data exposed auth-file secret")
		}
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(dataResponse.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) != 5 || raw["schema_version"] == nil || raw["hub"] == nil || raw["nodes"] == nil || raw["burst"] == nil || raw["events"] == nil {
		t.Fatalf("unexpected UI schema keys: %v", raw)
	}
	var data hubUIData
	if err := json.Unmarshal(dataResponse.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data.SchemaVersion != 1 || data.Hub.Version != hubVersion || len(data.Nodes) != 1 || data.Nodes[0].MachineID != "node-a" || data.Nodes[0].Version != "r13-test" || data.Burst.Policy == nil || data.Burst.Policy.TargetMachine != "node-b" {
		t.Fatalf("unexpected UI data: %+v", data)
	}
	if len(data.Events) != 2 || data.Events[0].Kind != "presence" || data.Events[1].Kind != "burst" || data.Events[1].Phase != "up" || data.Events[1].MachineID != "node-b" {
		t.Fatalf("UI events are not closed display records: %+v", data.Events)
	}
}

func TestHubUIRequiresExplicitCLIOptIn(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator=r13-cli-operator\nHUB_TOKEN_node-a=r13-cli-node\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--ui-allow-cf-only"}, nil)
	if err != nil || code != ExitOK || !hub.uiAllowCFOnly {
		t.Fatalf("UI CLI opt-in rejected: hub=%v code=%d err=%v", hub, code, err)
	}
}
