package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcceptingContainment(t *testing.T) {
	state := "open"
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tasks/12" {
			http.NotFound(w, r)
			return
		}
		if state == "unreachable" {
			http.Error(w, "unavailable", 503)
			return
		}
		fmt.Fprintf(w, `{"id":12,"state":%q}`, state)
	}))
	defer hk.Close()
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: hk.URL, Token: "test-token"}, hk.Client())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "overrides.json")
	if err := os.WriteFile(path, []byte(`{"overrides":{"host-a":"off"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	config := HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node"}, AcceptingOverridesPath: path, handoffkeep: client}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	if h.acceptingOverrideLocked("host-a") != "off" {
		t.Fatal("legacy override not loaded")
	}
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", accepting: true, remoteMeta: map[string]string{}}
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/nodes/host-a/accepting", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer op")
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, req)
		return w
	}
	for _, value := range []string{"", "hk:0", "hk:01", "hk:1234567890", "hk:12x"} {
		w := post(fmt.Sprintf(`{"mode":"off","containment":%q}`, value))
		if w.Code != 400 || !strings.Contains(w.Body.String(), "containment_required") {
			t.Fatalf("invalid %q: %d %s", value, w.Code, w.Body.String())
		}
	}
	w := post(`{"mode":"off","containment":"hk:12"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"containment_verified":true`) {
		t.Fatalf("open: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	req.Header.Set("Authorization", "Bearer op")
	nodes := httptest.NewRecorder()
	h.Handler().ServeHTTP(nodes, req)
	var data struct {
		Nodes []HubNode `json:"nodes"`
	}
	if err := json.Unmarshal(nodes.Body.Bytes(), &data); err != nil || len(data.Nodes) != 1 || data.Nodes[0].Containment != "hk:12" || !data.Nodes[0].ContainmentVerified {
		t.Fatalf("nodes: %d %s %v", nodes.Code, nodes.Body.String(), err)
	}
	restarted, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.r19a.acceptingOverride["host-a"]; got.Containment != "hk:12" || !got.ContainmentVerified {
		t.Fatalf("restart: %+v", got)
	}
	var out bytes.Buffer
	renderHubStatus(&out, data.Nodes)
	if !strings.Contains(out.String(), "hk:12") {
		t.Fatalf("CLI: %s", out.String())
	}
	for _, closed := range []string{"merged", "deployed", "dropped", "duplicate"} {
		state = closed
		w = post(`{"mode":"off","containment":"hk:12"}`)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "containment_closed") {
			t.Fatalf("%s: %d %s", closed, w.Code, w.Body.String())
		}
	}
	state = "unreachable"
	w = post(`{"mode":"off","containment":"hk:12"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"containment_verified":false`) {
		t.Fatalf("unreachable: %d %s", w.Code, w.Body.String())
	}
	restarted, err = NewHubServer(config)
	if err != nil || restarted.r19a.acceptingOverride["host-a"].ContainmentVerified {
		t.Fatalf("unverified restart: %v", err)
	}
	w = post(`{"mode":"on"}`)
	if w.Code != 200 || h.r19a.acceptingOverride["host-a"].Containment != "" {
		t.Fatalf("on: %d %s", w.Code, w.Body.String())
	}
	w = post(`{"mode":"auto"}`)
	if w.Code != 200 {
		t.Fatalf("auto: %d %s", w.Code, w.Body.String())
	}
}
