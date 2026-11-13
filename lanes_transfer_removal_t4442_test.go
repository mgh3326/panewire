package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestControlPlaneTransferRouteIsRetiredT4442 pins acceptance item 2 of hk 4442:
// the transfer endpoint is gone, so every method and credential gets 404.
func TestControlPlaneTransferRouteIsRetiredT4442(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha")
	for _, token := range []string{controlPlaneOperatorToken, ""} {
		writer := lanesWriteRequest(t, hub, http.MethodPost, "/v1/control-plane/transfer", token, `{}`)
		if writer.Code != http.StatusNotFound {
			t.Fatalf("transfer with token=%t status=%d body=%s, want 404", token != "", writer.Code, writer.Body.String())
		}
	}
}

// TestLanesAddRmKeepsControlBlockT4442 pins acceptance item 2 of hk 4442:
// `panewire lanes add` and `rm` still work on a hub whose lanes file carries a
// control block, the block survives both writes, and a configured authority
// lane is still refused with the lanes-file hint rather than a retired route.
func TestLanesAddRmKeepsControlBlockT4442(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	const control = `{"epoch":7,"owner":"machine-a","state":"ACTIVE"}`
	if err := os.WriteFile(path, []byte(`{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"}},"control":`+control+`}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub := controlPlaneHub(t, path, "lane-authority")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	envPath := controlPlaneSelfCheckEnv(t)
	cli := func(args ...string) (int, string) {
		t.Helper()
		var stdout, stderr strings.Builder
		code := runLanesCLI(append(args, "--hub-url", server.URL, "--hub-token-env", envPath), &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
		return code, stderr.String()
	}

	if code, stderr := cli("add", "lane-beta", "--machine", "machine-a", "--pane", "w1:p2"); code != ExitOK {
		t.Fatalf("lanes add code=%d stderr=%q", code, stderr)
	}
	if code, stderr := cli("rm", "lane-beta"); code != ExitOK {
		t.Fatalf("lanes rm code=%d stderr=%q", code, stderr)
	}

	var source struct {
		Lanes   map[string]reportRelayRoute `json:"lanes"`
		Control lanesFileControl            `json:"control"`
	}
	if err := json.Unmarshal(mustReadControlPlaneFile(t, path), &source); err != nil {
		t.Fatal(err)
	}
	if _, present := source.Lanes["lane-beta"]; present {
		t.Fatal("lanes rm left lane-beta in the lanes file")
	}
	if _, present := source.Lanes["lane-alpha"]; !present {
		t.Fatal("lanes add/rm dropped an unrelated lane")
	}
	if source.Control.Epoch != 7 || source.Control.Owner != "machine-a" || source.Control.State != controlPlaneStateActive {
		t.Fatalf("control block changed across lanes add/rm: %+v", source.Control)
	}

	code, stderr := cli("add", "lane-authority", "--machine", "machine-a", "--pane", "w1:p3")
	if code == ExitOK {
		t.Fatal("a configured authority lane was written through the lanes API")
	}
	if !strings.Contains(stderr, "authority_lane_direct_write") || !strings.Contains(stderr, "edit the lanes file on the hub host") {
		t.Fatalf("authority refusal lacks the lanes-file hint: %q", stderr)
	}
	if strings.Contains(stderr, "control-plane") {
		t.Fatalf("authority refusal still names a retired route: %q", stderr)
	}
}
