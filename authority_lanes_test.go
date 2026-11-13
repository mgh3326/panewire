package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	controlPlaneOperatorToken = "fixture-operator-token"
	controlPlaneMachineAToken = "fixture-a-token"
	controlPlaneMachineBToken = "fixture-b-token"
)

func controlPlaneHub(t *testing.T, path string, authority ...string) *HubServer {
	t.Helper()
	return controlPlaneHubWithConfig(t, HubServerConfig{ReportRelayPath: path, ControlPlaneLanes: authority})
}

func controlPlaneHubWithPolicy(t *testing.T, path, policyPath string) *HubServer {
	t.Helper()
	return controlPlaneHubWithConfig(t, HubServerConfig{ReportRelayPath: path, ControlPlaneLanesPath: policyPath})
}

func controlPlaneHubWithConfig(t *testing.T, config HubServerConfig) *HubServer {
	t.Helper()
	config.Tokens = map[string]string{
		hubOperatorMachineID: controlPlaneOperatorToken,
		"machine-a":          controlPlaneMachineAToken,
		"machine-b":          controlPlaneMachineBToken,
	}
	hub, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func controlPlaneRoutesFixture(t *testing.T, path string, extra string) {
	t.Helper()
	contents := `{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"},"lane-beta":{"machine":"machine-a","pane":"w1:p2"}` + extra + `}}`
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func controlPlaneLanesPolicyFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	// A deterministic modification time keeps the hot-reload comparison from
	// depending on filesystem timestamp granularity.
	stamp := time.Now().Add(time.Duration(controlPlanePolicyWrites(path)) * time.Second)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

var controlPlanePolicyWriteCounts sync.Map

func controlPlanePolicyWrites(path string) int {
	value, _ := controlPlanePolicyWriteCounts.LoadOrStore(path, new(int))
	counter := value.(*int)
	*counter++
	return *counter
}

func controlPlaneGetLanes(t *testing.T, hub *HubServer) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/lanes", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+controlPlaneOperatorToken)
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func controlPlaneJSONError(t *testing.T, writer *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var response map[string]string
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("body=%q err=%v", writer.Body.String(), err)
	}
	return response
}

func mustReadControlPlaneFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func controlPlaneEpoch(t *testing.T, hub *HubServer) uint64 {
	t.Helper()
	var envelope struct {
		ControlEpoch uint64 `json:"control_epoch"`
	}
	if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.ControlEpoch
}

// TestControlPlaneW11LanesFileWithoutControlStillWorks is the compatibility
// case that matters most: every lanes file in service today has no control
// block at all.
func TestControlPlaneW11LanesFileWithoutControlStillWorks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	hub := controlPlaneHub(t, path)
	if epoch := controlPlaneEpoch(t, hub); epoch != 0 {
		t.Fatalf("a lanes file without control must read as epoch 0, got %d", epoch)
	}
	put := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
	if put.Code != http.StatusOK {
		t.Fatalf("PUT on a control-less file status=%d body=%s", put.Code, put.Body.String())
	}
	if bytes.Contains(mustReadControlPlaneFile(t, path), []byte(`"control"`)) {
		t.Fatalf("an ordinary write invented a control block: %s", mustReadControlPlaneFile(t, path))
	}
}

func controlPlaneSelfCheckEnv(t *testing.T) string {
	t.Helper()
	envPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID="+hubOperatorMachineID+"\nHUB_TOKEN="+controlPlaneOperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return envPath
}

func TestControlPlaneW16SelfCheckWitnessAndExitCodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	envPath := controlPlaneSelfCheckEnv(t)
	before := mustReadControlPlaneFile(t, path)
	selfCheck := func(lane, machine, pane, epoch string) (int, string) {
		t.Helper()
		args := []string{"self-check", "--lane", lane, "--expect-machine", machine, "--expect-pane", pane, "--expect-epoch", epoch, "--hub-url", server.URL, "--hub-token-env", envPath}
		var stdout, stderr strings.Builder
		code := runLanesCLI(args, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
		return code, stdout.String()
	}

	code, out := selfCheck("lane-alpha", "machine-a", "w1:p1", "0")
	if code != ExitOK {
		t.Fatalf("owner code=%d stdout=%q", code, out)
	}
	var witness controlPlaneSelfCheckWitness
	if err := json.Unmarshal([]byte(out), &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Kind != "control_plane.self_check" || witness.Verdict != "owner" || witness.Action != "proceed" {
		t.Fatalf("witness=%+v", witness)
	}
	if witness.Lane != "lane-alpha" || witness.Expected != (controlPlaneSelfCheckExpected{Machine: "machine-a", Pane: "w1:p1", Epoch: 0}) {
		t.Fatalf("witness expectation=%+v", witness)
	}
	if witness.Observed.Machine != "machine-a" || witness.Observed.Pane != "w1:p1" || witness.Observed.Epoch != 0 || witness.At.IsZero() {
		t.Fatalf("witness observation=%+v", witness.Observed)
	}

	// A different machine is an observed loss of ownership: exit 3, self_stop.
	code, out = selfCheck("lane-alpha", "machine-b", "w2:p1", "0")
	if code != ExitTimeout {
		t.Fatalf("not-owner code=%d stdout=%q", code, out)
	}
	if err := json.Unmarshal([]byte(out), &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Verdict != "not_owner" || witness.Action != "self_stop" {
		t.Fatalf("not-owner witness=%+v", witness)
	}

	// So is a stale epoch on an otherwise matching route.
	code, out = selfCheck("lane-alpha", "machine-a", "w1:p1", "7")
	if code != ExitTimeout || !strings.Contains(out, `"verdict":"not_owner"`) {
		t.Fatalf("stale epoch code=%d stdout=%q", code, out)
	}

	// A lane the hub does not project cannot be confirmed either way. It never
	// exits 0, and its witness still records self_stop.
	code, out = selfCheck("lane-missing", "machine-a", "w1:p1", "0")
	if code != ExitConditionInvalid {
		t.Fatalf("unknown code=%d stdout=%q", code, out)
	}
	if err := json.Unmarshal([]byte(out), &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Verdict != "unknown" || witness.Action != "self_stop" {
		t.Fatalf("unknown witness=%+v", witness)
	}

	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("self-check wrote something: it is a read-only observation")
	}
}

func TestControlPlaneW17DocsContract(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("docs", "authority-lanes.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, sentence := range []string{
		"이것은 강제가 아니라 1회 관측이며, 확인을 건너뛴 세션은 여전히 행동할 수 있다.",
		"self-check 통과를 '차단됨'으로 쓰지 말 것",
		"hub API 로 권위 레인의 route 를 바꾸는 경로는 없다.",
	} {
		if !strings.Contains(string(contents), sentence) {
			t.Fatalf("docs are missing the required sentence: %q", sentence)
		}
	}
}

func TestControlPlaneW21AuthorityPutRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	before := mustReadControlPlaneFile(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "authority_lane_direct_write" {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("refused authority PUT changed the lanes file")
	}
}

func TestControlPlaneW22AuthorityDeleteRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	before := mustReadControlPlaneFile(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-beta", controlPlaneOperatorToken, "")
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "authority_lane_direct_write" {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("refused authority DELETE changed the lanes file")
	}
}

// TestControlPlaneW22AuthorityRefusalOutranksAMalformedFile keeps the refusal
// answerable from configuration alone. A hub that reported lanes_invalid here
// would send an operator looking at the file, which is the one path this API
// cannot take back.
func TestControlPlaneW22AuthorityRefusalOutranksAMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(path, []byte(`{"lanes":{"lane-alpha":`), 0600); err != nil {
		t.Fatal(err)
	}
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	for _, test := range []struct {
		method string
		body   string
	}{
		{http.MethodPut, `{"machine":"machine-b","pane":"w2:p1"}`},
		{http.MethodDelete, ""},
	} {
		writer := lanesWriteRequest(t, hub, test.method, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, test.body)
		if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "authority_lane_direct_write" {
			t.Fatalf("%s status=%d body=%s", test.method, writer.Code, writer.Body.String())
		}
	}
}

func TestControlPlaneW24GeneralLanePutDeleteRemainUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	created := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-new", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p5"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("new general lane status=%d body=%s", created.Code, created.Body.String())
	}
	updated := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
	if updated.Code != http.StatusOK {
		t.Fatalf("general PUT status=%d body=%s", updated.Code, updated.Body.String())
	}
	// The ordinary builder-lane removal that happened in practice stays an
	// ordinary removal under this rule.
	removed := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-general", controlPlaneOperatorToken, "")
	if removed.Code != http.StatusOK || removed.Body.String() != `{"lane":"lane-general","removed":true}`+"\n" {
		t.Fatalf("general DELETE status=%d body=%q", removed.Code, removed.Body.String())
	}
}

func TestControlPlaneW26UnconfiguredAuthoritySetRejectsNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path)
	if hub.controlPlaneLanesStatus != "default" || !hub.controlPlaneLanesLoaded {
		t.Fatalf("status=%q loaded=%t", hub.controlPlaneLanesStatus, hub.controlPlaneLanesLoaded)
	}
	put := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
	if put.Code != http.StatusOK {
		t.Fatalf("unconfigured PUT status=%d body=%s", put.Code, put.Body.String())
	}
	remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-beta", controlPlaneOperatorToken, "")
	if remove.Code != http.StatusOK {
		t.Fatalf("unconfigured DELETE status=%d body=%s", remove.Code, remove.Body.String())
	}
}

func TestControlPlaneW27FlagDefaultsToUnconfigured(t *testing.T) {
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_"+hubOperatorMachineID+"="+controlPlaneOperatorToken+"\nHUB_TOKEN_machine-a="+controlPlaneMachineAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	lanesPath := filepath.Join(root, "lanes.json")
	controlPlaneRoutesFixture(t, lanesPath, "")
	policyPath := filepath.Join(root, "control-plane-lanes.json")
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-beta"]}`)

	// Omitted: protection stays off, which is what every deployment that has
	// not opted in still gets.
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--lanes", lanesPath}, nil)
	if err != nil || code != ExitOK || hub == nil {
		t.Fatalf("default hub: code=%d err=%v", code, err)
	}
	if hub.controlPlaneLanesPath != "" || hub.controlPlaneLanesStatus != "default" {
		t.Fatalf("default path=%q status=%q", hub.controlPlaneLanesPath, hub.controlPlaneLanesStatus)
	}
	if writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p9"}`); writer.Code != http.StatusOK {
		t.Fatalf("default PUT status=%d body=%s", writer.Code, writer.Body.String())
	}

	// Given the flag, the operator's file is what decides.
	controlPlaneRoutesFixture(t, lanesPath, "")
	hub, _, code, err = newHubServerForCLI([]string{"--hub-auth", authPath, "--lanes", lanesPath, "--control-plane-lanes", policyPath}, nil)
	if err != nil || code != ExitOK || hub == nil {
		t.Fatalf("configured hub: code=%d err=%v", code, err)
	}
	if hub.controlPlaneLanesPath != policyPath || hub.controlPlaneLanesStatus != "current" || !hub.controlPlaneLanesLoaded {
		t.Fatalf("configured path=%q status=%q loaded=%t", hub.controlPlaneLanesPath, hub.controlPlaneLanesStatus, hub.controlPlaneLanesLoaded)
	}
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p9"}`)
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "authority_lane_direct_write" {
		t.Fatalf("configured PUT status=%d body=%s", writer.Code, writer.Body.String())
	}
	contents, err := os.ReadFile(filepath.Join("docs", "authority-lanes.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mention := range []string{"--control-plane-lanes", "/etc/panewire/control-plane-lanes.json", "authority_lanes"} {
		if !strings.Contains(string(contents), mention) {
			t.Fatalf("docs do not document %q", mention)
		}
	}
}

func TestControlPlaneW28LoadedPolicyRejectsOnlyItsLanes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	policyPath := filepath.Join(root, "control-plane-lanes.json")
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-beta"]}`)
	hub := controlPlaneHubWithPolicy(t, path, policyPath)
	if hub.controlPlaneLanesStatus != "current" || !hub.controlPlaneLanesLoaded {
		t.Fatalf("status=%q loaded=%t", hub.controlPlaneLanesStatus, hub.controlPlaneLanesLoaded)
	}
	refused := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-beta", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p2"}`)
	if refused.Code != http.StatusConflict || controlPlaneJSONError(t, refused)["error"] != "authority_lane_direct_write" {
		t.Fatalf("authority PUT status=%d body=%s", refused.Code, refused.Body.String())
	}
	allowed := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
	if allowed.Code != http.StatusOK {
		t.Fatalf("general PUT status=%d body=%s", allowed.Code, allowed.Body.String())
	}
}

// TestControlPlaneW29UnreadablePolicyRefusesEveryLane is the fail-closed case.
// Without the file the hub cannot tell an authority lane from an ordinary one,
// so it refuses both rather than guess. Letting writes through here would be
// the same as having no guard at all.
func TestControlPlaneW29UnreadablePolicyRefusesEveryLane(t *testing.T) {
	for _, test := range []struct {
		name    string
		policy  string
		absent  bool
		failure string
	}{
		{name: "absent", absent: true, failure: "unreadable"},
		{name: "malformed", policy: `{"authority_lanes":`, failure: "invalid"},
		{name: "wrong key", policy: `{"lanes":["lane-alpha"]}`, failure: "invalid"},
		{name: "invalid lane name", policy: `{"authority_lanes":["LANE-ALPHA"]}`, failure: "invalid"},
		{name: "duplicate lane", policy: `{"authority_lanes":["lane-alpha","lane-alpha"]}`, failure: "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "lanes.json")
			controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
			policyPath := filepath.Join(root, "control-plane-lanes.json")
			if !test.absent {
				controlPlaneLanesPolicyFixture(t, policyPath, test.policy)
			}
			hub := controlPlaneHubWithPolicy(t, path, policyPath)
			if hub.controlPlaneLanesLoaded || hub.controlPlaneLanesStatus != "invalid" {
				t.Fatalf("loaded=%t status=%q", hub.controlPlaneLanesLoaded, hub.controlPlaneLanesStatus)
			}
			before := mustReadControlPlaneFile(t, path)
			for _, lane := range []string{"lane-alpha", "lane-general"} {
				put := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
				if put.Code != http.StatusConflict {
					t.Fatalf("PUT %s status=%d body=%s", lane, put.Code, put.Body.String())
				}
				fields := controlPlaneJSONError(t, put)
				if fields["error"] != "authority_lane_policy_unavailable" || fields["use"] != "restore the --control-plane-lanes policy file" {
					t.Fatalf("PUT %s body=%s", lane, put.Body.String())
				}
				remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/"+lane, controlPlaneOperatorToken, "")
				if remove.Code != http.StatusConflict || controlPlaneJSONError(t, remove)["error"] != "authority_lane_policy_unavailable" {
					t.Fatalf("DELETE %s status=%d body=%s", lane, remove.Code, remove.Body.String())
				}
			}
			if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
				t.Fatal("a refused write changed the lanes file")
			}
			if hub.controlPlaneLanesLastFailure != test.failure {
				t.Fatalf("failure=%q want %q", hub.controlPlaneLanesLastFailure, test.failure)
			}
		})
	}
}

// TestControlPlaneW30StalePolicyKeepsLastKnownGood separates the two failures:
// a set that was read once still says what has to be protected, so only that
// set is refused and ordinary lanes keep working.
func TestControlPlaneW30StalePolicyKeepsLastKnownGood(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	policyPath := filepath.Join(root, "control-plane-lanes.json")
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-beta"]}`)
	hub := controlPlaneHubWithPolicy(t, path, policyPath)
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":`)

	refused := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
	if refused.Code != http.StatusConflict || controlPlaneJSONError(t, refused)["error"] != "authority_lane_direct_write" {
		t.Fatalf("stale authority PUT status=%d body=%s", refused.Code, refused.Body.String())
	}
	if hub.controlPlaneLanesStatus != "stale" || !hub.controlPlaneLanesLoaded {
		t.Fatalf("status=%q loaded=%t", hub.controlPlaneLanesStatus, hub.controlPlaneLanesLoaded)
	}
	allowed := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
	if allowed.Code != http.StatusOK {
		t.Fatalf("stale general PUT status=%d body=%s", allowed.Code, allowed.Body.String())
	}
}

func TestControlPlaneW31PolicyHotReloads(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	policyPath := filepath.Join(root, "control-plane-lanes.json")
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha"]}`)
	hub := controlPlaneHubWithPolicy(t, path, policyPath)

	if writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`); writer.Code != http.StatusOK {
		t.Fatalf("general PUT before reload status=%d body=%s", writer.Code, writer.Body.String())
	}
	// The operator adds a lane to the bundle without restarting the hub.
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-general"]}`)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p3"}`)
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "authority_lane_direct_write" {
		t.Fatalf("general PUT after reload status=%d body=%s", writer.Code, writer.Body.String())
	}
	if hub.controlPlaneLanesStatus != "current" {
		t.Fatalf("status=%q", hub.controlPlaneLanesStatus)
	}
	// And removing it again releases the lane.
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha"]}`)
	if writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p3"}`); writer.Code != http.StatusOK {
		t.Fatalf("general PUT after release status=%d body=%s", writer.Code, writer.Body.String())
	}
}

func TestControlPlaneLanesPolicyParsing(t *testing.T) {
	lanes, err := ParseControlPlaneLanes([]byte(`{"authority_lanes":["lane-alpha","lane-beta"]}`))
	if err != nil || len(lanes) != 2 {
		t.Fatalf("lanes=%v err=%v", lanes, err)
	}
	empty, err := ParseControlPlaneLanes([]byte(`{"authority_lanes":[]}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("an explicitly empty bundle is a valid choice: lanes=%v err=%v", empty, err)
	}
	many := make([]string, 0, controlPlaneLanesMaxEntries+1)
	for index := 0; index <= controlPlaneLanesMaxEntries; index++ {
		many = append(many, fmt.Sprintf("lane-%d", index))
	}
	encoded, err := json.Marshal(map[string][]string{"authority_lanes": many})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseControlPlaneLanes(encoded); err == nil {
		t.Fatal("an unbounded bundle was accepted")
	}
	for _, invalid := range []string{`{}`, `{"authority_lanes":null}`, `{"authority_lanes":["lane-alpha"],"extra":1}`, `{"authority_lanes":"lane-alpha"}`, `{"authority_lanes":["lane-alpha"]}{}`} {
		if _, err := ParseControlPlaneLanes([]byte(invalid)); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestControlPlaneConfigLanesKeepTheStrictContract(t *testing.T) {
	_, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{hubOperatorMachineID: controlPlaneOperatorToken},
		ReportRelayPath:   filepath.Join(t.TempDir(), "lanes.json"),
		ControlPlaneLanes: []string{"legacy.parent"},
	})
	if err == nil {
		t.Fatal("non-persisted control-plane configuration accepted a dotted lane")
	}
}

// TestControlPlaneControlBlockIsValidatedBeforeAWriteOverwritesIt follows the
// precedent already set for routes: refusing to write is safer than quietly
// serializing a normalized version of something an operator hand-edited.
func TestControlPlaneControlBlockIsValidatedBeforeAWriteOverwritesIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	contents := `{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"},"lane-beta":{"machine":"machine-a","pane":"w1:p2"},"lane-general":{"machine":"machine-a","pane":"w1:p3"}},"control":{"epoch":3,"owner":"machine-a","state":"NOT-A-STATE","history":[]}}`
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
	if writer.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if got := mustReadControlPlaneFile(t, path); string(got) != contents {
		t.Fatalf("the hand-edited file was rewritten: %s", got)
	}
}

// controlPlaneLogCapture collects structured records so a startup line can be
// asserted rather than merely claimed. A line nobody tests disappears in the
// next refactor, which is exactly the failure the visibility condition exists
// to prevent.
type controlPlaneLogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (capture *controlPlaneLogCapture) find(message string) (slog.Record, bool) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	for _, record := range capture.records {
		if record.Message == message {
			return record, true
		}
	}
	return slog.Record{}, false
}

func controlPlaneRecordAttrs(record slog.Record) map[string]string {
	attrs := make(map[string]string, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	return attrs
}

func controlPlaneStartHub(t *testing.T, lanesPath, policyPath string) *controlPlaneLogCapture {
	t.Helper()
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_"+hubOperatorMachineID+"="+controlPlaneOperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--hub-auth", authPath, "--lanes", lanesPath}
	if policyPath != "" {
		args = append(args, "--control-plane-lanes", policyPath)
	}
	capture := &controlPlaneLogCapture{}
	if _, _, code, err := newHubServerForCLI(args, slog.New(capture)); err != nil || code != ExitOK {
		t.Fatalf("hub start code=%d err=%v", code, err)
	}
	return capture
}

func TestControlPlaneW32DisabledProtectionIsLoggedAtStartup(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lanes.json")
	controlPlaneRoutesFixture(t, path, "")

	capture := controlPlaneStartHub(t, path, "")
	record, found := capture.find("authority-lane protection disabled")
	if !found {
		t.Fatal("an unconfigured guard started silently")
	}
	attrs := controlPlaneRecordAttrs(record)
	if attrs["reason"] != "no policy path configured" {
		t.Fatalf("disabled log attrs=%v", attrs)
	}
	if _, unexpected := capture.find("authority-lane protection enabled"); unexpected {
		t.Fatal("an unconfigured guard reported itself enabled")
	}

	// With a policy the disabled line must not appear, and the enabled line
	// carries a count only: lane names never reach the log.
	policyPath := filepath.Join(root, "control-plane-lanes.json")
	controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-beta"]}`)
	enabled := controlPlaneStartHub(t, path, policyPath)
	if _, unexpected := enabled.find("authority-lane protection disabled"); unexpected {
		t.Fatal("a configured guard reported itself disabled")
	}
	record, found = enabled.find("authority-lane protection enabled")
	if !found {
		t.Fatal("a configured guard did not report its state")
	}
	attrs = controlPlaneRecordAttrs(record)
	if attrs["policy_status"] != "current" || attrs["lanes"] != "2" {
		t.Fatalf("enabled log attrs=%v", attrs)
	}
	enabled.mu.Lock()
	defer enabled.mu.Unlock()
	for _, logged := range enabled.records {
		text := logged.Message + fmt.Sprint(controlPlaneRecordAttrs(logged))
		for _, lane := range []string{"lane-alpha", "lane-beta"} {
			if strings.Contains(text, lane) {
				t.Fatalf("a lane name reached the log: %q", text)
			}
		}
	}
}

func TestControlPlaneW33EnvelopeReportsProtectionState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path)
	var envelope struct {
		AuthorityLaneProtection string `json:"authority_lane_protection"`
	}
	if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.AuthorityLaneProtection != "disabled" {
		t.Fatalf("unconfigured protection=%q want disabled", envelope.AuthorityLaneProtection)
	}
}

func TestControlPlaneW34EnvelopeReflectsEveryProtectionState(t *testing.T) {
	protection := func(t *testing.T, hub *HubServer) string {
		t.Helper()
		var envelope struct {
			AuthorityLaneProtection string `json:"authority_lane_protection"`
		}
		if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.AuthorityLaneProtection
	}
	newFixture := func(t *testing.T) (string, string) {
		t.Helper()
		root := t.TempDir()
		path := filepath.Join(root, "lanes.json")
		controlPlaneRoutesFixture(t, path, "")
		return path, filepath.Join(root, "control-plane-lanes.json")
	}

	t.Run("disabled", func(t *testing.T) {
		path, _ := newFixture(t)
		if got := protection(t, controlPlaneHub(t, path)); got != "disabled" {
			t.Fatalf("protection=%q", got)
		}
	})
	t.Run("current", func(t *testing.T) {
		path, policyPath := newFixture(t)
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha"]}`)
		if got := protection(t, controlPlaneHubWithPolicy(t, path, policyPath)); got != "current" {
			t.Fatalf("protection=%q", got)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		path, policyPath := newFixture(t)
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":`)
		if got := protection(t, controlPlaneHubWithPolicy(t, path, policyPath)); got != "invalid" {
			t.Fatalf("protection=%q", got)
		}
	})
	t.Run("stale", func(t *testing.T) {
		path, policyPath := newFixture(t)
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha"]}`)
		hub := controlPlaneHubWithPolicy(t, path, policyPath)
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":`)
		if got := protection(t, hub); got != "stale" {
			t.Fatalf("protection=%q", got)
		}
	})
}

func TestControlPlaneW35DeploymentStepIsDocumented(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("docs", "authority-lanes.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"/etc/panewire/control-plane-lanes.json",
		"--control-plane-lanes",
		"지정하지 않으면 권위 레인 보호는 비활성이다",
	} {
		if !strings.Contains(string(contents), required) {
			t.Fatalf("the deployment step does not mention %q", required)
		}
	}
}

// TestControlPlaneDamagedControlBlockStaysReadable is the read/write split. A
// control block the hub cannot parse must not cost the operator the ability to
// look at the file they just edited, and it must not take the self-check
// witness down with it — the observation hook reads this same projection. The
// write side stays closed: authority lanes are still refused.
func TestControlPlaneDamagedControlBlockStaysReadable(t *testing.T) {
	for _, test := range []struct {
		name    string
		control string
	}{
		{"unknown state", `{"epoch":3,"owner":"machine-a","state":"NOT-A-STATE","history":[]}`},
		{"invalid owner", `{"epoch":3,"owner":"operator","state":"ACTIVE","history":[]}`},
		{"history over retention", `{"epoch":3,"owner":"machine-a","state":"ACTIVE","history":[{},{},{},{},{},{},{},{},{}]}`},
		{"wrong type", `"a string where an object belongs"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "lanes.json")
			contents := `{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"},"lane-beta":{"machine":"machine-a","pane":"w1:p2"},"lane-general":{"machine":"machine-a","pane":"w1:p3"}},"control":` + test.control + `}`
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")

			// (1) The read succeeds.
			response := controlPlaneGetLanes(t, hub)
			if response.Code != http.StatusOK {
				t.Fatalf("a damaged control block broke the read: status=%d body=%s", response.Code, response.Body.String())
			}
			var envelope struct {
				Lanes                   []hubLaneProjection `json:"lanes"`
				ControlEpoch            uint64              `json:"control_epoch"`
				ControlOwner            string              `json:"control_owner"`
				ControlState            string              `json:"control_state"`
				LastRequestID           string              `json:"last_request_id"`
				AuthorityLaneProtection string              `json:"authority_lane_protection"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}

			// (2) And says why it cannot vouch for the control state.
			if envelope.AuthorityLaneProtection != "invalid" {
				t.Fatalf("protection=%q want invalid", envelope.AuthorityLaneProtection)
			}
			// The damaged values are not passed through: safe defaults only.
			if envelope.ControlEpoch != 0 || envelope.ControlOwner != "" || envelope.ControlState != "" || envelope.LastRequestID != "" {
				t.Fatalf("a damaged control block leaked into the envelope: %+v", envelope)
			}

			// (3) Authority lanes are still refused, both verbs.
			before := mustReadControlPlaneFile(t, path)
			for _, lane := range []string{"lane-alpha", "lane-beta"} {
				put := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
				if put.Code != http.StatusConflict || controlPlaneJSONError(t, put)["error"] != "authority_lane_direct_write" {
					t.Fatalf("PUT %s status=%d body=%s", lane, put.Code, put.Body.String())
				}
				remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/"+lane, controlPlaneOperatorToken, "")
				if remove.Code != http.StatusConflict || controlPlaneJSONError(t, remove)["error"] != "authority_lane_direct_write" {
					t.Fatalf("DELETE %s status=%d body=%s", lane, remove.Code, remove.Body.String())
				}
			}
			// And the write path is still strict about overwriting what it did
			// not understand: an ordinary lane write does not rewrite the file.
			general := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p3"}`)
			if general.Code != http.StatusInternalServerError {
				t.Fatalf("a write overwrote a damaged control block: status=%d body=%s", general.Code, general.Body.String())
			}
			if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
				t.Fatalf("the lanes file changed: %s", mustReadControlPlaneFile(t, path))
			}

			// (4) The lanes themselves project normally.
			if len(envelope.Lanes) != 3 {
				t.Fatalf("lanes=%+v", envelope.Lanes)
			}
			want := map[string]hubLaneProjection{
				"lane-alpha":   {Lane: "lane-alpha", Machine: "machine-a", Pane: "w1:p1"},
				"lane-beta":    {Lane: "lane-beta", Machine: "machine-a", Pane: "w1:p2"},
				"lane-general": {Lane: "lane-general", Machine: "machine-a", Pane: "w1:p3"},
			}
			for _, lane := range envelope.Lanes {
				if expected, known := want[lane.Lane]; !known || !reflect.DeepEqual(lane, expected) {
					t.Fatalf("lane=%+v want %+v", lane, want[lane.Lane])
				}
			}

			// (5) The self-check witness survives and says stop. Surviving is
			// the point of F-1 — the observation hook must not die with the
			// thing it observes — but surviving into "proceed" would be worse
			// than dying: the hub has just said it cannot validate this control
			// state, and the route match is against an epoch it substituted.
			server := httptest.NewServer(hub.Handler())
			defer server.Close()
			envPath := controlPlaneSelfCheckEnv(t)
			args := []string{"self-check", "--lane", "lane-alpha", "--expect-machine", "machine-a", "--expect-pane", "w1:p1", "--expect-epoch", "0", "--hub-url", server.URL, "--hub-token-env", envPath}
			var stdout, stderr strings.Builder
			code := runLanesCLI(args, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
			var witness controlPlaneSelfCheckWitness
			if err := json.Unmarshal([]byte(stdout.String()), &witness); err != nil {
				t.Fatalf("self-check died with the control block: code=%d stdout=%q stderr=%q err=%v", code, stdout.String(), stderr.String(), err)
			}
			if witness.Kind != "control_plane.self_check" || witness.Observed.Machine != "machine-a" {
				t.Fatalf("the witness did not record what it read: %+v", witness)
			}
			if code == ExitOK || witness.Verdict == "owner" || witness.Action != "self_stop" {
				t.Fatalf("self-check confirmed ownership from a control state the hub disowned: code=%d witness=%+v", code, witness)
			}
			if code != ExitConditionInvalid || witness.Verdict != "unknown" {
				t.Fatalf("code=%d witness=%+v want unknown/self_stop and exit %d", code, witness, ExitConditionInvalid)
			}
		})
	}
}

// TestControlPlaneG2SelfCheckNeverProceedsOnDisownedState states the rule on
// its own: exit 0 requires a projection the hub is willing to stand behind.
func TestControlPlaneG2SelfCheckNeverProceedsOnDisownedState(t *testing.T) {
	run := func(t *testing.T, hub *HubServer, epoch string) (int, controlPlaneSelfCheckWitness) {
		t.Helper()
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		envPath := controlPlaneSelfCheckEnv(t)
		args := []string{"self-check", "--lane", "lane-alpha", "--expect-machine", "machine-a", "--expect-pane", "w1:p1", "--expect-epoch", epoch, "--hub-url", server.URL, "--hub-token-env", envPath}
		var stdout, stderr strings.Builder
		code := runLanesCLI(args, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
		var witness controlPlaneSelfCheckWitness
		if err := json.Unmarshal([]byte(stdout.String()), &witness); err != nil {
			t.Fatalf("code=%d stdout=%q stderr=%q err=%v", code, stdout.String(), stderr.String(), err)
		}
		return code, witness
	}

	// A hub that cannot validate its control state never yields exit 0, even
	// when every field the check compares happens to line up.
	t.Run("damaged control block", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lanes.json")
		contents := `{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"},"lane-beta":{"machine":"machine-a","pane":"w1:p2"}},"control":{"epoch":9,"owner":"machine-a","state":"NOT-A-STATE","history":[]}}`
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
		code, witness := run(t, hub, "0")
		if code != ExitConditionInvalid || witness.Verdict != "unknown" || witness.Action != "self_stop" {
			t.Fatalf("code=%d witness=%+v", code, witness)
		}
	})

	// An unloaded policy is the same answer: the hub is reporting invalid.
	t.Run("unloaded policy", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "lanes.json")
		controlPlaneRoutesFixture(t, path, "")
		policyPath := filepath.Join(root, "control-plane-lanes.json")
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":`)
		hub := controlPlaneHubWithPolicy(t, path, policyPath)
		code, witness := run(t, hub, "0")
		if code == ExitOK || witness.Verdict == "owner" {
			t.Fatalf("code=%d witness=%+v", code, witness)
		}
	})

	// "stale" is a different state and keeps deciding normally: a policy
	// reload failed, but the last-known-good set is held and the control block
	// itself parsed, so epoch and owner are trustworthy.
	t.Run("stale policy still decides", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "lanes.json")
		controlPlaneRoutesFixture(t, path, "")
		policyPath := filepath.Join(root, "control-plane-lanes.json")
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":["lane-alpha","lane-beta"]}`)
		hub := controlPlaneHubWithPolicy(t, path, policyPath)
		controlPlaneLanesPolicyFixture(t, policyPath, `{"authority_lanes":`)
		code, witness := run(t, hub, "0")
		if code != ExitOK || witness.Verdict != "owner" {
			t.Fatalf("stale must still decide: code=%d witness=%+v", code, witness)
		}
		code, witness = run(t, hub, "7")
		if code != ExitTimeout || witness.Verdict != "not_owner" {
			t.Fatalf("stale must still refuse a wrong epoch: code=%d witness=%+v", code, witness)
		}
	})
}

// TestControlPlaneW34UnreadableControlOutranksDisabled pins the precedence that
// makes the field usable as a trust signal.
func TestControlPlaneW34UnreadableControlOutranksDisabled(t *testing.T) {
	protection := func(t *testing.T, hub *HubServer) string {
		t.Helper()
		var envelope struct {
			AuthorityLaneProtection string `json:"authority_lane_protection"`
		}
		if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.AuthorityLaneProtection
	}
	root := t.TempDir()

	// No policy, readable control: the guard is simply off.
	readable := filepath.Join(root, "readable.json")
	controlPlaneRoutesFixture(t, readable, "")
	if got := protection(t, controlPlaneHub(t, readable)); got != "disabled" {
		t.Fatalf("unconfigured hub with a readable file: protection=%q want disabled", got)
	}

	// No policy, damaged control: still "invalid". Reporting "disabled" would
	// be more accurate about the guard and less safe about the data, and this
	// field is what self-check trusts.
	damaged := filepath.Join(root, "damaged.json")
	if err := os.WriteFile(damaged, []byte(`{"lanes":{"lane-alpha":{"machine":"machine-a","pane":"w1:p1"}},"control":{"state":"NOT-A-STATE"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := protection(t, controlPlaneHub(t, damaged)); got != "invalid" {
		t.Fatalf("unconfigured hub with a damaged control block: protection=%q want invalid", got)
	}
}

func (capture *controlPlaneLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (capture *controlPlaneLogCapture) Handle(_ context.Context, record slog.Record) error {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.records = append(capture.records, record.Clone())
	return nil
}

func (capture *controlPlaneLogCapture) WithAttrs([]slog.Attr) slog.Handler { return capture }

func (capture *controlPlaneLogCapture) WithGroup(string) slog.Handler { return capture }
