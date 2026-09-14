package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// The fixtures name lanes and machines that exist only in this file. Which
// lanes actually carry authority is deployment configuration, never source.
var (
	controlPlaneSideA = map[string]controlPlaneTransferRoute{
		"lane-alpha": {Machine: "machine-a", Pane: "w1:p1"},
		"lane-beta":  {Machine: "machine-a", Pane: "w1:p2"},
	}
	controlPlaneSideB = map[string]controlPlaneTransferRoute{
		"lane-alpha": {Machine: "machine-b", Pane: "w2:p1"},
		"lane-beta":  {Machine: "machine-b", Pane: "w2:p2"},
	}
	controlPlaneReady = map[string]bool{
		"model_login": true, "tools": true, "handoffkeep_read": true, "hub_read": true, "target_pane": true,
	}
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

func controlPlaneBody(requestID, action string, expectedEpoch uint64, expected, target map[string]controlPlaneTransferRoute, inflight uint64, readiness map[string]bool) []byte {
	lanes := make([]any, 0, len(expected))
	for lane := range expected {
		lanes = append(lanes, map[string]any{"lane": lane, "expected": expected[lane], "target": target[lane]})
	}
	sort.Slice(lanes, func(first, second int) bool {
		return lanes[first].(map[string]any)["lane"].(string) < lanes[second].(map[string]any)["lane"].(string)
	})
	body := map[string]any{
		"action": action, "request_id": requestID, "expected_epoch": expectedEpoch,
		"handover_doc_key": "handover/fixture", "inflight_operations": inflight,
		"lanes": lanes, "readiness": readiness,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return encoded
}

// controlPlaneForward is the common transfer body: machine-a today, machine-b
// after the commit, with every readiness check asserted.
func controlPlaneForward(requestID, action string, expectedEpoch uint64) []byte {
	return controlPlaneBody(requestID, action, expectedEpoch, controlPlaneSideA, controlPlaneSideB, 0, controlPlaneReady)
}

func controlPlaneID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", index)
}

func controlPlanePost(t *testing.T, hub *HubServer, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/control-plane/transfer", bytes.NewReader(body))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+controlPlaneOperatorToken)
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

// controlPlanePrepare drives ACTIVE to PREPARED so a commit under test is a
// legal transition and only the property being asserted can fail it.
func controlPlanePrepare(t *testing.T, hub *HubServer, requestID string, expectedEpoch uint64) {
	t.Helper()
	writer := controlPlanePost(t, hub, controlPlaneForward(requestID, "prepare", expectedEpoch))
	if writer.Code != http.StatusOK {
		t.Fatalf("prepare status=%d body=%s", writer.Code, writer.Body.String())
	}
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

// controlPlaneFileRoutes reads the lane routes out of the file. The control
// history keeps past routes too, so a byte scan would count those as well.
func controlPlaneFileRoutes(t *testing.T, path string) map[string]reportRelayRoute {
	t.Helper()
	var source struct {
		Lanes map[string]reportRelayRoute `json:"lanes"`
	}
	if err := json.Unmarshal(mustReadControlPlaneFile(t, path), &source); err != nil {
		t.Fatal(err)
	}
	return source.Lanes
}

func controlPlaneAssertRoutes(t *testing.T, path string, want map[string]controlPlaneTransferRoute) {
	t.Helper()
	routes := controlPlaneFileRoutes(t, path)
	if len(routes) < len(want) {
		t.Fatalf("routes=%+v want at least %+v", routes, want)
	}
	for lane, expected := range want {
		route, found := routes[lane]
		if !found || route.Machine != expected.Machine || route.Pane != expected.Pane {
			t.Fatalf("lane %s route=%+v want %+v", lane, route, expected)
		}
	}
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

func TestControlPlaneW1RouteRegistrationAndMethod(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	writer := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(1), "prepare", 0))
	if writer.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", writer.Code, writer.Body.String())
	}
	// The transfer path is POST only: no read endpoint was added anywhere.
	request := httptest.NewRequest(http.MethodGet, "/v1/control-plane/transfer", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+controlPlaneOperatorToken)
	read := httptest.NewRecorder()
	hub.Handler().ServeHTTP(read, request)
	if read.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on transfer status=%d body=%s", read.Code, read.Body.String())
	}
}

func TestControlPlaneW1UnauthorizedIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	before := mustReadControlPlaneFile(t, path)
	request := httptest.NewRequest(http.MethodPost, "/v1/control-plane/transfer", bytes.NewReader(controlPlaneForward(controlPlaneID(2), "prepare", 0)))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+controlPlaneMachineAToken)
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	if writer.Code != http.StatusUnauthorized {
		t.Fatalf("non-operator status=%d body=%s", writer.Code, writer.Body.String())
	}
	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("unauthorized request changed lanes file")
	}
}

func TestControlPlaneW2InvalidRequestsDoNotChangeFile(t *testing.T) {
	valid := map[string]any{}
	if err := json.Unmarshal(controlPlaneForward(controlPlaneID(3), "prepare", 0), &valid); err != nil {
		t.Fatal(err)
	}
	mutate := func(change func(map[string]any)) []byte {
		body := make(map[string]any, len(valid))
		for key, value := range valid {
			body[key] = value
		}
		change(body)
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	oneLane := []any{map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]}}
	cases := []struct {
		name string
		code string
		body []byte
	}{
		{"unknown field", "invalid_transfer_request", mutate(func(body map[string]any) { body["unexpected"] = true })},
		{"unknown action", "invalid_transfer_request", mutate(func(body map[string]any) { body["action"] = "rollback" })},
		{"bad request id", "invalid_transfer_request", mutate(func(body map[string]any) { body["request_id"] = "not-a-uuid" })},
		{"missing expected epoch", "invalid_transfer_request", mutate(func(body map[string]any) { delete(body, "expected_epoch") })},
		{"missing inflight", "invalid_transfer_request", mutate(func(body map[string]any) { delete(body, "inflight_operations") })},
		{"blank handover key", "invalid_transfer_request", mutate(func(body map[string]any) { body["handover_doc_key"] = "" })},
		{"spaced handover key", "invalid_transfer_request", mutate(func(body map[string]any) { body["handover_doc_key"] = "handover fixture" })},
		{"one lane", "invalid_transfer_request", mutate(func(body map[string]any) { body["lanes"] = oneLane })},
		{"three lanes", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": controlPlaneSideB["lane-beta"]},
				map[string]any{"lane": "lane-gamma", "expected": controlPlaneSideA["lane-beta"], "target": controlPlaneSideB["lane-beta"]},
			}
		})},
		{"duplicate lane", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]},
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]},
			}
		})},
		{"invalid lane name", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "LANE-ALPHA", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": controlPlaneSideB["lane-beta"]},
			}
		})},
		{"invalid machine", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": map[string]string{"machine": "bad machine", "pane": "w1:p1"}, "target": controlPlaneSideB["lane-alpha"]},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": controlPlaneSideB["lane-beta"]},
			}
		})},
		{"operator machine target", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": map[string]string{"machine": hubOperatorMachineID, "pane": "w2:p1"}},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": map[string]string{"machine": hubOperatorMachineID, "pane": "w2:p2"}},
			}
		})},
		{"invalid pane", "invalid_transfer_request", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": map[string]string{"machine": "machine-b", "pane": "pane-1"}},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": controlPlaneSideB["lane-beta"]},
			}
		})},
		{"mixed target machine", "mixed_target_machine", mutate(func(body map[string]any) {
			body["lanes"] = []any{
				map[string]any{"lane": "lane-alpha", "expected": controlPlaneSideA["lane-alpha"], "target": controlPlaneSideB["lane-alpha"]},
				map[string]any{"lane": "lane-beta", "expected": controlPlaneSideA["lane-beta"], "target": map[string]string{"machine": "machine-a", "pane": "w1:p9"}},
			}
		})},
		{"trailing json", "invalid_transfer_request", append(controlPlaneForward(controlPlaneID(4), "prepare", 0), []byte(`{}`)...)},
		{"not an object", "invalid_transfer_request", []byte(`[]`)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			controlPlaneRoutesFixture(t, path, "")
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
			original := mustReadControlPlaneFile(t, path)
			writer := controlPlanePost(t, hub, test.body)
			if writer.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if got := controlPlaneJSONError(t, writer)["error"]; got != test.code {
				t.Fatalf("error=%q want %q", got, test.code)
			}
			if !bytes.Equal(original, mustReadControlPlaneFile(t, path)) {
				t.Fatal("invalid request changed lanes file")
			}
		})
	}
}

func TestControlPlaneW3CommitUsesOneRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error { return nil }
	controlPlanePrepare(t, hub, controlPlaneID(0x10), 0)
	renameCount := 0
	hub.lanesWriteOps.rename = func(oldPath, newPath string) error {
		renameCount++
		return os.Rename(oldPath, newPath)
	}
	commit := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x11), "commit", 0))
	if commit.Code != http.StatusOK || renameCount != 1 {
		t.Fatalf("commit status=%d rename_count=%d body=%s", commit.Code, renameCount, commit.Body.String())
	}
	var response controlPlaneTransferResponse
	if err := json.Unmarshal(commit.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Epoch != 1 || response.Owner != "machine-b" || response.State != controlPlaneStateTransferredUnacked {
		t.Fatalf("response=%+v", response)
	}
	if len(response.Lanes) != 2 || response.Lanes[0].Machine != "machine-b" || response.Lanes[1].Machine != "machine-b" {
		t.Fatalf("both lanes must move together: %+v", response.Lanes)
	}
}

func TestControlPlaneW4StaleEpochIsByteStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	before := append([]byte(nil), controlPlaneGetLanes(t, hub).Body.Bytes()...)
	writer := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x20), "prepare", 1))
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "stale_epoch" {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if after := controlPlaneGetLanes(t, hub).Body.Bytes(); !bytes.Equal(before, after) {
		t.Fatalf("stale request changed GET response: before=%s after=%s", before, after)
	}
}

// TestControlPlaneW4StaleEpochAfterRoundTrip is the case only the epoch
// comparison can catch. After a full transfer and revert the state is ACTIVE
// again and both routes are back where they started, so the state machine and
// the route CAS both accept a request that the epoch shows is two transfers
// out of date.
func TestControlPlaneW4StaleEpochAfterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlaneRoundTrip(t, hub, 0x200)
	if epoch := controlPlaneEpoch(t, hub); epoch != 2 {
		t.Fatalf("round trip epoch=%d want 2", epoch)
	}
	before := append([]byte(nil), controlPlaneGetLanes(t, hub).Body.Bytes()...)
	replay := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x210), "prepare", 0))
	if replay.Code != http.StatusConflict || controlPlaneJSONError(t, replay)["error"] != "stale_epoch" {
		t.Fatalf("state-legal, route-matching, epoch-stale request: status=%d body=%s", replay.Code, replay.Body.String())
	}
	if after := controlPlaneGetLanes(t, hub).Body.Bytes(); !bytes.Equal(before, after) {
		t.Fatalf("stale replay changed GET response: before=%s after=%s", before, after)
	}
}

// controlPlaneRoundTrip runs both legal paths end to end: the transfer
// PREPARE -> COMMIT -> ACK and the revert DRAINING -> HANDOVER_READY ->
// COMMIT -> ACK, leaving the routes where they started at epoch 2.
func controlPlaneRoundTrip(t *testing.T, hub *HubServer, base int) {
	t.Helper()
	steps := []struct {
		action         string
		epoch          uint64
		expected       map[string]controlPlaneTransferRoute
		target         map[string]controlPlaneTransferRoute
		wantState      controlPlaneState
		wantEpochAfter uint64
	}{
		{"prepare", 0, controlPlaneSideA, controlPlaneSideB, controlPlaneStatePrepared, 0},
		{"commit", 0, controlPlaneSideA, controlPlaneSideB, controlPlaneStateTransferredUnacked, 1},
		{"ack", 1, controlPlaneSideB, controlPlaneSideB, controlPlaneStateActive, 1},
		{"drain", 1, controlPlaneSideB, controlPlaneSideA, controlPlaneStateDraining, 1},
		{"handover_ready", 1, controlPlaneSideB, controlPlaneSideA, controlPlaneStateHandoverReady, 1},
		{"commit", 1, controlPlaneSideB, controlPlaneSideA, controlPlaneStateTransferredUnacked, 2},
		{"ack", 2, controlPlaneSideA, controlPlaneSideA, controlPlaneStateActive, 2},
	}
	for index, step := range steps {
		body := controlPlaneBody(controlPlaneID(base+index), step.action, step.epoch, step.expected, step.target, 0, controlPlaneReady)
		writer := controlPlanePost(t, hub, body)
		if writer.Code != http.StatusOK {
			t.Fatalf("step=%d action=%s status=%d body=%s", index, step.action, writer.Code, writer.Body.String())
		}
		var response controlPlaneTransferResponse
		if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.State != step.wantState || response.Epoch != step.wantEpochAfter {
			t.Fatalf("step=%d action=%s state=%s epoch=%d want state=%s epoch=%d", index, step.action, response.State, response.Epoch, step.wantState, step.wantEpochAfter)
		}
	}
}

func TestControlPlaneW5EachRouteMismatchIsRejected(t *testing.T) {
	for _, mismatchLane := range []string{"lane-alpha", "lane-beta"} {
		t.Run(mismatchLane, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			controlPlaneRoutesFixture(t, path, "")
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
			expected := map[string]controlPlaneTransferRoute{
				"lane-alpha": controlPlaneSideA["lane-alpha"],
				"lane-beta":  controlPlaneSideA["lane-beta"],
			}
			expected[mismatchLane] = controlPlaneTransferRoute{Machine: "machine-a", Pane: "w9:p9"}
			before := controlPlaneGetLanes(t, hub).Body.String()
			writer := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x30), "prepare", 0, expected, controlPlaneSideB, 0, controlPlaneReady))
			if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "route_mismatch" {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if got := controlPlaneGetLanes(t, hub).Body.String(); got != before {
				t.Fatalf("route mismatch changed state: before=%s after=%s", before, got)
			}
		})
	}
}

func TestControlPlaneW6RequestIDConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	id := controlPlaneID(0x40)
	controlPlanePrepare(t, hub, id, 0)
	before := controlPlaneGetLanes(t, hub).Body.String()
	different := map[string]controlPlaneTransferRoute{
		"lane-alpha": {Machine: "machine-b", Pane: "w2:p7"},
		"lane-beta":  controlPlaneSideB["lane-beta"],
	}
	second := controlPlanePost(t, hub, controlPlaneBody(id, "prepare", 0, controlPlaneSideA, different, 0, controlPlaneReady))
	if second.Code != http.StatusConflict || controlPlaneJSONError(t, second)["error"] != "request_id_conflict" {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	if got := controlPlaneGetLanes(t, hub).Body.String(); got != before {
		t.Fatalf("conflicting retry changed state: before=%s after=%s", before, got)
	}
}

// TestControlPlaneW6LaneOrderIsNotADifferentRequest fixes the fingerprint rule:
// the two lanes are a set, so swapping their order is the same request.
func TestControlPlaneW6LaneOrderIsNotADifferentRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	id := controlPlaneID(0x45)
	first := controlPlanePost(t, hub, controlPlaneForward(id, "prepare", 0))
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	reordered := fmt.Sprintf(`{"action":"prepare","request_id":%q,"expected_epoch":0,"handover_doc_key":"handover/fixture","inflight_operations":0,"lanes":[{"lane":"lane-beta","expected":{"machine":"machine-a","pane":"w1:p2"},"target":{"machine":"machine-b","pane":"w2:p2"}},{"lane":"lane-alpha","expected":{"machine":"machine-a","pane":"w1:p1"},"target":{"machine":"machine-b","pane":"w2:p1"}}],"readiness":{"model_login":true,"tools":true,"handoffkeep_read":true,"hub_read":true,"target_pane":true}}`, id)
	second := controlPlanePost(t, hub, []byte(reordered))
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("reordered lanes were treated as a different request: status=%d body=%s", second.Code, second.Body.String())
	}
}

func TestControlPlaneW7IdempotentRetryReturnsIdenticalResponse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlanePrepare(t, hub, controlPlaneID(0x50), 0)
	body := controlPlaneForward(controlPlaneID(0x51), "commit", 0)
	first := controlPlanePost(t, hub, body)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	for retry := 0; retry < 3; retry++ {
		second := controlPlanePost(t, hub, body)
		if second.Code != first.Code || second.Body.String() != first.Body.String() {
			t.Fatalf("retry=%d status=%d body=%q want=%q", retry, second.Code, second.Body.String(), first.Body.String())
		}
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("epoch=%d want 1", epoch)
	}
}

// TestControlPlaneW7RetryReplaysStoredReadiness pins the replay to what was
// recorded. A reconstructed readiness list would be equal only by assumption,
// and AC1 asks for the same result, not a plausible one.
func TestControlPlaneW7RetryReplaysStoredReadiness(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	id := controlPlaneID(0x55)
	first := controlPlanePost(t, hub, controlPlaneForward(id, "prepare", 0))
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	var response controlPlaneTransferResponse
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	// prepare never reaches the readiness gate, so it must not claim it did.
	if len(response.Readiness) != 0 {
		t.Fatalf("prepare reported readiness it never ran: %+v", response.Readiness)
	}
	retry := controlPlanePost(t, hub, controlPlaneForward(id, "prepare", 0))
	if retry.Body.String() != first.Body.String() {
		t.Fatalf("replay=%s want=%s", retry.Body.String(), first.Body.String())
	}
	var stored struct {
		Control lanesFileControl `json:"control"`
	}
	if err := json.Unmarshal(mustReadControlPlaneFile(t, path), &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Control.History) != 1 || stored.Control.History[0].HandoverDocKey != "handover/fixture" {
		t.Fatalf("history did not store the response fields: %+v", stored.Control.History)
	}
}

func TestControlPlaneW8ConcurrentCASHasOneWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlanePrepare(t, hub, controlPlaneID(0x60), 0)
	var wait sync.WaitGroup
	type outcome struct {
		status int
		code   string
	}
	results := make(chan outcome, 8)
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1/control-plane/transfer", bytes.NewReader(controlPlaneForward(controlPlaneID(0x100+index), "commit", 0)))
			request.Header.Set(hubAuthorizationHeader, "Bearer "+controlPlaneOperatorToken)
			request.Header.Set("Content-Type", "application/json")
			writer := httptest.NewRecorder()
			hub.Handler().ServeHTTP(writer, request)
			var body map[string]string
			_ = json.Unmarshal(writer.Body.Bytes(), &body)
			results <- outcome{status: writer.Code, code: body["error"]}
		}(index)
	}
	wait.Wait()
	close(results)
	winners, conflicts := 0, 0
	for result := range results {
		switch result.status {
		case http.StatusOK:
			winners++
		case http.StatusConflict:
			conflicts++
			// The losers must lose on the epoch, not on a later check: the
			// compare-and-set is what makes a concurrent transfer safe.
			if result.code != "stale_epoch" {
				t.Fatalf("loser error=%q want stale_epoch", result.code)
			}
		default:
			t.Fatalf("unexpected status=%d", result.status)
		}
	}
	if winners != 1 || conflicts != 7 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("epoch=%d want 1", epoch)
	}
}

func TestControlPlaneW9WriteFailureLeavesRoutesAndEpoch(t *testing.T) {
	for _, test := range []struct {
		name    string
		inject  func(*HubServer)
		wantMsg string
	}{
		{"rename", func(hub *HubServer) {
			hub.lanesWriteOps.rename = func(string, string) error { return errors.New("injected rename failure") }
		}, "lanes_write_failed"},
		{"create temp", func(hub *HubServer) {
			hub.lanesWriteOps.createTemp = func(string, string) (*os.File, error) { return nil, errors.New("injected temp failure") }
		}, "lanes_write_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			controlPlaneRoutesFixture(t, path, "")
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
			hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error { return nil }
			controlPlanePrepare(t, hub, controlPlaneID(0x70), 0)
			before := mustReadControlPlaneFile(t, path)
			test.inject(hub)
			writer := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x71), "commit", 0))
			if writer.Code != http.StatusInternalServerError || controlPlaneJSONError(t, writer)["error"] != test.wantMsg {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
				t.Fatalf("failed commit changed the lanes file: %s", mustReadControlPlaneFile(t, path))
			}
			if epoch := controlPlaneEpoch(t, hub); epoch != 0 {
				t.Fatalf("failed commit changed epoch=%d", epoch)
			}
		})
	}
}

func TestControlPlaneW10LostResponseRetryConverges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlanePrepare(t, hub, controlPlaneID(0x80), 0)
	id := controlPlaneID(0x81)
	body := controlPlaneForward(id, "commit", 0)
	first := controlPlanePost(t, hub, body)
	if first.Code != http.StatusOK {
		t.Fatalf("first transfer status=%d body=%s", first.Code, first.Body.String())
	}
	// The caller never saw that response: it retries with the epoch it still
	// believes in, which is now one behind.
	second := controlPlanePost(t, hub, body)
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("retry did not converge: first=%s second=%s", first.Body.String(), second.Body.String())
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("retry transferred twice: epoch=%d", epoch)
	}
	controlPlaneAssertRoutes(t, path, controlPlaneSideB)
}

func TestControlPlaneW11LanesEnvelopeAddsOnlyControlFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	var response struct {
		Lanes         []map[string]any `json:"lanes"`
		ControlEpoch  uint64           `json:"control_epoch"`
		ControlOwner  string           `json:"control_owner"`
		ControlState  string           `json:"control_state"`
		LastRequestID string           `json:"last_request_id"`
	}
	if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.ControlEpoch != 0 || response.ControlOwner != "" || response.ControlState != "" || response.LastRequestID != "" {
		t.Fatalf("initial control=%+v", response)
	}
	wantKeys := map[string]struct{}{"lane": {}, "machine": {}, "pane": {}, "parent": {}, "sink": {}}
	for _, lane := range response.Lanes {
		keys := make(map[string]struct{}, len(lane))
		for key := range lane {
			keys[key] = struct{}{}
		}
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("lane keys=%v want %v", keys, wantKeys)
		}
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	wantEnvelope := map[string]struct{}{"lanes": {}, "control_epoch": {}, "control_owner": {}, "control_state": {}, "last_request_id": {}, "authority_lane_protection": {}}
	got := make(map[string]struct{}, len(envelope))
	for key := range envelope {
		got[key] = struct{}{}
	}
	if !reflect.DeepEqual(got, wantEnvelope) {
		t.Fatalf("envelope keys=%v want %v", got, wantEnvelope)
	}
	controlPlanePrepare(t, hub, controlPlaneID(0x90), 0)
	if err := json.Unmarshal(controlPlaneGetLanes(t, hub).Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.ControlState != string(controlPlaneStatePrepared) || response.LastRequestID != controlPlaneID(0x90) {
		t.Fatalf("control envelope after prepare=%+v", response)
	}
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

func TestControlPlaneW12PutPreservesControl(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlanePrepare(t, hub, controlPlaneID(0xA0), 0)
	commit := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0xA1), "commit", 0))
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", commit.Code, commit.Body.String())
	}
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-general", controlPlaneOperatorToken, `{"machine":"machine-a","pane":"w1:p4"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("general PUT status=%d body=%s", writer.Code, writer.Body.String())
	}
	var source struct {
		Control lanesFileControl `json:"control"`
	}
	if err := json.Unmarshal(mustReadControlPlaneFile(t, path), &source); err != nil {
		t.Fatal(err)
	}
	if source.Control.Epoch != 1 || source.Control.Owner != "machine-b" || source.Control.LastRequestID != controlPlaneID(0xA1) || len(source.Control.History) != 2 {
		t.Fatalf("an ordinary lane write erased control: %+v", source.Control)
	}
	// A delete has to preserve it too.
	if remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-general", controlPlaneOperatorToken, ""); remove.Code != http.StatusOK {
		t.Fatalf("general DELETE status=%d body=%s", remove.Code, remove.Body.String())
	}
	if err := json.Unmarshal(mustReadControlPlaneFile(t, path), &source); err != nil {
		t.Fatal(err)
	}
	if source.Control.Epoch != 1 || len(source.Control.History) != 2 {
		t.Fatalf("an ordinary lane delete erased control: %+v", source.Control)
	}
}

func TestControlPlaneW13TransferAndRevertStateMachines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlaneRoundTrip(t, hub, 0x300)
	controlPlaneAssertRoutes(t, path, controlPlaneSideA)
}

func TestControlPlaneW13IllegalTransitionsAreRejected(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, hub *HubServer)
		action  string
		epoch   uint64
	}{
		{"commit from active", func(*testing.T, *HubServer) {}, "commit", 0},
		{"ack from active", func(*testing.T, *HubServer) {}, "ack", 0},
		{"handover_ready from active", func(*testing.T, *HubServer) {}, "handover_ready", 0},
		{"prepare from prepared", func(t *testing.T, hub *HubServer) { controlPlanePrepare(t, hub, controlPlaneID(0x400), 0) }, "prepare", 0},
		{"handover_ready from prepared", func(t *testing.T, hub *HubServer) { controlPlanePrepare(t, hub, controlPlaneID(0x401), 0) }, "handover_ready", 0},
		{"drain from prepared", func(t *testing.T, hub *HubServer) { controlPlanePrepare(t, hub, controlPlaneID(0x402), 0) }, "drain", 0},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			controlPlaneRoutesFixture(t, path, "")
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
			test.prepare(t, hub)
			before := mustReadControlPlaneFile(t, path)
			writer := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x410+index), test.action, test.epoch))
			if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "illegal_transition" {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
				t.Fatal("illegal transition changed the lanes file")
			}
		})
	}
}

func TestControlPlaneW13HandoverReadyRequiresQuietInflight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	drain := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x420), "drain", 0, controlPlaneSideA, controlPlaneSideB, 0, controlPlaneReady))
	if drain.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", drain.Code, drain.Body.String())
	}
	before := mustReadControlPlaneFile(t, path)
	busy := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x421), "handover_ready", 0, controlPlaneSideA, controlPlaneSideB, 3, controlPlaneReady))
	if busy.Code != http.StatusConflict || controlPlaneJSONError(t, busy)["error"] != "illegal_transition" {
		t.Fatalf("status=%d body=%s", busy.Code, busy.Body.String())
	}
	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("rejected handover_ready changed the lanes file")
	}
	quiet := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x422), "handover_ready", 0, controlPlaneSideA, controlPlaneSideB, 0, controlPlaneReady))
	if quiet.Code != http.StatusOK {
		t.Fatalf("quiet handover_ready status=%d body=%s", quiet.Code, quiet.Body.String())
	}
}

func TestControlPlaneW14ReadinessFailuresDoNotMoveRoutes(t *testing.T) {
	cases := []struct {
		name      string
		readiness map[string]bool
		target    map[string]controlPlaneTransferRoute
	}{
		{"false check", map[string]bool{"model_login": false, "tools": true, "handoffkeep_read": true, "hub_read": true, "target_pane": true}, controlPlaneSideB},
		{"missing check", map[string]bool{"model_login": true, "tools": true, "handoffkeep_read": true, "hub_read": true}, controlPlaneSideB},
		{"no readiness at all", nil, controlPlaneSideB},
		{"server rejects unknown target machine", controlPlaneReady, map[string]controlPlaneTransferRoute{
			"lane-alpha": {Machine: "machine-c", Pane: "w3:p1"},
			"lane-beta":  {Machine: "machine-c", Pane: "w3:p2"},
		}},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			controlPlaneRoutesFixture(t, path, "")
			hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
			controlPlanePrepare(t, hub, controlPlaneID(0x500+index), 0)
			before := mustReadControlPlaneFile(t, path)
			body := controlPlaneBody(controlPlaneID(0x510+index), "commit", 0, controlPlaneSideA, test.target, 0, test.readiness)
			writer := controlPlanePost(t, hub, body)
			if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "target_not_ready" {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
				t.Fatal("readiness failure changed lanes file")
			}
			if epoch := controlPlaneEpoch(t, hub); epoch != 0 {
				t.Fatalf("readiness failure changed epoch=%d", epoch)
			}
		})
	}
}

// TestControlPlaneW14ServerOverridesReadinessClaim covers the claim the client
// cannot be trusted with: the hook says every check passed, and the server's
// own look at the target still refuses the commit.
func TestControlPlaneW14ServerOverridesReadinessClaim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	hub.controlReadiness = func(controlPlaneTransferRequest, map[string]reportRelayRoute) []controlPlaneReadinessCheck {
		return []controlPlaneReadinessCheck{
			{Name: "model_login", OK: true}, {Name: "tools", OK: true}, {Name: "handoffkeep_read", OK: true},
			{Name: "hub_read", OK: true}, {Name: "target_pane", OK: true},
		}
	}
	controlPlanePrepare(t, hub, controlPlaneID(0x520), 0)
	unknownTarget := map[string]controlPlaneTransferRoute{
		"lane-alpha": {Machine: "machine-c", Pane: "w3:p1"},
		"lane-beta":  {Machine: "machine-c", Pane: "w3:p2"},
	}
	writer := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x521), "commit", 0, controlPlaneSideA, unknownTarget, 0, controlPlaneReady))
	if writer.Code != http.StatusConflict || controlPlaneJSONError(t, writer)["error"] != "target_not_ready" {
		t.Fatalf("an all-true hook overrode the server check: status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// TestControlPlaneReadinessGateIsScopedToCommit records a deliberate reading of
// A3: readiness is what must precede a route commit. drain and handover_ready
// run while the target is still being spawned, so gating them would either
// invert that order or make the caller assert a readiness it does not have.
func TestControlPlaneReadinessGateIsScopedToCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	notReady := map[string]bool{"model_login": false, "tools": false, "handoffkeep_read": false, "hub_read": false, "target_pane": false}
	drain := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x530), "drain", 0, controlPlaneSideA, controlPlaneSideB, 0, notReady))
	if drain.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", drain.Code, drain.Body.String())
	}
	ready := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x531), "handover_ready", 0, controlPlaneSideA, controlPlaneSideB, 0, notReady))
	if ready.Code != http.StatusOK {
		t.Fatalf("handover_ready status=%d body=%s", ready.Code, ready.Body.String())
	}
	// The commit is where it is enforced, and the routes stay put when it fails.
	before := mustReadControlPlaneFile(t, path)
	commit := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x532), "commit", 0, controlPlaneSideA, controlPlaneSideB, 0, notReady))
	if commit.Code != http.StatusConflict || controlPlaneJSONError(t, commit)["error"] != "target_not_ready" {
		t.Fatalf("commit status=%d body=%s", commit.Code, commit.Body.String())
	}
	if !bytes.Equal(before, mustReadControlPlaneFile(t, path)) {
		t.Fatal("refused commit changed the lanes file")
	}
}

func TestControlPlaneW15ReadinessRunsBeforeRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error { return nil }
	controlPlanePrepare(t, hub, controlPlaneID(0x540), 0)
	renameCount := 0
	hub.lanesWriteOps.rename = func(oldPath, newPath string) error {
		renameCount++
		return os.Rename(oldPath, newPath)
	}
	readinessCalls := 0
	renamesAtReadiness := -1
	hub.controlReadiness = func(controlPlaneTransferRequest, map[string]reportRelayRoute) []controlPlaneReadinessCheck {
		readinessCalls++
		renamesAtReadiness = renameCount
		return []controlPlaneReadinessCheck{
			{Name: "model_login", OK: true}, {Name: "tools", OK: true}, {Name: "handoffkeep_read", OK: true},
			{Name: "hub_read", OK: true}, {Name: "target_pane", OK: true},
		}
	}
	writer := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x541), "commit", 0))
	if writer.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", writer.Code, writer.Body.String())
	}
	if readinessCalls != 1 || renamesAtReadiness != 0 || renameCount != 1 {
		t.Fatalf("readiness_calls=%d renames_at_readiness=%d rename_count=%d", readinessCalls, renamesAtReadiness, renameCount)
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
	contents, err := os.ReadFile(filepath.Join("docs", "control-plane-transfer.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, sentence := range []string{
		"이것은 강제가 아니라 1회 관측이며, 확인을 건너뛴 세션은 여전히 행동할 수 있다.",
		"self-check 통과를 '차단됨'으로 쓰지 말 것",
		"hub API 를 통한 권위 레인 route 변경은 transfer 로 단일화된다. 파일 직접 편집 경로는 남아 있다.",
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

func TestControlPlaneW23AuthorityErrorGuidanceKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, "")
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-alpha", controlPlaneOperatorToken, `{"machine":"machine-b","pane":"w2:p1"}`)
	var fields map[string]any
	if err := json.Unmarshal(writer.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, map[string]any{"error": "authority_lane_direct_write", "use": "POST /v1/control-plane/transfer"}) {
		t.Fatalf("fields=%v", fields)
	}
	// The error key keeps its meaning and position for existing consumers: the
	// route is additive.
	var legacy struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &legacy); err != nil || legacy.Error != "authority_lane_direct_write" {
		t.Fatalf("legacy decode err=%v error=%q", err, legacy.Error)
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

func TestControlPlaneW25GeneralLaneDoesNotIncrementEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	controlPlaneRoutesFixture(t, path, `,"lane-general":{"machine":"machine-a","pane":"w1:p3"}`)
	hub := controlPlaneHub(t, path, "lane-alpha", "lane-beta")
	controlPlanePrepare(t, hub, controlPlaneID(0x600), 0)
	if epoch := controlPlaneEpoch(t, hub); epoch != 0 {
		t.Fatalf("prepare moved the epoch: %d", epoch)
	}
	commit := controlPlanePost(t, hub, controlPlaneForward(controlPlaneID(0x601), "commit", 0))
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", commit.Code, commit.Body.String())
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("commit epoch=%d want 1", epoch)
	}
	for _, request := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPut, "/v1/lanes/lane-general", `{"machine":"machine-a","pane":"w1:p4"}`},
		{http.MethodDelete, "/v1/lanes/lane-general", ""},
	} {
		writer := lanesWriteRequest(t, hub, request.method, request.path, controlPlaneOperatorToken, request.body)
		if writer.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", request.method, writer.Code, writer.Body.String())
		}
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("an ordinary lane write moved the epoch to %d", epoch)
	}
	// And an ack does not move it either: only a commit does.
	ack := controlPlanePost(t, hub, controlPlaneBody(controlPlaneID(0x602), "ack", 1, controlPlaneSideB, controlPlaneSideB, 0, controlPlaneReady))
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status=%d body=%s", ack.Code, ack.Body.String())
	}
	if epoch := controlPlaneEpoch(t, hub); epoch != 1 {
		t.Fatalf("ack moved the epoch to %d", epoch)
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
	contents, err := os.ReadFile(filepath.Join("docs", "control-plane-transfer.md"))
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
				if fields["error"] != "authority_lane_policy_unavailable" || fields["use"] != "POST /v1/control-plane/transfer" {
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

func (capture *controlPlaneLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (capture *controlPlaneLogCapture) Handle(_ context.Context, record slog.Record) error {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.records = append(capture.records, record.Clone())
	return nil
}

func (capture *controlPlaneLogCapture) WithAttrs([]slog.Attr) slog.Handler { return capture }

func (capture *controlPlaneLogCapture) WithGroup(string) slog.Handler { return capture }

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
	contents, err := os.ReadFile(filepath.Join("docs", "control-plane-transfer.md"))
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
