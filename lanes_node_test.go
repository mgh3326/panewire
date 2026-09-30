package panewire

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// AC1-AC5 for task #1037 part A: a node credential (bearer token plus the
// matching X-Panewire-Machine-ID header) may manage only the lanes routed to
// its own machine; the operator credential is unchanged.

const (
	lanesNodeOperatorToken = "fixture-operator-token"
	lanesNodeAToken        = "fixture-node-a-token"
	lanesNodeBToken        = "fixture-node-b-token"
)

func lanesNodeHub(t *testing.T, lanesPath string, mutate func(*HubServerConfig)) *HubServer {
	t.Helper()
	config := HubServerConfig{
		Tokens: map[string]string{
			hubOperatorMachineID: lanesNodeOperatorToken,
			"machine-a":          lanesNodeAToken,
			"machine-b":          lanesNodeBToken,
		},
		ReportRelayPath: lanesPath,
	}
	if mutate != nil {
		mutate(&config)
	}
	hub, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	return hub
}

// lanesNodeRequest issues one request. machineHeader is the literal
// X-Panewire-Machine-ID value; "" leaves the header unset.
func lanesNodeRequest(t *testing.T, hub *HubServer, method, path, token, machineHeader, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	if machineHeader != "" {
		request.Header.Set(hubMachineIDHeader, machineHeader)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func lanesNodePut(t *testing.T, hub *HubServer, lane, body string) *httptest.ResponseRecorder {
	t.Helper()
	return lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, lanesNodeAToken, "machine-a", body)
}

func lanesNodeDelete(t *testing.T, hub *HubServer, lane string) *httptest.ResponseRecorder {
	t.Helper()
	return lanesNodeRequest(t, hub, http.MethodDelete, "/v1/lanes/"+lane, lanesNodeAToken, "machine-a", "")
}

func lanesNodeForbidden(t *testing.T, writer *httptest.ResponseRecorder, what string) {
	t.Helper()
	if writer.Code != http.StatusForbidden || writer.Body.String() != `{"error":"lane_machine_mismatch"}`+"\n" {
		t.Fatalf("%s status=%d body=%q want 403 lane_machine_mismatch", what, writer.Code, writer.Body.String())
	}
}

func lanesNodeFile(t *testing.T, path string) map[string]reportRelayRoute {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := parseReportRelayRoutes(contents)
	if err != nil {
		t.Fatalf("lanes file=%q err=%v", contents, err)
	}
	return routes
}

// AC1: a node token manages only lanes whose machine is its own.
func TestLanesNodeWriteScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"},"lane-b":{"machine":"machine-b","pane":"w9:p9"},"lane-sink":{"sink":true}}}`)
	hub := lanesNodeHub(t, path, nil)

	// Creating a lane on its own machine succeeds.
	if writer := lanesNodePut(t, hub, "lane-new", `{"machine":"machine-a","pane":"w1:p2"}`); writer.Code != http.StatusCreated {
		t.Fatalf("node create status=%d body=%s", writer.Code, writer.Body.String())
	}
	// Updating a lane on its own machine succeeds.
	if writer := lanesNodePut(t, hub, "lane-a", `{"machine":"machine-a","pane":"w1:p3"}`); writer.Code != http.StatusOK {
		t.Fatalf("node update status=%d body=%s", writer.Code, writer.Body.String())
	}
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-other-machine", `{"machine":"machine-b","pane":"w9:p1"}`), "node create for machine-b")
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-b", `{"machine":"machine-a","pane":"w1:p4"}`), "node overwrite of machine-b lane")
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-sink", `{"machine":"machine-a","pane":"w1:p5"}`), "node overwrite of a sink lane")
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-new-sink", `{"machine":"machine-a","pane":"w1:p6","sink":true}`), "node sink create")
	lanesNodeForbidden(t, lanesNodeDelete(t, hub, "lane-b"), "node delete of machine-b lane")
	lanesNodeForbidden(t, lanesNodeDelete(t, hub, "lane-missing"), "node delete of missing lane")

	// The refused writes left every foreign row byte-identical.
	routes := lanesNodeFile(t, path)
	if routes["lane-b"].Machine != "machine-b" || routes["lane-b"].Pane != "w9:p9" || !routes["lane-sink"].Sink {
		t.Fatalf("foreign lanes changed: %+v", routes)
	}
	if writer := lanesNodeDelete(t, hub, "lane-a"); writer.Code != http.StatusOK || writer.Body.String() != `{"lane":"lane-a","removed":true}`+"\n" {
		t.Fatalf("node delete own status=%d body=%s", writer.Code, writer.Body.String())
	}
	if _, exists := lanesNodeFile(t, path)["lane-a"]; exists {
		t.Fatal("node delete did not remove its own lane")
	}
}

// AC1 continued: parent must be a lane on the node's own machine, standby may
// not target another machine, and the authority guard still applies to a lane
// the node owns.
func TestLanesNodeWriteParentStandbyAndAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"},"lane-b":{"machine":"machine-b","pane":"w9:p9"},"lane-authority":{"machine":"machine-a","pane":"w1:p7"},"lane-sink":{"sink":true}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) {
		config.ControlPlaneLanes = []string{"lane-authority"}
	})

	if writer := lanesNodePut(t, hub, "lane-child", `{"machine":"machine-a","pane":"w1:p2","parent":"lane-a"}`); writer.Code != http.StatusCreated {
		t.Fatalf("own parent status=%d body=%s", writer.Code, writer.Body.String())
	}
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-child2", `{"machine":"machine-a","pane":"w1:p3","parent":"lane-b"}`), "foreign-machine parent")
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-child3", `{"machine":"machine-a","pane":"w1:p4","parent":"lane-missing"}`), "missing parent")
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-child4", `{"machine":"machine-a","pane":"w1:p5","parent":"lane-sink"}`), "sink parent")
	if writer := lanesNodePut(t, hub, "lane-standby", `{"machine":"machine-a","pane":"w1:p6","standby":{"machine":"machine-a","pane":"w1:p8"}}`); writer.Code != http.StatusCreated {
		t.Fatalf("own standby status=%d body=%s", writer.Code, writer.Body.String())
	}
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-standby2", `{"machine":"machine-a","pane":"w1:p6","standby":{"machine":"machine-b","pane":"w9:p8"}}`), "foreign-machine standby")

	// An authority lane on the node's own machine stays operator-managed;
	// the foreign lane reports only the scope refusal, never its protection.
	writer := lanesNodePut(t, hub, "lane-authority", `{"machine":"machine-a","pane":"w1:p9"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"authority_lane_direct_write","use":"POST /v1/control-plane/transfer"}`+"\n" {
		t.Fatalf("own authority lane status=%d body=%s", writer.Code, writer.Body.String())
	}
	writer = lanesNodeDelete(t, hub, "lane-authority")
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"authority_lane_direct_write","use":"POST /v1/control-plane/transfer"}`+"\n" {
		t.Fatalf("own authority delete status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// AC1 operator side: nothing about the operator write path changed.
func TestLanesNodeWriteOperatorUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-b":{"machine":"machine-b","pane":"w9:p9"}}}`)
	hub := lanesNodeHub(t, path, nil)

	put := func(lane, body string) *httptest.ResponseRecorder {
		return lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, lanesNodeOperatorToken, "", body)
	}
	if writer := put("lane-any", `{"machine":"machine-b","pane":"w9:p2"}`); writer.Code != http.StatusCreated {
		t.Fatalf("operator create foreign status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := put("lane-b", `{"machine":"machine-a","pane":"w1:p1"}`); writer.Code != http.StatusOK {
		t.Fatalf("operator move foreign lane status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := put("lane-new-sink", `{"machine":"machine-a","pane":"w1:p2","sink":true}`); writer.Code != http.StatusCreated {
		t.Fatalf("operator sink create status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-b", lanesNodeOperatorToken, "", ""); writer.Code != http.StatusOK {
		t.Fatalf("operator delete status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-missing", lanesNodeOperatorToken, "", ""); writer.Code != http.StatusNotFound || writer.Body.String() != `{"error":"lane_not_found"}`+"\n" {
		t.Fatalf("operator missing delete status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// AC2: a node GET sees only its own machine's lanes and no control-plane
// fields; the operator response is byte-identical to the main contract.
func TestLanesNodeGetFilteredAndOperatorGolden(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-a","pane":"w1:p9"}},"lane-b":{"machine":"machine-b","pane":"w9:p9"},"lane-sink":{"sink":true}},"control":{"epoch":7,"owner":"machine-b","state":"ACTIVE","last_request_id":"12345678-1234-4123-8123-123456789abc","handover_doc_key":"hk:doc/fixture","updated_at":"2026-09-30T00:00:00Z"}}`)
	hub := lanesNodeHub(t, path, nil)

	node := lanesNodeRequest(t, hub, http.MethodGet, "/v1/lanes", lanesNodeAToken, "machine-a", "")
	if node.Code != http.StatusOK {
		t.Fatalf("node GET status=%d body=%s", node.Code, node.Body.String())
	}
	want := `{"lanes":[{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false,"standby":{"machine":"machine-a","pane":"w1:p9"}}]}` + "\n"
	if node.Body.String() != want {
		t.Fatalf("node GET body=%s want %s", node.Body.String(), want)
	}
	for _, field := range []string{"control_epoch", "control_owner", "control_state", "last_request_id", "authority_lane_protection", "lane-b", "lane-sink"} {
		if strings.Contains(node.Body.String(), field) {
			t.Fatalf("node GET exposed %q: %s", field, node.Body.String())
		}
	}

	operator := lanesNodeRequest(t, hub, http.MethodGet, "/v1/lanes", lanesNodeOperatorToken, "", "")
	if operator.Code != http.StatusOK {
		t.Fatalf("operator GET status=%d body=%s", operator.Code, operator.Body.String())
	}
	golden := `{"lanes":[` +
		`{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false,"standby":{"machine":"machine-a","pane":"w1:p9"}},` +
		`{"lane":"lane-b","machine":"machine-b","pane":"w9:p9","parent":"","sink":false},` +
		`{"lane":"lane-sink","machine":"","pane":"","parent":"","sink":true}` +
		`],"control_epoch":7,"control_owner":"machine-b","control_state":"ACTIVE","last_request_id":"12345678-1234-4123-8123-123456789abc","authority_lane_protection":"disabled"}` + "\n"
	if operator.Body.String() != golden {
		t.Fatalf("operator GET body=%s want %s", operator.Body.String(), golden)
	}
}

// AC4: the machine-id header must match the bearer's machine; "operator" can
// never authenticate as a node, and an operator bearer stays the operator
// even when a machine header rides along.
func TestLanesNodeHeaderSpoofing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	hub := lanesNodeHub(t, path, nil)

	for _, test := range []struct {
		name          string
		token         string
		machineHeader string
	}{
		{name: "node token names another machine", token: lanesNodeAToken, machineHeader: "machine-b"},
		{name: "node token names operator", token: lanesNodeAToken, machineHeader: hubOperatorMachineID},
		{name: "node token without header", token: lanesNodeAToken},
		{name: "node token with empty header", token: lanesNodeAToken, machineHeader: " "},
		{name: "other node token names machine-a", token: lanesNodeBToken, machineHeader: "machine-a"},
		{name: "no credentials with machine header", machineHeader: "machine-a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, target := range []struct {
				method, path, body string
			}{
				{method: http.MethodGet, path: "/v1/lanes"},
				{method: http.MethodPut, path: "/v1/lanes/lane-a", body: `{"machine":"machine-a","pane":"w1:p2"}`},
				{method: http.MethodDelete, path: "/v1/lanes/lane-a"},
			} {
				writer := lanesNodeRequest(t, hub, target.method, target.path, test.token, test.machineHeader, target.body)
				if writer.Code != http.StatusUnauthorized || writer.Header().Get("WWW-Authenticate") != "Bearer" {
					t.Fatalf("%s %s status=%d auth=%q body=%s", target.method, target.path, writer.Code, writer.Header().Get("WWW-Authenticate"), writer.Body.String())
				}
			}
		})
	}

	// The operator bearer wins over a node machine header: this request is
	// operator, not node, and sees every lane.
	writer := lanesNodeRequest(t, hub, http.MethodGet, "/v1/lanes", lanesNodeOperatorToken, "machine-a", "")
	if writer.Code != http.StatusOK || !strings.Contains(writer.Body.String(), "control_epoch") {
		t.Fatalf("operator-with-header status=%d body=%s", writer.Code, writer.Body.String())
	}
	// An operator bearer naming "operator" as its machine id is unchanged.
	writer = lanesNodeRequest(t, hub, http.MethodGet, "/v1/lanes", lanesNodeOperatorToken, hubOperatorMachineID, "")
	if writer.Code != http.StatusOK || !strings.Contains(writer.Body.String(), "control_epoch") {
		t.Fatalf("operator-with-operator-header status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// AC3: a node token gets 401/403 on every operator route. The operator token
// behaves exactly as on main. The only exceptions are the routes that already
// accepted nodes on main — the /v1/agent websocket and the quota v2
// node-scoped views — plus the three lanes routes this task opens with a
// machine scope.
func TestLanesNodeTokenRouteTable(t *testing.T) {
	lanesPath := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, lanesPath, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"},"lane-b":{"machine":"machine-b","pane":"w9:p9"}}}`)
	hub := lanesNodeHub(t, lanesPath, func(config *HubServerConfig) {
		config.QuotaV2StorePath = filepath.Join(t.TempDir(), "quota-v2.json")
	})

	for _, test := range []struct {
		name     string
		method   string
		path     string
		body     string
		nodeWant []int
		opWant   int
	}{
		{name: "healthz is public", method: http.MethodGet, path: "/healthz", nodeWant: []int{200}, opWant: 200},

		{name: "lanes list scoped for node", method: http.MethodGet, path: "/v1/lanes", nodeWant: []int{200}, opWant: 200},
		{name: "node puts own lane", method: http.MethodPut, path: "/v1/lanes/rt-own", body: `{"machine":"machine-a","pane":"w1:p1"}`, nodeWant: []int{201}, opWant: 200},
		{name: "node cannot put foreign lane", method: http.MethodPut, path: "/v1/lanes/rt-foreign", body: `{"machine":"machine-b","pane":"w9:p1"}`, nodeWant: []int{403}, opWant: 201},
		{name: "node deletes own lane", method: http.MethodDelete, path: "/v1/lanes/lane-a", nodeWant: []int{200}, opWant: 404},
		{name: "node cannot delete foreign lane", method: http.MethodDelete, path: "/v1/lanes/lane-b", nodeWant: []int{403}, opWant: 200},
		{name: "node delete of missing lane is 403", method: http.MethodDelete, path: "/v1/lanes/rt-missing", nodeWant: []int{403}, opWant: 404},

		{name: "nodes", method: http.MethodGet, path: "/v1/nodes", nodeWant: []int{401}, opWant: 200},
		{name: "control-plane transfer", method: http.MethodPost, path: "/v1/control-plane/transfer", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "accepting override", method: http.MethodPost, path: "/v1/nodes/machine-a/accepting", body: `{"mode":"off"}`, nodeWant: []int{401}, opWant: 200},
		{name: "burst", method: http.MethodGet, path: "/v1/burst", nodeWant: []int{401}, opWant: 200},
		{name: "burst request", method: http.MethodPost, path: "/v1/burst/request", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "burst release", method: http.MethodPost, path: "/v1/burst/release", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "burst holds", method: http.MethodGet, path: "/v1/burst/holds", nodeWant: []int{401}, opWant: 200},
		{name: "placement", method: http.MethodGet, path: "/v1/placement", nodeWant: []int{401}, opWant: 400},
		{name: "placement slots", method: http.MethodGet, path: "/v1/placement/slots", nodeWant: []int{401}, opWant: 200},
		{name: "quota", method: http.MethodGet, path: "/v1/quota", nodeWant: []int{401}, opWant: 200},
		{name: "jobs", method: http.MethodGet, path: "/v1/jobs", nodeWant: []int{401}, opWant: 200},
		{name: "orphaned jobs", method: http.MethodGet, path: "/v1/jobs/orphaned", nodeWant: []int{401}, opWant: 200},
		{name: "session reap", method: http.MethodGet, path: "/v1/session-reap", nodeWant: []int{401}, opWant: 200},
		{name: "job reassign", method: http.MethodPost, path: "/v1/jobs/reassign", body: `{}`, nodeWant: []int{401}, opWant: 409},
		{name: "operator events websocket", method: http.MethodGet, path: "/v1/events", nodeWant: []int{401}, opWant: 426},
		{name: "relay ingress", method: http.MethodPost, path: "/v1/relay/events", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "relay held", method: http.MethodGet, path: "/v1/relay/held", nodeWant: []int{401}, opWant: 200},
		{name: "relay held edit", method: http.MethodPatch, path: "/v1/relay/events/1", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "relay held delete", method: http.MethodDelete, path: "/v1/relay/events/1", nodeWant: []int{401}, opWant: 409},
		{name: "spawn", method: http.MethodPost, path: "/v1/spawn", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "spawn status", method: http.MethodGet, path: "/v1/spawn/12345678-1234-4123-8123-123456789abc", nodeWant: []int{401}, opWant: 404},
		{name: "update publish", method: http.MethodPost, path: "/v1/update", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "per-machine quota get", method: http.MethodGet, path: "/v1/quota/machine-a", nodeWant: []int{401}, opWant: 404},
		{name: "per-machine quota request", method: http.MethodPost, path: "/v1/quota/machine-a", body: `{}`, nodeWant: []int{401}, opWant: 503},
		{name: "chat question upsert", method: http.MethodPost, path: "/v1/chat/questions", body: `{}`, nodeWant: []int{401}, opWant: 503},

		{name: "quota v2 binding put", method: http.MethodPut, path: "/v2/quota/bindings", body: `{}`, nodeWant: []int{401}, opWant: 400},
		{name: "quota v2 binding delete", method: http.MethodDelete, path: "/v2/quota/bindings?machine_id=machine-a&provider=claude&local_slot_ref=slot-x", nodeWant: []int{401}, opWant: 404},
		// Pre-existing shared and node routes, unchanged by this task.
		{name: "quota v2 bindings list already node-scoped", method: http.MethodGet, path: "/v2/quota/bindings", nodeWant: []int{200}, opWant: 200},
		{name: "quota v2 observations list needs a binding", method: http.MethodGet, path: "/v2/quota/observations?provider=claude&account_ref=acct_fixture1", nodeWant: []int{403}, opWant: 200},
		{name: "quota v2 observations post is node-only", method: http.MethodPost, path: "/v2/quota/observations", body: `{}`, nodeWant: []int{400}, opWant: 401},
		{name: "agent websocket is the node route", method: http.MethodGet, path: "/v1/agent", nodeWant: []int{200, 400, 426}, opWant: 401},

		// The browser surfaces are gated by authorizeUI/authorizeChat (operator
		// bearer or Cloudflare Access), never by a node credential.
		{name: "ui page", method: http.MethodGet, path: "/ui", nodeWant: []int{404}, opWant: 404},
		{name: "ui data", method: http.MethodGet, path: "/ui/data.json", nodeWant: []int{404}, opWant: 404},
		{name: "chat page", method: http.MethodGet, path: "/chat", nodeWant: []int{404}, opWant: 200},
		{name: "chat data", method: http.MethodGet, path: "/chat/data", nodeWant: []int{404}, opWant: 503},
		{name: "chat message create", method: http.MethodPost, path: "/chat/messages", body: `{}`, nodeWant: []int{404}, opWant: 503},
		{name: "chat message retry", method: http.MethodPost, path: "/chat/messages/1/retry", body: `{}`, nodeWant: []int{404}, opWant: 503},
		{name: "chat message cancel", method: http.MethodPost, path: "/chat/messages/1/cancel", body: `{}`, nodeWant: []int{404}, opWant: 503},
		{name: "chat question transition", method: http.MethodPost, path: "/chat/questions/q-1/transition", body: `{}`, nodeWant: []int{404}, opWant: 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := lanesNodeRequest(t, hub, test.method, test.path, lanesNodeAToken, "machine-a", test.body)
			nodeOK := false
			for _, want := range test.nodeWant {
				if node.Code == want {
					nodeOK = true
				}
			}
			if !nodeOK {
				t.Fatalf("node %s %s status=%d body=%s want one of %v", test.method, test.path, node.Code, node.Body.String(), test.nodeWant)
			}
			operator := lanesNodeRequest(t, hub, test.method, test.path, lanesNodeOperatorToken, "", test.body)
			if operator.Code != test.opWant {
				t.Fatalf("operator %s %s status=%d body=%s want %d", test.method, test.path, operator.Code, operator.Body.String(), test.opWant)
			}
		})
	}
}

// AC5: the lanes CLI sends the token file's HUB_MACHINE_ID as the machine-id
// header, so a mode-0600 node token file drives the scoped rules end to end.
func TestLanesCLINodeTokenEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-b":{"machine":"machine-b","pane":"w9:p9"}}}`)
	hub := lanesNodeHub(t, path, nil)
	var machineHeaderSeen []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		machineHeaderSeen = append(machineHeaderSeen, request.Header.Get(hubMachineIDHeader))
		hub.Handler().ServeHTTP(writer, request)
	}))
	defer server.Close()

	envDir := t.TempDir()
	nodeEnv := filepath.Join(envDir, "node.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=machine-a\nHUB_TOKEN="+lanesNodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	operatorEnv := filepath.Join(envDir, "operator.env")
	if err := os.WriteFile(operatorEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+lanesNodeOperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	var stdout, stderr strings.Builder

	run := func(args ...string) (int, string, string) {
		stdout.Reset()
		stderr.Reset()
		return runLanesCLI(args, &stdout, &stderr, deps), stdout.String(), stderr.String()
	}

	code, out, errOut := run("add", "lane-a", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitOK || !strings.Contains(out, `"lane":"lane-a"`) {
		t.Fatalf("node add code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = run("add", "lane-bad", "--machine", "machine-b", "--pane", "w9:p1", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitConditionInvalid || !strings.Contains(errOut, "lanes rejected by hub") {
		t.Fatalf("node foreign add code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = run("ls", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitOK || !strings.Contains(out, `"lane":"lane-a"`) || strings.Contains(out, `"lane":"lane-b"`) {
		t.Fatalf("node ls code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = run("rm", "lane-b", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitConditionInvalid || !strings.Contains(errOut, "lanes rejected by hub") {
		t.Fatalf("node foreign rm code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = run("rm", "lane-a", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitOK || out != `{"lane":"lane-a","removed":true}`+"\n" {
		t.Fatalf("node rm code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, _, errOut = run("self-check", "--lane", "lane-a", "--expect-machine", "machine-a", "--expect-pane", "w1:p1", "--expect-epoch", "0", "--hub-url", server.URL, "--hub-token-env", nodeEnv)
	if code != ExitConditionInvalid || !strings.Contains(errOut, "invalid operator token env") {
		t.Fatalf("node self-check code=%d stderr=%q", code, errOut)
	}

	// The operator file is unchanged: full listing, every command works.
	code, out, _ = run("add", "lane-b2", "--machine", "machine-b", "--pane", "w9:p2", "--hub-url", server.URL, "--hub-token-env", operatorEnv)
	if code != ExitOK {
		t.Fatalf("operator add code=%d stdout=%q", code, out)
	}
	code, out, _ = run("ls", "--hub-url", server.URL, "--hub-token-env", operatorEnv)
	if code != ExitOK || !strings.Contains(out, `"lane":"lane-b"`) || !strings.Contains(out, "control_epoch") {
		t.Fatalf("operator ls code=%d stdout=%q", code, out)
	}
	code, out, _ = run("rm", "lane-b2", "--hub-url", server.URL, "--hub-token-env", operatorEnv)
	if code != ExitOK {
		t.Fatalf("operator rm code=%d stdout=%q", code, out)
	}

	// Every lanes request carried the machine id from its token file. The
	// refused self-check never reaches the hub: five node requests, three
	// operator requests.
	if len(machineHeaderSeen) != 8 {
		t.Fatalf("machine header seen=%v", machineHeaderSeen)
	}
	for _, seen := range machineHeaderSeen[:5] {
		if seen != "machine-a" {
			t.Fatalf("node header=%q", seen)
		}
	}
	for _, seen := range machineHeaderSeen[5:] {
		if seen != hubOperatorMachineID {
			t.Fatalf("operator header=%q", seen)
		}
	}
}
