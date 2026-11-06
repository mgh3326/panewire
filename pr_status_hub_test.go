package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	prHubServiceToken = "svc-pr-fixture-token-not-for-logs"
	prHubOtherToken   = "svc-other-fixture-token-not-for-logs"
)

// prStatusHub is a hub whose mergewake host is machine-a, with one service
// that holds the pr.status scope and one that does not.
func prStatusHub(t *testing.T, host string) (*HubServer, *httptest.Server) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:       map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "svc-pr": prHubServiceToken, "svc-other": prHubOtherToken},
		Scopes:       map[string]string{"svc-pr": "node.command:pr.status@machine-a", "svc-other": "node.command:agents.list@machine-a"},
		PRStatusHost: host,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	t.Cleanup(func() { _ = hub.Close() })
	return hub, server
}

func prStatusServiceCall(t *testing.T, server *httptest.Server, machine, client, token string, body any) (int, []byte) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, nodeCommandURL(server, machine), bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(hubMachineIDHeader, client)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	contents, _ := io.ReadAll(response.Body)
	return response.StatusCode, contents
}

var prStatusCommandBody = map[string]any{"op": "pr.status", "args": map[string]any{"repo": "owner/name", "pr": 152, "fresh": false}}

func prStatusNodeAnswer(t *testing.T) map[string]any {
	t.Helper()
	var snapshot PRStatusSnapshot
	if err := json.Unmarshal([]byte(prStatusFixture), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = boundPRStatusSnapshot(snapshot)
	return map[string]any{"result": nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot},
		HK:        nodePRStatusHK{Outcome: prStatusNotFound, Status: 404},
	}}
}

// pr.status is refused for every machine but the configured mergewake host,
// before a node sees it, for the operator and for a service holding @*.
func TestHubPRStatusRefusedOffTheMergewakeHost(t *testing.T) {
	hub, server := prStatusHub(t, "oci")
	connection := t14Node(t, hub, server, true) // machine-a, not the host
	status, contents := prStatusServiceCall(t, server, "machine-a", "operator", t14OperatorToken, prStatusCommandBody)
	if status != http.StatusBadRequest || nodeCommandDecode(t, contents)["error"] != "wrong_machine" {
		t.Fatalf("operator status=%d body=%s", status, contents)
	}
	// Nothing was sent: the first frame the node reads is the next command.
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, map[string]any{"op": "agents.list"})
	if command := nodeCommandRead(t, connection); string(command["op"]) != `"agents.list"` {
		t.Fatalf("first command the node saw: %v", command)
	}
	_ = connection.CloseNow()
	if result := t14Result(t, results); result.status != http.StatusServiceUnavailable {
		t.Fatalf("lost node status=%d body=%s", result.status, result.body)
	}
}

func TestHubPRStatusRoundTripOnTheHost(t *testing.T) {
	hub, server := prStatusHub(t, "machine-a")
	connection := t14Node(t, hub, server, true)
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, prStatusCommandBody)
	command := nodeCommandRead(t, connection)
	var sent map[string]any
	if err := json.Unmarshal(command["args"], &sent); err != nil || string(command["op"]) != `"pr.status"` || len(sent) != 3 || sent["repo"] != "owner/name" || sent["pr"] != float64(152) || sent["fresh"] != false {
		t.Fatalf("node received %s %s", command["op"], command["args"])
	}
	nodeCommandReplyTo(t, connection, command, prStatusNodeAnswer(t))
	result := t14Result(t, results)
	body := nodeCommandDecode(t, result.body)
	merge := body["result"].(map[string]any)["mergewake"].(map[string]any)
	if result.status != http.StatusOK || merge["outcome"] != "ok" || merge["snapshot"].(map[string]any)["state"] != "blocked" || body["result"].(map[string]any)["hk"].(map[string]any)["outcome"] != "not_found" {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
}

func TestHubPRStatusScope(t *testing.T) {
	hub, server := prStatusHub(t, "machine-a")
	connection := t14Node(t, hub, server, true)
	if status, contents := prStatusServiceCall(t, server, "machine-a", "svc-other", prHubOtherToken, prStatusCommandBody); status != http.StatusForbidden || nodeCommandDecode(t, contents)["error"] != "scope_forbidden" {
		t.Fatalf("service without the scope: status=%d body=%s", status, contents)
	}
	if status, _ := prStatusServiceCall(t, server, "machine-a", "svc-pr", "wrong-token-value", prStatusCommandBody); status != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d", status)
	}
	// The scoped service is admitted, on the host only.
	done := make(chan struct{})
	var status int
	var contents []byte
	go func() {
		defer close(done)
		status, contents = prStatusServiceCall(t, server, "machine-a", "svc-pr", prHubServiceToken, prStatusCommandBody)
	}()
	command := nodeCommandRead(t, connection)
	nodeCommandReplyTo(t, connection, command, prStatusNodeAnswer(t))
	<-done
	if status != http.StatusOK || nodeCommandDecode(t, contents)["result"] == nil {
		t.Fatalf("scoped service: status=%d body=%s", status, contents)
	}
}

func TestHubPRStatusRejectsBadArgsBeforeSending(t *testing.T) {
	hub, server := prStatusHub(t, "machine-a")
	_ = t14Node(t, hub, server, true)
	for name, args := range map[string]any{
		"missing fresh": map[string]any{"repo": "owner/name", "pr": 1},
		"bad repo":      map[string]any{"repo": "owner/../x", "pr": 1, "fresh": false},
		"extra field":   map[string]any{"repo": "owner/name", "pr": 1, "fresh": false, "path": "/etc"},
		"zero pr":       map[string]any{"repo": "owner/name", "pr": 0, "fresh": false},
	} {
		status, contents := prStatusServiceCall(t, server, "machine-a", "operator", t14OperatorToken, map[string]any{"op": "pr.status", "args": args})
		if status != http.StatusBadRequest || strings.Contains(string(contents), "wrong_machine") {
			t.Errorf("%s: status=%d body=%s", name, status, contents)
		}
	}
}

func TestHubPRStatusHostConfig(t *testing.T) {
	for _, host := range []string{"operator", "Bad Host", "has/slash"} {
		if _, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, PRStatusHost: host}); err == nil {
			t.Errorf("host %q accepted", host)
		}
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}})
	if err != nil || hub.prStatusHost != "oci" {
		t.Fatalf("default host %q err=%v", hub.prStatusHost, err)
	}
	if _, err := newHubAuthSet(map[string]string{"svc-pr": prHubServiceToken}, map[string]string{"svc-pr": "node.command:pr.status@oci"}); err != nil {
		t.Fatalf("pr.status scope refused: %v", err)
	}
}

func TestValidNodePRStatusResultPairsOutcomeWithFields(t *testing.T) {
	var snapshot PRStatusSnapshot
	if err := json.Unmarshal([]byte(prStatusFixture), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = boundPRStatusSnapshot(snapshot)
	ok := nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot}
	hkNone := nodePRStatusHK{Outcome: prStatusUnconfigured}
	for name, tc := range map[string]struct {
		result nodePRStatusResult
		valid  bool
	}{
		"ok with snapshot":            {nodePRStatusResult{ok, hkNone}, true},
		"ok without snapshot":         {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusOK}, hkNone}, false},
		"not_found with snapshot":     {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusNotFound, Snapshot: &snapshot}, hkNone}, false},
		"not_found":                   {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusNotFound}, hkNone}, true},
		"failed with a reason":        {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonTimeout}, hkNone}, true},
		"failed with a free reason":   {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusFailed, Reason: "token ghp_x rejected"}, hkNone}, false},
		"failed without a reason":     {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusFailed}, hkNone}, false},
		"unknown outcome":             {nodePRStatusResult{nodePRStatusMergewake{Outcome: "weird"}, hkNone}, false},
		"hk ok with change":           {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: json.RawMessage(`{"a":1}`)}}, true},
		"hk ok without change":        {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusOK, Status: 200}}, false},
		"hk ok with a non-object":     {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: json.RawMessage(`[1]`)}}, false},
		"hk not_found carries a body": {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusNotFound, Status: 404, Change: json.RawMessage(`{}`)}}, false},
		"hk 404":                      {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusNotFound, Status: 404}}, true},
		"hk failed free reason":       {nodePRStatusResult{ok, nodePRStatusHK{Outcome: prStatusFailed, Reason: "Bearer abc"}}, false},
		"hk unknown":                  {nodePRStatusResult{ok, nodePRStatusHK{Outcome: "weird"}}, false},
	} {
		if got := validNodePRStatusResult(tc.result); got != tc.valid {
			t.Errorf("%s: valid=%v want %v", name, got, tc.valid)
		}
	}
}
