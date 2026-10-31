package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const t14OperatorToken = "operator-token"

func t14RequestID(number int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", number)
}

func t14SpawnBody(requestID string) map[string]any {
	return map[string]any{
		"request_id":   requestID,
		"machine":      "machine-a",
		"cwd_key":      "repo-a",
		"brief":        map[string]any{"inline": "implement the scoped change"},
		"args":         []string{"-m", "codex-terra", "-w", "worker", "-l", "label-a", "--t", "T1", "--job", "job-a", "--owner", "lane-a"},
		"wait_seconds": 1,
	}
}

func t14Hub(t *testing.T) (*HubServer, *httptest.Server) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	return hub, server
}

func t14WSURL(serverURL, path string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + path
}

func t14Node(t *testing.T, hub *HubServer, server *httptest.Server, accepting bool) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, "machine-a")
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "hello", "machine_id": "machine-a", "version": "t14", "accepting": accepting}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		record := hub.nodes["machine-a"]
		connected := record != nil && record.agent != nil && record.state == "connected"
		hub.mu.Unlock()
		if connected {
			return connection
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("node did not connect")
	return nil
}

func t14HTTP(t *testing.T, method, url, token string, body any) (int, http.Header, []byte) {
	t.Helper()
	var source *bytes.Reader
	if body == nil {
		source = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		source = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, source)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, contents
}

type t14HTTPResult struct {
	status int
	header http.Header
	body   []byte
}

func t14HTTPAsync(t *testing.T, method, url, token string, body any) <-chan t14HTTPResult {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan t14HTTPResult, 1)
	go func() {
		request, requestErr := http.NewRequest(method, url, bytes.NewReader(encoded))
		if requestErr != nil {
			result <- t14HTTPResult{status: -1, body: []byte(requestErr.Error())}
			return
		}
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
		response, responseErr := http.DefaultClient.Do(request)
		if responseErr != nil {
			result <- t14HTTPResult{status: -1, body: []byte(responseErr.Error())}
			return
		}
		defer response.Body.Close()
		contents, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			result <- t14HTTPResult{status: -1, body: []byte(readErr.Error())}
			return
		}
		result <- t14HTTPResult{status: response.StatusCode, header: response.Header, body: contents}
	}()
	return result
}

func t14ReadNodeSpawn(t *testing.T, connection *websocket.Conn) hubOutboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, payload, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message, valid := parseHubOutbound(payload)
	if !valid || message.Type != "job.spawn" {
		t.Fatalf("message=%s valid=%v", payload, valid)
	}
	return message
}

func t14WriteSpawnResult(t *testing.T, connection *websocket.Conn, requestID string, rc int, jobID, pane, tail, failure string) {
	t.Helper()
	message := map[string]any{"type": "job.spawn.result", "request_id": requestID, "rc": rc, "job_id": jobID, "pane": pane, "stdout_tail": tail}
	if failure != "" {
		message["error"] = failure
	}
	if err := wsjson.Write(t.Context(), connection, message); err != nil {
		t.Fatal(err)
	}
}

func t14DecodeSpawn(t *testing.T, contents []byte) hubSpawnResult {
	t.Helper()
	var response hubSpawnResult
	if err := json.Unmarshal(contents, &response); err != nil {
		t.Fatalf("body=%s err=%v", contents, err)
	}
	return response
}

func t14Result(t *testing.T, results <-chan t14HTTPResult) t14HTTPResult {
	t.Helper()
	select {
	case result := <-results:
		if result.status < 0 {
			t.Fatal(string(result.body))
		}
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP response did not arrive")
		return t14HTTPResult{}
	}
}

func TestT14SpawnHTTPAC1AC4(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	body := t14SpawnBody(t14RequestID(1))
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	if message.RequestID != body["request_id"] || message.CWDKey != "repo-a" || message.BriefInline != "implement the scoped change" || len(message.Args) != 12 {
		t.Fatalf("spawn message=%+v", message)
	}
	t14WriteSpawnResult(t, node, message.RequestID, 0, "job-a", "pane-a", strings.Repeat("x", hubSpawnResultTail), "")
	result := t14Result(t, response)
	if result.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	decoded := t14DecodeSpawn(t, result.body)
	if decoded.RC != 0 || decoded.JobID != "job-a" || decoded.Pane != "pane-a" || len(decoded.StdoutTail) != hubSpawnResultTail || decoded.CompletedAt == nil {
		t.Fatalf("response=%+v", decoded)
	}

	status, _, duplicate := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	if status != http.StatusConflict {
		t.Fatalf("duplicate status=%d body=%s", status, duplicate)
	}
	previous := t14DecodeSpawn(t, duplicate)
	if previous.JobID != "job-a" || previous.CompletedAt == nil {
		t.Fatalf("duplicate did not return original result=%+v", previous)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := node.Read(ctx); err == nil {
		t.Fatal("duplicate request dispatched another job.spawn")
	}
}

func TestT14SpawnHTTPAC2Validation(t *testing.T) {
	_, server := t14Hub(t)
	base := t14SpawnBody(t14RequestID(2))
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"request id", func(body map[string]any) { body["request_id"] = "not-a-uuid" }},
		{"machine", func(body map[string]any) { body["machine"] = "Machine A" }},
		{"cwd key", func(body map[string]any) { body["cwd_key"] = "repo/a" }},
		{"empty inline", func(body map[string]any) { body["brief"] = map[string]any{"inline": ""} }},
		{"both brief forms", func(body map[string]any) {
			body["brief"] = map[string]any{"inline": "text", "doc": "briefs/job/brief.md"}
		}},
		{"both fields even if inline empty", func(body map[string]any) {
			body["brief"] = map[string]any{"inline": "", "doc": "briefs/job/brief.md"}
		}},
		{"neither brief form", func(body map[string]any) { body["brief"] = map[string]any{} }},
		{"wait zero", func(body map[string]any) { body["wait_seconds"] = 0 }},
		{"wait over", func(body map[string]any) { body["wait_seconds"] = 301 }},
		{"unknown flag", func(body map[string]any) { body["args"] = []string{"--job-dup-ok", "yes"} }},
		{"space value", func(body map[string]any) { body["args"] = []string{"-m", "two words"} }},
		{"shell value", func(body map[string]any) { body["args"] = []string{"-m", "x;touch"} }},
		{"brief too long", func(body map[string]any) {
			body["brief"] = map[string]any{"inline": strings.Repeat("x", hubSpawnMaxBriefBytes+1)}
		}},
		{"unknown JSON field", func(body map[string]any) { body["extra"] = true }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			body := t14CloneBody(t, base)
			test.mutate(body)
			status, _, response := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
			if status != http.StatusBadRequest || !bytes.Contains(response, []byte(`"invalid_request"`)) {
				t.Fatalf("status=%d body=%s", status, response)
			}
		})
	}
}

func TestPW6SpawnDocCheckoutRoundTrip(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	body := t14SpawnBody(t14RequestID(90))
	body["brief"] = map[string]any{"doc": "briefs/job-90/brief.md"}
	body["checkout"] = map[string]any{"origin": "https://github.com/acme/repo.git", "mode": "branch", "branch": "feature/pw6", "dir": "repo-job-90"}
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	if message.BriefDoc != "briefs/job-90/brief.md" || message.BriefInline != "" || message.Checkout == nil || message.Checkout.Branch != "feature/pw6" {
		t.Fatalf("spawn message=%+v", message)
	}
	t14WriteSpawnResult(t, node, message.RequestID, 0, "job-90", "pane-90", "ok", "")
	result := t14Result(t, response)
	if result.status != http.StatusOK || t14DecodeSpawn(t, result.body).RC != 0 {
		t.Fatalf("response=%d %s", result.status, result.body)
	}
}

func TestPW6InlineSpawnWireGolden(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	completed := time.Date(2026, 10, 31, 1, 2, 3, 0, time.UTC)
	hub.now = func() time.Time { return completed }
	requestBody := `{"request_id":"00000000-0000-4000-8000-000000000094","machine":"machine-a","cwd_key":"repo-a","brief":{"inline":"implement the scoped change"},"args":["-m","codex-terra","-w","worker","-l","label-a","--t","T1","--job","job-a","--owner","lane-a"],"wait_seconds":1}`
	posted := make(chan t14HTTPResult, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/spawn", strings.NewReader(requestBody))
		req.Header.Set(hubAuthorizationHeader, "Bearer "+t14OperatorToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			posted <- t14HTTPResult{status: -1, body: []byte(err.Error())}
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		posted <- t14HTTPResult{status: resp.StatusCode, body: data}
	}()
	_, raw, err := node.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wantNode := `{"type":"job.spawn","request_id":"00000000-0000-4000-8000-000000000094","cwd_key":"repo-a","brief":{"inline":"implement the scoped change"},"args":["-m","codex-terra","-w","worker","-l","label-a","--t","T1","--job","job-a","--owner","lane-a"],"wait_seconds":1}`
	if string(raw) != wantNode+"\n" {
		t.Fatalf("node request bytes=%s want=%s", raw, wantNode)
	}
	t14WriteSpawnResult(t, node, t14RequestID(94), 0, "job-a", "pane-a", "tail", "")
	got := <-posted
	wantResponse := `{"request_id":"00000000-0000-4000-8000-000000000094","machine":"machine-a","rc":0,"job_id":"job-a","pane":"pane-a","stdout_tail":"tail","completed_at":"2026-10-31T01:02:03Z"}` + "\n"
	if got.status != http.StatusOK || string(got.body) != wantResponse {
		t.Fatalf("response status=%d bytes=%s want=%s", got.status, got.body, wantResponse)
	}
}

func TestPW6SpawnArgsValidation(t *testing.T) {
	valid := []string{"--role", "reviewer", "--builder-family", "openai", "--parent-job", "parent-1", "--builder-job", "builder-1"}
	if !validHubSpawnArgs(valid) {
		t.Fatal("valid new spawn args rejected")
	}
	for _, args := range [][]string{{"--role", "root"}, {"--builder-family", "made-up"}, {"--parent-job", "../secret"}, {"--builder-job", "two words"}, {"--unknown", "value"}} {
		if validHubSpawnArgs(args) {
			t.Errorf("invalid args accepted: %q", args)
		}
	}
}

func TestPW6NodeFetchesBriefDocFromHandoffkeep(t *testing.T) {
	const key = "briefs/job-93/brief.md"
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/v1/documents/briefs/job-93/brief.md" || r.Header.Get("Authorization") != "Bearer node-hk-token" {
			t.Errorf("handoffkeep request method=%s path=%s", r.Method, r.URL.EscapedPath())
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"key":"briefs/job-93/brief.md","body":"document text from hk"}`)
	}))
	defer hk.Close()
	root := t.TempDir()
	hkEnv := filepath.Join(root, "hk.env")
	if err := os.WriteFile(hkEnv, []byte("HANDOFFKEEP_URL="+hk.URL+"\nHANDOFFKEEP_TOKEN=node-hk-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "spawn.json")
	configText := `{"cwd_map":{"repo-a":"/old/cwd"},"workspace":"worker","herdr_session":"worker","handoffkeep_env":"` + hkEnv + `","enabled":true}`
	if err := os.WriteFile(config, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}
	briefPath := filepath.Join(root, "brief")
	t.Setenv("T14_BRIEF_FILE", briefPath)
	t14FakeRuntug(t, "cat \"$5\" > \"$T14_BRIEF_FILE\"\nprintf 'OK pane=p job=j\\n'")
	client := &HubClient{spawnConfigPath: config}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(93), CWDKey: "repo-a", BriefDoc: key, WaitSeconds: 1})
	if result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}
	if got := string(t14WaitFile(t, briefPath)); got != "document text from hk" {
		t.Fatalf("fetched brief=%q", got)
	}
}

func TestPW6CheckoutRefusalSkipsSpawnAndPreparedCWD(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(root, "spawn.json")
	if err := os.WriteFile(config, []byte(`{"cwd_map":{"repo-a":"/old/cwd"},"workspace":"worker","herdr_session":"worker","enabled":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	checkout := &hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: "branch", Branch: "feature/pw6", Dir: "repo-job"}
	for _, outcome := range []string{nodeOutcomeBusy, nodePrepareBranchElsewhere, nodePrepareShaMissing, nodePrepareOriginMismatch, nodeOutcomeDirty, nodeOutcomeGitFailed, nodeOutcomeGitTooOld, nodeRemoveLocked} {
		client := &HubClient{spawnConfigPath: config, spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
			return nodeWorktreePrepareResult{Outcome: outcome}, ""
		}}
		result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(91), CWDKey: "repo-a", BriefInline: "brief", Checkout: checkout, WaitSeconds: 1})
		if result.RC != 81 || result.Error != "checkout_refused:"+outcome {
			t.Errorf("outcome=%s result=%+v", outcome, result)
		}
	}
	argsPath := filepath.Join(root, "args")
	t.Setenv("T14_ARGS_FILE", argsPath)
	t14FakeRuntug(t, "printf '%s\\000' \"$@\" > \"$T14_ARGS_FILE\"\nprintf 'OK pane=p job=j\\n'")
	client := &HubClient{spawnConfigPath: config, spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: filepath.Join(root, "prepared")}}, ""
	}}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(92), CWDKey: "repo-a", BriefInline: "brief", Checkout: checkout, WaitSeconds: 1})
	if result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}
	got := strings.Split(strings.TrimSuffix(string(t14WaitFile(t, argsPath)), "\x00"), "\x00")
	if len(got) < 3 || got[1] != "-c" || got[2] != filepath.Join(root, "prepared") {
		t.Fatalf("argv=%q", got)
	}
}

func TestT14SpawnHTTPAllowsRegexValidLeadingHyphenValue(t *testing.T) {
	_, server := t14Hub(t)
	body := t14SpawnBody(t14RequestID(21))
	body["args"] = []string{"-m", "-model"}
	status, _, response := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	if status != http.StatusServiceUnavailable || !bytes.Contains(response, []byte(`"node_unavailable"`)) {
		t.Fatalf("leading-hyphen value was rejected before availability: status=%d body=%s", status, response)
	}
}

func t14CloneBody(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var copy map[string]any
	if err := json.Unmarshal(encoded, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func TestT14SpawnHTTPAC3Authorization(t *testing.T) {
	_, server := t14Hub(t)
	for _, token := range []string{"", "wrong-token"} {
		status, header, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", token, t14SpawnBody(t14RequestID(3)))
		if status != http.StatusUnauthorized || header.Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("token present=%v status=%d header=%q", token != "", status, header.Get("WWW-Authenticate"))
		}
	}
}

func TestT14SpawnHTTPAC5Unavailable(t *testing.T) {
	hub, server := t14Hub(t)
	status, _, response := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, t14SpawnBody(t14RequestID(5)))
	if status != http.StatusServiceUnavailable || !bytes.Contains(response, []byte(`"error":"node_unavailable"`)) || !bytes.Contains(response, []byte(`"state":"disconnected"`)) {
		t.Fatalf("disconnected status=%d body=%s", status, response)
	}
	_ = t14Node(t, hub, server, false)
	status, _, response = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, t14SpawnBody(t14RequestID(6)))
	if status != http.StatusServiceUnavailable || !bytes.Contains(response, []byte(`"state":"not_accepting"`)) {
		t.Fatalf("not accepting status=%d body=%s", status, response)
	}
}

func TestT14SpawnHTTPAC6PendingThenGet(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	body := t14SpawnBody(t14RequestID(7))
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	pending := t14Result(t, response)
	if pending.status != http.StatusGatewayTimeout || !bytes.Contains(pending.body, []byte(`"status":"pending"`)) {
		t.Fatalf("pending=%d %s", pending.status, pending.body)
	}
	t14WriteSpawnResult(t, node, message.RequestID, 0, "job-a", "pane-a", "later", "")
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawn/"+message.RequestID, t14OperatorToken, nil)
		if status == http.StatusOK && bytes.Contains(body, []byte(`"job_id":"job-a"`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("late result status=%d body=%s", status, body)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestT14SpawnHTTPAC7Lost(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	body := t14SpawnBody(t14RequestID(8))
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	if err := node.CloseNow(); err != nil {
		t.Fatal(err)
	}
	if result := t14Result(t, response); result.status != http.StatusOK || !bytes.Contains(result.body, []byte(`"status":"lost"`)) {
		t.Fatalf("post response=%d %s", result.status, result.body)
	}
	deadline := time.Now().Add(time.Second)
	for {
		status, _, response := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawn/"+message.RequestID, t14OperatorToken, nil)
		if status == http.StatusOK && bytes.Contains(response, []byte(`"status":"lost"`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost lookup=%d %s", status, response)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestT14SpawnHTTPAC8NodeRejections(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	for index, failure := range []string{"spawn_disabled", "cwd_unmapped"} {
		body := t14SpawnBody(t14RequestID(10 + index))
		response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
		message := t14ReadNodeSpawn(t, node)
		t14WriteSpawnResult(t, node, message.RequestID, 2, "", "", "", failure)
		result := t14Result(t, response)
		decoded := t14DecodeSpawn(t, result.body)
		if result.status != http.StatusOK || decoded.RC != 2 || decoded.Error != failure {
			t.Fatalf("failure=%s status=%d response=%+v", failure, result.status, decoded)
		}
	}
}

func TestT14SpawnHTTPAC14EventsAreMetadataOnly(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	headers := http.Header{}
	headers.Set(hubAuthorizationHeader, "Bearer "+t14OperatorToken)
	events, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/events"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer events.CloseNow()
	body := t14SpawnBody(t14RequestID(20))
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := t14ReadNodeSpawn(t, node)
	t14WriteSpawnResult(t, node, message.RequestID, 0, "job-a", "pane-a", "done", "")
	if result := t14Result(t, response); result.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	seen := make(map[string]map[string]any)
	for len(seen) < 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, payload, readErr := events.Read(ctx)
		cancel()
		if readErr != nil {
			t.Fatal(readErr)
		}
		var event struct {
			Kind    string         `json:"kind"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.Kind == "spawn.requested" || event.Kind == "spawn.result" {
			seen[event.Kind] = event.Payload
		}
	}
	for kind, payload := range seen {
		if _, found := payload["brief"]; found {
			t.Fatalf("%s leaked brief: %v", kind, payload)
		}
		if _, found := payload["args"]; found {
			t.Fatalf("%s leaked args: %v", kind, payload)
		}
	}
	if seen["spawn.requested"]["request_id"] != body["request_id"] || seen["spawn.requested"]["machine"] != "machine-a" || seen["spawn.result"]["rc"] != float64(0) {
		t.Fatalf("events=%v", seen)
	}
}

func t14SpawnConfig(t *testing.T, cwd string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spawn.json")
	contents := []byte(`{"cwd_map":{"repo-a":"` + cwd + `"},"workspace":"worker","herdr_session":"worker","enabled":true}`)
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func t14FakeRuntug(t *testing.T, script string) {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "runtug")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	// HOME must not leak the developer's real ~/.local/bin/runtug, which the
	// resolver prefers over PATH.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func t14WaitFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil {
			return contents
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("file was not created: %s", path)
	return nil
}

func TestT14NodeAC9AC10AC13ArgvAndTemporary(t *testing.T) {
	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	sessionPath := filepath.Join(root, "session")
	temporaryPath := filepath.Join(root, "temporary")
	parentPath := filepath.Join(root, "parent")
	releasePath := filepath.Join(root, "release")
	markerPath := filepath.Join(root, "shell-marker")
	t.Setenv("T14_ARGS_FILE", argsPath)
	t.Setenv("T14_SESSION_FILE", sessionPath)
	t.Setenv("T14_TEMPORARY_FILE", temporaryPath)
	t.Setenv("T14_PARENT_FILE", parentPath)
	t.Setenv("T14_RELEASE_FILE", releasePath)
	t.Setenv("T14_SHELL_MARKER", markerPath)
	t14FakeRuntug(t, `printf '%s\000' "$@" > "$T14_ARGS_FILE"
printf '%s' "$HERDR_SESSION" > "$T14_SESSION_FILE"
printf '%s' "$5" > "$T14_TEMPORARY_FILE"
ps -o comm= -p "$PPID" > "$T14_PARENT_FILE"
while [ ! -f "$T14_RELEASE_FILE" ]; do sleep 0.01; done
printf 'OK pane=pane-a model=m label=l status=s landed=y job=job-a quota_record=q\n'`)

	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnTimeoutExtra: 20 * time.Millisecond}
	message := hubOutboundMessage{RequestID: t14RequestID(30), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"-m", "model-a", "--owner", "x; touch $T14_SHELL_MARKER"}, WaitSeconds: 1}
	completed := make(chan hubSpawnResult, 1)
	go func() { completed <- client.runHubSpawn(t.Context(), message) }()
	temporary := strings.TrimSpace(string(t14WaitFile(t, temporaryPath)))
	info, err := os.Stat(temporary)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("temporary=%s info=%v err=%v", temporary, info, err)
	}
	if err := os.WriteFile(releasePath, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	result := <-completed
	if result.RC != 0 || result.JobID != "job-a" || result.Pane != "pane-a" || !strings.HasPrefix(result.StdoutTail, "OK pane=") {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(temporary); !os.IsNotExist(err) {
		t.Fatalf("temporary remains after success: %v", err)
	}
	gotArgs := strings.Split(strings.TrimSuffix(string(t14WaitFile(t, argsPath)), "\x00"), "\x00")
	wantArgs := append([]string{"spawn", "-c", root, "-p", temporary, "--host", "local", "-w", "worker"}, append(message.Args, "--override-reason", hubSpawnOverrideReason)...)
	if strings.Join(gotArgs, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("argv=%q want=%q", gotArgs, wantArgs)
	}
	if session := string(t14WaitFile(t, sessionPath)); session != "worker" {
		t.Fatalf("HERDR_SESSION=%q", session)
	}
	if parent := strings.TrimSpace(string(t14WaitFile(t, parentPath))); parent == "sh" {
		t.Fatalf("runtug was invoked through a shell: parent=%q", parent)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("shell metacharacter was expanded: %v", err)
	}
	if _, ok := parseHubOutbound([]byte(`{"type":"job.spawn","request_id":"00000000-0000-4000-8000-000000000031","cwd_key":"repo-a","brief":{"inline":"brief"},"args":["-m","x;touch"],"wait_seconds":1}`)); ok {
		t.Fatal("node accepted a metacharacter-bearing spawn directive")
	}
	if pane, jobID := parseHubSpawnOK([]byte("not an OK line\n")); pane != "" || jobID != "" {
		t.Fatalf("missing OK line parsed pane=%q job=%q", pane, jobID)
	}
}

func TestT14NodeAC10FailureAndAC11TimeoutCleanup(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		root := t.TempDir()
		temporaryPath := filepath.Join(root, "temporary")
		t.Setenv("T14_TEMPORARY_FILE", temporaryPath)
		t14FakeRuntug(t, `printf '%s' "$5" > "$T14_TEMPORARY_FILE"
exit 7`)
		client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnTimeoutExtra: 20 * time.Millisecond}
		result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(32), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1})
		temporary := strings.TrimSpace(string(t14WaitFile(t, temporaryPath)))
		if result.RC != 7 || result.Error != "spawn_failed" {
			t.Fatalf("result=%+v", result)
		}
		if _, err := os.Stat(temporary); !os.IsNotExist(err) {
			t.Fatalf("temporary remains after failure: %v", err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		root := t.TempDir()
		temporaryPath := filepath.Join(root, "temporary")
		childPath := filepath.Join(root, "child")
		t.Setenv("T14_TEMPORARY_FILE", temporaryPath)
		t.Setenv("T14_CHILD_FILE", childPath)
		t14FakeRuntug(t, `printf '%s' "$5" > "$T14_TEMPORARY_FILE"
sleep 10 &
printf '%s' "$!" > "$T14_CHILD_FILE"
wait`)
		client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnTimeoutExtra: 20 * time.Millisecond}
		started := time.Now()
		result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(33), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1})
		if result.Error != "timeout" || time.Since(started) > 3*time.Second {
			t.Fatalf("result=%+v elapsed=%s", result, time.Since(started))
		}
		temporary := strings.TrimSpace(string(t14WaitFile(t, temporaryPath)))
		if _, err := os.Stat(temporary); !os.IsNotExist(err) {
			t.Fatalf("temporary remains after timeout: %v", err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(t14WaitFile(t, childPath))))
		if err != nil {
			t.Fatal(err)
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for processRunning(process, pid) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if processRunning(process, pid) {
			t.Fatalf("timed-out process group left child %d alive", pid)
		}
	})
}

func TestT14NodeSpawnConfigRejections(t *testing.T) {
	message := hubOutboundMessage{RequestID: t14RequestID(34), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1}
	disabled := (&HubClient{spawnConfigPath: filepath.Join(t.TempDir(), "missing.json")}).runHubSpawn(t.Context(), message)
	if disabled.RC != 2 || disabled.Error != "spawn_disabled" {
		t.Fatalf("disabled=%+v", disabled)
	}
	path := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(path, []byte(`{"cwd_map":{},"workspace":"worker","herdr_session":"worker","enabled":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	unmapped := (&HubClient{spawnConfigPath: path}).runHubSpawn(t.Context(), message)
	if unmapped.RC != 2 || unmapped.Error != "cwd_unmapped" {
		t.Fatalf("unmapped=%+v", unmapped)
	}
}

func TestT14NodeAC12SerialAndIdempotent(t *testing.T) {
	root := t.TempDir()
	ledger := filepath.Join(root, "ledger")
	t.Setenv("T14_LEDGER", ledger)
	t14FakeRuntug(t, `printf 'begin\n' >> "$T14_LEDGER"
sleep 0.05
printf 'end\n' >> "$T14_LEDGER"
printf 'OK pane=pane-a model=m label=l status=s landed=y job=job-a quota_record=q\n'`)
	configPath := t14SpawnConfig(t, root)
	results := make(chan []map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		collected := make([]map[string]any, 0, 2)
		for received := 0; received < 2; {
			var message map[string]any
			if wsjson.Read(t.Context(), connection, &message) != nil {
				return
			}
			if message["type"] == "hello" {
				for _, requestID := range []string{t14RequestID(40), t14RequestID(41), t14RequestID(40)} {
					if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "job.spawn", "request_id": requestID, "cwd_key": "repo-a", "brief": map[string]any{"inline": "brief"}, "args": []string{"-m", "model-a"}, "wait_seconds": 1}); err != nil {
						return
					}
				}
				continue
			}
			if message["type"] == "job.spawn.result" {
				received++
				collected = append(collected, message)
			}
		}
		results <- collected
	}))
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, SpawnConfigPath: configPath, PingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.serve(ctx, connection) }()
	var got []map[string]any
	select {
	case got = <-results:
	case <-time.After(3 * time.Second):
		t.Fatal("node did not return two spawn results")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("node client did not stop")
	}
	if len(got) != 2 || got[0]["request_id"] == got[1]["request_id"] {
		t.Fatalf("results=%v", got)
	}
	if entries := strings.Fields(string(t14WaitFile(t, ledger))); strings.Join(entries, ",") != "begin,end,begin,end" {
		t.Fatalf("spawns overlapped or duplicate ran: %v", entries)
	}
}

// processRunning reports whether pid is alive and not a zombie. The killed
// child is reparented to pid 1, and a container init that never reaps leaves
// it as a zombie that still answers signal 0; an unreaped child of ours does
// the same until Wait.
func processRunning(process *os.Process, pid int) bool {
	if process.Signal(syscall.Signal(0)) != nil {
		return false
	}
	return !processZombie(pid)
}

// processZombie reports whether pid is in the zombie state. Linux exposes the
// state in /proc; macOS has no /proc, so it asks ps. An unreadable state reads
// as not a zombie, so a broken probe makes the caller wait out its deadline
// and fail, instead of passing vacuously.
func processZombie(pid int) bool {
	if runtime.GOOS == "linux" {
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			return false
		}
		// The state field follows the parenthesised command name.
		if end := bytes.LastIndexByte(stat, ')'); end >= 0 && end+2 < len(stat) {
			return stat[end+2] == 'Z'
		}
		return false
	}
	out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	return err == nil && bytes.HasPrefix(bytes.TrimSpace(out), []byte("Z"))
}

func TestProcessRunningIgnoresUnreapedKilledChild(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Wait() })
	pid := cmd.Process.Pid
	if !processRunning(cmd.Process, pid) {
		t.Fatal("live child reported as not running")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	// Our own child stays a zombie until Wait, which is the case under test.
	eventually(t, 5*time.Second, "killed unreaped child to stop counting as running", func() bool {
		return !processRunning(cmd.Process, pid)
	})
}

func TestHubSpawnRuntugPathResolution(t *testing.T) {
	lookup := func(good map[string]bool) func(string) (string, error) {
		return func(name string) (string, error) {
			if good[name] {
				return "/usr/local/bin/" + name, nil
			}
			return "", errors.New("not found")
		}
	}
	stat := func(existing ...string) func(string) bool {
		set := map[string]bool{}
		for _, p := range existing {
			set[p] = true
		}
		return func(path string) bool { return set[path] }
	}
	for _, tc := range []struct {
		name       string
		configured string
		existing   []string
		onPath     bool
		want       string
	}{
		{"configured wins", "/opt/tools/runtug", []string{"/opt/tools/runtug"}, true, "/opt/tools/runtug"},
		{"configured missing is fatal", "/opt/tools/runtug", nil, true, ""},
		{"home bin fallback", "", []string{"/h/.local/bin/runtug"}, true, "/h/.local/bin/runtug"},
		{"home bin beats PATH", "", []string{"/h/.local/bin/runtug"}, true, "/h/.local/bin/runtug"},
		{"PATH fallback", "", nil, true, "/usr/local/bin/runtug"},
		{"nothing found", "", nil, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := hubSpawnRuntugPath(tc.configured, "/h", stat(tc.existing...), lookup(map[string]bool{"runtug": tc.onPath}))
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
