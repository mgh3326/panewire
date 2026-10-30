package panewire

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	pw2OperatorToken = "pw2-fixture-operator-token-not-for-logs"
	pw2NodeAToken    = "pw2-fixture-node-a-token-not-for-logs"
	pw2NodeBToken    = "pw2-fixture-node-b-token-not-for-logs"
	pw2SpawnID       = "0d9c6a52-3f1e-4a7b-8c2d-5e6f7a8b9c0d"
	// pw2MergewakeScopes is operator decision D1 for the reaper.
	pw2MergewakeScopes = "nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:pane.capture@*,node.command:pane.close@*"
)

type pw2LockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *pw2LockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *pw2LockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func pw2ServiceToken(client string) string { return "pw2-fixture-" + client + "-token-not-for-logs" }

type pw2Caller struct {
	name, id, token string
}

func (caller pw2Caller) do(t *testing.T, server *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if caller.id != "" {
		request.Header.Set(hubMachineIDHeader, caller.id)
	}
	if caller.token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+caller.token)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func pw2Hub(t *testing.T, scopes map[string]string, authPath string, logs io.Writer) (*HubServer, *httptest.Server) {
	t.Helper()
	tokens := map[string]string{"operator": pw2OperatorToken, "node-a": pw2NodeAToken, "node-b": pw2NodeBToken}
	for client := range scopes {
		tokens[client] = pw2ServiceToken(client)
	}
	if logs == nil {
		logs = io.Discard
	}
	now := time.Date(2026, 10, 30, 3, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: tokens, Scopes: scopes, AuthPath: authPath,
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"},"lane-b":{"machine":"node-b","pane":"w2:p1"}}}`),
		Now:             func() time.Time { return now },
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	return hub, server
}

// TestHubAuthScopesPerRoute is the per-route table: operator, node, a
// service with the scope, without it, and with a scope naming another
// machine or lane. Granted means the request got past authorization (the
// fixture has no live node or inbox, so the handler then answers 404/503).
func TestHubAuthScopesPerRoute(t *testing.T) {
	scopes := map[string]string{
		"svc-ok":        "nodes.agents.read,node.command:agents.list@node-a,spawn@node-a,wake@node-a,inbox.read:lane-a,inbox.ack:lane-a",
		"svc-none":      "inbox.read:lane-z",
		"svc-other":     "node.command:agents.list@node-b,node.command:pane.close@node-a,spawn@node-b,wake@node-b,inbox.read:lane-b,inbox.ack:lane-b",
		"svc-wild":      "nodes.agents.read,node.command:agents.list@*,spawn@*,wake@*",
		"svc-readonly":  "inbox.read:lane-a",
		"svc-mergewake": pw2MergewakeScopes,
	}
	hub, server := pw2Hub(t, scopes, "", nil)
	hub.mu.Lock()
	hub.spawnRecords[pw2SpawnID] = &hubSpawnRecord{request: hubSpawnRequest{RequestID: pw2SpawnID, Machine: "node-a", CWDKey: "repo", Brief: hubSpawnBrief{Inline: "x"}, Args: []string{}, WaitSeconds: 1}, status: "pending", created: hub.now().UTC(), waiter: make(chan hubSpawnResult, 1)}
	hub.mu.Unlock()

	type route struct {
		name, method, path, body string
	}
	routes := []route{
		{"nodes", "GET", "/v1/nodes", ""},
		{"nodes-agents", "GET", "/v1/nodes/agents", ""},
		{"session-reap", "GET", "/v1/session-reap", ""},
		{"node-command", "POST", "/v1/nodes/node-a/command", `{"op":"agents.list","args":{}}`},
		{"spawn", "POST", "/v1/spawn", `{"request_id":"7b1e2c3d-4a5b-4c6d-8e7f-9a0b1c2d3e4f","machine":"node-a","cwd_key":"repo","brief":{"inline":"brief"},"args":[],"wait_seconds":1}`},
		{"spawn-get", "GET", "/v1/spawn/" + pw2SpawnID, ""},
		{"wake", "POST", "/v1/nodes/wake", `{"machine":"node-a"}`},
		{"inbox-list", "GET", "/v1/inbox?lane=lane-a", ""},
		{"inbox-count", "GET", "/v1/inbox/count?lane=lane-a", ""},
		{"inbox-wait", "GET", "/v1/inbox/wait?lane=lane-a&timeout=1ms", ""},
		{"inbox-read", "POST", "/v1/inbox/1/read?lane=lane-a", ""},
		{"inbox-ack", "POST", "/v1/inbox/ack?lane=lane-a", `{"id":1}`},
	}
	callers := []pw2Caller{
		{"operator", "", pw2OperatorToken},
		{"node-a", "node-a", pw2NodeAToken},
		{"node-b", "node-b", pw2NodeBToken},
		{"anonymous", "", ""},
	}
	for client := range scopes {
		callers = append(callers, pw2Caller{client, client, pw2ServiceToken(client)})
	}
	callers = append(callers, pw2Caller{"svc-ok-wrong-token", "svc-ok", pw2NodeAToken})

	// G granted, D 401, F 403 — in routes order.
	want := map[string]string{
		"operator":           "GGGGGGGGGGGG",
		"node-a":             "DDDDDDDGGGGG",
		"node-b":             "DDDDDDDDDDDD",
		"anonymous":          "DDDDDDDDDDDD",
		"svc-ok":             "GGGGGGGGGGGG",
		"svc-none":           "FFFFFFFFFFFF",
		"svc-other":          "FFFFFFFFFFFF",
		"svc-wild":           "GGGGGGGFFFFF",
		"svc-readonly":       "FFFFFFFGGGGF",
		"svc-mergewake":      "GGGGFFFFFFFF",
		"svc-ok-wrong-token": "DDDDDDDDDDDD",
	}
	for _, caller := range callers {
		for index, route := range routes {
			status, body := caller.do(t, server, route.method, route.path, route.body)
			got := "G"
			switch status {
			case http.StatusUnauthorized:
				got = "D"
			case http.StatusForbidden:
				got = "F"
				if !strings.Contains(body, `"scope_forbidden"`) {
					t.Errorf("%s %s: 403 body %q", caller.name, route.name, body)
				}
			}
			if expected := want[caller.name][index : index+1]; got != expected {
				t.Errorf("%s %s: status %d (%s), want %s", caller.name, route.name, status, got, expected)
			}
		}
	}

	// The op is part of the scope: pane.close@node-a does not grant agents.list.
	status, _ := pw2Caller{"svc-other", "svc-other", pw2ServiceToken("svc-other")}.do(t, server, "POST", "/v1/nodes/node-a/command", `{"op":"agents.list","args":{}}`)
	if status != http.StatusForbidden {
		t.Fatalf("op outside scope: status %d", status)
	}
	// A lane scope has no wildcard and the lane must come from ?lane=.
	status, _ = pw2Caller{"svc-ok", "svc-ok", pw2ServiceToken("svc-ok")}.do(t, server, "POST", "/v1/inbox/ack", `{"id":1,"lane":"lane-a"}`)
	if status != http.StatusForbidden {
		t.Fatalf("ack without ?lane=: status %d", status)
	}
}

// TestHubAuthServiceIsNeverANode covers the node-only surfaces: the agent
// websocket and the lanes API.
func TestHubAuthServiceIsNeverANode(t *testing.T) {
	_, server := pw2Hub(t, map[string]string{"svc-mergewake": pw2MergewakeScopes}, "", nil)
	headers := make(http.Header)
	headers.Set(hubMachineIDHeader, "svc-mergewake")
	headers.Set(hubAuthorizationHeader, "Bearer "+pw2ServiceToken("svc-mergewake"))
	connection, response, err := websocket.Dial(t.Context(), r6WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err == nil {
		connection.CloseNow()
		t.Fatal("service token opened a node websocket")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("service websocket response=%v", response)
	}
	node := r6DialAgent(t, server.URL, "node-a", pw2NodeAToken)
	node.CloseNow()

	service := pw2Caller{"svc", "svc-mergewake", pw2ServiceToken("svc-mergewake")}
	if status, _ := service.do(t, server, "GET", "/v1/lanes", ""); status != http.StatusUnauthorized {
		t.Fatalf("service lanes status %d", status)
	}
	if status, _ := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, "GET", "/v1/lanes", ""); status != http.StatusOK {
		t.Fatalf("node lanes status %d", status)
	}
	// Operator-only routes stay operator-only.
	for _, path := range []string{"/v1/jobs", "/v1/placement", "/v1/events", "/v1/relay/held"} {
		if status, _ := service.do(t, server, "GET", path, ""); status != http.StatusUnauthorized {
			t.Fatalf("service %s status %d", path, status)
		}
	}
}

// TestHubAuthNodeSharingOperatorToken keeps the pre-scope rule that a node is
// admitted on its own token even when an auth file gives it the operator's.
func TestHubAuthNodeSharingOperatorToken(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": pw2OperatorToken, "node-a": pw2OperatorToken},
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	node := r6DialAgent(t, server.URL, "node-a", pw2OperatorToken)
	node.CloseNow()
	if status, _ := (pw2Caller{"node", "node-a", pw2OperatorToken}).do(t, server, "GET", "/v1/lanes", ""); status != http.StatusOK {
		t.Fatalf("node lanes status %d", status)
	}
}

func TestHubAuthConfigRejects(t *testing.T) {
	base := "HUB_TOKEN_operator=" + pw2OperatorToken + "\nHUB_TOKEN_node-a=" + pw2NodeAToken + "\n"
	service := "HUB_TOKEN_svc-a=" + pw2ServiceToken("svc-a") + "\n"
	cases := map[string]string{
		"operator scoped":      base + "HUB_SCOPES_operator=nodes.agents.read\n",
		"scopes without token": base + "HUB_SCOPES_svc-a=nodes.agents.read\n",
		"unknown scope":        base + service + "HUB_SCOPES_svc-a=nodes.write\n",
		"unknown op":           base + service + "HUB_SCOPES_svc-a=node.command:pane.kill@*\n",
		"no resource":          base + service + "HUB_SCOPES_svc-a=spawn\n",
		"operator machine":     base + service + "HUB_SCOPES_svc-a=spawn@operator\n",
		"lane wildcard":        base + service + "HUB_SCOPES_svc-a=inbox.read:*\n",
		"empty item":           base + service + "HUB_SCOPES_svc-a=nodes.agents.read,\n",
		"empty line":           base + service + "HUB_SCOPES_svc-a=\n",
		"shared token":         base + "HUB_TOKEN_svc-a=" + pw2NodeAToken + "\nHUB_SCOPES_svc-a=nodes.agents.read\n",
		"other key":            base + "HUB_GRANT_svc-a=x\n",
	}
	for name, body := range cases {
		path := filepath.Join(t.TempDir(), "hub-auth.env")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadHubAuthConfig(path); err == nil {
			t.Errorf("%s: loaded", name)
		} else if strings.Contains(err.Error(), "fixture") {
			t.Errorf("%s: error reflects a value: %v", name, err)
		}
	}

	path := filepath.Join(t.TempDir(), "hub-auth.env")
	if err := os.WriteFile(path, []byte(base+service+"HUB_SCOPES_svc-a= "+pw2MergewakeScopes+" \n"), 0600); err != nil {
		t.Fatal(err)
	}
	set, err := loadHubAuthConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, node := set.tokens["svc-a"]; node || len(set.services["svc-a"].grants) != 5 || set.tokens["node-a"] != pw2NodeAToken {
		t.Fatalf("service split wrong: tokens=%d services=%d", len(set.tokens), len(set.services))
	}
	if _, err := parseHubAlertNodes("svc-a", set.tokens); err == nil {
		t.Fatal("a service was accepted as an alert node")
	}
}

func pw2WriteAuth(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	temporary := path + ".next"
	if err := os.WriteFile(temporary, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(temporary, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}

// TestHubAuthHotReload: adding a line enables a client, removing it revokes
// it, and a bad file keeps the previous set with one alert per bad version.
func TestHubAuthHotReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-auth.env")
	base := "HUB_TOKEN_operator=" + pw2OperatorToken + "\nHUB_TOKEN_node-a=" + pw2NodeAToken + "\nHUB_TOKEN_node-b=" + pw2NodeBToken + "\n"
	svcA := "HUB_TOKEN_svc-a=" + pw2ServiceToken("svc-a") + "\nHUB_SCOPES_svc-a=nodes.agents.read\n"
	svcB := "HUB_TOKEN_svc-b=" + pw2ServiceToken("svc-b") + "\nHUB_SCOPES_svc-b=nodes.agents.read\n"
	pw2WriteAuth(t, path, base+svcA, 0600)
	logs := &pw2LockedBuffer{}
	_, server := pw2Hub(t, map[string]string{"svc-a": "nodes.agents.read"}, path, logs)
	a := pw2Caller{"svc-a", "svc-a", pw2ServiceToken("svc-a")}
	b := pw2Caller{"svc-b", "svc-b", pw2ServiceToken("svc-b")}
	expect := func(label string, caller pw2Caller, want int) {
		t.Helper()
		if status, _ := caller.do(t, server, "GET", "/v1/nodes", ""); status != want {
			t.Fatalf("%s: %s status %d, want %d", label, caller.name, status, want)
		}
	}
	expect("start", a, http.StatusOK)
	expect("start", b, http.StatusUnauthorized)

	pw2WriteAuth(t, path, base+svcB, 0600)
	expect("swap", a, http.StatusUnauthorized)
	expect("swap", b, http.StatusOK)

	// A scope change takes effect: svc-b loses nodes.agents.read.
	pw2WriteAuth(t, path, base+"HUB_TOKEN_svc-b="+pw2ServiceToken("svc-b")+"\nHUB_SCOPES_svc-b=wake@*\n", 0600)
	expect("narrowed", b, http.StatusForbidden)
	pw2WriteAuth(t, path, base+svcB, 0600)
	expect("restored", b, http.StatusOK)

	for label, bad := range map[string]struct {
		body string
		mode os.FileMode
	}{
		"bad scope":        {base + svcA + "HUB_TOKEN_svc-b=" + pw2ServiceToken("svc-b") + "\nHUB_SCOPES_svc-b=nodes.agents.write\n", 0600},
		"missing operator": {"HUB_TOKEN_svc-a=" + pw2ServiceToken("svc-a") + "\nHUB_SCOPES_svc-a=nodes.agents.read\n", 0600},
		"wrong mode":       {base + svcA, 0644},
	} {
		before := strings.Count(logs.String(), "hub auth reload failed")
		pw2WriteAuth(t, path, bad.body, bad.mode)
		expect(label, b, http.StatusOK)
		expect(label, a, http.StatusUnauthorized)
		expect(label, b, http.StatusOK)
		if got := strings.Count(logs.String(), "hub auth reload failed") - before; got != 1 {
			t.Fatalf("%s: %d reload alerts, want 1", label, got)
		}
		pw2WriteAuth(t, path, base+svcB, 0600)
		expect(label+" repaired", b, http.StatusOK)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	expect("removed file", b, http.StatusOK)
	if status, _ := (pw2Caller{"operator", "", pw2OperatorToken}).do(t, server, "GET", "/v1/nodes", ""); status != http.StatusOK {
		t.Fatalf("operator after removed file: %d", status)
	}
	for _, marker := range []string{pw2OperatorToken, pw2NodeAToken, pw2ServiceToken("svc-a"), pw2ServiceToken("svc-b")} {
		if strings.Contains(logs.String(), marker) {
			t.Fatal("hub logs exposed a fixture credential")
		}
	}
}

func TestHubTokenMintScript(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	root := t.TempDir()
	envPath := filepath.Join(root, "svc-mergewake.env")
	run := func(args ...string) ([]byte, error) {
		command := exec.Command("bash", append([]string{"deploy/hub-token-mint.sh"}, args...)...)
		command.Stderr = io.Discard
		return command.Output()
	}
	lines, err := run("svc-mergewake", envPath, pw2MergewakeScopes)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	info, err := os.Stat(envPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("client env file: info=%v err=%v", info, err)
	}
	client, err := loadMode0600Env(envPath)
	if err != nil || client["HUB_MACHINE_ID"] != "svc-mergewake" || len(client["HUB_TOKEN"]) != 64 {
		t.Fatalf("client env file content invalid: err=%v", err)
	}
	authPath := filepath.Join(root, "hub-auth.env")
	if err := os.WriteFile(authPath, append([]byte("HUB_TOKEN_operator="+pw2OperatorToken+"\n"), lines...), 0600); err != nil {
		t.Fatal(err)
	}
	set, err := loadHubAuthConfig(authPath)
	if err != nil {
		t.Fatalf("minted lines do not load: %v", err)
	}
	if service := set.services["svc-mergewake"]; service.token != client["HUB_TOKEN"] || len(service.grants) != 5 {
		t.Fatal("minted service does not match its env file")
	}

	if _, err := run("svc-mergewake", envPath, pw2MergewakeScopes); err == nil {
		t.Fatal("mint overwrote an existing env file")
	}
	for _, args := range [][]string{{"operator", filepath.Join(root, "op.env")}, {"Bad_ID", filepath.Join(root, "bad.env")}, {"svc-x", filepath.Join(root, "x.env"), "a b"}} {
		if _, err := run(args...); err == nil {
			t.Fatalf("mint accepted %q", args)
		}
	}
	// Without scopes it mints a node credential.
	nodeLines, err := run("node-c", filepath.Join(root, "node-c.env"))
	if err != nil || strings.Contains(string(nodeLines), "HUB_SCOPES_") || !strings.HasPrefix(string(nodeLines), "HUB_TOKEN_node-c=") {
		t.Fatalf("node mint: err=%v", err)
	}
}

// TestHubAuthFileOverReadLimit: a file over the env reader's bound is refused
// whole, at startup and on reload, instead of loading without its tail — a
// dropped HUB_SCOPES_ line would otherwise turn a service into a node.
func TestHubAuthFileOverReadLimit(t *testing.T) {
	base := "HUB_TOKEN_operator=" + pw2OperatorToken + "\nHUB_TOKEN_node-a=" + pw2NodeAToken + "\nHUB_TOKEN_svc-a=" + pw2ServiceToken("svc-a") + "\n"
	padding := strings.Repeat("#"+strings.Repeat("x", 1023)+"\n", envFileMaxBytes/1024)
	oversized := base + padding + "HUB_SCOPES_svc-a=nodes.agents.read\n"
	path := filepath.Join(t.TempDir(), "hub-auth.env")
	pw2WriteAuth(t, path, oversized, 0600)
	if _, err := loadHubAuthConfig(path); err == nil {
		t.Fatal("an auth file over the read limit loaded")
	}

	pw2WriteAuth(t, path, base+"HUB_SCOPES_svc-a=nodes.agents.read\n", 0600)
	logs := &pw2LockedBuffer{}
	_, server := pw2Hub(t, map[string]string{"svc-a": "nodes.agents.read"}, path, logs)
	service := pw2Caller{"svc-a", "svc-a", pw2ServiceToken("svc-a")}
	pw2WriteAuth(t, path, oversized, 0600)
	if status, _ := service.do(t, server, "GET", "/v1/nodes", ""); status != http.StatusOK {
		t.Fatalf("service after oversized reload: status %d", status)
	}
	headers := make(http.Header)
	headers.Set(hubMachineIDHeader, "svc-a")
	headers.Set(hubAuthorizationHeader, "Bearer "+pw2ServiceToken("svc-a"))
	if connection, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers}); err == nil {
		connection.CloseNow()
		t.Fatal("service became a node through an oversized auth file")
	}
	if !strings.Contains(logs.String(), "hub auth reload failed") {
		t.Fatal("oversized reload raised no alert")
	}
}

// TestHubAuthInPlaceRewriteReloads: rotating a token in place with the same
// length and the old mtime restored still reloads (the change time moves).
func TestHubAuthInPlaceRewriteReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-auth.env")
	base := "HUB_TOKEN_operator=" + pw2OperatorToken + "\nHUB_TOKEN_node-a=" + pw2NodeAToken + "\nHUB_TOKEN_node-b=" + pw2NodeBToken + "\n"
	oldToken, newToken := pw2ServiceToken("svc-a"), pw2ServiceToken("svc-z")
	pw2WriteAuth(t, path, base+"HUB_TOKEN_svc-a="+oldToken+"\nHUB_SCOPES_svc-a=nodes.agents.read\n", 0600)
	_, server := pw2Hub(t, map[string]string{"svc-a": "nodes.agents.read"}, path, nil)
	if status, _ := (pw2Caller{"old", "svc-a", oldToken}).do(t, server, "GET", "/v1/nodes", ""); status != http.StatusOK {
		t.Fatalf("old token before rotation: %d", status)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(base+"HUB_TOKEN_svc-a="+newToken+"\nHUB_SCOPES_svc-a=nodes.agents.read\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("fixture did not keep inode, size and mtime: err=%v", err)
	}
	if status, _ := (pw2Caller{"old", "svc-a", oldToken}).do(t, server, "GET", "/v1/nodes", ""); status != http.StatusUnauthorized {
		t.Fatalf("rotated-out token still accepted: %d", status)
	}
	if status, _ := (pw2Caller{"new", "svc-a", newToken}).do(t, server, "GET", "/v1/nodes", ""); status != http.StatusOK {
		t.Fatalf("rotated-in token: %d", status)
	}
}

// TestHubAuthReloadKeepsAlertNodesNodes: a reload that would make a watched
// node a service keeps the previous set.
func TestHubAuthReloadKeepsAlertNodesNodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-auth.env")
	base := "HUB_TOKEN_operator=" + pw2OperatorToken + "\nHUB_TOKEN_node-a=" + pw2NodeAToken + "\n"
	pw2WriteAuth(t, path, base, 0600)
	logs := &pw2LockedBuffer{}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": pw2OperatorToken, "node-a": pw2NodeAToken},
		AuthPath:        path,
		AlertNodes:      map[string]struct{}{"node-a": {}},
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	node := pw2Caller{"node-a", "node-a", pw2NodeAToken}
	pw2WriteAuth(t, path, base+"HUB_SCOPES_node-a=nodes.agents.read\n", 0600)
	if status, _ := node.do(t, server, "GET", "/v1/lanes", ""); status != http.StatusOK {
		t.Fatalf("watched node lost node access on reload: %d", status)
	}
	if status, _ := node.do(t, server, "GET", "/v1/nodes", ""); status != http.StatusUnauthorized {
		t.Fatalf("watched node gained a service scope: %d", status)
	}
	if !strings.Contains(logs.String(), "alert_node_scoped") {
		t.Fatal("refused reload raised no alert")
	}
	// Removing the watched node's lines is a revocation and still loads.
	pw2WriteAuth(t, path, "HUB_TOKEN_operator="+pw2OperatorToken+"\n", 0600)
	if status, _ := node.do(t, server, "GET", "/v1/lanes", ""); status != http.StatusUnauthorized {
		t.Fatalf("revoked watched node: %d", status)
	}
}

// TestHubAuthWakeTimeoutBeforeScope: a malformed wake body answers 400 to any
// authenticated caller, before the scope check.
func TestHubAuthWakeTimeoutBeforeScope(t *testing.T) {
	_, server := pw2Hub(t, map[string]string{"svc-none": "inbox.read:lane-z"}, "", nil)
	status, _ := (pw2Caller{"svc-none", "svc-none", pw2ServiceToken("svc-none")}).do(t, server, "POST", "/v1/nodes/wake", `{"machine":"node-a","timeout":"bogus"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("invalid wake timeout: status %d, want 400", status)
	}
}
