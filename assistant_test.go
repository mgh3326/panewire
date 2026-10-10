package panewire

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Planted secrets for AC7: every failure path is exercised and then the
// captured outputs are grepped for these exact strings.
const (
	assistantTestHKToken    = "hk-token-PLANT-7f3a9c"
	assistantTestBerryToken = "berry-token-PLANT-4d8e"
	assistantTestCFSecret   = "cf-secret-PLANT-2b6f"
	assistantTestCFID       = "cf-id-PLANT-1a3c"
	assistantTestAUD        = "assistant-test-aud"
	assistantTestLocation   = "planted-location-99z"
)

// fakeAssistantHK serves the PR-1 response shapes and records every request
// line. With failNonGET it fails the test on any non-GET request — the wire
// assertion behind AC3's "no write endpoint" clause.
type fakeAssistantHK struct {
	t      *testing.T
	server *httptest.Server

	mu        sync.Mutex
	requests  []string
	cfHeaders [][2]string
	pending   assistantPending
	tasks     map[int64]assistantTask
	questions map[string]assistantChatQuestion
	outbox    []assistantOutboxRow
	relays    []handoffkeepRelayEvent
	// statusFor and bodyFor force one path's response (failure injection).
	statusFor  map[string]int
	bodyFor    map[string]string
	failNonGET bool
}

func newFakeAssistantHK(t *testing.T) *fakeAssistantHK {
	f := &fakeAssistantHK{
		t:         t,
		tasks:     map[int64]assistantTask{},
		questions: map[string]assistantChatQuestion{},
		statusFor: map[string]int{},
		bodyFor:   map[string]string{},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeAssistantHK) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	f.cfHeaders = append(f.cfHeaders, [2]string{r.Header.Get("Cf-Access-Client-Id"), r.Header.Get("Cf-Access-Client-Secret")})
	fail := f.failNonGET
	f.mu.Unlock()
	if fail && r.Method != http.MethodGet {
		f.t.Errorf("assistant client issued non-GET request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	path := r.URL.Path
	f.mu.Lock()
	status, overriddenStatus := f.statusFor[path]
	body, overriddenBody := f.bodyFor[path]
	f.mu.Unlock()
	if overriddenBody {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
		return
	}
	if overriddenStatus {
		w.Header().Set("Location", "http://attacker.example/"+assistantTestLocation)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"upstream planted body ` + assistantTestLocation + `"}`))
		return
	}
	switch {
	case path == "/v1/assistant/pending":
		f.writeJSON(w, f.snapshotPending())
	case path == "/v1/assistant/outbox":
		f.writeJSON(w, map[string]any{"notifications": f.snapshotOutbox()})
	case path == "/v1/tasks":
		lane := r.URL.Query().Get("lane")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		f.mu.Lock()
		var tasks []assistantTask
		for _, task := range f.tasks {
			if lane == "" || task.Lane == lane {
				tasks = append(tasks, task)
			}
		}
		f.mu.Unlock()
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
		if limit > 0 && len(tasks) > limit {
			tasks = tasks[:limit]
		}
		f.writeJSON(w, map[string]any{"tasks": tasks})
	case strings.HasPrefix(path, "/v1/tasks/"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(path, "/v1/tasks/"), 10, 64)
		f.mu.Lock()
		task, ok := f.tasks[id]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found"}`))
			return
		}
		f.writeJSON(w, task)
	case strings.HasPrefix(path, "/v1/chat/questions/"):
		id, _ := url.PathUnescape(strings.TrimPrefix(path, "/v1/chat/questions/"))
		f.mu.Lock()
		question, ok := f.questions[id]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"chat_question_not_found"}`))
			return
		}
		f.writeJSON(w, question)
	case path == "/v1/relay/events":
		lane := r.URL.Query().Get("lane")
		undelivered := r.URL.Query().Get("undelivered") == "1"
		afterID, _ := strconv.ParseInt(r.URL.Query().Get("after_id"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		f.mu.Lock()
		var events []handoffkeepRelayEvent
		for _, row := range f.relays {
			if lane != "" && row.OwnerLane != lane {
				continue
			}
			if undelivered && row.DeliveredAt != "" {
				continue
			}
			if row.ID <= afterID {
				continue
			}
			events = append(events, row)
		}
		f.mu.Unlock()
		sort.Slice(events, func(i, j int) bool { return events[i].ID < events[j].ID })
		if limit > 0 && len(events) > limit {
			events = events[:limit]
		}
		f.writeJSON(w, map[string]any{"events": events})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
	}
}

func (f *fakeAssistantHK) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeAssistantHK) snapshotPending() assistantPending {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.pending
	if out.ServerTime.IsZero() {
		// A real hk always stamps server_time; only a test fixture leaves it
		// zero, and the client treats a missing clock as an invalid response.
		out.ServerTime = time.Now().UTC()
	}
	out.DecisionRequests = append([]assistantPendingDecision{}, f.pending.DecisionRequests...)
	out.ChatQuestions = append([]assistantChatQuestion{}, f.pending.ChatQuestions...)
	return out
}

func (f *fakeAssistantHK) snapshotOutbox() []assistantOutboxRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]assistantOutboxRow{}, f.outbox...)
}

func (f *fakeAssistantHK) requestLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.requests...)
}

// cfHeaderLog returns the CF-Access-Client-{Id,Secret} header pair each
// request carried — the M12 assertion surface for the outbound CF path.
func (f *fakeAssistantHK) cfHeaderLog() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]string{}, f.cfHeaders...)
}

func (f *fakeAssistantHK) seedTask(task assistantTask) {
	f.mu.Lock()
	f.tasks[task.ID] = task
	f.mu.Unlock()
}

func (f *fakeAssistantHK) seedQuestion(q assistantChatQuestion) {
	f.mu.Lock()
	f.questions[q.ID] = q
	f.mu.Unlock()
}

func writeMode0600(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

const assistantTestTargets = `{"targets":{"ops":{"kind":"lane","lane":"lane-a","description":"ops lane"},"desk":{"kind":"conversation","conversation":"operator-desk"}}}`

// assistantTestServer builds the server against a fake hk. mutate adjusts the
// config before construction (CF gate, hub pair, bad paths).
func assistantTestServer(t *testing.T, hk *fakeAssistantHK, mutate func(*assistantConfig)) (*assistantServer, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	tokenFile := writeMode0600(t, dir, "berry.token", assistantTestBerryToken)
	targetsFile := writeMode0600(t, dir, "targets.json", assistantTestTargets)
	cfg := assistantConfig{
		Listen:      "127.0.0.1:0",
		HKURL:       hk.server.URL,
		HKToken:     assistantTestHKToken,
		TokenFile:   tokenFile,
		TargetsFile: targetsFile,
		ClientName:  "berry-test",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	logs := &bytes.Buffer{}
	deps := assistantServerDeps{Logger: slog.New(slog.NewJSONHandler(logs, nil))}
	srv, err := newAssistantServer(cfg, deps)
	if err != nil {
		t.Fatalf("newAssistantServer: %v", err)
	}
	return srv, logs
}

// assistantServe feeds one request through the full auth+route stack.
func assistantServe(t *testing.T, srv *assistantServer, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	writer := httptest.NewRecorder()
	srv.ServeHTTP(writer, request)
	return writer
}

type assistantToolResult struct {
	isError  bool
	text     string
	decoded  map[string]any
	rpcErr   *assistantRPCError
	httpCode int
}

// assistantCall invokes one tool over the JSON-RPC surface and decodes the
// CallToolResult text payload.
func assistantCall(t *testing.T, srv *assistantServer, token, tool string, args map[string]any) assistantToolResult {
	t.Helper()
	params, err := json.Marshal(map[string]any{"name": tool, "arguments": args})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": json.RawMessage(params)})
	if err != nil {
		t.Fatal(err)
	}
	writer := assistantServe(t, srv, token, http.MethodPost, "/mcp", string(body))
	out := assistantToolResult{httpCode: writer.Code}
	if writer.Code != http.StatusOK {
		return out
	}
	var response struct {
		Result *struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *assistantRPCError `json:"error"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("rpc response undecodable: %v body=%q", err, writer.Body.String())
	}
	out.rpcErr = response.Error
	if response.Result == nil {
		return out
	}
	out.isError = response.Result.IsError
	if len(response.Result.Content) != 1 {
		t.Fatalf("tool result content blocks=%d, want 1", len(response.Result.Content))
	}
	out.text = response.Result.Content[0].Text
	if !out.isError {
		if err := json.Unmarshal([]byte(out.text), &out.decoded); err != nil {
			t.Fatalf("tool payload undecodable: %v text=%q", err, out.text)
		}
	}
	return out
}

func assistantCallError(t *testing.T, srv *assistantServer, token, tool string, args map[string]any) string {
	t.Helper()
	result := assistantCall(t, srv, token, tool, args)
	if !result.isError {
		t.Fatalf("%s(%v) expected tool error, got %s", tool, args, result.text)
	}
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(result.text), &payload); err != nil {
		t.Fatalf("tool error undecodable: %v text=%q", err, result.text)
	}
	return payload.Error
}

func assistantFixturePending(now time.Time) assistantPending {
	return assistantPending{
		ServerTime: now,
		DecisionRequests: []assistantPendingDecision{
			{TaskID: 42, Lane: "lane-a", State: "needs_decision", Title: "ship the release",
				Request: assistantDecisionRequest{ID: "dr-42-3", Revision: 3, Status: "open", Question: "ship?", DefaultAction: "wait", RequestedBy: "wrk-a", RequestedAt: now}},
			// Terminal tasks must never surface — even if upstream sends one.
			{TaskID: 43, Lane: "lane-b", State: "merged", Title: "merged",
				Request: assistantDecisionRequest{ID: "dr-43-1", Revision: 1, Status: "open", Question: "stale", DefaultAction: "wait", RequestedBy: "wrk-b", RequestedAt: now}},
			{TaskID: 44, Lane: "lane-c", State: "dropped", Title: "dropped",
				Request: assistantDecisionRequest{ID: "dr-44-1", Revision: 1, Status: "open", Question: "stale", HumanOnly: true, DefaultAction: "wait", RequestedBy: "wrk-b", RequestedAt: now}},
		},
		ChatQuestions: []assistantChatQuestion{
			{ID: "Q-20261010-01", ConversationID: "operator-desk", Lane: "lane-a", Body: "which lane?", State: "pending", Revision: 2, CreatedAt: now, UpdatedAt: now},
		},
	}
}

// AC1: the mapping file's mode is enforced at startup and on every read, an
// unknown id fails closed with a named error, and no argument shape lets a
// caller supply a lane, URL, route or command.
func TestAssistantTargetsAC1(t *testing.T) {
	hk := newFakeAssistantHK(t)
	dir := t.TempDir()
	tokenFile := writeMode0600(t, dir, "berry.token", assistantTestBerryToken)

	// A targets file that is not mode 0600 refuses startup.
	looseTargets := writeMode0600(t, dir, "loose.json", assistantTestTargets)
	if err := os.Chmod(looseTargets, 0644); err != nil {
		t.Fatal(err)
	}
	cfg := assistantConfig{Listen: "127.0.0.1:0", HKURL: hk.server.URL, HKToken: assistantTestHKToken, TokenFile: tokenFile, TargetsFile: looseTargets}
	if _, err := newAssistantServer(cfg, assistantServerDeps{}); err == nil || !strings.Contains(err.Error(), "mode-0600") {
		t.Fatalf("loose targets file: err=%v, want mode-0600 refusal", err)
	}
	// A token file that is not mode 0600 refuses startup.
	looseToken := writeMode0600(t, dir, "loose.token", assistantTestBerryToken)
	if err := os.Chmod(looseToken, 0640); err != nil {
		t.Fatal(err)
	}
	cfg.TokenFile, cfg.TargetsFile = looseToken, writeMode0600(t, dir, "targets.json", assistantTestTargets)
	if _, err := newAssistantServer(cfg, assistantServerDeps{}); err == nil || !strings.Contains(err.Error(), "mode-0600") {
		t.Fatalf("loose token file: err=%v, want mode-0600 refusal", err)
	}
	// A token file that is a symlink refuses startup.
	linkToken := filepath.Join(dir, "link.token")
	if err := os.Symlink(looseToken, linkToken); err != nil {
		t.Fatal(err)
	}
	cfg.TokenFile = linkToken
	if _, err := newAssistantServer(cfg, assistantServerDeps{}); err == nil {
		t.Fatal("symlink token file accepted")
	}

	srv, _ := assistantTestServer(t, hk, nil)

	// The tool surface exposes only opaque ids.
	listed := assistantCall(t, srv, assistantTestBerryToken, "targets", nil)
	if listed.isError {
		t.Fatalf("targets error: %s", listed.text)
	}
	targets, _ := listed.decoded["targets"].([]any)
	if len(targets) != 2 {
		t.Fatalf("targets=%v, want 2 entries", targets)
	}
	for _, entry := range targets {
		row, _ := entry.(map[string]any)
		if row["id"] != "desk" && row["id"] != "ops" {
			t.Fatalf("unexpected target row %v", row)
		}
	}

	// Unknown ids fail closed on every tool that takes a target.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "nowhere"}); got != "unknown_target" {
		t.Fatalf("unknown target error=%q", got)
	}
	// A caller-supplied URL or lane-shaped string is just an unknown id.
	for _, hostile := range []string{"http://evil.example/x", "lane-a", "ops; rm -rf /", "../secrets"} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": hostile}); got != "unknown_target" {
			t.Fatalf("hostile target %q error=%q", hostile, got)
		}
	}
	// A caller-supplied lane/URL/route field is rejected by the schema.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops", "lane": "lane-b"}); got != "invalid_arguments" {
		t.Fatalf("extra lane field error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "targets", map[string]any{"url": "http://x"}); got != "invalid_arguments" {
		t.Fatalf("extra url field error=%q", got)
	}
	// The hostile ids never reached hk: no request was dispatched for them.
	for _, line := range hk.requestLog() {
		if strings.Contains(line, "evil") || strings.Contains(line, "secrets") {
			t.Fatalf("hostile input reached hk: %q", line)
		}
	}

	// A chmod away from 0600 after startup fails the next read closed.
	targetsPath := srv.targetsPath
	if err := os.Chmod(targetsPath, 0644); err != nil {
		t.Fatal(err)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "targets", nil); got != "targets_file_invalid" {
		t.Fatalf("post-chmod targets error=%q", got)
	}
	if err := os.Chmod(targetsPath, 0600); err != nil {
		t.Fatal(err)
	}
}

// AC2 first half: missing, wrong and empty bearers each get 401 and no tool
// executes. The comparison is constant-time — asserted at source level since
// timing cannot be measured reliably in a unit test.
func TestAssistantBearerAC2(t *testing.T) {
	hk := newFakeAssistantHK(t)
	srv, logs := assistantTestServer(t, hk, nil)

	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"poll","arguments":{}}}`
	for name, token := range map[string]string{"missing": "", "empty": " ", "wrong": "berry-token-PLANT-aaaa"} {
		request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		writer := httptest.NewRecorder()
		srv.ServeHTTP(writer, request)
		if writer.Code != http.StatusUnauthorized {
			t.Fatalf("%s bearer status=%d, want 401", name, writer.Code)
		}
	}
	// A scheme other than Bearer grants nothing.
	basic := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call))
	basic.Header.Set("Authorization", "Basic "+assistantTestBerryToken)
	writer := httptest.NewRecorder()
	srv.ServeHTTP(writer, basic)
	if writer.Code != http.StatusUnauthorized {
		t.Fatalf("basic-scheme status=%d, want 401", writer.Code)
	}
	if len(hk.requestLog()) != 0 {
		t.Fatalf("unauthenticated requests reached hk: %v", hk.requestLog())
	}
	if !strings.Contains(logs.String(), "unauthorized") {
		t.Fatal("401 requests were not audit logged")
	}

	// Constant-time compare is the code-level contract — assert the
	// implementation actually uses subtle.ConstantTimeCompare on the token.
	source, err := os.ReadFile("assistant.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "subtle.ConstantTimeCompare") {
		t.Fatal("bearer comparison does not use subtle.ConstantTimeCompare")
	}
	if strings.Contains(string(source), `token == `) {
		t.Fatal("bearer comparison uses a plain string equality")
	}
}

// AC2 second half: with the Cloudflare gate on, a verified assertion still
// needs an allowlisted common_name — the identity binding the hub verifier
// lacks.
func TestAssistantCFBindingAC2(t *testing.T) {
	hk := newFakeAssistantHK(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, logs := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFServiceNames = []string{"berry.svc", "berry-alt.svc"}
	})

	call := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	serve := func(jwt string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call))
		request.Header.Set("Authorization", "Bearer "+assistantTestBerryToken)
		if jwt != "" {
			request.Header.Set("Cf-Access-Jwt-Assertion", jwt)
		}
		writer := httptest.NewRecorder()
		srv.ServeHTTP(writer, request)
		return writer
	}
	now := time.Now()
	sign := func(claims map[string]any) string {
		return chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	}
	// Bearer alone is not enough once the CF gate is configured.
	if writer := serve(""); writer.Code != http.StatusUnauthorized {
		t.Fatalf("no JWT status=%d, want 401", writer.Code)
	}
	// A valid assertion whose common_name is not on the allowlist is refused —
	// this is the mutant-3 gate.
	stranger := sign(map[string]any{"aud": []string{assistantTestAUD}, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "common_name": "intruder.svc"})
	if writer := serve(stranger); writer.Code != http.StatusUnauthorized {
		t.Fatalf("unlisted common_name status=%d, want 401", writer.Code)
	}
	// No common_name at all is refused: an Access user login must not act as
	// the assistant service.
	userLogin := sign(map[string]any{"aud": []string{assistantTestAUD}, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "email": "op@example.test"})
	if writer := serve(userLogin); writer.Code != http.StatusUnauthorized {
		t.Fatalf("missing common_name status=%d, want 401", writer.Code)
	}
	// The allowlisted service identity passes and is audit-logged under its
	// bound name, not the bearer client label.
	allowed := sign(map[string]any{"aud": []string{assistantTestAUD}, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "common_name": "berry.svc"})
	if writer := serve(allowed); writer.Code != http.StatusOK {
		t.Fatalf("allowlisted common_name status=%d body=%q, want 200", writer.Code, writer.Body.String())
	}
	if !strings.Contains(logs.String(), "service:berry.svc") {
		t.Fatalf("bound identity not in audit log: %s", logs.String())
	}
}

// AC3: the tool inventory is exactly the five read tools, and no code path
// can issue a non-GET request to handoffkeep.
func TestAssistantReadOnlyAC3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(time.Now().UTC())
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-01", ConversationID: "operator-desk", Lane: "lane-a", Body: "which lane?", State: "pending", Revision: 2})
	hk.failNonGET = true
	srv, _ := assistantTestServer(t, hk, nil)

	writer := assistantServe(t, srv, assistantTestBerryToken, http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	var response struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, tool := range response.Result.Tools {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	want := []string{"pending_detail", "pending_list", "poll", "progress", "targets"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("tool set %v, want exactly %v", names, want)
	}

	// Every tool against the GET-only fake.
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"targets", nil},
		{"pending_list", nil},
		{"pending_detail", map[string]any{"id": "dr-42-3"}},
		{"pending_detail", map[string]any{"id": "Q-20261010-01"}},
		{"progress", map[string]any{"target": "ops"}},
		{"progress", map[string]any{"request_id": "dr-42-3"}},
		{"poll", nil},
	} {
		assistantCall(t, srv, assistantTestBerryToken, tc.tool, tc.args)
	}
	if len(hk.requestLog()) == 0 {
		t.Fatal("no hk reads recorded — tools did not reach the fake")
	}
}

// AC4: pending_list and pending_detail carry stable id, revision, human_only,
// status and server_time; merged and dropped tasks never appear — even when
// the upstream payload includes one (the binary is the trust boundary).
func TestAssistantPendingAC4(t *testing.T) {
	hk := newFakeAssistantHK(t)
	now := time.Now().UTC().Truncate(time.Second)
	hk.pending = assistantFixturePending(now)
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-01", ConversationID: "operator-desk", Lane: "lane-a", Body: "which lane?", State: "pending", Revision: 2, CreatedAt: now, UpdatedAt: now})
	hk.seedTask(assistantTask{ID: 42, Lane: "lane-a", Title: "ship the release", State: "needs_decision", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-42-3", Revision: 3, Status: "open", Question: "ship?", DefaultAction: "wait", RequestedBy: "wrk-a", RequestedAt: now}}})
	srv, _ := assistantTestServer(t, hk, nil)

	listed := assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil)
	if listed.isError {
		t.Fatalf("pending_list error: %s", listed.text)
	}
	if got := listed.decoded["server_time"]; got != now.Format(time.RFC3339Nano) {
		t.Fatalf("server_time=%v, want %v", got, now)
	}
	items, _ := listed.decoded["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items=%v, want 2 (merged/dropped filtered)", items)
	}
	seen := map[string]map[string]any{}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		id, _ := item["id"].(string)
		seen[id] = item
		if id == "dr-43-1" || id == "dr-44-1" {
			t.Fatalf("terminal task surfaced in pending_list: %v", item)
		}
		if item["server_time"] == nil || item["server_time"] == "" {
			t.Fatalf("item %v lacks server_time", item)
		}
	}
	dr := seen["dr-42-3"]
	if dr == nil {
		t.Fatalf("dr-42-3 missing from %v", seen)
	}
	if dr["revision"].(float64) != 3 || dr["kind"] != "decision_request" || dr["status"] != "open" || dr["human_only"] != false || dr["key"] != "dr-42-3:3" {
		t.Fatalf("decision item malformed: %v", dr)
	}
	if dr["server_time"] == nil || dr["server_time"] == "" {
		t.Fatalf("item lacks server_time: %v", dr)
	}
	q := seen["Q-20261010-01"]
	if q == nil || q["revision"].(float64) != 2 || q["kind"] != "chat_question" || q["status"] != "pending" {
		t.Fatalf("question item malformed: %v", q)
	}

	// Detail on a current open request: pending true, full item.
	detail := assistantCall(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-42-3"})
	if detail.isError || detail.decoded["pending"] != true {
		t.Fatalf("detail pending=%v err=%v", detail.decoded["pending"], detail.text)
	}
	// A superseded revision fails closed with the same generic not_found as a
	// missing id — the live id is never echoed.
	hk.seedTask(assistantTask{ID: 45, Lane: "lane-a", State: "needs_decision", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-45-2", Revision: 2, Status: "open", Question: "newer", DefaultAction: "wait", RequestedBy: "wrk-a", RequestedAt: now}}})
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-45-1"}); got != "request_not_found" {
		t.Fatalf("superseded detail error=%q", got)
	}
	// Detail on a pending question and on a nonexistent id.
	if detail := assistantCall(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-01"}); detail.isError || detail.decoded["pending"] != true {
		t.Fatalf("question detail pending=%v err=%v", detail.decoded["pending"], detail.text)
	}
	if detail := assistantCall(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-01"}); detail.decoded["server_time"] == nil {
		t.Fatalf("question detail lacks server_time: %v", detail.decoded)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-99"}); got != "request_not_found" {
		t.Fatalf("missing question error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "whatever"}); got != "request_not_found" {
		t.Fatalf("unparseable id error=%q", got)
	}
	// A settled item is not an open item: detail fails closed with the same
	// generic not_found rather than reporting pending:false.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-02", ConversationID: "operator-desk", Lane: "lane-a", Body: "old", State: "resolved", Revision: 3, CreatedAt: now, UpdatedAt: now})
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-02"}); got != "request_not_found" {
		t.Fatalf("settled question detail error=%q, want request_not_found", got)
	}
}

// AC5: the closed progress vocabulary, produced from hk fields only, with
// receipts — and a persisted-but-undelivered relay row is never done.
func TestAssistantProgressAC5(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	mkTask := func(id int64, state string, req *assistantDecisionRequest) assistantTask {
		task := assistantTask{ID: id, Lane: "lane-a", Title: "t", State: state}
		task.Refs.DecisionRequest = req
		return task
	}
	openReq := func(id string, rev int) *assistantDecisionRequest {
		return &assistantDecisionRequest{ID: id, Revision: rev, Status: "open", Question: "q", DefaultAction: "wait", RequestedBy: "wrk", RequestedAt: now}
	}
	resolvedReq := func(id string, rev int, kind, responder string) *assistantDecisionRequest {
		r := openReq(id, rev)
		r.Status = kind
		r.Resolution = &assistantDecisionResolution{Kind: kind, Option: "go", Responder: responder, By: "op", At: now}
		return r
	}

	cases := []struct {
		name      string
		requestID string
		task      *assistantTask
		question  *assistantChatQuestion
		outbox    []assistantOutboxRow
		relays    []handoffkeepRelayEvent
		want      string
		reason    string
	}{
		{name: "open request is decision-pending", requestID: "dr-1-1", task: ptrTask(mkTask(1, "needs_decision", openReq("dr-1-1", 1))), want: "decision-pending"},
		{name: "open request on merged task is failed", requestID: "dr-2-1", task: ptrTask(mkTask(2, "merged", openReq("dr-2-1", 1))), want: "failed", reason: "task_terminal"},
		{name: "human answered request is done", requestID: "dr-3-1", task: ptrTask(mkTask(3, "needs_decision", resolvedReq("dr-3-1", 1, "answered", "operator"))), want: "done"},
		{name: "default applied is done", requestID: "dr-4-1", task: ptrTask(mkTask(4, "queued", resolvedReq("dr-4-1", 1, "default_applied", "operator"))), want: "done"},
		{name: "withdrawn is done", requestID: "dr-5-1", task: ptrTask(mkTask(5, "queued", resolvedReq("dr-5-1", 1, "withdrawn", "operator"))), want: "done"},
		{name: "assistant answer with unsent outbox is accepted", requestID: "dr-6-1",
			task:   ptrTask(mkTask(6, "needs_decision", resolvedReq("dr-6-1", 1, "answered", "operator-via-berry"))),
			outbox: []assistantOutboxRow{{ID: 9, Kind: "decision_answered", TargetLane: "lane-a", EventID: "dr-6-1-answered", CreatedAt: now}},
			want:   "accepted"},
		{name: "assistant answer with persisted relay is in-progress", requestID: "dr-7-1",
			task:   ptrTask(mkTask(7, "needs_decision", resolvedReq("dr-7-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 50, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-7-1-answered", Attempts: 1}},
			want:   "in-progress"},
		{name: "assistant answer with delivered relay is done", requestID: "dr-8-1",
			task:   ptrTask(mkTask(8, "needs_decision", resolvedReq("dr-8-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 51, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-8-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "host-a/w1:p1"}},
			want:   "done"},
		{name: "assistant answer with exhausted relay is failed", requestID: "dr-9-1",
			task:   ptrTask(mkTask(9, "needs_decision", resolvedReq("dr-9-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 52, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-9-1-answered", Attempts: relayReplayMaxAttempts}},
			want:   "failed", reason: "relay_exhausted"},
		{name: "assistant answer with no notice row yet is in-progress", requestID: "dr-10-1",
			task: ptrTask(mkTask(10, "needs_decision", resolvedReq("dr-10-1", 1, "answered", "operator-via-berry"))),
			want: "in-progress"},
		// Stamps are set together with delivered_at by markDelivered — the
		// real store shape for retired, cancelled, chat-terminal and sink
		// rows. None of them may report done.
		{name: "retired stamp is failed", requestID: "dr-11-1",
			task:   ptrTask(mkTask(11, "needs_decision", resolvedReq("dr-11-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 53, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-11-1-answered", Attempts: 2, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "hub/replay-retired:stale"}},
			want:   "failed", reason: "non_delivery_stamp"},
		{name: "cancelled stamp is failed", requestID: "dr-12-1",
			task:   ptrTask(mkTask(12, "needs_decision", resolvedReq("dr-12-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 54, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-12-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "m1/cancelled"}},
			want:   "failed", reason: "non_delivery_stamp"},
		{name: "chat-terminal stamp is failed", requestID: "dr-13-1",
			task:   ptrTask(mkTask(13, "needs_decision", resolvedReq("dr-13-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 55, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-13-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "hub/chat-terminal"}},
			want:   "failed", reason: "non_delivery_stamp"},
		{name: "sink stamp is failed with sink_lane", requestID: "dr-14-1",
			task:   ptrTask(mkTask(14, "needs_decision", resolvedReq("dr-14-1", 1, "answered", "operator-via-berry"))),
			relays: []handoffkeepRelayEvent{{ID: 56, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-14-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "sink/sink:lane-a"}},
			want:   "failed", reason: "sink_lane"},
		{name: "pending question is decision-pending", requestID: "Q-20261010-05",
			question: &assistantChatQuestion{ID: "Q-20261010-05", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 1},
			want:     "decision-pending"},
		{name: "question resolved by human is done", requestID: "Q-20261010-06",
			question: &assistantChatQuestion{ID: "Q-20261010-06", ConversationID: "operator-desk", Lane: "lane-a", State: "resolved", Revision: 1},
			want:     "done"},
		// The real PR-1 shape: the answer CAS sets answer_message_id and
		// leaves state pending — an answered pending question follows the
		// outbox/relay chain, it is not decision-pending.
		{name: "answered pending question with outbox is accepted", requestID: "Q-20261010-07",
			question: func() *assistantChatQuestion {
				id := int64(77)
				q := &assistantChatQuestion{ID: "Q-20261010-07", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 4}
				q.AnswerMessageID = &id
				return q
			}(),
			outbox: []assistantOutboxRow{{ID: 12, Kind: "chat_answer", TargetLane: "lane-a", EventID: "Q-20261010-07-rev4-answered", CreatedAt: now}},
			want:   "accepted"},
		{name: "answered pending question with delivered relay is done", requestID: "Q-20261010-09",
			question: func() *assistantChatQuestion {
				id := int64(78)
				q := &assistantChatQuestion{ID: "Q-20261010-09", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 2}
				q.AnswerMessageID = &id
				return q
			}(),
			relays: []handoffkeepRelayEvent{{ID: 57, Kind: "lane.event", OwnerLane: "lane-a", EventID: "Q-20261010-09-rev2-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "host-a/w1:p1"}},
			want:   "done"},
		{name: "answered pending question with no notice row is in-progress", requestID: "Q-20261010-10",
			question: func() *assistantChatQuestion {
				id := int64(79)
				q := &assistantChatQuestion{ID: "Q-20261010-10", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 1}
				q.AnswerMessageID = &id
				return q
			}(),
			want: "in-progress", reason: "notice_row_not_visible"},
		{name: "answered pending question with sink stamp is failed", requestID: "Q-20261010-11",
			question: func() *assistantChatQuestion {
				id := int64(80)
				q := &assistantChatQuestion{ID: "Q-20261010-11", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 1}
				q.AnswerMessageID = &id
				return q
			}(),
			relays: []handoffkeepRelayEvent{{ID: 58, Kind: "lane.event", OwnerLane: "lane-a", EventID: "Q-20261010-11-rev1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "sink/sink:lane-a"}},
			want:   "failed", reason: "sink_lane"},
		{name: "withdrawn question is failed", requestID: "Q-20261010-08",
			question: &assistantChatQuestion{ID: "Q-20261010-08", ConversationID: "operator-desk", Lane: "lane-a", State: "withdrawn", Revision: 1},
			want:     "failed", reason: "question_withdrawn"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hk := newFakeAssistantHK(t)
			if tc.task != nil {
				hk.seedTask(*tc.task)
			}
			if tc.question != nil {
				hk.seedQuestion(*tc.question)
			}
			hk.outbox = tc.outbox
			hk.relays = tc.relays
			srv, _ := assistantTestServer(t, hk, nil)
			result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": tc.requestID})
			if result.isError {
				t.Fatalf("progress error: %s", result.text)
			}
			if result.decoded["state"] != tc.want {
				t.Fatalf("state=%v, want %q; receipts=%v", result.decoded["state"], tc.want, result.decoded["receipts"])
			}
			if result.decoded["request_id"] != tc.requestID {
				t.Fatalf("request_id=%v", result.decoded["request_id"])
			}
			if tc.reason != "" {
				receipts, _ := result.decoded["receipts"].(map[string]any)
				if receipts["reason"] != tc.reason {
					t.Fatalf("reason=%v, want %q", receipts["reason"], tc.reason)
				}
			}
		})
	}

	// Request-id-level guard: persisted but undelivered is never done, and
	// ids that name nothing fail closed.
	hk := newFakeAssistantHK(t)
	srv, _ := assistantTestServer(t, hk, nil)
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-999-1"}); got != "request_not_found" {
		t.Fatalf("missing task progress error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{}); got != "invalid_arguments" {
		t.Fatalf("empty progress error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops", "request_id": "dr-1-1"}); got != "invalid_arguments" {
		t.Fatalf("ambiguous progress error=%q", got)
	}
}

func ptrTask(t assistantTask) *assistantTask { return &t }

// AC5 target-level: lane progress aggregates pending decisions, live relay
// rows, outbox debt and live tasks into the same closed vocabulary.
func TestAssistantProgressTargetAC5(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// A lane with open decision work reports decision-pending.
	hk := newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(now)
	srv, _ := assistantTestServer(t, hk, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.isError || result.decoded["state"] != "decision-pending" {
		t.Fatalf("pending lane state=%v err=%s", result.decoded["state"], result.text)
	}

	// A lane with only an undelivered persisted row is in-progress — never done.
	hk = newFakeAssistantHK(t)
	hk.relays = []handoffkeepRelayEvent{{ID: 60, Kind: "lane.event", OwnerLane: "lane-a", EventID: "ev-persisted", Attempts: 1}}
	srv, _ = assistantTestServer(t, hk, nil)
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.isError || result.decoded["state"] != "in-progress" {
		t.Fatalf("persisted-only lane state=%v, want in-progress", result.decoded["state"])
	}

	// An exhausted live row is failed.
	hk.relays = []handoffkeepRelayEvent{{ID: 61, Kind: "lane.event", OwnerLane: "lane-a", EventID: "ev-dead", Attempts: relayReplayMaxAttempts}}
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.decoded["state"] != "failed" {
		t.Fatalf("exhausted lane state=%v, want failed", result.decoded["state"])
	}

	// Delivered history with nothing live reports delivered with the receipt.
	hk.relays = []handoffkeepRelayEvent{{ID: 62, Kind: "lane.event", OwnerLane: "lane-a", EventID: "ev-ok", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "host-a/w1:p1"}}
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.decoded["state"] != "delivered" {
		t.Fatalf("delivered lane state=%v, want delivered", result.decoded["state"])
	}
	receipts, _ := result.decoded["receipts"].(map[string]any)
	relay, _ := receipts["relay"].(map[string]any)
	if relay["delivered_to"] != "host-a/w1:p1" {
		t.Fatalf("delivered receipt missing delivered_to: %v", receipts)
	}

	// A conversation target aggregates its pending questions.
	hk = newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(now)
	srv, _ = assistantTestServer(t, hk, nil)
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "desk"})
	if result.isError || result.decoded["state"] != "decision-pending" {
		t.Fatalf("conversation target state=%v err=%s", result.decoded["state"], result.text)
	}
	hk.pending = assistantPending{ServerTime: now}
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "desk"})
	if result.decoded["state"] != "done" {
		t.Fatalf("settled conversation state=%v, want done", result.decoded["state"])
	}
}

// AC6: the no-push contract ships in the poll description, the initialize
// instructions and the docs page — byte-identical sentences.
func TestAssistantPollContractAC6(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(time.Now().UTC())
	srv, _ := assistantTestServer(t, hk, nil)

	writer := assistantServe(t, srv, assistantTestBerryToken, http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	var listResponse struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &listResponse); err != nil {
		t.Fatal(err)
	}
	var pollDescription string
	for _, tool := range listResponse.Result.Tools {
		if tool.Name == "poll" {
			pollDescription = tool.Description
		}
	}
	for _, sentence := range []string{
		"never pushes",
		"Being registered only means the tools can be called",
		"Missing a poll means missing a deadline",
		"deadline passing is never consent",
	} {
		if !strings.Contains(pollDescription, sentence) {
			t.Fatalf("poll description missing %q: %s", sentence, pollDescription)
		}
	}

	writer = assistantServe(t, srv, assistantTestBerryToken, http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{}}`)
	var initResponse struct {
		Result struct {
			Instructions string `json:"instructions"`
		} `json:"result"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &initResponse); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(initResponse.Result.Instructions, "never pushes") {
		t.Fatalf("initialize instructions missing the contract: %q", initResponse.Result.Instructions)
	}

	docs, err := os.ReadFile("docs/assistant-mcp.md")
	if err != nil {
		t.Fatalf("docs page missing: %v", err)
	}
	if !strings.Contains(string(docs), "This server never pushes") || !strings.Contains(string(docs), "deadline passing is never consent") {
		t.Fatal("docs page lacks the no-push contract sentences")
	}

	// The snapshot itself is keyed by <id>:<revision> with server_time.
	result := assistantCall(t, srv, assistantTestBerryToken, "poll", nil)
	if result.isError {
		t.Fatalf("poll error: %s", result.text)
	}
	items, _ := result.decoded["items"].(map[string]any)
	if _, ok := items["dr-42-3:3"]; !ok {
		t.Fatalf("poll items not keyed by id:revision: %v", items)
	}
	if _, ok := items["Q-20261010-01:2"]; !ok {
		t.Fatalf("poll question key missing: %v", items)
	}
	if result.decoded["server_time"] == nil || result.decoded["server_time"] == "" {
		t.Fatal("poll lacks server_time")
	}
}

// AC7: across every forced failure path, no error string, tool payload or
// audit line may carry a token, a CF secret, URL userinfo or a redirect
// Location.
func TestAssistantSecretsAC7(t *testing.T) {
	hk := newFakeAssistantHK(t)
	logs := &bytes.Buffer{}
	var outputs []string
	capture := func(v string) { outputs = append(outputs, v) }

	srv, err := newAssistantServer(assistantConfig{
		Listen:      "127.0.0.1:0",
		HKURL:       hk.server.URL,
		HKToken:     assistantTestHKToken,
		HKCFID:      assistantTestCFID,
		HKCFSecret:  assistantTestCFSecret,
		TokenFile:   writeMode0600(t, t.TempDir(), "tok", assistantTestBerryToken),
		TargetsFile: writeMode0600(t, t.TempDir(), "tgt", assistantTestTargets),
		ClientName:  "berry-test",
	}, assistantServerDeps{Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}

	// Failure mode 1: hk 500s with a planted-secret body and a planted
	// redirect Location; the tool must surface the named error only.
	hk.statusFor["/v1/assistant/pending"] = http.StatusInternalServerError
	capture(assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil))
	hk.statusFor["/v1/assistant/pending"] = http.StatusFound // redirect refused
	capture(assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil))
	// Failure mode 2: malformed JSON upstream.
	hk.bodyFor["/v1/assistant/pending"] = `{not json ` + assistantTestLocation + `}`
	delete(hk.statusFor, "/v1/assistant/pending")
	capture(assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil))
	// Failure mode 3: hk unreachable (connection refused on a closed port).
	hk.server.Close()
	capture(assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil))
	// Failure mode 4: 401 and 404 bodies.
	writer := assistantServe(t, srv, "wrong-token", http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"poll"}}`)
	capture(writer.Body.String())
	writer = assistantServe(t, srv, assistantTestBerryToken, http.MethodGet, "/elsewhere", "")
	capture(writer.Body.String())
	capture(assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "unknown"}))
	capture(assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-1-1"}))
	capture(logs.String())

	joined := strings.Join(outputs, "\n")
	for _, secret := range []string{assistantTestHKToken, assistantTestBerryToken, assistantTestCFSecret, assistantTestCFID, assistantTestLocation, "attacker.example"} {
		if strings.Contains(joined, secret) {
			t.Fatalf("planted secret %q leaked into outputs:\n%s", secret, joined)
		}
	}
	if !strings.Contains(joined, "hk_rejected_http_500") || !strings.Contains(joined, "hk_unreachable") || !strings.Contains(joined, "hk_response_invalid") {
		t.Fatalf("expected named errors in outputs, got:\n%s", joined)
	}
}

// AC9: the binary holds no durable state — a second instance built fresh over
// the same fakes returns byte-identical tool results, and construction writes
// nothing into the filesystem it was handed.
func TestAssistantRestartSafetyAC9(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(time.Now().UTC().Truncate(time.Second))
	dir := t.TempDir()

	var first, second string
	for i := 0; i < 2; i++ {
		tokenFile := writeMode0600(t, dir, fmt.Sprintf("tok%d", i), assistantTestBerryToken)
		targetsFile := writeMode0600(t, dir, "targets.json", assistantTestTargets)
		srv, err := newAssistantServer(assistantConfig{
			Listen:      "127.0.0.1:0",
			HKURL:       hk.server.URL,
			HKToken:     assistantTestHKToken,
			TokenFile:   tokenFile,
			TargetsFile: targetsFile,
			ClientName:  "berry-test",
		}, assistantServerDeps{Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		poll := assistantCall(t, srv, assistantTestBerryToken, "poll", nil)
		list := assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil)
		if poll.isError || list.isError {
			t.Fatalf("instance %d tool errors", i)
		}
		result := poll.text + "\n" + list.text
		if i == 0 {
			first = result
		} else {
			second = result
		}
	}
	if first != second {
		t.Fatalf("restart changed results:\nfirst=%s\nsecond=%s", first, second)
	}
	// The server's state footprint is the three operator files it was handed;
	// it creates nothing of its own.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "tok0", "tok1", "targets.json":
		default:
			t.Fatalf("server created unexpected state file %q", entry.Name())
		}
	}
}

// The standing series rule: no 409 from hk or hub is ever treated as a
// receipt — the read path maps every non-200 to a named rejection.
func TestAssistantConflictNotAReceipt(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.statusFor["/v1/tasks/55"] = http.StatusConflict
	hk.seedTask(assistantTask{ID: 55, Lane: "lane-a", State: "queued"})
	srv, _ := assistantTestServer(t, hk, nil)
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-55-1"}); got != "hk_rejected_http_409" {
		t.Fatalf("409 surfaced as %q", got)
	}
}

// The env-style config file is itself a mode-0600 secret-bearing file: a
// loose mode refuses startup, and half-configured credential pairs refuse
// rather than sit loaded.
func TestAssistantConfigFileLoad(t *testing.T) {
	dir := t.TempDir()
	tokenFile := writeMode0600(t, dir, "berry.token", assistantTestBerryToken)
	targetsFile := writeMode0600(t, dir, "targets.json", assistantTestTargets)

	write := func(name, body string) string {
		t.Helper()
		return writeMode0600(t, dir, name, body)
	}
	base := "HANDOFFKEEP_URL=http://127.0.0.1:8080\nHANDOFFKEEP_TOKEN=" + assistantTestHKToken + "\n" +
		"PANEWIRE_ASSISTANT_TOKEN_FILE=" + tokenFile + "\nPANEWIRE_ASSISTANT_TARGETS_FILE=" + targetsFile + "\n"

	cfg, err := loadAssistantConfig(write("good.env", base))
	if err != nil {
		t.Fatalf("valid config refused: %v", err)
	}
	if cfg.Listen != "127.0.0.1:9471" {
		t.Fatalf("default listen=%q, want loopback 9471", cfg.Listen)
	}
	if cfg.HKCFID != "" || cfg.CFTeam != "" {
		t.Fatal("CF gate enabled without configuration")
	}

	loose := write("loose.env", base)
	if err := os.Chmod(loose, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAssistantConfig(loose); err == nil || !strings.Contains(err.Error(), "mode-0600") {
		t.Fatalf("loose config file: err=%v", err)
	}
	// Half a CF pair fails; half a hub pair fails; a CF gate without the
	// service-name allowlist fails — each is all-or-nothing.
	for name, extra := range map[string]string{
		"half_cf":   "HANDOFFKEEP_CF_ACCESS_CLIENT_ID=x\n",
		"half_hub":  "PANEWIRE_ASSISTANT_HUB_URL=http://127.0.0.1:9000\n",
		"cf_no_acl": "PANEWIRE_ASSISTANT_CF_TEAM=team\nPANEWIRE_ASSISTANT_CF_AUD=aud\n",
	} {
		if _, err := loadAssistantConfig(write(name+".env", base+extra)); err == nil {
			t.Fatalf("%s config accepted", name)
		}
	}
}

// Every request must carry the bearer — there is no unauthenticated route,
// and the MCP endpoint speaks POST only (GET/DELETE are stream-management
// verbs this stateless server does not implement).
func TestAssistantRouteSurface(t *testing.T) {
	hk := newFakeAssistantHK(t)
	srv, _ := assistantTestServer(t, hk, nil)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, "/mcp", http.StatusOK}, // empty body → in-band JSON-RPC parse error under a 200
		{http.MethodGet, "/mcp", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/mcp", http.StatusMethodNotAllowed},
		{http.MethodGet, "/", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusNotFound},
		{http.MethodPost, "/v1/tasks", http.StatusNotFound},
	} {
		writer := assistantServe(t, srv, assistantTestBerryToken, tc.method, tc.path, "")
		if writer.Code != tc.want {
			t.Fatalf("%s %s authed status=%d, want %d", tc.method, tc.path, writer.Code, tc.want)
		}
	}
}

// Fix-round MAJOR-1: a relay row is delivered only when delivered_to names a
// real <machine>/<pane>. Every other stamp — retired, cancelled, chat-*, sink
// — is failed with the stamp as the receipt, and sink additionally carries
// reason sink_lane. Berry must never be told done for a sink.
func TestAssistantDeliveryStampsR2(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	resolvedTask := func(id int64) assistantTask {
		task := assistantTask{ID: id, Lane: "lane-a", Title: "t", State: "needs_decision"}
		task.Refs.DecisionRequest = &assistantDecisionRequest{
			ID: fmt.Sprintf("dr-%d-1", id), Revision: 1, Status: "answered", Question: "q",
			DefaultAction: "wait", RequestedBy: "wrk", RequestedAt: now,
			Resolution: &assistantDecisionResolution{Kind: "answered", Responder: "operator-via-berry", At: now},
		}
		return task
	}
	for _, tc := range []struct {
		name        string
		deliveredTo string
		want        string
		reason      string
	}{
		{"real pane is done", "host-a/w1:p1", "done", ""},
		{"retired stamp", "hub/replay-retired:stale", "failed", "non_delivery_stamp"},
		{"retired reason stamp", "hub/replay-retired:unconfirmed", "failed", "non_delivery_stamp"},
		{"cancelled stamp", "m1/cancelled", "failed", "non_delivery_stamp"},
		{"chat-terminal stamp", "hub/chat-terminal", "failed", "non_delivery_stamp"},
		{"chat-cancel stamp", "hub/chat-cancel", "failed", "non_delivery_stamp"},
		{"sink stamp", "sink/sink:lane-a", "failed", "sink_lane"},
		{"bare machine is not a delivery", "host-a", "failed", "non_delivery_stamp"},
		{"non-pane suffix is not a delivery", "host-a/xterm", "failed", "non_delivery_stamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := newFakeAssistantHK(t)
			hk.seedTask(resolvedTask(70))
			hk.relays = []handoffkeepRelayEvent{{ID: 70, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-70-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: tc.deliveredTo}}
			srv, _ := assistantTestServer(t, hk, nil)
			result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-70-1"})
			if result.decoded["state"] != tc.want {
				t.Fatalf("state=%v, want %q (receipts=%v)", result.decoded["state"], tc.want, result.decoded["receipts"])
			}
			receipts, _ := result.decoded["receipts"].(map[string]any)
			if tc.reason != "" && receipts["reason"] != tc.reason {
				t.Fatalf("reason=%v, want %q", receipts["reason"], tc.reason)
			}
			// The stamp itself is the receipt.
			relay, _ := receipts["relay"].(map[string]any)
			if tc.want == "failed" && relay["delivered_to"] != tc.deliveredTo {
				t.Fatalf("stamp not carried as receipt: %v", receipts)
			}
			// Lane aggregate on a lane whose only evidence is the stamped
			// row: the same stamp is the newest row → same verdict.
			hkLane := newFakeAssistantHK(t)
			hkLane.relays = []handoffkeepRelayEvent{{ID: 70, Kind: "lane.event", OwnerLane: "lane-a", EventID: "dr-70-1-answered", Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: tc.deliveredTo}}
			srvLane, _ := assistantTestServer(t, hkLane, nil)
			lane := assistantCall(t, srvLane, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
			wantLane := tc.want
			if tc.want == "done" {
				wantLane = "delivered"
			}
			if lane.decoded["state"] != wantLane {
				t.Fatalf("lane state=%v, want %q", lane.decoded["state"], wantLane)
			}
		})
	}
}

// Fix-round MAJOR-2 + MINOR-1: a truncated view can never produce done or
// delivered — a tasks page at its limit, a relay walk at its bound, a capped
// pending list or a capped outbox all report in-progress with reason
// view_truncated.
func TestAssistantTruncatedViewsR2(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// A lane whose tasks page fills the limit can hide a live task past the
	// cap — the checker's claimed-1001 scenario. done is unprovable.
	hk := newFakeAssistantHK(t)
	for i := int64(1); i <= assistantTasksPageLimit; i++ {
		hk.seedTask(assistantTask{ID: i, Lane: "lane-a", State: "merged", Title: "old"})
	}
	hk.seedTask(assistantTask{ID: assistantTasksPageLimit + 1, Lane: "lane-a", State: "claimed", Title: "live past the cap"})
	srv, _ := assistantTestServer(t, hk, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.decoded["state"] != "in-progress" {
		t.Fatalf("full tasks page state=%v, want in-progress", result.decoded["state"])
	}
	receipts, _ := result.decoded["receipts"].(map[string]any)
	if receipts["reason"] != "view_truncated" {
		t.Fatalf("reason=%v, want view_truncated", receipts["reason"])
	}

	// A relay walk that reaches its bound cannot prove the tail is quiet.
	hk = newFakeAssistantHK(t)
	for i := int64(1); i <= assistantRelayScanPages*assistantRelayPageSize; i++ {
		hk.relays = append(hk.relays, handoffkeepRelayEvent{
			ID: i, Kind: "lane.event", OwnerLane: "lane-a", EventID: "ev-old",
			Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: "host-a/w1:p1",
		})
	}
	srv, _ = assistantTestServer(t, hk, nil)
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.decoded["state"] != "in-progress" {
		t.Fatalf("bounded relay walk state=%v, want in-progress", result.decoded["state"])
	}
	receipts, _ = result.decoded["receipts"].(map[string]any)
	if receipts["reason"] != "view_truncated" {
		t.Fatalf("relay-walk reason=%v, want view_truncated", receipts["reason"])
	}

	// One row under the bound is still provable delivered.
	hk.relays = hk.relays[:1]
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.decoded["state"] != "delivered" {
		t.Fatalf("complete relay view state=%v, want delivered", result.decoded["state"])
	}

	// A capped pending list cannot prove a conversation has nothing pending.
	hk = newFakeAssistantHK(t)
	pending := assistantPending{ServerTime: now}
	for i := 0; i < assistantChatCap; i++ {
		pending.ChatQuestions = append(pending.ChatQuestions, assistantChatQuestion{
			ID: fmt.Sprintf("Q-20261010-%02d", i%100), ConversationID: "other-desk",
			Lane: "other-lane", State: "pending", Revision: 1,
		})
	}
	hk.pending = pending
	srv, _ = assistantTestServer(t, hk, nil)
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "desk"})
	if result.decoded["state"] != "in-progress" {
		t.Fatalf("capped pending list state=%v, want in-progress", result.decoded["state"])
	}
	receipts, _ = result.decoded["receipts"].(map[string]any)
	if receipts["reason"] != "view_truncated" {
		t.Fatalf("pending-cap reason=%v, want view_truncated", receipts["reason"])
	}
}

// Fix-round MAJOR-3 + MINOR-6: every by-id read is scoped to the allowlist —
// an out-of-scope id answers the same generic request_not_found as a missing
// one, an invalid targets file fails every tool closed, and resolution text
// or resolver identity never leaves the binary.
func TestAssistantScopeClosedR2(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	hk := newFakeAssistantHK(t)
	// In-scope task on lane-a (mapped by "ops"), out-of-scope on lane-b (unmapped).
	hk.seedTask(assistantTask{ID: 80, Lane: "lane-a", State: "needs_decision", Title: "in scope", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-80-1", Revision: 1, Status: "open", Question: "q", DefaultAction: "wait", RequestedBy: "wrk", RequestedAt: now}}})
	hk.seedTask(assistantTask{ID: 90, Lane: "lane-b", State: "needs_decision", Title: "out of scope", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-90-4", Revision: 4, Status: "open", Question: "q", DefaultAction: "wait", RequestedBy: "wrk", RequestedAt: now}}})
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-20", ConversationID: "other-desk", Lane: "lane-b", State: "pending", Revision: 1})
	// A merged task whose lane is mapped: still not an open item.
	hk.seedTask(assistantTask{ID: 91, Lane: "lane-a", State: "merged", Title: "gone", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-91-1", Revision: 1, Status: "open", Question: "q", DefaultAction: "wait", RequestedBy: "wrk", RequestedAt: now}}})
	srv, _ := assistantTestServer(t, hk, nil)

	// Missing and out-of-scope ids answer identically — no existence oracle.
	missing := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-999-1"})
	outOfScope := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-90-4"})
	if missing != "request_not_found" || outOfScope != missing {
		t.Fatalf("detail oracle split: missing=%q out-of-scope=%q", missing, outOfScope)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-90-4"}); got != "request_not_found" {
		t.Fatalf("out-of-scope progress error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-20"}); got != "request_not_found" {
		t.Fatalf("out-of-scope question detail error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "Q-20261010-20"}); got != "request_not_found" {
		t.Fatalf("out-of-scope question progress error=%q", got)
	}
	// Merged, dropped and stale ids are the same not_found — never
	// request_not_current and never a detail leak.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-91-1"}); got != "request_not_found" {
		t.Fatalf("merged-task detail error=%q", got)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-90-1"}); got != "request_not_found" {
		t.Fatalf("stale out-of-scope progress error=%q", got)
	}
	for _, err := range []string{missing, outOfScope} {
		if strings.Contains(err, "not_current") || strings.Contains(err, "dr-90") {
			t.Fatalf("error leaks current/existence detail: %q", err)
		}
	}

	// An invalid targets file fails every tool closed — not only the
	// target-taking ones.
	targetsPath := srv.targetsPath
	if err := os.Chmod(targetsPath, 0644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"targets", nil},
		{"pending_list", nil},
		{"pending_detail", map[string]any{"id": "dr-80-1"}},
		{"progress", map[string]any{"target": "ops"}},
		{"progress", map[string]any{"request_id": "dr-80-1"}},
		{"poll", nil},
	} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, tc.tool, tc.args); got != "targets_file_invalid" {
			t.Fatalf("%s under chmod-0644 targets: error=%q, want targets_file_invalid", tc.tool, got)
		}
	}
	if err := os.Chmod(targetsPath, 0600); err != nil {
		t.Fatal(err)
	}

	// Resolution text, receipt text and resolver identity never leave —
	// kind, responder and resolved_at are the whole resolution receipt.
	hk.seedTask(assistantTask{ID: 92, Lane: "lane-a", State: "needs_decision", Title: "resolved", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{
		ID: "dr-92-1", Revision: 1, Status: "answered", Question: "q", DefaultAction: "wait",
		RequestedBy: "wrk", RequestedAt: now,
		Resolution: &assistantDecisionResolution{Kind: "answered", Option: "go", Text: "PLANTED-RESOLUTION-TEXT", Receipt: "PLANTED-RECEIPT", Responder: "operator", By: "PLANTED-BY", At: now},
	}}})
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-92-1"})
	if result.isError {
		t.Fatalf("resolved progress error: %s", result.text)
	}
	for _, secret := range []string{"PLANTED-RESOLUTION-TEXT", "PLANTED-RECEIPT", "PLANTED-BY"} {
		if strings.Contains(result.text, secret) {
			t.Fatalf("resolution field %q leaked: %s", secret, result.text)
		}
	}
	receipts, _ := result.decoded["receipts"].(map[string]any)
	resolution, _ := receipts["resolution"].(map[string]any)
	if resolution["kind"] != "answered" || resolution["responder"] != "operator" || resolution["at"] == nil {
		t.Fatalf("resolution receipt missing kind/responder/at: %v", resolution)
	}
	if len(resolution) != 3 {
		t.Fatalf("resolution receipt carries extra fields: %v", resolution)
	}
}

// Fix-round MAJOR-4: the real PR-1 chain — a question whose answer CAS set
// answer_message_id while leaving state pending is answered, and its
// progress is the outbox/relay notice chain, not decision-pending.
func TestAssistantAnswerChainR2(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	answeredID := int64(55)

	// pending + answer_message_id + unsent outbox row → accepted.
	hk := newFakeAssistantHK(t)
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-30", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 3, AnswerMessageID: &answeredID})
	hk.outbox = []assistantOutboxRow{{ID: 31, Kind: "chat_answer", TargetLane: "lane-a", EventID: "Q-20261010-30-rev3-answered", CreatedAt: now}}
	srv, _ := assistantTestServer(t, hk, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "Q-20261010-30"})
	if result.decoded["state"] != "accepted" {
		t.Fatalf("answered+pending+outbox state=%v, want accepted", result.decoded["state"])
	}

	// The same question with a persisted-but-undelivered relay row is
	// in-progress; delivered to a real pane is done.
	hk.relays = []handoffkeepRelayEvent{{ID: 32, Kind: "lane.event", OwnerLane: "lane-a", EventID: "Q-20261010-30-rev3-answered", Attempts: 1}}
	hk.outbox = nil
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "Q-20261010-30"})
	if result.decoded["state"] != "in-progress" {
		t.Fatalf("answered+pending+persisted state=%v, want in-progress", result.decoded["state"])
	}
	hk.relays[0].DeliveredAt = now.Format(time.RFC3339Nano)
	hk.relays[0].DeliveredTo = "host-a/w1:p1"
	result = assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "Q-20261010-30"})
	if result.decoded["state"] != "done" {
		t.Fatalf("answered+pending+delivered state=%v, want done", result.decoded["state"])
	}

	// pending_list and poll items carry the answered flag so the consumer
	// knows the question already has an assistant answer.
	hk.pending = assistantPending{ServerTime: now, ChatQuestions: []assistantChatQuestion{
		{ID: "Q-20261010-30", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 3, AnswerMessageID: &answeredID},
		{ID: "Q-20261010-31", ConversationID: "operator-desk", Lane: "lane-a", State: "pending", Revision: 1},
	}}
	listed := assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil)
	items, _ := listed.decoded["items"].([]any)
	flagged := map[string]any{}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		flagged[item["id"].(string)] = item["answered"]
	}
	if flagged["Q-20261010-30"] != true || flagged["Q-20261010-31"] != false {
		t.Fatalf("answered flags=%v", flagged)
	}
	polled := assistantCall(t, srv, assistantTestBerryToken, "poll", nil)
	pollItems, _ := polled.decoded["items"].(map[string]any)
	row, _ := pollItems["Q-20261010-30:3"].(map[string]any)
	if row["answered"] != true {
		t.Fatalf("poll item lacks answered=true: %v", row)
	}
}

// Fix-round MINOR-3 (M6): every claim the CF verifier checks must actually be
// enforced — expired, not-yet-valid, wrong-audience and wrongly-signed
// assertions are all refused at 401 even with a valid bearer.
func TestAssistantCFClaimsR2(t *testing.T) {
	hk := newFakeAssistantHK(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, _ := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFServiceNames = []string{"berry.svc"}
	})
	call := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	serve := func(jwt string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call))
		request.Header.Set("Authorization", "Bearer "+assistantTestBerryToken)
		request.Header.Set("Cf-Access-Jwt-Assertion", jwt)
		writer := httptest.NewRecorder()
		srv.ServeHTTP(writer, request)
		return writer
	}
	now := time.Now()
	sign := func(claims map[string]any) string {
		return chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	}
	base := func() map[string]any {
		return map[string]any{"aud": []string{assistantTestAUD}, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "common_name": "berry.svc"}
	}

	expired := base()
	expired["exp"] = now.Add(-time.Hour).Unix()
	if w := serve(sign(expired)); w.Code != http.StatusUnauthorized {
		t.Fatalf("expired JWT status=%d, want 401", w.Code)
	}
	notYet := base()
	notYet["nbf"] = now.Add(time.Hour).Unix()
	if w := serve(sign(notYet)); w.Code != http.StatusUnauthorized {
		t.Fatalf("future-nbf JWT status=%d, want 401", w.Code)
	}
	wrongAud := base()
	wrongAud["aud"] = []string{"someone-else"}
	if w := serve(sign(wrongAud)); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-aud JWT status=%d, want 401", w.Code)
	}
	// A token signed by an unknown key fails signature verification even
	// though every claim is right.
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	forged := chatSignJWTClaims(t, otherKey, "k1", base())
	if w := serve(forged); w.Code != http.StatusUnauthorized {
		t.Fatalf("forged-signature JWT status=%d, want 401", w.Code)
	}
	if w := serve(sign(base())); w.Code != http.StatusOK {
		t.Fatalf("valid JWT status=%d, want 200", w.Code)
	}
}

// Fix-round MINOR-3 (M12): when the hk CF pair is configured the outbound
// client stamps both Access headers on every handoffkeep call; with no pair
// configured it stamps neither.
func TestAssistantOutboundCFHeadersR2(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.pending = assistantFixturePending(time.Now().UTC())
	srv, _ := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.HKCFID = assistantTestCFID
		cfg.HKCFSecret = assistantTestCFSecret
	})
	assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil)
	assistantCall(t, srv, assistantTestBerryToken, "poll", nil)
	headers := hk.cfHeaderLog()
	if len(headers) == 0 {
		t.Fatal("no hk requests recorded")
	}
	for i, pair := range headers {
		if pair[0] != assistantTestCFID || pair[1] != assistantTestCFSecret {
			t.Fatalf("request %d missing CF headers: id=%q secret=%q", i, pair[0], pair[1])
		}
	}

	hk2 := newFakeAssistantHK(t)
	hk2.pending = assistantFixturePending(time.Now().UTC())
	srv2, _ := assistantTestServer(t, hk2, nil)
	assistantCall(t, srv2, assistantTestBerryToken, "pending_list", nil)
	for i, pair := range hk2.cfHeaderLog() {
		if pair[0] != "" || pair[1] != "" {
			t.Fatalf("unconfigured request %d carried CF headers: %v", i, pair)
		}
	}
}

// Fix-round MINOR-5: a panic inside a tool is caught, audit-logged as
// internal_error, and answered with a named JSON-RPC error — the panic text
// leaves through neither channel.
func TestAssistantPanicRecoveryR2(t *testing.T) {
	dir := t.TempDir()
	targetsFile := writeMode0600(t, dir, "targets.json", assistantTestTargets)
	logs := &bytes.Buffer{}
	// A server with no hk client panics inside the tool on first use — a
	// stand-in for any in-tool panic.
	srv := &assistantServer{
		targetsPath: targetsFile,
		audit:       slog.New(slog.NewJSONHandler(logs, nil)),
	}
	params, _ := json.Marshal(map[string]any{"name": "pending_list", "arguments": map[string]any{}})
	result, rpcErr := srv.dispatchToolCall(context.Background(), "berry-test", params)
	if result != nil || rpcErr == nil {
		t.Fatalf("panic result=%v rpcErr=%v", result, rpcErr)
	}
	if rpcErr.Code != rpcInternalError || rpcErr.Message != "internal_error" {
		t.Fatalf("panic rpc error=%v, want -32603 internal_error", rpcErr)
	}
	if strings.Contains(rpcErr.Message, "nil pointer") || strings.Contains(rpcErr.Message, "panic") {
		t.Fatalf("panic text leaked into rpc error: %q", rpcErr.Message)
	}
	if !strings.Contains(logs.String(), "internal_error") {
		t.Fatalf("panic not audit logged: %s", logs.String())
	}
}
