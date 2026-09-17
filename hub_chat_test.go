package panewire

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	chatOperatorToken = "chat-test-operator-secret-must-never-reach-browser"
	chatNodeToken     = "chat-test-node-secret"
	chatTestAUD       = "chat-test-access-aud"
)

// chatTestSigningKey is one RSA key shared by every test that needs a signed
// Cf-Access-Jwt-Assertion; the matching public key is served by chatTestCerts.
var chatTestSigningKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

// chatTestCerts serves the Access certs document shape for the test key.
func chatTestCerts(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()
	jwk := map[string]any{
		"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{jwk}})
	}))
	t.Cleanup(server.Close)
	return server
}

// chatSignJWT builds a synthetic Cf-Access-Jwt-Assertion: the same shape and
// signature Cloudflare emits, so the verifier exercises its real code path.
func chatSignJWT(t *testing.T, key *rsa.PrivateKey, aud string, exp time.Time) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"k1","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"aud": []string{aud}, "exp": exp.Unix(), "iat": time.Now().Unix(), "sub": "op@example.test", "email": "op@example.test"})
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(header + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func chatTestToken(t *testing.T) string {
	t.Helper()
	return chatSignJWT(t, chatTestSigningKey(), chatTestAUD, time.Now().Add(time.Hour))
}

// fakeChatStore mirrors handoffkeep's chat state machine faithfully:
// stored → delivered|failed, delivered idempotent, failed a sink.
type fakeChatStore struct {
	mu        sync.Mutex
	questions map[string]*ChatQuestion
	messages  map[int64]*ChatMessage
	order     []int64
	nextMsgID int64
	err       error
	panicOn   string
	calls     []string
}

func newFakeChatStore() *fakeChatStore {
	return &fakeChatStore{questions: map[string]*ChatQuestion{}, messages: map[int64]*ChatMessage{}, nextMsgID: 1}
}

func (f *fakeChatStore) fail(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, method)
	if f.panicOn == method {
		// Panic happens before any state is touched, like a real client bug.
		panic("fake chat store panic: " + method)
	}
	return f.err
}

func (f *fakeChatStore) UpsertChatQuestion(_ context.Context, in ChatQuestionUpsert) (ChatQuestion, bool, error) {
	if err := f.fail("UpsertChatQuestion"); err != nil {
		return ChatQuestion{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !chatQuestionIDPattern.MatchString(in.ID) || in.Lane == "" || in.Body == "" {
		return ChatQuestion{}, false, &chatHTTPError{op: "question upsert", status: http.StatusBadRequest}
	}
	now := time.Now().UTC()
	if existing, ok := f.questions[in.ID]; ok {
		existing.Lane, existing.Body, existing.UpdatedAt = in.Lane, in.Body, now
		return *existing, false, nil
	}
	question := &ChatQuestion{ID: in.ID, Lane: in.Lane, Body: in.Body, State: "pending", CreatedAt: now, UpdatedAt: now}
	f.questions[in.ID] = question
	return *question, true, nil
}

func (f *fakeChatStore) TransitionChatQuestion(_ context.Context, id, to string) (ChatQuestion, error) {
	if err := f.fail("TransitionChatQuestion"); err != nil {
		return ChatQuestion{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	question, ok := f.questions[id]
	if !ok {
		return ChatQuestion{}, &chatHTTPError{op: "question transition", status: http.StatusNotFound}
	}
	if question.State != "pending" {
		return ChatQuestion{}, &chatHTTPError{op: "question transition", status: http.StatusConflict}
	}
	question.State = to
	question.UpdatedAt = time.Now().UTC()
	if to == "resolved" {
		now := time.Now().UTC()
		question.ResolvedAt = &now
	}
	return *question, nil
}

func (f *fakeChatStore) ListChatQuestions(_ context.Context, state, afterID string, limit int) ([]ChatQuestion, error) {
	if err := f.fail("ListChatQuestions"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.questions))
	for id := range f.questions {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	out := []ChatQuestion{}
	for _, id := range ids {
		if afterID != "" && id <= afterID {
			continue
		}
		if state != "" && f.questions[id].State != state {
			continue
		}
		out = append(out, *f.questions[id])
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeChatStore) CreateChatMessage(_ context.Context, author, body string) (ChatMessage, error) {
	if err := f.fail("CreateChatMessage"); err != nil {
		return ChatMessage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if (author != "operator" && author != "desk") || body == "" {
		return ChatMessage{}, &chatHTTPError{op: "message create", status: http.StatusBadRequest}
	}
	message := &ChatMessage{ID: f.nextMsgID, Author: author, Body: body, RelayState: "stored", CreatedAt: time.Now().UTC()}
	f.nextMsgID++
	f.messages[message.ID] = message
	f.order = append(f.order, message.ID)
	return *message, nil
}

func (f *fakeChatStore) MarkChatMessageDelivered(_ context.Context, id int64) (ChatMessage, error) {
	if err := f.fail("MarkChatMessageDelivered"); err != nil {
		return ChatMessage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	message, ok := f.messages[id]
	if !ok {
		return ChatMessage{}, &chatHTTPError{op: "message delivered", status: http.StatusNotFound}
	}
	if message.RelayState == "stored" {
		now := time.Now().UTC()
		message.RelayState = "delivered"
		message.DeliveredAt = &now
	}
	// handoffkeep answers a repeat mark with the unchanged row, so a failed
	// row visibly stays failed here rather than silently transitioning.
	return *message, nil
}

func (f *fakeChatStore) MarkChatMessageFailed(_ context.Context, id int64) (ChatMessage, error) {
	if err := f.fail("MarkChatMessageFailed"); err != nil {
		return ChatMessage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	message, ok := f.messages[id]
	if !ok {
		return ChatMessage{}, &chatHTTPError{op: "message failed", status: http.StatusNotFound}
	}
	if message.RelayState != "stored" {
		return ChatMessage{}, &chatHTTPError{op: "message failed", status: http.StatusConflict}
	}
	message.RelayState = "failed"
	return *message, nil
}

func (f *fakeChatStore) ListChatMessages(_ context.Context, undelivered bool, afterID int64, limit int) ([]ChatMessage, error) {
	if err := f.fail("ListChatMessages"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []ChatMessage{}
	for _, id := range f.order {
		message := f.messages[id]
		if id <= afterID {
			continue
		}
		if undelivered && message.DeliveredAt != nil {
			continue
		}
		out = append(out, *message)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeChatStore) GetChatMessage(_ context.Context, id int64) (ChatMessage, bool, error) {
	if err := f.fail("GetChatMessage"); err != nil {
		return ChatMessage{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	message, ok := f.messages[id]
	if !ok {
		return ChatMessage{}, false, nil
	}
	return *message, true, nil
}

func (f *fakeChatStore) messageState(id int64) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if message, ok := f.messages[id]; ok {
		return message.RelayState
	}
	return ""
}

func (f *fakeChatStore) message(id int64) ChatMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.messages[id]
}

func (f *fakeChatStore) questionState(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if question, ok := f.questions[id]; ok {
		return question.State
	}
	return ""
}

// chatTestHub builds a hub with the chat store wired, UI auth enabled, and a
// Cloudflare Access verifier backed by the test certs server. The dispatcher
// is never started here: tests drive drainChatOutbox explicitly so the stored
// → relayed transition is observed deterministically.
func chatTestHub(t *testing.T, lanes string, relay *handoffkeepRelayClient, store ChatStore) *HubServer {
	t.Helper()
	certs := chatTestCerts(t, chatTestSigningKey())
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": chatOperatorToken, "host-a": chatNodeToken, "host-b": chatNodeToken},
		ReportRelayPath: r20LanesFile(t, lanes),
		UIAllowCFOnly:   true,
		CFAccessTeam:    "team",
		CFAccessAUD:     chatTestAUD,
		// The test seam: the verifier fetches this loopback JWKS document
		// instead of <team>.cloudflareaccess.com.
		CFAccessCertsURL:   certs.URL,
		CFAccessHTTPClient: certs.Client(),
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:        relay,
		ChatStore:          store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

// chatUIRequest builds a browser-shaped request arriving through Cloudflare
// Access: a verified Cf-Access-Jwt-Assertion plus same-origin headers, so the
// pass case needs no extra work.
func chatUIRequest(t *testing.T, method, target string, body string) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, target, reader)
	request.RemoteAddr = "127.0.0.1:49000"
	request.Header.Set("Cf-Access-Jwt-Assertion", chatTestToken(t))
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func chatServe(t *testing.T, hub *HubServer, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func chatPostMessage(t *testing.T, hub *HubServer, body string) *httptest.ResponseRecorder {
	t.Helper()
	return chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages", body))
}

func chatDecodeMessage(t *testing.T, writer *httptest.ResponseRecorder) ChatMessage {
	t.Helper()
	var message ChatMessage
	if err := json.Unmarshal(writer.Body.Bytes(), &message); err != nil {
		t.Fatalf("response=%q err=%v", writer.Body.String(), err)
	}
	return message
}

func chatPostQuestion(t *testing.T, hub *HubServer, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/questions", strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return chatServe(t, hub, request)
}

// AC1: /chat and /chat/data are gated by the strict chat gate — a verified
// Access assertion or the operator bearer token. Loopback alone grants
// nothing here: this surface can inject lane directives and local processes
// are not trusted. The unsigned identity headers grant nothing anywhere.
func TestHubChatPageRequiresUIAuth(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	for _, path := range []string{"/chat", "/chat/data"} {
		denied := httptest.NewRequest(http.MethodGet, path, nil)
		denied.RemoteAddr = "198.51.100.25:4444"
		if writer := chatServe(t, hub, denied); writer.Code != http.StatusNotFound {
			t.Fatalf("%s without auth status=%d, want 404", path, writer.Code)
		}
		// Bare loopback is not proof for the chat surface.
		loopback := httptest.NewRequest(http.MethodGet, path, nil)
		loopback.RemoteAddr = "127.0.0.1:4444"
		if writer := chatServe(t, hub, loopback); writer.Code != http.StatusNotFound {
			t.Fatalf("%s bare loopback status=%d, want 404", path, writer.Code)
		}
		// The unsigned identity header is not proof either — any client can
		// set it, whether the peer is loopback or remote.
		forged := httptest.NewRequest(http.MethodGet, path, nil)
		forged.RemoteAddr = "127.0.0.1:4444"
		forged.Header.Set("Cf-Ray", "fixture-ray")
		forged.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
		if writer := chatServe(t, hub, forged); writer.Code != http.StatusNotFound {
			t.Fatalf("%s forged identity headers status=%d, want 404", path, writer.Code)
		}
		// A verified assertion authorizes regardless of the transport peer.
		tokened := httptest.NewRequest(http.MethodGet, path, nil)
		tokened.RemoteAddr = "198.51.100.25:4444"
		tokened.Header.Set("Cf-Access-Jwt-Assertion", chatTestToken(t))
		if writer := chatServe(t, hub, tokened); writer.Code != http.StatusOK {
			t.Fatalf("%s with verified assertion status=%d, want 200", path, writer.Code)
		}
		// The operator bearer token is the machine-client gate.
		bearer := httptest.NewRequest(http.MethodGet, path, nil)
		bearer.RemoteAddr = "127.0.0.1:4444"
		bearer.Header.Set("Authorization", "Bearer "+chatOperatorToken)
		if writer := chatServe(t, hub, bearer); writer.Code != http.StatusOK {
			t.Fatalf("%s with operator token status=%d, want 200", path, writer.Code)
		}
	}
	page := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat", ""))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "panewire chat") {
		t.Fatalf("Access /chat status=%d", page.Code)
	}
	data := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if data.Code != http.StatusOK || !strings.Contains(data.Body.String(), `"questions":[]`) {
		t.Fatalf("Access /chat/data status=%d body=%q", data.Code, data.Body.String())
	}
}

// The forged-header bypass B4 reported: an unsigned
// Cf-Access-Authenticated-User-Email from a non-loopback peer must never
// authorize a lane-injecting write. This is the mutant-sensitive gate — the
// old `if accessIdentity { return true }` branch turns every assertion below
// red.
func TestHubChatWriteRejectsForgedIdentity(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	body := `{"lane":"lane-a","body":"rm -rf /"}`

	// The reported attack: tailnet peer, forged email header, nothing else.
	forged := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	forged.RemoteAddr = "100.64.0.9:4444"
	forged.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
	if writer := chatServe(t, hub, forged); writer.Code != http.StatusNotFound {
		t.Fatalf("forged email header on non-loopback write status=%d, want 404", writer.Code)
	}
	// Same forgery with Cloudflare routing headers — still unsigned, denied.
	forgedRouted := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	forgedRouted.RemoteAddr = "100.64.0.9:4444"
	forgedRouted.Header.Set("Cf-Ray", "fixture-ray")
	forgedRouted.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
	if writer := chatServe(t, hub, forgedRouted); writer.Code != http.StatusNotFound {
		t.Fatalf("forged CF headers on non-loopback write status=%d, want 404", writer.Code)
	}
	// Headerless loopback is not trusted for a write either.
	bareLoopback := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	bareLoopback.RemoteAddr = "127.0.0.1:4444"
	if writer := chatServe(t, hub, bareLoopback); writer.Code != http.StatusNotFound {
		t.Fatalf("bare loopback write status=%d, want 404", writer.Code)
	}
	// A local process that forges the identity header is equally untrusted.
	forgedLoopback := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	forgedLoopback.RemoteAddr = "127.0.0.1:4444"
	forgedLoopback.Header.Set("Cf-Access-Authenticated-User-Email", "operator@example.test")
	if writer := chatServe(t, hub, forgedLoopback); writer.Code != http.StatusNotFound {
		t.Fatalf("forged email header on loopback write status=%d, want 404", writer.Code)
	}
	// Control: the operator token still authorizes the same write.
	tokened := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	tokened.RemoteAddr = "127.0.0.1:4444"
	tokened.Header.Set("Authorization", "Bearer "+chatOperatorToken)
	if writer := chatServe(t, hub, tokened); writer.Code != http.StatusCreated {
		t.Fatalf("operator-token write status=%d body=%q, want 201", writer.Code, writer.Body.String())
	}
}

// Invalid assertions — bad signature, wrong audience, expired — each fail
// closed. These are the three claim checks a mutant skipping any one of them
// turns red.
func TestHubChatJWTRejections(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"bad_signature": chatSignJWT(t, otherKey, chatTestAUD, time.Now().Add(time.Hour)),
		"wrong_aud":     chatSignJWT(t, chatTestSigningKey(), "other-app-aud", time.Now().Add(time.Hour)),
		"expired":       chatSignJWT(t, chatTestSigningKey(), chatTestAUD, time.Now().Add(-time.Hour)),
		"garbage":       "not.a.jwt",
	}
	for name, token := range cases {
		request := httptest.NewRequest(http.MethodGet, "/chat/data", nil)
		request.RemoteAddr = "127.0.0.1:4444"
		request.Header.Set("Cf-Access-Jwt-Assertion", token)
		if writer := chatServe(t, hub, request); writer.Code != http.StatusNotFound {
			t.Fatalf("%s assertion status=%d, want 404", name, writer.Code)
		}
	}
	valid := httptest.NewRequest(http.MethodGet, "/chat/data", nil)
	valid.RemoteAddr = "127.0.0.1:4444"
	valid.Header.Set("Cf-Access-Jwt-Assertion", chatTestToken(t))
	if writer := chatServe(t, hub, valid); writer.Code != http.StatusOK {
		t.Fatalf("valid assertion status=%d body=%q, want 200", writer.Code, writer.Body.String())
	}
}

// AC2: cookie/identity-authenticated POSTs reject cross-origin callers.
func TestHubChatMessageCSRF(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	body := `{"lane":"lane-a","body":"hello"}`

	crossOrigin := chatUIRequest(t, http.MethodPost, "/chat/messages", body)
	crossOrigin.Header.Set("Origin", "https://evil.example")
	if writer := chatServe(t, hub, crossOrigin); writer.Code != http.StatusForbidden || !strings.Contains(writer.Body.String(), "cross_origin") {
		t.Fatalf("cross-origin Origin status=%d body=%q, want 403", writer.Code, writer.Body.String())
	}
	crossFetch := chatUIRequest(t, http.MethodPost, "/chat/messages", body)
	crossFetch.Header.Del("Origin")
	crossFetch.Header.Set("Sec-Fetch-Site", "cross-site")
	if writer := chatServe(t, hub, crossFetch); writer.Code != http.StatusForbidden {
		t.Fatalf("Sec-Fetch-Site cross-site status=%d, want 403", writer.Code)
	}
	sameSite := chatUIRequest(t, http.MethodPost, "/chat/messages", body)
	sameSite.Header.Del("Origin")
	sameSite.Header.Set("Sec-Fetch-Site", "same-site")
	if writer := chatServe(t, hub, sameSite); writer.Code != http.StatusForbidden {
		t.Fatalf("Sec-Fetch-Site same-site status=%d, want 403", writer.Code)
	}
	sameOrigin := chatUIRequest(t, http.MethodPost, "/chat/messages", body)
	if writer := chatServe(t, hub, sameOrigin); writer.Code != http.StatusCreated {
		t.Fatalf("same-origin status=%d body=%q, want 201", writer.Code, writer.Body.String())
	}
	// Auth runs before the origin check: an unauthenticated POST stays 404.
	unauthenticated := httptest.NewRequest(http.MethodPost, "/chat/messages", strings.NewReader(body))
	unauthenticated.RemoteAddr = "198.51.100.25:4444"
	if writer := chatServe(t, hub, unauthenticated); writer.Code != http.StatusNotFound {
		t.Fatalf("unauthenticated POST status=%d, want 404", writer.Code)
	}
}

// AC3: the desk hook path is operator-token authenticated.
func TestHubChatQuestionHookAuth(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	body := `{"id":"Q-20260917-01","lane":"lane-a","body":"배포해도 되나요?"}`
	if writer := chatPostQuestion(t, hub, "", body); writer.Code != http.StatusUnauthorized {
		t.Fatalf("no token status=%d, want 401", writer.Code)
	}
	if writer := chatPostQuestion(t, hub, "wrong", body); writer.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d, want 401", writer.Code)
	}
	if writer := chatPostQuestion(t, hub, chatNodeToken, body); writer.Code != http.StatusUnauthorized {
		t.Fatalf("node token status=%d, want 401", writer.Code)
	}
	writer := chatPostQuestion(t, hub, chatOperatorToken, body)
	if writer.Code != http.StatusCreated {
		t.Fatalf("operator token status=%d body=%q, want 201", writer.Code, writer.Body.String())
	}
	var question ChatQuestion
	if err := json.Unmarshal(writer.Body.Bytes(), &question); err != nil || question.State != "pending" {
		t.Fatalf("question=%+v err=%v", question, err)
	}
	if again := chatPostQuestion(t, hub, chatOperatorToken, body); again.Code != http.StatusOK {
		t.Fatalf("repeat upsert status=%d, want 200", again.Code)
	}
	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"bad","lane":"lane-a","body":"x"}`); writer.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status=%d, want 400", writer.Code)
	}
	// Without a configured chat store the hook gets a clean 503, not a crash.
	bare := chatTestHub(t, `{"lanes":{}}`, nil, nil)
	if writer := chatPostQuestion(t, bare, chatOperatorToken, body); writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("storeless hook status=%d, want 503", writer.Code)
	}
}

// AC4: the operator token never appears in any browser-served surface.
func TestHubChatNoSecretsInBrowserSurface(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-02","lane":"lane-a","body":"q"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"a"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	for _, forbidden := range []string{chatOperatorToken, chatNodeToken, "HUB_TOKEN", "Bearer", "Authorization", "handoffkeep"} {
		if strings.Contains(hubChatHTML, forbidden) {
			t.Fatalf("hub_chat.html embeds %q", forbidden)
		}
	}
	page := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat", ""))
	data := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	for _, forbidden := range []string{chatOperatorToken, chatNodeToken, "HUB_TOKEN", "Bearer", "Authorization"} {
		if strings.Contains(page.Body.String(), forbidden) {
			t.Fatalf("/chat page exposes %q", forbidden)
		}
		if strings.Contains(data.Body.String(), forbidden) {
			t.Fatalf("/chat/data exposes %q", forbidden)
		}
	}
}

// AC5: the full send flow is observable step by step — stored first, then the
// lane receives the [chat]-prefixed directive, then the row turns delivered
// and the answered question resolves.
func TestHubChatAnswerDeliveredFlow(t *testing.T) {
	fake, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}

	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-03","lane":"lane-a","body":"배포해도 되나요?"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"응 해라","question_id":"Q-20260917-03"}`)
	if writer.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%q", writer.Code, writer.Body.String())
	}
	message := chatDecodeMessage(t, writer)
	if message.ID != 1 || message.RelayState != "stored" {
		t.Fatalf("message=%+v, want stored id=1 (persistence precedes relay)", message)
	}
	select {
	case directive := <-destination.relays:
		t.Fatalf("relay happened inside the request path: %+v", directive)
	default:
	}

	hub.drainChatOutbox(context.Background())

	select {
	case directive := <-destination.relays:
		want := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: [chat] 응 해라"
		if directive.Type != "relay.inject" || directive.Kind != "lane.event" || directive.Pane != "w1:p1" || directive.Text != want {
			t.Fatalf("directive=%+v want text=%q", directive, want)
		}
	default:
		t.Fatal("lane did not receive the chat directive")
	}
	if got := store.messageState(1); got != "delivered" {
		t.Fatalf("message relay_state=%q, want delivered", got)
	}
	if store.message(1).DeliveredAt == nil {
		t.Fatal("delivered message has no delivered_at")
	}
	if got := store.questionState("Q-20260917-03"); got != "resolved" {
		t.Fatalf("question state=%q, want resolved", got)
	}
	fake.mu.Lock()
	var relayedText, relayedEventID string
	for _, call := range fake.calls {
		if call.Method == http.MethodPost && call.Path == "/v1/relay/events" {
			relayedText, _ = call.Body["text"].(string)
			relayedEventID, _ = call.Body["event_id"].(string)
		}
	}
	fake.mu.Unlock()
	if relayedText != "[chat] 응 해라" || relayedEventID != chatRelayEventID(1, "응 해라") {
		t.Fatalf("handoffkeep relay row text=%q event_id=%q", relayedText, relayedEventID)
	}
}

// AC6: over-limit answers reach the lane as a one-line reference; the full
// body stays in the store. The boundary is exact: 2048 bytes go inline.
func TestHubChatLongMessageReference(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}

	longBody := strings.Repeat("a", 2042) // "[chat] "+body = 2049 bytes
	writer := chatPostMessage(t, hub, fmt.Sprintf(`{"lane":"lane-a","body":%q}`, longBody))
	if writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%q", writer.Code, writer.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	select {
	case directive := <-destination.relays:
		want := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: [chat] 긴 메시지 id=1"
		if directive.Text != want {
			t.Fatalf("directive.Text=%q, want reference %q", directive.Text, want)
		}
	default:
		t.Fatal("no directive for long message")
	}
	if got := store.message(1); got.Body != longBody || got.RelayState != "delivered" {
		t.Fatalf("stored body len=%d state=%q", len(got.Body), got.RelayState)
	}

	exactBody := strings.Repeat("b", 2041) // "[chat] "+body = exactly 2048 bytes
	if writer := chatPostMessage(t, hub, fmt.Sprintf(`{"lane":"lane-a","body":%q}`, exactBody)); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	select {
	case directive := <-destination.relays:
		if directive.Text != "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: [chat] "+exactBody {
			t.Fatalf("2048-byte answer was not sent inline: %.80q", directive.Text)
		}
	default:
		t.Fatal("no directive for boundary message")
	}

	// Relay text may never carry control characters; the pane gets a
	// single-line rendering while the store keeps the original.
	multi := "첫 줄\n둘째 줄\t탭"
	if writer := chatPostMessage(t, hub, fmt.Sprintf(`{"lane":"lane-a","body":%q}`, multi)); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	select {
	case directive := <-destination.relays:
		if !strings.HasSuffix(directive.Text, "[chat] 첫 줄 둘째 줄 탭") || strings.ContainsAny(directive.Text, "\n\t") {
			t.Fatalf("directive.Text=%q", directive.Text)
		}
	default:
		t.Fatal("no directive for multi-line message")
	}
	if got := store.message(3); got.Body != multi {
		t.Fatalf("stored body=%q, want the original multi-line body", got.Body)
	}
}

// AC7: a dead chat store changes nothing for relay ingress or hub startup.
func TestHubChatFailureIsolation(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	deadStore, err := newHandoffkeepChatStore(hubHandoffkeepEnv{URL: "http://127.0.0.1:1", Token: "dead-store-token"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Startup with an unreachable chat store must succeed.
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, deadStore)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}

	if writer := r24PostIngress(t, hub, chatOperatorToken, r24IngressBody("isolation-1", "relay still works")); writer.Code != http.StatusCreated {
		t.Fatalf("relay ingress with dead chat store status=%d body=%q", writer.Code, writer.Body.String())
	}
	start := time.Now()
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", "")); writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("/chat/data with dead store status=%d, want 503", writer.Code)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("chat data call took %v against a dead store", elapsed)
	}
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"x"}`); writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("message create with dead store status=%d, want 503", writer.Code)
	}
	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-04","lane":"lane-a","body":"q"}`); writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("hook with dead store status=%d, want 503", writer.Code)
	}
	// The outbox drain against the dead store must degrade quietly too.
	hub.drainChatOutbox(context.Background())
	// Relay still works after every chat failure above.
	if writer := r24PostIngress(t, hub, chatOperatorToken, r24IngressBody("isolation-2", "relay still works")); writer.Code != http.StatusCreated {
		t.Fatalf("relay ingress after chat failures status=%d", writer.Code)
	}
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat", "")); writer.Code != http.StatusOK {
		t.Fatalf("/chat page with dead store status=%d, want 200", writer.Code)
	}
}

// AC8: a panicking chat store cannot take the hub down.
func TestHubChatPanicIsolation(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)

	store.mu.Lock()
	store.panicOn = "ListChatQuestions"
	store.mu.Unlock()
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", "")); writer.Code != http.StatusInternalServerError || !strings.Contains(writer.Body.String(), "chat_internal") {
		t.Fatalf("panicking /chat/data status=%d body=%q, want 500 chat_internal", writer.Code, writer.Body.String())
	}
	store.mu.Lock()
	store.panicOn = "CreateChatMessage"
	store.mu.Unlock()
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"x"}`); writer.Code != http.StatusInternalServerError {
		t.Fatalf("panicking create status=%d, want 500", writer.Code)
	}
	store.mu.Lock()
	store.panicOn = "UpsertChatQuestion"
	store.mu.Unlock()
	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-05","lane":"lane-a","body":"q"}`); writer.Code != http.StatusInternalServerError {
		t.Fatalf("panicking hook status=%d, want 500", writer.Code)
	}
	// The hub and its relay path are untouched.
	if writer := chatServe(t, hub, httptest.NewRequest(http.MethodGet, "/healthz", nil)); writer.Code != http.StatusOK {
		t.Fatalf("healthz after chat panics status=%d", writer.Code)
	}
	if writer := r24PostIngress(t, hub, chatOperatorToken, r24IngressBody("panic-1", "relay alive")); writer.Code != http.StatusCreated {
		t.Fatalf("relay after chat panics status=%d", writer.Code)
	}
}

// AC9: failed rows are prominent and carry working retry/cancel; cancel of a
// stored row ends it in the terminal state the retention rules cover.
func TestHubChatFailedRetryCancel(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}

	// The UI keeps the failed state loud: styling, retry, and cancel hooks.
	for _, hook := range []string{"전송 실패", "재전송", "취소", ".msg.failed", `relay_state==="failed"`} {
		if !strings.Contains(hubChatHTML, hook) {
			t.Fatalf("hub_chat.html lacks the %q failed-state hook", hook)
		}
	}

	// A relay row that cannot be persisted fails honestly. Toggle the durable
	// client off for this one send, the way a handoffkeep outage reads.
	writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"안녕"}`)
	if writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.handoffkeep = nil
	hub.drainChatOutbox(context.Background())
	hub.handoffkeep = relay
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("unpersisted message state=%q, want failed", got)
	}
	data := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if !strings.Contains(data.Body.String(), `"relay_state":"failed"`) {
		t.Fatalf("/chat/data does not surface the failed row: %q", data.Body.String())
	}

	// Retry re-sends the body as a new row; the failure record stays. The
	// recorded lane survives, so the retry body can be empty.
	retry := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`))
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry status=%d body=%q", retry.Code, retry.Body.String())
	}
	resent := chatDecodeMessage(t, retry)
	if resent.ID != 2 || resent.RelayState != "stored" || resent.Body != "안녕" {
		t.Fatalf("resent=%+v", resent)
	}
	hub.drainChatOutbox(context.Background())
	if got := store.messageState(2); got != "delivered" {
		t.Fatalf("resent message state=%q, want delivered", got)
	}
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("original failure record state=%q, want failed", got)
	}
	select {
	case directive := <-destination.relays:
		if !strings.HasSuffix(directive.Text, "[chat] 안녕") {
			t.Fatalf("retried directive text=%q", directive.Text)
		}
	default:
		t.Fatal("retry did not reach the lane")
	}
	// Retry of a non-failed row is a conflict.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/2/retry", `{}`)); writer.Code != http.StatusConflict {
		t.Fatalf("retry of delivered status=%d, want 409", writer.Code)
	}

	// Cancel of a stored (not yet relayed) row ends it delivered: terminal and
	// inside the retention rules.
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"잠깐 보류"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	cancel := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/3/cancel", `{}`))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%q", cancel.Code, cancel.Body.String())
	}
	cancelled := chatDecodeMessage(t, cancel)
	if cancelled.RelayState != "delivered" || cancelled.DeliveredAt == nil {
		t.Fatalf("cancelled=%+v, want terminal delivered", cancelled)
	}
	// The store records cancel as delivered; the view must still tell the
	// operator it was a close-out, not a delivery that reached a pane.
	data = chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if !strings.Contains(data.Body.String(), `"cancelled":true`) {
		t.Fatalf("/chat/data does not mark the cancelled row: %q", data.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	if drained := drainRelays(destination); drained != 0 {
		t.Fatalf("cancelled message was relayed (%d directives)", drained)
	}

	// handoffkeep makes failed a sink: cancel there is an honest 409, and
	// cancel of a delivered row is idempotent.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/cancel", `{}`)); writer.Code != http.StatusConflict || !strings.Contains(writer.Body.String(), "chat_message_terminal") {
		t.Fatalf("cancel of failed status=%d body=%q, want 409 chat_message_terminal", writer.Code, writer.Body.String())
	}
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/3/cancel", `{}`)); writer.Code != http.StatusOK {
		t.Fatalf("repeat cancel status=%d, want 200", writer.Code)
	}
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/999/cancel", `{}`)); writer.Code != http.StatusNotFound {
		t.Fatalf("cancel of missing status=%d, want 404", writer.Code)
	}
}

// A stored row the hub has no lane for (a restart lost the in-memory mapping)
// must not display as "전송 중" forever: after the grace period the sweep turns
// it into a visible failed row.
func TestHubChatOrphanSweep(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	ctx := context.Background()
	old, err := store.CreateChatMessage(ctx, "operator", "재시작 전에 만든 메시지")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.messages[old.ID].CreatedAt = time.Now().UTC().Add(-time.Hour)
	store.mu.Unlock()
	fresh, err := store.CreateChatMessage(ctx, "operator", "방금 만든 메시지")
	if err != nil {
		t.Fatal(err)
	}
	hub.drainChatOutbox(ctx)
	if got := store.messageState(old.ID); got != "failed" {
		t.Fatalf("orphan state=%q, want failed", got)
	}
	if got := store.messageState(fresh.ID); got != "stored" {
		t.Fatalf("fresh row state=%q, want stored inside the grace period", got)
	}
}

// Without the durable relay store the lane.event path cannot run; the chat row
// must surface that as failed rather than pretend delivery.
func TestHubChatMessageFailsWithoutRelayPersistence(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, store)
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"x"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("state=%q, want failed without relay persistence", got)
	}
}

// The store's 400/404/409 keep their meaning through the hub.
func TestHubChatStoreErrorMapping(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-06","lane":"lane-a","body":"q"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	transition := func(id, to string) *httptest.ResponseRecorder {
		return chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/questions/"+id+"/transition", fmt.Sprintf(`{"to":%q}`, to)))
	}
	if writer := transition("Q-20260917-06", "resolved"); writer.Code != http.StatusOK {
		t.Fatalf("transition status=%d body=%q", writer.Code, writer.Body.String())
	}
	if writer := transition("Q-20260917-06", "resolved"); writer.Code != http.StatusConflict {
		t.Fatalf("repeat transition status=%d, want 409", writer.Code)
	}
	if writer := transition("Q-20260917-99", "resolved"); writer.Code != http.StatusNotFound {
		t.Fatalf("missing transition status=%d, want 404", writer.Code)
	}
	if writer := transition("Q-20260917-06", "bogus"); writer.Code != http.StatusBadRequest {
		t.Fatalf("bogus transition status=%d, want 400", writer.Code)
	}
	store.mu.Lock()
	store.err = errors.New("store down")
	store.mu.Unlock()
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", "")); writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("data with store error status=%d, want 503", writer.Code)
	}
}

// The maintenance loop's kick delivers without any test-visible sleep.
func TestHubChatDispatcherKick(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.RunMaintenance(ctx)

	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"킥으로 전달"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	r6Eventually(t, "chat message delivered via dispatcher kick", func() bool {
		return store.messageState(1) == "delivered"
	})
	select {
	case directive := <-destination.relays:
		if !strings.HasSuffix(directive.Text, "[chat] 킥으로 전달") {
			t.Fatalf("directive=%q", directive.Text)
		}
	default:
		t.Fatal("dispatcher never injected")
	}
}

// The CLI wires --chat-env, defaulting to --handoffkeep-env.
func TestHubChatCLIWiring(t *testing.T) {
	dir := t.TempDir()
	authPath := dir + "/hub-auth.env"
	if err := writeFile0600(authPath, "HUB_TOKEN_operator=cli-op\nHUB_TOKEN_node-a=cli-node\n"); err != nil {
		t.Fatal(err)
	}
	hkEnv := dir + "/handoffkeep.env"
	if err := writeFile0600(hkEnv, "HANDOFFKEEP_URL=http://127.0.0.1:18080\nHANDOFFKEEP_TOKEN=hk-token\n"); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--handoffkeep-env", hkEnv}, nil)
	if err != nil || code != ExitOK {
		t.Fatalf("hub with handoffkeep env: code=%d err=%v", code, err)
	}
	if hub.chatStore == nil {
		t.Fatal("chat store did not default to the handoffkeep env")
	}
	bare, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath}, nil)
	if err != nil || code != ExitOK {
		t.Fatalf("bare hub: code=%d err=%v", code, err)
	}
	if bare.chatStore != nil {
		t.Fatal("chat store without any env must stay disabled")
	}
	chatEnv := dir + "/chat.env"
	if err := writeFile0600(chatEnv, "HANDOFFKEEP_URL=http://127.0.0.1:1\nHANDOFFKEEP_TOKEN=chat-token\n"); err != nil {
		t.Fatal(err)
	}
	split, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--handoffkeep-env", hkEnv, "--chat-env", chatEnv}, nil)
	if err != nil || code != ExitOK {
		t.Fatalf("split hub: code=%d err=%v", code, err)
	}
	chatStore, ok := split.chatStore.(*handoffkeepChatStore)
	if !ok || chatStore.baseURL.Host != "127.0.0.1:1" {
		t.Fatalf("--chat-env did not override the chat store: %+v", split.chatStore)
	}
}

func writeFile0600(path, contents string) error {
	return os.WriteFile(path, []byte(contents), 0600)
}

// B2: a send that persists but cannot be injected stays stored — the durable
// relay row is the queue, and replay owns it. When the node registers, the
// row is injected exactly once and the chat row flips to delivered; the
// question link rides the relay row and resolves too. Reverting the fix
// (marking the unrouted send failed) turns the stored assertion red, and the
// row would then be injected while displaying "전송 실패".
func TestHubChatQueuedThenReplayedOnNodeRegister(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	ctx := context.Background()

	if writer := chatPostQuestion(t, hub, chatOperatorToken, `{"id":"Q-20260917-10","lane":"lane-a","body":"배포해도 되나요?"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	// The destination node is not connected: the row persists unrouted.
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"응 해라","question_id":"Q-20260917-10"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(ctx)
	if got := store.messageState(1); got != "stored" {
		t.Fatalf("uninjected message state=%q, want stored (queued, not failed)", got)
	}
	// The queued row must survive repeated drains without turning failed.
	hub.drainChatOutbox(ctx)
	if got := store.messageState(1); got != "stored" {
		t.Fatalf("queued message decayed to %q across drains", got)
	}

	// The node registers: replay injects the durable row exactly once, the
	// chat row turns delivered, and the linked question resolves.
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.replayUndeliveredLaneEvents(ctx)
	injected := drainRelays(destination)
	if injected != 1 {
		t.Fatalf("node registration injected %d directives, want exactly 1", injected)
	}
	if got := store.messageState(1); got != "delivered" {
		t.Fatalf("replayed message state=%q, want delivered", got)
	}
	if got := store.questionState("Q-20260917-10"); got != "resolved" {
		t.Fatalf("linked question state=%q, want resolved", got)
	}
	// A second replay must not inject again.
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("second replay re-injected %d directives", injected)
	}
}

// B2: a failed message never has a live relay row, so nothing about it can be
// replayed into a pane — and retry produces exactly one injection.
func TestHubChatFailedNeverReplayed(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	ctx := context.Background()

	// Persist failure is the only path to failed under the new contract.
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"첫 지시"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.handoffkeep = nil
	hub.drainChatOutbox(ctx)
	hub.handoffkeep = relay
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("state=%q, want failed", got)
	}
	// Node (re)registration replays nothing for the failed row.
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("failed message was injected on node registration (%d directives)", injected)
	}
	// Retry is the only way forward — exactly one injection.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`)); writer.Code != http.StatusCreated {
		t.Fatalf("retry status=%d body=%q", writer.Code, writer.Body.String())
	}
	hub.drainChatOutbox(ctx)
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 1 {
		t.Fatalf("retried message injected %d times, want exactly 1", injected)
	}
	if got := store.messageState(2); got != "delivered" {
		t.Fatalf("retried message state=%q, want delivered", got)
	}
}

// B2 legacy guard: a row that is failed while a live relay row still exists
// (the pre-fix lie, or a cancel/replay race) must still never double-deliver.
// Replay retires the stray row without injecting, and retry refuses while the
// row is live — then succeeds once it is gone.
func TestHubChatFailedWithLiveRelayRowNeverInjects(t *testing.T) {
	fake, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	ctx := context.Background()

	// Route exists but the node is offline: persisted, unrouted, queued.
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"보류 지시"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(ctx)
	if got := store.messageState(1); got != "stored" {
		t.Fatalf("state=%q, want stored", got)
	}
	// Recreate the pre-fix state: chat row failed while the relay row lives.
	if _, err := store.MarkChatMessageFailed(ctx, 1); err != nil {
		t.Fatal(err)
	}
	// Retry while the relay row is live is refused — it would double-inject.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`)); writer.Code != http.StatusConflict {
		t.Fatalf("retry of failed-but-queued status=%d body=%q, want 409", writer.Code, writer.Body.String())
	}
	// The node registers: replay sees the failed chat row, retires the relay
	// row, and injects nothing.
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("failed message injected on registration (%d directives)", injected)
	}
	fake.mu.Lock()
	var liveUndelivered int
	for _, row := range fake.rows {
		if row.DeliveredAt == "" && row.Kind == "lane.event" {
			liveUndelivered++
		}
	}
	fake.mu.Unlock()
	if liveUndelivered != 0 {
		t.Fatalf("%d undelivered chat relay rows remain after failed-row replay", liveUndelivered)
	}
	// With the stray row retired, retry works and injects exactly once.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`)); writer.Code != http.StatusCreated {
		t.Fatalf("retry after retire status=%d body=%q", writer.Code, writer.Body.String())
	}
	hub.drainChatOutbox(ctx)
	if injected := drainRelays(destination); injected != 1 {
		t.Fatalf("retry injected %d times, want exactly 1", injected)
	}
	if got := store.messageState(2); got != "delivered" {
		t.Fatalf("retried message state=%q, want delivered", got)
	}
}

// B3: retry resolves the question the original message answered — the hub's
// recorded link wins over whatever the client happens to have selected, and
// over an empty field. The C2 mutant (dropping QuestionID) and a client-
// wins mutant both turn these assertions red.
func TestHubChatRetryResolvesOriginalQuestion(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	ctx := context.Background()

	for _, id := range []string{"Q-20260917-20", "Q-20260917-21"} {
		if writer := chatPostQuestion(t, hub, chatOperatorToken, fmt.Sprintf(`{"id":%q,"lane":"lane-a","body":"질문"}`, id)); writer.Code != http.StatusCreated {
			t.Fatal(writer.Body.String())
		}
	}
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"원 질문 답변","question_id":"Q-20260917-20"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	// Fail the send so it becomes retryable.
	hub.handoffkeep = nil
	hub.drainChatOutbox(ctx)
	hub.handoffkeep = relay
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("state=%q, want failed", got)
	}
	if got := store.questionState("Q-20260917-20"); got != "pending" {
		t.Fatalf("original question state=%q, want pending", got)
	}
	// The operator has a different question selected; the retry names the
	// wrong id and even a different (unroutable) lane on the wire — the hub's
	// recorded link must win on both.
	retry := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{"lane":"lane-b","question_id":"Q-20260917-21"}`))
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry status=%d body=%q", retry.Code, retry.Body.String())
	}
	hub.drainChatOutbox(ctx)
	if got := store.messageState(2); got != "delivered" {
		t.Fatalf("retried message state=%q, want delivered", got)
	}
	if got := store.questionState("Q-20260917-20"); got != "resolved" {
		t.Fatalf("original question state=%q, want resolved", got)
	}
	if got := store.questionState("Q-20260917-21"); got != "pending" {
		t.Fatalf("unrelated question state=%q, want pending (never resolved by this retry)", got)
	}
}

// B1: /chat/data serves the newest rows no matter how large the table grows.
// The old after_id=0/limit=200 head window is the mutant — with it, the new
// question and message created below never appear and every assertion is red.
func TestHubChatDataServesTail(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, store)
	ctx := context.Background()

	// 220 old rows — all beyond the 200-row head window.
	for i := 0; i < 220; i++ {
		if _, err := store.CreateChatMessage(ctx, "desk", fmt.Sprintf("옛 메시지 %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	oldDay := time.Now().UTC().Add(-10 * 24 * time.Hour).Format("20060102")
	for i := 0; i < 220; i++ {
		if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: fmt.Sprintf("Q-%s-%02d", oldDay, i+1), Lane: "lane-a", Body: "옛 질문"}); err != nil {
			t.Fatal(err)
		}
	}

	// New rows arrive after the table is already past the head window: one
	// fresh pending question, one fresh question already resolved, one answer.
	today := time.Now().UTC().Format("20060102")
	newPending := fmt.Sprintf("Q-%s-99", today)
	newResolved := fmt.Sprintf("Q-%s-98", today)
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: newPending, Lane: "lane-a", Body: "오늘 질문"}); err != nil {
		t.Fatal(err)
	}
	resolved, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: newResolved, Lane: "lane-a", Body: "오늘 처리된 질문"})
	if err != nil {
		t.Fatal(err)
	}
	_ = resolved
	if _, err := store.TransitionChatQuestion(ctx, newResolved, "resolved"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateChatMessage(ctx, "operator", "가장 새 답변"); err != nil {
		t.Fatal(err)
	}

	writer := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if writer.Code != http.StatusOK {
		t.Fatalf("data status=%d", writer.Code)
	}
	var data struct {
		Questions []ChatQuestion       `json:"questions"`
		Messages  []hubChatMessageView `json:"messages"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	var sawPending, sawResolved bool
	for _, question := range data.Questions {
		if question.ID == newPending {
			sawPending = true
		}
		if question.ID == newResolved {
			sawResolved = true
		}
	}
	if !sawPending {
		t.Fatalf("new pending question missing from /chat/data (%d questions served)", len(data.Questions))
	}
	if !sawResolved {
		t.Fatalf("new resolved question missing from /chat/data tail (%d questions served)", len(data.Questions))
	}
	if len(data.Messages) == 0 || data.Messages[len(data.Messages)-1].Body != "가장 새 답변" {
		t.Fatalf("newest message not at the tail of /chat/data (%d served)", len(data.Messages))
	}
	if len(data.Messages) > hubChatListLimit {
		t.Fatalf("/chat/data served %d messages, want at most %d", len(data.Messages), hubChatListLimit)
	}
}

// B1 sweep half: the orphan sweep walks past a large failed backlog instead of
// stalling on the oldest 200 undelivered rows.
func TestHubChatOrphanSweepSeesTail(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 250; i++ {
		message, err := store.CreateChatMessage(ctx, "operator", fmt.Sprintf("죽은 메시지 %d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkChatMessageFailed(ctx, message.ID); err != nil {
			t.Fatal(err)
		}
	}
	orphan, err := store.CreateChatMessage(ctx, "operator", "고아 메시지")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.messages[orphan.ID].CreatedAt = old
	store.mu.Unlock()

	hub.drainChatOutbox(ctx)
	if got := store.messageState(orphan.ID); got != "failed" {
		t.Fatalf("orphan beyond the old head window state=%q, want failed", got)
	}
	// The cursor keeps the sweep from re-reading the consumed backlog.
	hub.chatMu.Lock()
	cursor := hub.chatSweepCursor
	hub.chatMu.Unlock()
	if cursor != 250 {
		t.Fatalf("sweep cursor=%d, want 250 (the consumed failed backlog)", cursor)
	}
}

// S4: cancelling a queued message retires its durable relay row, so a later
// node registration can never inject what the operator cancelled.
func TestHubChatCancelRetiresRelayRow(t *testing.T) {
	fake, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	ctx := context.Background()

	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"보내지 마라"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(ctx) // persisted, unrouted (no node), queued
	if got := store.messageState(1); got != "stored" {
		t.Fatalf("state=%q, want stored", got)
	}
	cancel := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/cancel", `{}`))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%q", cancel.Code, cancel.Body.String())
	}
	fake.mu.Lock()
	var liveUndelivered int
	for _, row := range fake.rows {
		if row.DeliveredAt == "" && row.Kind == "lane.event" {
			liveUndelivered++
		}
	}
	fake.mu.Unlock()
	if liveUndelivered != 0 {
		t.Fatalf("cancel left %d undelivered relay rows behind", liveUndelivered)
	}
	// Node registers: nothing is injected.
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("cancelled message injected on registration (%d directives)", injected)
	}
}

// C1 regression: the question hook validates the lane with the same pattern
// the answer path requires, or an unanswerable question gets persisted. The
// round-1 mutant (length-only check) makes this assertion red.
func TestHubChatQuestionLanePattern(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	for _, lane := range []string{"bad lane", "lane with spaces", "레인", "LANE!"} {
		body := fmt.Sprintf(`{"id":"Q-20260917-30","lane":%q,"body":"질문"}`, lane)
		if writer := chatPostQuestion(t, hub, chatOperatorToken, body); writer.Code != http.StatusBadRequest {
			t.Fatalf("lane %q status=%d body=%q, want 400", lane, writer.Code, writer.Body.String())
		}
	}
}

// B2 backstop: if cancel cannot reach handoffkeep, the chat row still closes
// but its relay row survives. Replay must retire that row, never inject it —
// the disposition gate checks the store, not just the failed state.
func TestHubChatCancelledWithLiveRelayRowNeverInjects(t *testing.T) {
	fake, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	ctx := context.Background()

	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"취소된 지시"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(ctx)
	if got := store.messageState(1); got != "stored" {
		t.Fatalf("state=%q, want stored", got)
	}
	// Cancel while handoffkeep is unreachable: the chat row closes but the
	// durable relay row survives.
	hub.handoffkeep = nil
	cancel := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/1/cancel", `{}`))
	hub.handoffkeep = relay
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%q", cancel.Code, cancel.Body.String())
	}
	if got := store.messageState(1); got != "delivered" {
		t.Fatalf("cancelled message state=%q, want delivered", got)
	}
	// Node registers: the surviving relay row is retired, never injected.
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.replayUndeliveredLaneEvents(ctx)
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("cancelled message injected on registration (%d directives)", injected)
	}
	fake.mu.Lock()
	var liveUndelivered int
	for _, row := range fake.rows {
		if row.DeliveredAt == "" && row.Kind == "lane.event" {
			liveUndelivered++
		}
	}
	fake.mu.Unlock()
	if liveUndelivered != 0 {
		t.Fatalf("%d undelivered chat relay rows remain after cancelled-row replay", liveUndelivered)
	}
}
