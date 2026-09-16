package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
)

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

func (f *fakeChatStore) ListChatQuestions(_ context.Context, state string, limit int) ([]ChatQuestion, error) {
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

// chatTestHub builds a hub with the chat store wired and UI auth enabled. The
// dispatcher is never started here: tests drive drainChatOutbox explicitly so
// the stored → relayed transition is observed deterministically.
func chatTestHub(t *testing.T, lanes string, relay *handoffkeepRelayClient, store ChatStore) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": chatOperatorToken, "host-a": chatNodeToken},
		ReportRelayPath: r20LanesFile(t, lanes),
		UIAllowCFOnly:   true,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     relay,
		ChatStore:       store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

// chatUIRequest builds a browser-shaped request: loopback (authorizeUI) with
// same-origin headers already set, so the pass case needs no extra work.
func chatUIRequest(method, target string, body string) *http.Request {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, target, reader)
	request.RemoteAddr = "127.0.0.1:49000"
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
	return chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages", body))
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

// AC1: /chat and /chat/data are UI-auth gated exactly like /ui.
func TestHubChatPageRequiresUIAuth(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	for _, path := range []string{"/chat", "/chat/data"} {
		denied := httptest.NewRequest(http.MethodGet, path, nil)
		denied.RemoteAddr = "198.51.100.25:4444"
		if writer := chatServe(t, hub, denied); writer.Code != http.StatusNotFound {
			t.Fatalf("%s without UI auth status=%d, want 404", path, writer.Code)
		}
		cloudflareNoIdentity := httptest.NewRequest(http.MethodGet, path, nil)
		cloudflareNoIdentity.RemoteAddr = "127.0.0.1:4444"
		cloudflareNoIdentity.Header.Set("Cf-Ray", "fixture-ray")
		if writer := chatServe(t, hub, cloudflareNoIdentity); writer.Code != http.StatusNotFound {
			t.Fatalf("%s with Cf-Ray but no identity status=%d, want 404", path, writer.Code)
		}
	}
	page := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat", ""))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "panewire chat") {
		t.Fatalf("loopback /chat status=%d", page.Code)
	}
	data := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", ""))
	if data.Code != http.StatusOK || !strings.Contains(data.Body.String(), `"questions":[]`) {
		t.Fatalf("loopback /chat/data status=%d body=%q", data.Code, data.Body.String())
	}
}

// AC2: cookie/identity-authenticated POSTs reject cross-origin callers.
func TestHubChatMessageCSRF(t *testing.T) {
	hub := chatTestHub(t, `{"lanes":{}}`, nil, newFakeChatStore())
	body := `{"lane":"lane-a","body":"hello"}`

	crossOrigin := chatUIRequest(http.MethodPost, "/chat/messages", body)
	crossOrigin.Header.Set("Origin", "https://evil.example")
	if writer := chatServe(t, hub, crossOrigin); writer.Code != http.StatusForbidden || !strings.Contains(writer.Body.String(), "cross_origin") {
		t.Fatalf("cross-origin Origin status=%d body=%q, want 403", writer.Code, writer.Body.String())
	}
	crossFetch := chatUIRequest(http.MethodPost, "/chat/messages", body)
	crossFetch.Header.Del("Origin")
	crossFetch.Header.Set("Sec-Fetch-Site", "cross-site")
	if writer := chatServe(t, hub, crossFetch); writer.Code != http.StatusForbidden {
		t.Fatalf("Sec-Fetch-Site cross-site status=%d, want 403", writer.Code)
	}
	sameSite := chatUIRequest(http.MethodPost, "/chat/messages", body)
	sameSite.Header.Del("Origin")
	sameSite.Header.Set("Sec-Fetch-Site", "same-site")
	if writer := chatServe(t, hub, sameSite); writer.Code != http.StatusForbidden {
		t.Fatalf("Sec-Fetch-Site same-site status=%d, want 403", writer.Code)
	}
	sameOrigin := chatUIRequest(http.MethodPost, "/chat/messages", body)
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
	page := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat", ""))
	data := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", ""))
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
	if relayedText != "[chat] 응 해라" || relayedEventID != "chat-1" {
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
	if writer := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", "")); writer.Code != http.StatusServiceUnavailable {
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
	if writer := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat", "")); writer.Code != http.StatusOK {
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
	if writer := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", "")); writer.Code != http.StatusInternalServerError || !strings.Contains(writer.Body.String(), "chat_internal") {
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

	// lane-missing has no route: the send turns the row failed.
	writer := chatPostMessage(t, hub, `{"lane":"lane-missing","body":"안녕"}`)
	if writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	hub.drainChatOutbox(context.Background())
	if got := store.messageState(1); got != "failed" {
		t.Fatalf("unrouted message state=%q, want failed", got)
	}
	data := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", ""))
	if !strings.Contains(data.Body.String(), `"relay_state":"failed"`) {
		t.Fatalf("/chat/data does not surface the failed row: %q", data.Body.String())
	}

	// Retry re-sends the body as a new row; the failure record stays.
	retry := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/1/retry", `{"lane":"lane-a"}`))
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
	if writer := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/2/retry", `{"lane":"lane-a"}`)); writer.Code != http.StatusConflict {
		t.Fatalf("retry of delivered status=%d, want 409", writer.Code)
	}

	// Cancel of a stored (not yet relayed) row ends it delivered: terminal and
	// inside the retention rules.
	if writer := chatPostMessage(t, hub, `{"lane":"lane-a","body":"잠깐 보류"}`); writer.Code != http.StatusCreated {
		t.Fatal(writer.Body.String())
	}
	cancel := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/3/cancel", `{}`))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%q", cancel.Code, cancel.Body.String())
	}
	cancelled := chatDecodeMessage(t, cancel)
	if cancelled.RelayState != "delivered" || cancelled.DeliveredAt == nil {
		t.Fatalf("cancelled=%+v, want terminal delivered", cancelled)
	}
	hub.drainChatOutbox(context.Background())
	if drained := drainRelays(destination); drained != 0 {
		t.Fatalf("cancelled message was relayed (%d directives)", drained)
	}

	// handoffkeep makes failed a sink: cancel there is an honest 409, and
	// cancel of a delivered row is idempotent.
	if writer := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/1/cancel", `{}`)); writer.Code != http.StatusConflict || !strings.Contains(writer.Body.String(), "chat_message_terminal") {
		t.Fatalf("cancel of failed status=%d body=%q, want 409 chat_message_terminal", writer.Code, writer.Body.String())
	}
	if writer := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/3/cancel", `{}`)); writer.Code != http.StatusOK {
		t.Fatalf("repeat cancel status=%d, want 200", writer.Code)
	}
	if writer := chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/messages/999/cancel", `{}`)); writer.Code != http.StatusNotFound {
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
		return chatServe(t, hub, chatUIRequest(http.MethodPost, "/chat/questions/"+id+"/transition", fmt.Sprintf(`{"to":%q}`, to)))
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
	if writer := chatServe(t, hub, chatUIRequest(http.MethodGet, "/chat/data", "")); writer.Code != http.StatusServiceUnavailable {
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
