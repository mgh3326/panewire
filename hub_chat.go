package panewire

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The operator chat screen persists through handoffkeep's /v1/chat/* API
// (handoffkeep@84e8b14). This file mirrors that contract exactly: question ids
// are producer-assigned Q-YYYYMMDD-NN, question states are
// pending/resolved/withdrawn, message relay states are stored/delivered/failed,
// and the only retention-terminal message state is delivered. handoffkeep's
// state machine makes failed a sink (stored → delivered|failed only), so a
// failed row can never transition again; retry therefore records a new row and
// cancel is a stored → delivered close-out.
const (
	hubChatStoreTimeout     = 5 * time.Second
	hubChatDispatchInterval = 5 * time.Second
	// hubChatOrphanGrace keeps a freshly created row out of the orphan sweep
	// until its registering POST has had time to register the pending entry.
	hubChatOrphanGrace = 30 * time.Second
	// hubChatMaxConnsPerHost bounds the chat store connection pool so a slow
	// chat backend can never exhaust the hub's sockets.
	hubChatMaxConnsPerHost = 8
	// hubChatMaxBodyBytes mirrors handoffkeep's store.MaxBytes.
	hubChatMaxBodyBytes  = 64 << 10
	hubChatListLimit     = 200
	hubChatRequestSlop   = 4096
	hubChatOperatorName  = "operator"
	hubChatTerminalState = "delivered"
)

var chatQuestionIDPattern = regexp.MustCompile(`^Q-[0-9]{8}-[0-9]{2,}$`)

// ChatQuestion is handoffkeep's durable desk question, mirrored field for field.
type ChatQuestion struct {
	ID         string     `json:"id"`
	Lane       string     `json:"lane"`
	Body       string     `json:"body"`
	State      string     `json:"state"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	ResolvedAt *time.Time `json:"resolved_at"`
}

// ChatMessage is handoffkeep's durable operator/desk message, mirrored field
// for field. Messages deliberately carry no lane: the delivery target is a
// property of the relay attempt, not of the record.
type ChatMessage struct {
	ID          int64      `json:"id"`
	Author      string     `json:"author"`
	Body        string     `json:"body"`
	RelayState  string     `json:"relay_state"`
	CreatedAt   time.Time  `json:"created_at"`
	DeliveredAt *time.Time `json:"delivered_at"`
}

// ChatQuestionUpsert is the producer's question write. The id is the upsert key.
type ChatQuestionUpsert struct {
	ID   string `json:"id"`
	Lane string `json:"lane"`
	Body string `json:"body"`
}

// ChatStore hides chat persistence behind an interface so the storage backend
// can be split out of handoffkeep later without touching the hub handlers.
type ChatStore interface {
	UpsertChatQuestion(ctx context.Context, in ChatQuestionUpsert) (ChatQuestion, bool, error)
	TransitionChatQuestion(ctx context.Context, id, to string) (ChatQuestion, error)
	ListChatQuestions(ctx context.Context, state string, limit int) ([]ChatQuestion, error)
	CreateChatMessage(ctx context.Context, author, body string) (ChatMessage, error)
	MarkChatMessageDelivered(ctx context.Context, id int64) (ChatMessage, error)
	MarkChatMessageFailed(ctx context.Context, id int64) (ChatMessage, error)
	ListChatMessages(ctx context.Context, undelivered bool, afterID int64, limit int) ([]ChatMessage, error)
	GetChatMessage(ctx context.Context, id int64) (ChatMessage, bool, error)
}

// chatHTTPError carries the store's HTTP status so handlers can map 400/404/409
// faithfully instead of flattening every failure into one error.
type chatHTTPError struct {
	op     string
	status int
}

func (e *chatHTTPError) Error() string { return "chat store " + e.op + " rejected" }

// handoffkeepChatStore is the ChatStore over handoffkeep's /v1/chat/* HTTP API.
// It is a separate client from the relay client so a chat outage has its own
// timeout and connection pool and cannot stall relay traffic.
type handoffkeepChatStore struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

func newHandoffkeepChatStore(env hubHandoffkeepEnv, httpClient *http.Client) (*handoffkeepChatStore, error) {
	if !validHandoffkeepBaseURL(env.URL) || !validHandoffkeepToken(env.Token) {
		return nil, errors.New("hub chat store configuration is invalid")
	}
	parsed, err := url.Parse(strings.TrimSpace(env.URL))
	if err != nil {
		return nil, errors.New("hub chat store configuration is invalid")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout:   hubChatStoreTimeout,
			Transport: &http.Transport{MaxConnsPerHost: hubChatMaxConnsPerHost, MaxIdleConnsPerHost: hubChatMaxConnsPerHost},
		}
	}
	return &handoffkeepChatStore{baseURL: parsed, token: env.Token, client: httpClient}, nil
}

func (c *handoffkeepChatStore) endpoint(path string) string {
	target := *c.baseURL
	target.Path = strings.TrimSuffix(target.Path, "/") + path
	return target.String()
}

func (c *handoffkeepChatStore) do(ctx context.Context, method, endpoint string, body []byte) (int, []byte, error) {
	requestContext, cancel := context.WithTimeout(ctx, hubChatStoreTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(requestContext, method, endpoint, reader)
	if err != nil {
		return 0, nil, errors.New("chat store request failed")
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, nil, errors.New("chat store request failed")
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, payload, nil
}

func chatStoreStatusError(op string, status int) error {
	return &chatHTTPError{op: op, status: status}
}

func (c *handoffkeepChatStore) UpsertChatQuestion(ctx context.Context, in ChatQuestionUpsert) (ChatQuestion, bool, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return ChatQuestion{}, false, errors.New("chat store request encoding failed")
	}
	status, payload, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/chat/questions"), body)
	if err != nil {
		return ChatQuestion{}, false, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return ChatQuestion{}, false, chatStoreStatusError("question upsert", status)
	}
	var question ChatQuestion
	if json.Unmarshal(payload, &question) != nil || question.ID == "" {
		return ChatQuestion{}, false, errors.New("chat store returned an unusable question")
	}
	return question, status == http.StatusCreated, nil
}

func (c *handoffkeepChatStore) TransitionChatQuestion(ctx context.Context, id, to string) (ChatQuestion, error) {
	body, err := json.Marshal(struct {
		To string `json:"to"`
	}{To: to})
	if err != nil {
		return ChatQuestion{}, errors.New("chat store request encoding failed")
	}
	status, payload, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/chat/questions/"+url.PathEscape(id)+"/transition"), body)
	if err != nil {
		return ChatQuestion{}, err
	}
	if status != http.StatusOK {
		return ChatQuestion{}, chatStoreStatusError("question transition", status)
	}
	var question ChatQuestion
	if json.Unmarshal(payload, &question) != nil || question.ID == "" {
		return ChatQuestion{}, errors.New("chat store returned an unusable question")
	}
	return question, nil
}

func (c *handoffkeepChatStore) ListChatQuestions(ctx context.Context, state string, limit int) ([]ChatQuestion, error) {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	if state != "" {
		query.Set("state", state)
	}
	status, payload, err := c.do(ctx, http.MethodGet, c.endpoint("/v1/chat/questions")+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, chatStoreStatusError("question list", status)
	}
	var result struct {
		Questions []ChatQuestion `json:"questions"`
	}
	if json.Unmarshal(payload, &result) != nil {
		return nil, errors.New("chat store returned an unusable question list")
	}
	return result.Questions, nil
}

func (c *handoffkeepChatStore) CreateChatMessage(ctx context.Context, author, body string) (ChatMessage, error) {
	payloadBody, err := json.Marshal(struct {
		Author string `json:"author"`
		Body   string `json:"body"`
	}{Author: author, Body: body})
	if err != nil {
		return ChatMessage{}, errors.New("chat store request encoding failed")
	}
	status, payload, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/chat/messages"), payloadBody)
	if err != nil {
		return ChatMessage{}, err
	}
	if status != http.StatusCreated {
		return ChatMessage{}, chatStoreStatusError("message create", status)
	}
	var message ChatMessage
	if json.Unmarshal(payload, &message) != nil || message.ID < 1 {
		return ChatMessage{}, errors.New("chat store returned an unusable message")
	}
	return message, nil
}

func (c *handoffkeepChatStore) markChatMessage(ctx context.Context, id int64, op string) (ChatMessage, error) {
	status, payload, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/chat/messages/"+strconv.FormatInt(id, 10)+"/"+op), nil)
	if err != nil {
		return ChatMessage{}, err
	}
	if status != http.StatusOK {
		return ChatMessage{}, chatStoreStatusError("message "+op, status)
	}
	var message ChatMessage
	if json.Unmarshal(payload, &message) != nil || message.ID < 1 {
		return ChatMessage{}, errors.New("chat store returned an unusable message")
	}
	return message, nil
}

func (c *handoffkeepChatStore) MarkChatMessageDelivered(ctx context.Context, id int64) (ChatMessage, error) {
	return c.markChatMessage(ctx, id, "delivered")
}

func (c *handoffkeepChatStore) MarkChatMessageFailed(ctx context.Context, id int64) (ChatMessage, error) {
	return c.markChatMessage(ctx, id, "failed")
}

func (c *handoffkeepChatStore) ListChatMessages(ctx context.Context, undelivered bool, afterID int64, limit int) ([]ChatMessage, error) {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	if undelivered {
		query.Set("undelivered", "1")
	}
	if afterID > 0 {
		query.Set("after_id", strconv.FormatInt(afterID, 10))
	}
	status, payload, err := c.do(ctx, http.MethodGet, c.endpoint("/v1/chat/messages")+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, chatStoreStatusError("message list", status)
	}
	var result struct {
		Messages []ChatMessage `json:"messages"`
	}
	if json.Unmarshal(payload, &result) != nil {
		return nil, errors.New("chat store returned an unusable message list")
	}
	return result.Messages, nil
}

// GetChatMessage reads one row through the id cursor: handoffkeep exposes no
// single-message GET, but list order is id order, so the first row after id-1
// is either this message or proof it does not exist.
func (c *handoffkeepChatStore) GetChatMessage(ctx context.Context, id int64) (ChatMessage, bool, error) {
	messages, err := c.ListChatMessages(ctx, false, id-1, 1)
	if err != nil {
		return ChatMessage{}, false, err
	}
	if len(messages) == 0 || messages[0].ID != id {
		return ChatMessage{}, false, nil
	}
	return messages[0], true, nil
}

// chatPendingMessage is the hub's in-memory half of one outbound chat message:
// the store row deliberately has no lane column, so the delivery target lives
// here until the relay attempt settles.
type chatPendingMessage struct {
	Lane       string
	Body       string
	QuestionID string
}

//go:embed hub_chat.html
var hubChatHTML string

var hubChatTemplate = template.Must(template.New("hub-chat").Parse(hubChatHTML))

type hubChatData struct {
	SchemaVersion int            `json:"schema_version"`
	Questions     []ChatQuestion `json:"questions"`
	Messages      []ChatMessage  `json:"messages"`
}

// recoverChatPanic confines a chat handler panic to its own request. The hub
// carries the whole fleet's relay traffic; a chat bug must never take it down.
func (h *HubServer) recoverChatPanic(writer http.ResponseWriter) {
	if recovered := recover(); recovered != nil {
		h.logger.Error("chat handler panic", "panic", recovered)
		writeHubJSON(writer, http.StatusInternalServerError, map[string]string{"error": "chat_internal"})
	}
}

// hubChatSameOrigin is the CSRF boundary for the cookie/identity-authenticated
// browser POSTs: a cross-origin form or fetch may not ride the browser's
// ambient authority. Origin, when present, must match the request host;
// Sec-Fetch-Site, when present, must be same-origin (or the browser's own
// "none" for direct navigation).
func hubChatSameOrigin(request *http.Request) bool {
	if origin := strings.TrimSpace(request.Header.Get("Origin")); origin != "" {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Host == "" || !strings.EqualFold(parsed.Host, request.Host) {
			return false
		}
	}
	switch strings.TrimSpace(request.Header.Get("Sec-Fetch-Site")) {
	case "", "same-origin", "none":
		return true
	default:
		return false
	}
}

func (h *HubServer) authorizeChatUI(writer http.ResponseWriter, request *http.Request) bool {
	if !h.authorizeUI(request) {
		http.NotFound(writer, request)
		return false
	}
	return true
}

func (h *HubServer) authorizeChatUIPost(writer http.ResponseWriter, request *http.Request) bool {
	if !h.authorizeChatUI(writer, request) {
		return false
	}
	if !hubChatSameOrigin(request) {
		writeHubJSON(writer, http.StatusForbidden, map[string]string{"error": "cross_origin"})
		return false
	}
	return true
}

// writeChatStoreError maps store failures onto honest statuses: the store's own
// 400/404/409 keep their meaning, anything else upstream is 502, and an
// unreachable store is 503.
func (h *HubServer) writeChatStoreError(writer http.ResponseWriter, err error) {
	var httpErr *chatHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.status {
		case http.StatusBadRequest:
			writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		case http.StatusNotFound:
			writeHubJSON(writer, http.StatusNotFound, map[string]string{"error": "not_found"})
		case http.StatusConflict:
			writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_conflict"})
		default:
			writeHubJSON(writer, http.StatusBadGateway, map[string]string{"error": "chat_store_rejected"})
		}
		return
	}
	writeHubJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "chat_unavailable"})
}

func (h *HubServer) chatStoreOrUnavailable(writer http.ResponseWriter) ChatStore {
	if h.chatStore == nil {
		writeHubJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "chat_unavailable"})
		return nil
	}
	return h.chatStore
}

func (h *HubServer) handleChat(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUI(writer, request) {
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := hubChatTemplate.Execute(writer, nil); err != nil {
		h.logger.Error("hub chat template failed")
	}
}

func (h *HubServer) handleChatData(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUI(writer, request) {
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	questions, err := store.ListChatQuestions(ctx, "", hubChatListLimit)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	messages, err := store.ListChatMessages(ctx, false, 0, hubChatListLimit)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	if questions == nil {
		questions = []ChatQuestion{}
	}
	if messages == nil {
		messages = []ChatMessage{}
	}
	writeHubJSON(writer, http.StatusOK, hubChatData{SchemaVersion: 1, Questions: questions, Messages: messages})
}

type hubChatMessageInput struct {
	Lane       string `json:"lane"`
	Body       string `json:"body"`
	QuestionID string `json:"question_id,omitempty"`
}

func validChatMessageBody(body string) bool {
	return body != "" && len(body) <= hubChatMaxBodyBytes
}

// handleChatMessageCreate stores the operator's answer first (handoffkeep owns
// the durable record), then hands delivery to the background dispatcher. The
// request never waits on the relay path, so a slow lane cannot stall the UI.
func (h *HubServer) handleChatMessageCreate(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUIPost(writer, request) {
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	var input hubChatMessageInput
	if !decodeHubJSON(writer, request, hubChatMaxBodyBytes+hubChatRequestSlop, &input) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if !validReportRelayLaneName(input.Lane) || !validChatMessageBody(input.Body) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if input.QuestionID != "" && !chatQuestionIDPattern.MatchString(input.QuestionID) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	message, err := store.CreateChatMessage(ctx, hubChatOperatorName, input.Body)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	h.chatMu.Lock()
	h.chatPending[message.ID] = chatPendingMessage{Lane: input.Lane, Body: input.Body, QuestionID: input.QuestionID}
	h.chatMu.Unlock()
	h.kickChatDispatcher()
	writeHubJSON(writer, http.StatusCreated, message)
}

type hubChatMessageRetryInput struct {
	Lane       string `json:"lane"`
	QuestionID string `json:"question_id,omitempty"`
}

// handleChatMessageRetry re-sends a failed message's body as a new store row.
// handoffkeep's state machine makes failed a sink (stored → delivered|failed
// only), so retrying in place is impossible; the new row is the fresh attempt
// and the failed row remains as the permanent failure record.
func (h *HubServer) handleChatMessageRetry(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUIPost(writer, request) {
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	var input hubChatMessageRetryInput
	if !decodeHubJSON(writer, request, hubChatRequestSlop, &input) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if !validReportRelayLaneName(input.Lane) || (input.QuestionID != "" && !chatQuestionIDPattern.MatchString(input.QuestionID)) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	message, found, err := store.GetChatMessage(ctx, id)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	if !found {
		writeHubJSON(writer, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if message.RelayState != "failed" {
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_not_failed"})
		return
	}
	resent, err := store.CreateChatMessage(ctx, hubChatOperatorName, message.Body)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	h.chatMu.Lock()
	h.chatPending[resent.ID] = chatPendingMessage{Lane: input.Lane, Body: message.Body, QuestionID: input.QuestionID}
	h.chatMu.Unlock()
	h.kickChatDispatcher()
	writeHubJSON(writer, http.StatusCreated, resent)
}

// handleChatMessageCancel closes out a message without sending it. handoffkeep
// has exactly one retention-terminal message state — delivered — so cancel
// transitions stored → delivered and the row becomes eligible for the daily
// retention prune. A cancelled row never enters the dispatcher, so nothing is
// injected; delivered_at doubles as the close-out stamp. A failed row is
// already a sink in the store's state machine and cannot transition; the hub
// says so with 409 instead of pretending.
func (h *HubServer) handleChatMessageCancel(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUIPost(writer, request) {
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	message, found, err := store.GetChatMessage(ctx, id)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	if !found {
		writeHubJSON(writer, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	switch message.RelayState {
	case "delivered":
		// Already terminal; cancel is idempotent.
		writeHubJSON(writer, http.StatusOK, message)
	case "failed":
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_terminal", "relay_state": "failed"})
	default: // stored
		h.chatMu.Lock()
		delete(h.chatPending, id)
		h.chatMu.Unlock()
		closed, err := store.MarkChatMessageDelivered(ctx, id)
		if err != nil {
			h.writeChatStoreError(writer, err)
			return
		}
		if closed.RelayState != hubChatTerminalState {
			// The store is the authority: if the row raced to another state
			// (the dispatcher delivered or failed it first), report that truth.
			writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_terminal", "relay_state": closed.RelayState})
			return
		}
		writeHubJSON(writer, http.StatusOK, closed)
	}
}

type hubChatQuestionTransitionInput struct {
	To string `json:"to"`
}

func (h *HubServer) handleChatQuestionTransition(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeChatUIPost(writer, request) {
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	id := request.PathValue("id")
	if !chatQuestionIDPattern.MatchString(id) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	var input hubChatQuestionTransitionInput
	if !decodeHubJSON(writer, request, hubChatRequestSlop, &input) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if input.To != "resolved" && input.To != "withdrawn" {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	question, err := store.TransitionChatQuestion(ctx, id, input.To)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	writeHubJSON(writer, http.StatusOK, question)
}

// handleChatQuestionUpsert is the desk-session hook path. It is operator-token
// authenticated like every other /v1 machine API; the browser never sees it.
func (h *HubServer) handleChatQuestionUpsert(writer http.ResponseWriter, request *http.Request) {
	defer h.recoverChatPanic(writer)
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	store := h.chatStoreOrUnavailable(writer)
	if store == nil {
		return
	}
	var input ChatQuestionUpsert
	if !decodeHubJSON(writer, request, hubChatMaxBodyBytes+hubChatRequestSlop, &input) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if !chatQuestionIDPattern.MatchString(input.ID) || !validReportRelayLaneName(input.Lane) || input.Body == "" || len(input.Body) > hubChatMaxBodyBytes {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), hubChatStoreTimeout)
	defer cancel()
	question, created, err := store.UpsertChatQuestion(ctx, input)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeHubJSON(writer, status, question)
}

func (h *HubServer) kickChatDispatcher() {
	select {
	case h.chatKick <- struct{}{}:
	default:
	}
}

// runChatDispatcher is the chat outbox worker. It runs inside RunMaintenance's
// context and keeps every chat store call and relay wait out of the request
// path and out of h.mu. The ticker is the backstop; POSTs kick it directly.
func (h *HubServer) runChatDispatcher(ctx context.Context) {
	if h.chatStore == nil {
		return
	}
	ticker := time.NewTicker(hubChatDispatchInterval)
	defer ticker.Stop()
	h.drainChatOutboxSafely(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.chatKick:
			h.drainChatOutboxSafely(ctx)
		case <-ticker.C:
			h.drainChatOutboxSafely(ctx)
		}
	}
}

// drainChatOutboxSafely keeps a chat-side panic from ever killing the worker.
func (h *HubServer) drainChatOutboxSafely(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Error("chat outbox drain panic", "panic", recovered)
		}
	}()
	h.drainChatOutbox(ctx)
}

func (h *HubServer) drainChatOutbox(ctx context.Context) {
	if h.chatStore == nil {
		return
	}
	h.failOrphanChatMessages(ctx)
	for {
		h.chatMu.Lock()
		if len(h.chatPending) == 0 {
			h.chatMu.Unlock()
			return
		}
		var id int64
		var pending chatPendingMessage
		for candidateID, candidate := range h.chatPending {
			id, pending = candidateID, candidate
			break
		}
		delete(h.chatPending, id)
		h.chatMu.Unlock()
		// The relay and the store marks below are network calls made with no
		// hub lock held, matching the established relayLaneEvent discipline.
		h.relayChatMessage(ctx, id, pending)
	}
}

// failOrphanChatMessages fails stored rows the hub has no lane for. A hub that
// restarted between create and relay lost the in-memory lane mapping; leaving
// such a row in stored would display "전송 중" forever, so after a grace period
// it becomes a visible failed row the operator can retry or cancel.
func (h *HubServer) failOrphanChatMessages(ctx context.Context) {
	messages, err := h.chatStore.ListChatMessages(ctx, true, 0, hubChatListLimit)
	if err != nil {
		h.logger.Warn("chat undelivered sweep could not read the store")
		return
	}
	for _, message := range messages {
		if message.RelayState != "stored" {
			continue
		}
		h.chatMu.Lock()
		_, pending := h.chatPending[message.ID]
		h.chatMu.Unlock()
		if pending {
			continue
		}
		if h.now().UTC().Sub(message.CreatedAt) < hubChatOrphanGrace {
			continue
		}
		if _, err := h.chatStore.MarkChatMessageFailed(ctx, message.ID); err != nil {
			h.logger.Warn("chat orphan message was not marked failed", "id", message.ID)
			continue
		}
		h.logger.Warn("chat message had no delivery lane after restart; marked failed", "id", message.ID)
	}
}

// relayChatMessage performs the ordered send contract: the row is already
// stored, so relay through the existing lane.event path, then mark delivered
// only when the relay actually routed the directive. Every other outcome —
// persist failure, no route, rejection — is a visible failed row.
func (h *HubServer) relayChatMessage(ctx context.Context, id int64, pending chatPendingMessage) {
	eventID := fmt.Sprintf("chat-%d", id)
	event := hubJobEventPayload{
		JobID:     laneEventTransportID(pending.Lane, eventID),
		Epoch:     1,
		OwnerLane: pending.Lane,
		EventID:   eventID,
		Text:      chatRelayText(id, pending.Body),
		Label:     "operator-chat",
		Host:      "hub",
		Reason:    "operator_chat",
	}
	result := h.relayLaneEvent(event, nil)
	if result.Routed || result.Duplicate {
		if _, err := h.chatStore.MarkChatMessageDelivered(ctx, id); err != nil {
			h.logger.Warn("chat message delivery was not recorded", "id", id)
		}
		if pending.QuestionID != "" {
			if _, err := h.chatStore.TransitionChatQuestion(ctx, pending.QuestionID, "resolved"); err != nil {
				h.logger.Warn("answered chat question was not resolved", "id", pending.QuestionID)
			}
		}
		return
	}
	if _, err := h.chatStore.MarkChatMessageFailed(ctx, id); err != nil {
		h.logger.Warn("chat message failure was not recorded", "id", id)
	}
}

// chatRelayText builds the lane.event text. The relay contract forbids control
// characters, so the pane text is a single-line rendering; the full body is
// always in the store. A body that would exceed the non-sink 2048-byte lane
// limit is replaced by a reference line — the lane reads the full text from
// handoffkeep instead of a silently truncated answer.
func chatRelayText(id int64, body string) string {
	full := "[chat] " + sanitizeChatRelayText(body)
	if len(full) > laneEventTextLimit {
		return fmt.Sprintf("[chat] 긴 메시지 id=%d", id)
	}
	return full
}

func sanitizeChatRelayText(body string) string {
	return strings.Map(func(r rune) rune {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return ' '
		}
		return r
	}, body)
}
