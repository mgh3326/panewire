package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
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
	// hubChatPageLimit is the store's maximum page size; tail fetches page
	// through a window at this size rather than a single oldest-first page.
	hubChatPageLimit = 1000
	// hubChatTailWindow is the look-behind from the message high-water mark
	// that /chat/data fetches each poll, so new rows stay visible no matter
	// how large the table grows.
	hubChatTailWindow = 512
	// hubChatQuestionTailAge bounds the non-pending question tail; every
	// pending question is always fetched regardless of age.
	hubChatQuestionTailAge = 72 * time.Hour
	// hubChatQuestionWalkPages bounds one fetch loop so a misbehaving store
	// cannot pin a request in an unbounded page walk.
	hubChatQuestionWalkPages = 8
	// hubChatRelayScanPages bounds the durable relay-row walk a cold retry
	// performs to recover a pre-restart row's lane/question link.
	hubChatRelayScanPages = 20
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
	ListChatQuestions(ctx context.Context, state, afterID string, limit int) ([]ChatQuestion, error)
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

func (c *handoffkeepChatStore) ListChatQuestions(ctx context.Context, state, afterID string, limit int) ([]ChatQuestion, error) {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	if state != "" {
		query.Set("state", state)
	}
	if afterID != "" {
		query.Set("after_id", afterID)
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
// here until the relay attempt settles — and on the durable relay row once it
// persists, which is what cold retry recovery reads after a restart.
type chatPendingMessage struct {
	Lane       string
	Body       string
	QuestionID string
	// RetryOf records the failed source row this message retries; the relay
	// stamps it on the durable row so a later hub knows the source was
	// already retried once.
	RetryOf int64
}

//go:embed hub_chat.html
var hubChatHTML string

var hubChatTemplate = template.Must(template.New("hub-chat").Parse(hubChatHTML))

// hubChatMessageView decorates a stored row with the hub-side retry context
// the durable record does not carry: the target lane and the linked question.
// The browser needs both so retry can rebuild the original attempt without
// asking the operator to re-pick anything.
type hubChatMessageView struct {
	ChatMessage
	Lane       string `json:"lane,omitempty"`
	QuestionID string `json:"question_id,omitempty"`
	// Cancelled distinguishes an operator close-out from a real delivery; the
	// store records both as delivered, so the hub carries the distinction.
	Cancelled bool `json:"cancelled,omitempty"`
}

type hubChatData struct {
	SchemaVersion int                  `json:"schema_version"`
	Questions     []ChatQuestion       `json:"questions"`
	Messages      []hubChatMessageView `json:"messages"`
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

// authorizeChatUI applies the strict chat gate to every /chat endpoint: the
// operator bearer token or a verified Cloudflare Access assertion. Loopback
// and unsigned identity headers grant nothing here — this surface can inject
// lane directives, and local processes are not trusted.
func (h *HubServer) authorizeChatUI(writer http.ResponseWriter, request *http.Request) bool {
	if !h.authorizeChat(request) {
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

// writeChatListError is writeChatStoreError for list endpoints. A list route
// has no row-level 404, so a 404 there means the handoffkeep build predates
// the chat API — an absent store, not an absent row.
func (h *HubServer) writeChatListError(writer http.ResponseWriter, err error) {
	var httpErr *chatHTTPError
	if errors.As(err, &httpErr) && httpErr.status == http.StatusNotFound {
		writeHubJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "chat_unavailable"})
		return
	}
	h.writeChatStoreError(writer, err)
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
	questions, err := h.chatTailQuestions(ctx)
	if err != nil {
		h.writeChatListError(writer, err)
		return
	}
	messages, err := h.chatTailMessages(ctx)
	if err != nil {
		h.writeChatListError(writer, err)
		return
	}
	views := make([]hubChatMessageView, 0, len(messages))
	h.chatMu.Lock()
	for _, message := range messages {
		_, cancelled := h.chatCancelled[message.ID]
		views = append(views, hubChatMessageView{ChatMessage: message, Lane: h.chatLaneOf[message.ID], QuestionID: h.chatQuestionOf[message.ID], Cancelled: cancelled})
	}
	h.chatMu.Unlock()
	writeHubJSON(writer, http.StatusOK, hubChatData{SchemaVersion: 1, Questions: questions, Messages: views})
}

// chatTailQuestions returns every pending question plus the recent tail of
// resolved/withdrawn ones. Question ids carry their date (Q-YYYYMMDD-NN), so
// the tail cursor is a dated after_id rather than an oldest-first page — the
// fixed 200-row head window is what hid new questions forever once the table
// grew past it.
func (h *HubServer) chatTailQuestions(ctx context.Context) ([]ChatQuestion, error) {
	store := h.chatStore
	seen := make(map[string]ChatQuestion)
	after := ""
	for pages := 0; pages < hubChatQuestionWalkPages; pages++ {
		page, err := store.ListChatQuestions(ctx, "pending", after, hubChatPageLimit)
		if err != nil {
			return nil, err
		}
		for _, question := range page {
			seen[question.ID] = question
			after = question.ID
		}
		if len(page) < hubChatPageLimit {
			break
		}
		if pages == hubChatQuestionWalkPages-1 {
			h.logger.Warn("chat pending question list exceeded the walk bound; oldest pending rows may be hidden")
		}
	}
	after = "Q-" + h.now().UTC().Add(-hubChatQuestionTailAge).Format("20060102") + "-00"
	for pages := 0; pages < hubChatQuestionWalkPages; pages++ {
		page, err := store.ListChatQuestions(ctx, "", after, hubChatPageLimit)
		if err != nil {
			return nil, err
		}
		for _, question := range page {
			if _, exists := seen[question.ID]; !exists {
				seen[question.ID] = question
			}
			after = question.ID
		}
		if len(page) < hubChatPageLimit {
			break
		}
	}
	questions := make([]ChatQuestion, 0, len(seen))
	for _, question := range seen {
		questions = append(questions, question)
	}
	sort.Slice(questions, func(i, j int) bool { return questions[i].ID < questions[j].ID })
	return questions, nil
}

// chatTailMessages returns the latest hubChatListLimit messages. The store
// only offers an ascending id cursor, so the hub keeps a high-water mark and
// reads the window just behind it; the mark is advanced to the newest id seen
// so each poll stays a bounded fetch instead of a full table walk.
func (h *HubServer) chatTailMessages(ctx context.Context) ([]ChatMessage, error) {
	store := h.chatStore
	h.chatMu.Lock()
	highWater := h.chatMsgHighWater
	h.chatMu.Unlock()
	after := int64(0)
	if highWater > hubChatTailWindow {
		after = highWater - hubChatTailWindow
	}
	var messages []ChatMessage
	for {
		page, err := store.ListChatMessages(ctx, false, after, hubChatPageLimit)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		messages = append(messages, page...)
		after = page[len(page)-1].ID
		if len(page) < hubChatPageLimit {
			break
		}
	}
	if after > highWater {
		h.chatMu.Lock()
		if after > h.chatMsgHighWater {
			h.chatMsgHighWater = after
		}
		h.chatMu.Unlock()
	}
	if len(messages) > hubChatListLimit {
		messages = messages[len(messages)-hubChatListLimit:]
	}
	if messages == nil {
		messages = []ChatMessage{}
	}
	return messages, nil
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
	h.chatLaneOf[message.ID] = input.Lane
	h.chatQuestionOf[message.ID] = input.QuestionID
	h.chatMu.Unlock()
	h.kickChatDispatcher()
	writeHubJSON(writer, http.StatusCreated, message)
}

type hubChatMessageRetryInput struct {
	Lane       string `json:"lane,omitempty"`
	QuestionID string `json:"question_id,omitempty"`
}

// handleChatMessageRetry re-sends a failed message's body as a new store row.
// handoffkeep's state machine makes failed a sink (stored → delivered|failed
// only), so retrying in place is impossible; the new row is the fresh attempt
// and the failed row remains as the permanent failure record.
//
// Two invariants protect the pane. First, a failed row is retried at most
// once: chatRetryMu serializes the decision, chatRetriedFrom covers this
// process, and the retry's durable relay row carries a chat-retry-of-N marker
// so a restarted hub recovers the same answer — a second retry is refused,
// never a second directive. Second, the routing belongs to the message, not
// the client: lane and question come from the hub's in-memory record or, for
// rows written before this process started, from the durable relay row. What
// the client sends is never consulted — trusting it is how a restarted hub
// sent a retry to whatever lane the UI happened to have selected.
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
	h.chatRetryMu.Lock()
	defer h.chatRetryMu.Unlock()
	h.chatMu.Lock()
	lane := h.chatLaneOf[id]
	questionID := h.chatQuestionOf[id]
	retried := h.chatRetriedFrom[id] != 0
	h.chatMu.Unlock()
	if lane == "" {
		// The row predates this process: recover its routing from the durable
		// relay row, and learn whether an earlier hub already retried it.
		link, err := h.chatRelayLinkFor(ctx, id, message.Body)
		if err != nil {
			h.writeChatListError(writer, err)
			return
		}
		if link.Live {
			writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_queued"})
			return
		}
		if link.RetriedID != 0 {
			h.chatMu.Lock()
			h.chatRetriedFrom[id] = link.RetriedID
			h.chatMu.Unlock()
			retried = true
		}
		lane, questionID = link.Lane, link.QuestionID
	} else {
		// A failed row must have no live relay row: retrying one that is
		// still undelivered would put the directive in the pane twice. Rows
		// from before this invariant existed can carry both, so verify.
		live, err := h.liveChatRelayRows(ctx)
		if err != nil {
			h.writeChatListError(writer, err)
			return
		}
		if _, queued := live[id]; queued {
			writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_queued"})
			return
		}
	}
	if retried {
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_already_retried"})
		return
	}
	if !validReportRelayLaneName(lane) {
		// No durable relay row carries this row's routing (its relay row was
		// pruned or never persisted): refuse rather than trust client input.
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "chat_message_link_lost"})
		return
	}
	resent, err := store.CreateChatMessage(ctx, hubChatOperatorName, message.Body)
	if err != nil {
		h.writeChatStoreError(writer, err)
		return
	}
	h.chatMu.Lock()
	h.chatRetriedFrom[id] = resent.ID
	h.chatPending[resent.ID] = chatPendingMessage{Lane: lane, Body: message.Body, QuestionID: questionID, RetryOf: id}
	h.chatLaneOf[resent.ID] = lane
	h.chatQuestionOf[resent.ID] = questionID
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
		// Retire the durable relay row before closing out: otherwise replay
		// would still inject the cancelled answer on the next node hello.
		if live, err := h.liveChatRelayRows(ctx); err == nil {
			if row, queued := live[id]; queued {
				if err := h.handoffkeep.markDelivered(ctx, row.ID, "hub", "chat-cancel"); err != nil {
					h.logger.Warn("cancelled chat message relay row was not retired", "id", id)
				}
			}
		}
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
		h.chatMu.Lock()
		h.chatCancelled[id] = struct{}{}
		h.chatMu.Unlock()
		h.noteChatTerminal(id)
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

// failOrphanChatMessages fails stored rows nothing will ever deliver: no
// in-memory pending entry and no live (undelivered) relay row. A row whose
// durable relay event still exists is queued, not orphaned — replay owns it.
// The sweep walks from chatSweepCursor, which only advances past permanently
// failed rows, so a growing failed backlog can never pin the sweep on the
// oldest window.
func (h *HubServer) failOrphanChatMessages(ctx context.Context) {
	live, err := h.liveChatRelayRows(ctx)
	if err != nil {
		h.warnChatSweepOnce("chat orphan sweep paused: live relay rows unreadable")
		return
	}
	h.chatMu.Lock()
	after := h.chatSweepCursor
	h.chatMu.Unlock()
	for {
		messages, err := h.chatStore.ListChatMessages(ctx, true, after, hubChatPageLimit)
		if err != nil {
			h.warnChatSweepOnce("chat undelivered sweep could not read the store")
			return
		}
		for _, message := range messages {
			if message.RelayState != "stored" {
				// failed is permanent; the cursor never revisits it.
				h.chatMu.Lock()
				if message.ID > h.chatSweepCursor {
					h.chatSweepCursor = message.ID
				}
				h.chatMu.Unlock()
			} else {
				h.chatMu.Lock()
				_, pending := h.chatPending[message.ID]
				h.chatMu.Unlock()
				if pending {
					continue
				}
				if _, queued := live[message.ID]; queued {
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
			if message.ID > after {
				after = message.ID
			}
		}
		if len(messages) < hubChatPageLimit {
			break
		}
	}
	h.chatMu.Lock()
	h.chatSweepWarned = false
	h.chatMu.Unlock()
}

// warnChatSweepOnce keeps a broken store from logging on every dispatch tick:
// one warning per failing period, reset by the next completed sweep.
func (h *HubServer) warnChatSweepOnce(message string) {
	h.chatMu.Lock()
	if h.chatSweepWarned {
		h.chatMu.Unlock()
		return
	}
	h.chatSweepWarned = true
	h.chatMu.Unlock()
	h.logger.Warn(message)
}

// liveChatRelayRows maps chat message id → its undelivered durable relay row.
// The relay row is the only durable record that a chat directive is still
// owed to a lane, so it is the sweep/cancel/retry source of truth.
func (h *HubServer) liveChatRelayRows(ctx context.Context) (map[int64]handoffkeepRelayEvent, error) {
	rows := make(map[int64]handoffkeepRelayEvent)
	if h.handoffkeep == nil {
		return rows, nil
	}
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, "", "lane.event", afterID, handoffkeepReplayLimit)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if match := chatRelayEventPattern.FindStringSubmatch(record.EventID); match != nil {
				if chatID, err := strconv.ParseInt(match[1], 10, 64); err == nil {
					rows[chatID] = record
				}
			}
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return rows, nil
		}
		if afterID <= pageStart {
			return nil, errors.New("chat live relay cursor did not advance")
		}
	}
}

// chatRelayEventID derives the durable relay event id. The body hash keeps
// regenerated stores honest: a fresh database that reuses message id 7 for a
// different body cannot collide with the old chat-7 row and mark the wrong
// directive delivered. The hash suffix is optional in the pattern so rows
// written before the hash existed (plain chat-7) still hit the replay
// disposition gate instead of being injected blind.
var chatRelayEventPattern = regexp.MustCompile(`^chat-([0-9]+)(-[0-9a-f]{8})?$`)

func chatRelayEventID(id int64, body string) string {
	sum := sha256.Sum256([]byte(body))
	return fmt.Sprintf("chat-%d-%x", id, sum[:4])
}

// chatRetryMarker is stamped on the durable relay row a retry produces, so a
// restarted hub can tell the failed source row already minted one attempt.
func chatRetryMarker(id int64) string {
	return fmt.Sprintf("chat-retry-of-%d", id)
}

// chatRelayLink is what the durable relay rows still know about a chat
// message whose in-memory lane/question maps were lost to a restart.
type chatRelayLink struct {
	Lane       string
	QuestionID string
	// Live reports that the row's own relay row is still undelivered.
	Live bool
	// RetriedID is the message id a previous retry minted, or 0; a marker
	// whose event id cannot be parsed still counts (-1).
	RetriedID int64
}

// chatRelayLinkFor walks the durable relay rows for a message this process
// never saw: the row's own relay record carries the recorded lane and
// question, and a retry row's head marker proves the source was already
// retried. The walk is bounded and happens only on cold retries — client
// input is never a substitute.
func (h *HubServer) chatRelayLinkFor(ctx context.Context, id int64, body string) (chatRelayLink, error) {
	var link chatRelayLink
	if h.handoffkeep == nil {
		return link, nil
	}
	want := chatRelayEventID(id, body)
	legacy := fmt.Sprintf("chat-%d", id)
	marker := chatRetryMarker(id)
	var afterID int64
	for pages := 0; pages < hubChatRelayScanPages; pages++ {
		pageStart := afterID
		records, err := h.handoffkeep.listRelayEvents(ctx, "lane.event", afterID, handoffkeepReplayLimit)
		if err != nil {
			return link, err
		}
		for _, record := range records {
			if record.EventID == want || record.EventID == legacy {
				link.Lane = record.OwnerLane
				link.QuestionID = record.Question
				link.Live = record.DeliveredAt == ""
			}
			if record.Head == marker {
				link.RetriedID = -1
				if match := chatRelayEventPattern.FindStringSubmatch(record.EventID); match != nil {
					if retryID, err := strconv.ParseInt(match[1], 10, 64); err == nil {
						link.RetriedID = retryID
					}
				}
			}
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return link, nil
		}
		if afterID <= pageStart {
			return link, errors.New("chat relay link cursor did not advance")
		}
	}
	return link, nil
}

// relayChatMessage performs the ordered send contract: the row is already
// stored, so relay through the existing lane.event path, then mark delivered
// only when the relay actually routed the directive — or already had. A
// durable-but-uninjected row is queued, not failed: replay owns it and the
// replay hook flips the chat row when the injection finally happens. Only
// outcomes with no durable row (persist failure, oversized text) mark failed.
func (h *HubServer) relayChatMessage(ctx context.Context, id int64, pending chatPendingMessage) {
	eventID := chatRelayEventID(id, pending.Body)
	event := hubJobEventPayload{
		JobID:     laneEventTransportID(pending.Lane, eventID),
		Epoch:     1,
		OwnerLane: pending.Lane,
		EventID:   eventID,
		Text:      chatRelayText(id, pending.Body),
		Label:     "operator-chat",
		Host:      "hub",
		Reason:    "operator_chat",
		// The question id rides the durable relay row so a replayed delivery
		// can still resolve it after a hub restart emptied the in-memory maps.
		Question: pending.QuestionID,
	}
	if pending.RetryOf > 0 {
		// The durable marker is what lets a restarted hub enforce
		// one-retry-per-failed-row after the in-memory map is gone.
		event.Head = chatRetryMarker(pending.RetryOf)
	}
	result := h.relayLaneEvent(event, nil)
	switch {
	case result.Routed || result.Duplicate || result.AlreadyDelivered:
		if _, err := h.chatStore.MarkChatMessageDelivered(ctx, id); err != nil {
			h.logger.Warn("chat message delivery was not recorded", "id", id)
		}
		h.noteChatTerminal(id)
		if pending.QuestionID != "" {
			if _, err := h.chatStore.TransitionChatQuestion(ctx, pending.QuestionID, "resolved"); err != nil {
				h.logger.Warn("answered chat question was not resolved", "id", pending.QuestionID)
			}
		}
	case result.PersistFailed || result.RejectedTooLong || result.ID == 0:
		if _, err := h.chatStore.MarkChatMessageFailed(ctx, id); err != nil {
			h.logger.Warn("chat message failure was not recorded", "id", id)
		}
	default:
		// Persisted but not injected: the undelivered relay row is the queue.
		// Leaving the chat row stored is what keeps "failed" honest — a failed
		// row never has a live relay row, so it can never be injected later.
	}
}

// noteChatTerminal drops the hub-side context for a row that reached its
// terminal delivered state. Failed rows keep their lane/question entries so a
// later retry can rebuild the attempt.
func (h *HubServer) noteChatTerminal(id int64) {
	h.chatMu.Lock()
	delete(h.chatPending, id)
	delete(h.chatQuestionOf, id)
	delete(h.chatLaneOf, id)
	h.chatMu.Unlock()
}

// noteChatRelayDelivered is the replay-complete hook: a queued chat directive
// was just injected, so the chat row becomes delivered and its linked
// question — carried on the durable relay row — is resolved.
func (h *HubServer) noteChatRelayDelivered(id int64, question string) {
	if h.chatStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubChatStoreTimeout)
	defer cancel()
	delivered, err := h.chatStore.MarkChatMessageDelivered(ctx, id)
	if err != nil || delivered.RelayState != hubChatTerminalState {
		h.logger.Warn("chat message replayed delivery was not recorded", "id", id)
		return
	}
	h.noteChatTerminal(id)
	if question != "" {
		if _, err := h.chatStore.TransitionChatQuestion(ctx, question, "resolved"); err != nil {
			h.logger.Warn("answered chat question was not resolved", "id", question)
		}
	}
}

// chatReplayDisposition decides what replay does with a chat relay row.
// "inject" while the chat row is still stored — or when no chat store is
// configured at all, in which case the row is just an owed directive with no
// chat state to contradict. "retire" when the store authoritatively shows the
// row terminal (delivered covers cancel) or gone. "defer" when the store
// cannot be read: the row stays queued for the next replay rather than being
// injected on a guess or dropped on a transient error.
func (h *HubServer) chatReplayDisposition(id int64) string {
	if h.chatStore == nil {
		return "inject"
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubChatStoreTimeout)
	defer cancel()
	message, found, err := h.chatStore.GetChatMessage(ctx, id)
	if err != nil {
		return "defer"
	}
	if !found || message.RelayState != "stored" {
		return "retire"
	}
	return "inject"
}

// noteChatRelayExhausted marks a queued chat row failed when its durable
// relay row has spent every replay attempt: nothing will inject it now, and
// the operator should see the honest terminal state.
func (h *HubServer) noteChatRelayExhausted(id int64) {
	if h.chatStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubChatStoreTimeout)
	defer cancel()
	if _, err := h.chatStore.MarkChatMessageFailed(ctx, id); err != nil {
		h.logger.Warn("exhausted chat relay row was not marked failed", "id", id)
		return
	}
	h.logger.Warn("chat relay row spent every replay attempt; marked failed", "id", id)
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
