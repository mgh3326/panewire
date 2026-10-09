package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// assistantHK is the read-only handoffkeep client behind the assistant
// tools. It has no way to issue a write: the single request path only ever
// builds GET requests (note 1200's shared-path rule), so a tool bug can
// never reach a mutating endpoint. Errors are fixed strings — no URL, no
// token, no Location, no upstream body ever leaves this type (note 1207).
type assistantHK struct {
	base     *url.URL
	token    string
	cfID     string
	cfSecret string
	client   *http.Client
}

// assistantHKError is a named, value-free failure. The name is the whole
// contract: callers map it onto tool results and audit lines verbatim.
type assistantHKError struct{ name string }

func (e *assistantHKError) Error() string { return e.name }

var (
	errAssistantHKUnreachable = &assistantHKError{name: "hk_unreachable"}
	errAssistantHKInvalid     = &assistantHKError{name: "hk_response_invalid"}
)

func hkRejectedError(status int) error {
	return &assistantHKError{name: fmt.Sprintf("hk_rejected_http_%d", status)}
}

func newAssistantHK(rawURL, token, cfID, cfSecret string, client *http.Client) (*assistantHK, error) {
	if !validHandoffkeepBaseURL(rawURL) || !validHandoffkeepToken(token) {
		return nil, configError("handoffkeep url or token missing or invalid")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, configError("handoffkeep url invalid")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	if client == nil {
		client = &http.Client{Timeout: assistantHKTimeout}
	}
	// Never follow a redirect: a redirect response is a failure, not an
	// instruction — forwarding the Authorization or CF-Access headers
	// anywhere but the configured origin is how they leak (note 1207).
	wrapped := *client
	wrapped.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &assistantHK{base: parsed, token: token, cfID: cfID, cfSecret: cfSecret, client: &wrapped}, nil
}

// get is the one and only request path: every call builds a GET against the
// configured hk origin with bearer auth, the client User-Agent, and — only
// when both are configured — the Cloudflare Access service-token pair.
func (c *assistantHK) get(ctx context.Context, path string, query url.Values) (int, []byte, error) {
	requestContext, cancel := context.WithTimeout(ctx, assistantHKTimeout)
	defer cancel()
	target := *c.base
	target.Path = c.base.Path + path
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, target.String(), nil)
	if err != nil {
		return 0, nil, errAssistantHKUnreachable
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "panewire-assistant/0")
	if c.cfID != "" && c.cfSecret != "" {
		request.Header.Set("CF-Access-Client-Id", c.cfID)
		request.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, nil, errAssistantHKUnreachable
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, payload, nil
}

// The wire types below mirror handoffkeep's PR-1 shapes field for field —
// the same JSON the server's own internal/remote client decodes.

type assistantDecisionResolution struct {
	Kind      string    `json:"kind"`
	Option    string    `json:"option,omitempty"`
	Text      string    `json:"text,omitempty"`
	Receipt   string    `json:"receipt,omitempty"`
	Responder string    `json:"responder,omitempty"`
	By        string    `json:"by"`
	At        time.Time `json:"at"`
}

type assistantDecisionRequest struct {
	ID             string                       `json:"id"`
	Revision       int                          `json:"revision"`
	Supersedes     string                       `json:"supersedes,omitempty"`
	Status         string                       `json:"status"`
	Question       string                       `json:"question"`
	Reason         string                       `json:"reason,omitempty"`
	DefaultAction  string                       `json:"default_action"`
	DefaultOption  string                       `json:"default_option,omitempty"`
	DefaultTrigger string                       `json:"default_trigger,omitempty"`
	DueAt          *time.Time                   `json:"due_at,omitempty"`
	Doc            string                       `json:"doc,omitempty"`
	RequestedBy    string                       `json:"requested_by"`
	RequestedAt    time.Time                    `json:"requested_at"`
	HumanOnly      bool                         `json:"human_only,omitempty"`
	Resolution     *assistantDecisionResolution `json:"resolution,omitempty"`
}

// assistantDecisionOption is the option shape inside refs.decision_options.
type assistantDecisionOption struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Recommended bool   `json:"recommended,omitempty"`
}

type assistantPendingDecision struct {
	TaskID  int64                     `json:"task_id"`
	Lane    string                    `json:"lane"`
	State   string                    `json:"state"`
	Title   string                    `json:"title"`
	Request assistantDecisionRequest  `json:"request"`
	Options *assistantDecisionOptions `json:"options,omitempty"`
}

type assistantDecisionOptions struct {
	Options   []assistantDecisionOption `json:"options"`
	AllowFree bool                      `json:"allow_free"`
}

type assistantChatQuestion struct {
	ID              string     `json:"id"`
	ConversationID  string     `json:"conversation_id"`
	Lane            string     `json:"lane"`
	Body            string     `json:"body"`
	State           string     `json:"state"`
	Revision        int        `json:"revision"`
	AnswerMessageID *int64     `json:"answer_message_id"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	ResolvedAt      *time.Time `json:"resolved_at"`
}

type assistantPending struct {
	ServerTime       time.Time                  `json:"server_time"`
	DecisionRequests []assistantPendingDecision `json:"decision_requests"`
	ChatQuestions    []assistantChatQuestion    `json:"chat_questions"`
}

type assistantOutboxRow struct {
	ID         int64      `json:"id"`
	Kind       string     `json:"kind"`
	TargetLane string     `json:"target_lane"`
	EventID    string     `json:"event_id"`
	Text       string     `json:"text"`
	CreatedAt  time.Time  `json:"created_at"`
	SentAt     *time.Time `json:"sent_at"`
	HubRowID   *int64     `json:"hub_row_id"`
}

type assistantTask struct {
	ID    int64  `json:"id"`
	Lane  string `json:"lane"`
	Title string `json:"title"`
	Kind  string `json:"kind"`
	State string `json:"state"`
	Refs  struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	} `json:"refs"`
}

func (c *assistantHK) pending(ctx context.Context) (assistantPending, error) {
	status, payload, err := c.get(ctx, "/v1/assistant/pending", nil)
	if err != nil {
		return assistantPending{}, err
	}
	if status != http.StatusOK {
		return assistantPending{}, hkRejectedError(status)
	}
	var out assistantPending
	if json.Unmarshal(payload, &out) != nil || out.ServerTime.IsZero() {
		return assistantPending{}, errAssistantHKInvalid
	}
	return out, nil
}

func (c *assistantHK) outbox(ctx context.Context, limit int) ([]assistantOutboxRow, error) {
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	status, payload, err := c.get(ctx, "/v1/assistant/outbox", query)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, hkRejectedError(status)
	}
	var out struct {
		Notifications []assistantOutboxRow `json:"notifications"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return nil, errAssistantHKInvalid
	}
	return out.Notifications, nil
}

// chatQuestion reads one question row by id — the detail and progress paths
// take the direct GET rather than the capped pending list, so a settled or
// 1001st question is still addressable.
func (c *assistantHK) chatQuestion(ctx context.Context, id string) (assistantChatQuestion, bool, error) {
	status, payload, err := c.get(ctx, "/v1/chat/questions/"+url.PathEscape(id), nil)
	if err != nil {
		return assistantChatQuestion{}, false, err
	}
	if status == http.StatusNotFound {
		return assistantChatQuestion{}, false, nil
	}
	if status != http.StatusOK {
		return assistantChatQuestion{}, false, hkRejectedError(status)
	}
	var out assistantChatQuestion
	if json.Unmarshal(payload, &out) != nil || out.ID != id {
		return assistantChatQuestion{}, false, errAssistantHKInvalid
	}
	return out, true, nil
}

func (c *assistantHK) task(ctx context.Context, id int64) (assistantTask, bool, error) {
	status, payload, err := c.get(ctx, "/v1/tasks/"+strconv.FormatInt(id, 10), nil)
	if err != nil {
		return assistantTask{}, false, err
	}
	if status == http.StatusNotFound {
		return assistantTask{}, false, nil
	}
	if status != http.StatusOK {
		return assistantTask{}, false, hkRejectedError(status)
	}
	var out assistantTask
	if json.Unmarshal(payload, &out) != nil || out.ID != id {
		return assistantTask{}, false, errAssistantHKInvalid
	}
	return out, true, nil
}

// tasksForLane lists the lane's tasks for the target progress view.
func (c *assistantHK) tasksForLane(ctx context.Context, lane string) ([]assistantTask, error) {
	query := url.Values{}
	query.Set("lane", lane)
	query.Set("limit", "1000")
	status, payload, err := c.get(ctx, "/v1/tasks", query)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, hkRejectedError(status)
	}
	var out struct {
		Tasks []assistantTask `json:"tasks"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return nil, errAssistantHKInvalid
	}
	return out.Tasks, nil
}

// relayEvents reads one page of durable relay rows. lane, undelivered,
// afterID and limit map onto the existing GET /v1/relay/events query
// parameters; callers page explicitly so the walk bound is theirs.
func (c *assistantHK) relayEvents(ctx context.Context, lane string, undelivered bool, afterID int64, limit int) ([]handoffkeepRelayEvent, error) {
	query := url.Values{}
	query.Set("kind", "lane.event")
	if lane != "" {
		query.Set("lane", lane)
	}
	if undelivered {
		query.Set("undelivered", "1")
	}
	if afterID > 0 {
		query.Set("after_id", strconv.FormatInt(afterID, 10))
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	status, payload, err := c.get(ctx, "/v1/relay/events", query)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, hkRejectedError(status)
	}
	var out struct {
		Events []handoffkeepRelayEvent `json:"events"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return nil, errAssistantHKInvalid
	}
	return out.Events, nil
}
