package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// assistantUpstream is the single request path every upstream call (read or
// write, handoffkeep or hub) flows through: fixed headers, bearer auth, the
// optional Cloudflare Access pair only on the hk client, no redirect
// following, and a hard 1 MiB response bound. The prefix names the error
// class — hk_* for handoffkeep, hub_* for the hub — so every failure stays
// a fixed string: no URL, no token, no Location, no upstream body ever
// leaves this type (note 1207).
type assistantUpstream struct {
	prefix   string
	base     *url.URL
	token    string
	cfID     string
	cfSecret string
	client   *http.Client
}

// assistantHK is the handoffkeep client behind the assistant tools; the
// write methods added for PR-3 sit next to the reads but still pass through
// the same bounded, redirect-refusing request path.
type assistantHK struct {
	*assistantUpstream
}

// assistantHub is the hub client. It exists only when writes are enabled
// and calls exactly POST /v1/relay/events — the lane-event ingress the
// outbox drainer and the deliver tool share. It carries no CF pair: the hub
// is authenticated by its operator token alone.
type assistantHub struct {
	*assistantUpstream
}

// assistantUpstreamError is a named, value-free failure. The name is the
// whole contract: callers map it onto tool results and audit lines
// verbatim.
type assistantUpstreamError struct{ name string }

func (e *assistantUpstreamError) Error() string { return e.name }

func (c *assistantUpstream) named(suffix string) error {
	return &assistantUpstreamError{name: c.prefix + "_" + suffix}
}

func (c *assistantUpstream) rejectedError(status int) error {
	return &assistantUpstreamError{name: fmt.Sprintf("%s_rejected_http_%d", c.prefix, status)}
}

var (
	errAssistantHKUnreachable = &assistantUpstreamError{name: "hk_unreachable"}
	errAssistantHKInvalid     = &assistantUpstreamError{name: "hk_response_invalid"}
	errAssistantHKTruncated   = &assistantUpstreamError{name: "hk_response_truncated"}
)

func hkRejectedError(status int) error {
	return &assistantUpstreamError{name: fmt.Sprintf("hk_rejected_http_%d", status)}
}

// assistantErrorNamePattern is the allowlist an upstream error name must
// satisfy before it may cross into the tool vocabulary: value-free token
// characters only, so a name can never smuggle a message.
var assistantErrorNamePattern = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// apiErrorName reads the one value-free field of an upstream error body:
// the {"error":"name"} token when it is a clean identifier. A reason-
// suffixed name ("invalid_decision_request: option X…") never matches —
// callers collapse those onto a fixed class instead of echoing the reason.
func apiErrorName(payload []byte) string {
	var out struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(payload, &out) != nil || !assistantErrorNamePattern.MatchString(out.Error) {
		return ""
	}
	return out.Error
}

// apiErrorRaw returns the upstream error string with any reason suffix cut
// at the first colon. Callers only prefix-match it ("invalid…"); the
// suffix — which can quote caller input — never leaves this package.
func apiErrorRaw(payload []byte) string {
	var out struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return ""
	}
	if i := strings.IndexByte(out.Error, ':'); i >= 0 {
		return out.Error[:i]
	}
	return out.Error
}

func newAssistantUpstream(prefix, rawURL, token, cfID, cfSecret string, client *http.Client) (*assistantUpstream, error) {
	label := "handoffkeep"
	if prefix == "hub" {
		label = "hub"
	}
	if !validHandoffkeepBaseURL(rawURL) || !validHandoffkeepToken(token) {
		return nil, configError(label + " url or token missing or invalid")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, configError(label + " url invalid")
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
	return &assistantUpstream{prefix: prefix, base: parsed, token: token, cfID: cfID, cfSecret: cfSecret, client: &wrapped}, nil
}

func newAssistantHK(rawURL, token, cfID, cfSecret string, client *http.Client) (*assistantHK, error) {
	upstream, err := newAssistantUpstream("hk", rawURL, token, cfID, cfSecret, client)
	if err != nil {
		return nil, err
	}
	return &assistantHK{upstream}, nil
}

func newAssistantHub(rawURL, token string, client *http.Client) (*assistantHub, error) {
	upstream, err := newAssistantUpstream("hub", rawURL, token, "", "", client)
	if err != nil {
		return nil, err
	}
	return &assistantHub{upstream}, nil
}

// do is the one and only request path: every call builds a request against
// the configured origin with bearer auth, the client User-Agent, and — only
// when both are configured — the Cloudflare Access service-token pair. The
// returned clock is the upstream's HTTP Date header: call sites that need a
// server time (pending_detail) borrow it instead of paying a second GET.
func (c *assistantUpstream) do(ctx context.Context, method, path string, query url.Values, body []byte) (int, []byte, time.Time, error) {
	requestContext, cancel := context.WithTimeout(ctx, assistantHKTimeout)
	defer cancel()
	target := *c.base
	target.Path = c.base.Path + path
	target.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(requestContext, method, target.String(), reader)
	if err != nil {
		return 0, nil, time.Time{}, c.named("unreachable")
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "panewire-assistant/0")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.cfID != "" && c.cfSecret != "" {
		request.Header.Set("CF-Access-Client-Id", c.cfID)
		request.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, nil, time.Time{}, c.named("unreachable")
	}
	defer response.Body.Close()
	// One byte past the cap proves the body fit: a cut read or an oversized
	// body is an unreliable response and gets its own named error — never
	// partial payload text.
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(payload) > 1<<20 {
		return response.StatusCode, nil, time.Time{}, c.named("response_truncated")
	}
	date, _ := http.ParseTime(response.Header.Get("Date"))
	return response.StatusCode, payload, date, nil
}

func (c *assistantUpstream) get(ctx context.Context, path string, query url.Values) (int, []byte, time.Time, error) {
	return c.do(ctx, http.MethodGet, path, query, nil)
}

// post issues one bounded JSON POST through the same request path. Request
// bodies are small by construction (resolve, chat answer, mark-sent, lane
// event), but the body still counts against the caller's argument caps
// enforced upstream.
func (c *assistantUpstream) post(ctx context.Context, path string, body any) (int, []byte, time.Time, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, nil, time.Time{}, c.named("request_invalid")
	}
	return c.do(ctx, http.MethodPost, path, nil, payload)
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
	status, payload, _, err := c.get(ctx, "/v1/assistant/pending", nil)
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
	status, payload, _, err := c.get(ctx, "/v1/assistant/outbox", query)
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
// 1001st question is still addressable. The returned clock is the
// response's HTTP Date, zero when absent or unparsable.
func (c *assistantHK) chatQuestion(ctx context.Context, id string) (assistantChatQuestion, bool, time.Time, error) {
	status, payload, date, err := c.get(ctx, "/v1/chat/questions/"+url.PathEscape(id), nil)
	if err != nil {
		return assistantChatQuestion{}, false, time.Time{}, err
	}
	if status == http.StatusNotFound {
		return assistantChatQuestion{}, false, date, nil
	}
	if status != http.StatusOK {
		return assistantChatQuestion{}, false, date, hkRejectedError(status)
	}
	var out assistantChatQuestion
	if json.Unmarshal(payload, &out) != nil || out.ID != id {
		return assistantChatQuestion{}, false, date, errAssistantHKInvalid
	}
	return out, true, date, nil
}

func (c *assistantHK) task(ctx context.Context, id int64) (assistantTask, bool, time.Time, error) {
	status, payload, date, err := c.get(ctx, "/v1/tasks/"+strconv.FormatInt(id, 10), nil)
	if err != nil {
		return assistantTask{}, false, time.Time{}, err
	}
	if status == http.StatusNotFound {
		return assistantTask{}, false, date, nil
	}
	if status != http.StatusOK {
		return assistantTask{}, false, date, hkRejectedError(status)
	}
	var out assistantTask
	if json.Unmarshal(payload, &out) != nil || out.ID != id {
		return assistantTask{}, false, date, errAssistantHKInvalid
	}
	return out, true, date, nil
}

// tasksForLane pages the lane's tasks with after_id until a short page —
// the single-page fetch used before could only prove a lane quiet by luck
// on a mature lane (PR-2 carry-forward). after_id is sent on every page,
// including after_id=0 on the first: handoffkeep only walks the
// id-ordered ListTasksPage path when the parameter is present — a page
// without it is priority-ordered, and the biggest id on that page is not
// a safe cursor for the next (a live low-priority task could sit behind a
// page of merged high-priority rows and be skipped entirely). truncated
// reports whether the page bound was reached with the last page still
// full.
func (c *assistantHK) tasksForLane(ctx context.Context, lane string) ([]assistantTask, bool, error) {
	var out []assistantTask
	var afterID int64
	for pages := 0; pages < assistantTasksScanPages; pages++ {
		query := url.Values{}
		query.Set("lane", lane)
		query.Set("limit", strconv.Itoa(assistantTasksPageLimit))
		query.Set("after_id", strconv.FormatInt(afterID, 10))
		status, payload, _, err := c.get(ctx, "/v1/tasks", query)
		if err != nil {
			return nil, false, err
		}
		if status != http.StatusOK {
			return nil, false, hkRejectedError(status)
		}
		var page struct {
			Tasks []assistantTask `json:"tasks"`
		}
		if json.Unmarshal(payload, &page) != nil {
			return nil, false, errAssistantHKInvalid
		}
		out = append(out, page.Tasks...)
		if len(page.Tasks) < assistantTasksPageLimit {
			return out, false, nil
		}
		afterID = page.Tasks[len(page.Tasks)-1].ID
	}
	return out, true, nil
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
	status, payload, _, err := c.get(ctx, "/v1/relay/events", query)
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

// --- PR-3 write surface -------------------------------------------------

// assistantResolveRequest is the whole body POST
// /v1/assistant/decisions/resolve accepts from this binary. There is no
// kind, responder or by field — handoffkeep pins kind=answered, responder
// operator-via-berry and by=<token identity> itself, so caller attribution
// can never reach the wire through this client.
type assistantResolveRequest struct {
	RequestID string `json:"request_id"`
	Option    string `json:"option,omitempty"`
	Text      string `json:"text,omitempty"`
}

// resolveAssistant posts one assistant answer. The caller maps the status
// and the error token; the body is never surfaced.
func (c *assistantHK) resolveAssistant(ctx context.Context, requestID, option, text string) (int, []byte, error) {
	status, payload, _, err := c.post(ctx, "/v1/assistant/decisions/resolve", assistantResolveRequest{RequestID: requestID, Option: option, Text: text})
	return status, payload, err
}

// assistantChatAnswerPost is the PR-1 assistant answer shape: a chat
// message on the assistant channel authored as operator, carrying the
// expected-revision CAS for the question it answers. The origin event id is
// deterministic so a retried answer dedupes instead of double-posting.
type assistantChatAnswerPost struct {
	ConversationID string                   `json:"conversation_id"`
	Author         string                   `json:"author"`
	Body           string                   `json:"body"`
	SourceChannel  string                   `json:"source_channel"`
	OriginEventID  string                   `json:"origin_event_id"`
	Answers        []assistantChatCASAnswer `json:"answers"`
}

type assistantChatCASAnswer struct {
	QuestionID string `json:"question_id"`
	Revision   int    `json:"revision"`
}

// postAssistantChatAnswer posts one assistant answer through POST
// /v1/chat/messages — handoffkeep's assistant channel, never the hub chat
// route and never source_channel web.
func (c *assistantHK) postAssistantChatAnswer(ctx context.Context, conversationID, questionID string, revision int, body, originEventID string) (int, []byte, error) {
	status, payload, _, err := c.post(ctx, "/v1/chat/messages", assistantChatAnswerPost{
		ConversationID: conversationID,
		Author:         "operator",
		Body:           body,
		SourceChannel:  "assistant",
		OriginEventID:  originEventID,
		Answers:        []assistantChatCASAnswer{{QuestionID: questionID, Revision: revision}},
	})
	return status, payload, err
}

// outboxMarkSent records one hub receipt against one outbox event id.
func (c *assistantHK) outboxMarkSent(ctx context.Context, eventID string, hubRowID int64) (int, []byte, error) {
	status, payload, _, err := c.post(ctx, "/v1/assistant/outbox/sent", struct {
		EventID  string `json:"event_id"`
		HubRowID int64  `json:"hub_row_id"`
	}{EventID: eventID, HubRowID: hubRowID})
	return status, payload, err
}

// postRelayEvent posts one lane.event to the hub's HTTP ingress. It returns
// the durable row id (when the reply carries one), whether the row already
// existed (409 duplicate_event_id), and a named error otherwise. An id of 0
// is passed through: the caller owns the standing rule that an id of 0 —
// whether a 409 racing an in-flight claim or a malformed receipt — never
// proves anything.
func (c *assistantHub) postRelayEvent(ctx context.Context, lane, eventID, text, label string) (int64, bool, error) {
	status, payload, _, err := c.post(ctx, "/v1/relay/events", struct {
		Kind    string `json:"kind"`
		Lane    string `json:"lane"`
		EventID string `json:"event_id"`
		Text    string `json:"text"`
		Label   string `json:"label"`
	}{Kind: "lane.event", Lane: lane, EventID: eventID, Text: text, Label: label})
	if err != nil {
		return 0, false, err
	}
	switch status {
	case http.StatusCreated:
		var out struct {
			ID      int64  `json:"id"`
			EventID string `json:"event_id"`
		}
		if json.Unmarshal(payload, &out) != nil || out.EventID != eventID {
			return 0, false, c.named("response_invalid")
		}
		if out.ID < 1 {
			// A fresh-create reply that cannot name its row is malformed,
			// not merely unreceiptable.
			return 0, false, c.named("response_invalid")
		}
		return out.ID, false, nil
	case http.StatusConflict:
		var out struct {
			Error string `json:"error"`
			ID    int64  `json:"id"`
		}
		if json.Unmarshal(payload, &out) != nil || out.Error != "duplicate_event_id" {
			return 0, false, c.rejectedError(status)
		}
		return out.ID, true, nil
	default:
		return 0, false, c.rejectedError(status)
	}
}
