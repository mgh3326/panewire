package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// assistantToolError is the whole vocabulary a caller can ever see: a named
// error, and at most a whitelisted identifier detail (a request id, never a
// secret or an upstream message). Everything upstream is already a named
// assistantHKError or a configError; nothing else crosses the boundary.
type assistantToolError struct {
	name   string
	detail string
}

func (e *assistantToolError) Error() string {
	if e.detail != "" {
		return e.name + ":" + e.detail
	}
	return e.name
}

func toolError(name string) *assistantToolError { return &assistantToolError{name: name} }

var (
	errAssistantUnknownTarget     = toolError("unknown_target")
	errAssistantTargetsInvalid    = toolError("targets_file_invalid")
	errAssistantInvalidArgs       = toolError("invalid_arguments")
	errAssistantRequestNotFound   = toolError("request_not_found")
	errAssistantRequestNotCurrent = toolError("request_not_current")
)

func toolErrorDetail(name, detail string) *assistantToolError {
	if len(detail) > 160 {
		detail = detail[:160]
	}
	return &assistantToolError{name: name, detail: detail}
}

// The five read tools of PR-2. There is no write tool and no parameter that
// accepts a lane, URL, route or shell string — the only identifiers a caller
// may supply are opaque target ids and hk-shaped request ids.
func assistantToolList() []map[string]any {
	noArgs := map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}
	return []map[string]any{
		{
			"name": "targets",
			"description": "List the operator-verified delivery targets as opaque ids. Other tools " +
				"reference a target only by the opaque id returned here; an unknown id fails closed. " +
				"No caller-supplied lane, URL, route or shell string is ever accepted. Read-only.",
			"inputSchema": noArgs,
		},
		{
			"name": "pending_list",
			"description": "List every open decision request and pending chat question visible to the assistant, " +
				"with each item's stable id (dr-<task>-<revision> for decision requests, the Q id for chat " +
				"questions), revision, human_only flag, current status and the handoffkeep server_time. " +
				"Merged or dropped tasks never appear. Read-only.",
			"inputSchema": noArgs,
		},
		{
			"name": "pending_detail",
			"description": "Read one item by its stable id: dr-<task>-<revision> for a decision request " +
				"or the Q id for a chat question. Returns the item's current handoffkeep state with a " +
				"pending flag; a settled item reports pending:false, and an id that is stale or unknown " +
				"fails closed with a named error. Read-only.",
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"id": map[string]any{"type": "string"}},
				"required":             []string{"id"},
				"additionalProperties": false,
			},
		},
		{
			"name": "progress",
			"description": "Report the state of one opaque target id or one request id using only the closed " +
				"vocabulary accepted, delivered, in-progress, decision-pending, done and failed, derived " +
				"exclusively from handoffkeep fields (task state, decision resolution, relay delivered_at/" +
				"delivered_to/attempts, outbox sent_at/hub_row_id) and returned with the receipts that " +
				"justify it. A relay row that is only persisted is never done. Exactly one of target or " +
				"request_id is required. Read-only.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"target":     map[string]any{"type": "string"},
					"request_id": map[string]any{"type": "string"},
				},
				"additionalProperties": false,
			},
		},
		{
			"name": "poll",
			"description": assistantPollContract + " Returns a snapshot of pending items keyed by " +
				"<stable id>:<revision> with the handoffkeep server_time, so the consumer can dedupe and " +
				"drop stale items. Read-only.",
			"inputSchema": noArgs,
		},
	}
}

func assistantToolKnown(name string) bool {
	for _, tool := range assistantToolList() {
		if tool["name"] == name {
			return true
		}
	}
	return false
}

func decodeAssistantArgs(raw json.RawMessage, v any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return errAssistantInvalidArgs
	}
	return nil
}

func (s *assistantServer) callAssistantTool(ctx context.Context, name string, args json.RawMessage) (any, error) {
	switch name {
	case "targets":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolTargets(ctx)
	case "pending_list":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPendingList(ctx)
	case "pending_detail":
		var in struct {
			ID string `json:"id"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPendingDetail(ctx, in.ID)
	case "progress":
		var in struct {
			Target    string `json:"target"`
			RequestID string `json:"request_id"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolProgress(ctx, in.Target, in.RequestID)
	case "poll":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPoll(ctx)
	default:
		return nil, errAssistantInvalidArgs
	}
}

// toolTargets lists the operator-verified targets. The file is re-read and
// re-validated on every call: a chmod or a broken edit fails closed here,
// not at some later restart.
func (s *assistantServer) toolTargets(_ context.Context) (any, error) {
	targets, err := loadAssistantTargets(s.targetsPath)
	if err != nil {
		return nil, errAssistantTargetsInvalid
	}
	list := make([]assistantTarget, 0, len(targets))
	for _, target := range targets {
		list = append(list, target)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return map[string]any{"targets": list}, nil
}

// assistantPendingItem is the uniform pending shape both pending_list and
// poll emit: one flat item per open decision request or pending question.
type assistantPendingItem struct {
	ID             string                    `json:"id"`
	Key            string                    `json:"key"`
	Kind           string                    `json:"kind"`
	Revision       int                       `json:"revision"`
	Status         string                    `json:"status"`
	HumanOnly      bool                      `json:"human_only"`
	Lane           string                    `json:"lane,omitempty"`
	TaskID         int64                     `json:"task_id,omitempty"`
	TaskState      string                    `json:"task_state,omitempty"`
	Title          string                    `json:"title,omitempty"`
	Question       string                    `json:"question,omitempty"`
	Body           string                    `json:"body,omitempty"`
	ConversationID string                    `json:"conversation_id,omitempty"`
	DueAt          *time.Time                `json:"due_at,omitempty"`
	RequestedAt    time.Time                 `json:"requested_at,omitempty"`
	RequestedBy    string                    `json:"requested_by,omitempty"`
	CreatedAt      time.Time                 `json:"created_at,omitempty"`
	UpdatedAt      time.Time                 `json:"updated_at,omitempty"`
	Options        *assistantDecisionOptions `json:"options,omitempty"`
	Answered       bool                      `json:"answered,omitempty"`
	// ServerTime is the handoffkeep clock at list time; detail reads take the
	// direct GET path and carry no list snapshot, so it stays nil there.
	ServerTime *time.Time `json:"server_time,omitempty"`
}

func assistantItemKey(id string, revision int) string {
	return id + ":" + strconv.Itoa(revision)
}

var assistantTerminalTaskStates = map[string]bool{"merged": true, "dropped": true}

// pendingItems flattens the hk pending response into the uniform item list.
// Terminal tasks are filtered here too — not because hk is expected to send
// them (its SQL already excludes them) but because this binary is the trust
// boundary: a merged or dropped task must never appear no matter what the
// upstream payload says.
func pendingItems(pending assistantPending) []assistantPendingItem {
	serverTime := pending.ServerTime
	items := make([]assistantPendingItem, 0, len(pending.DecisionRequests)+len(pending.ChatQuestions))
	for _, d := range pending.DecisionRequests {
		if assistantTerminalTaskStates[d.State] {
			continue
		}
		items = append(items, assistantPendingItem{
			ID:          d.Request.ID,
			Key:         assistantItemKey(d.Request.ID, d.Request.Revision),
			Kind:        "decision_request",
			Revision:    d.Request.Revision,
			Status:      d.Request.Status,
			HumanOnly:   d.Request.HumanOnly,
			Lane:        d.Lane,
			TaskID:      d.TaskID,
			TaskState:   d.State,
			Title:       d.Title,
			Question:    d.Request.Question,
			DueAt:       d.Request.DueAt,
			RequestedAt: d.Request.RequestedAt,
			RequestedBy: d.Request.RequestedBy,
			Options:     d.Options,
			ServerTime:  &serverTime,
		})
	}
	for _, q := range pending.ChatQuestions {
		items = append(items, assistantPendingItem{
			ID:             q.ID,
			Key:            assistantItemKey(q.ID, q.Revision),
			Kind:           "chat_question",
			Revision:       q.Revision,
			Status:         q.State,
			Lane:           q.Lane,
			ConversationID: q.ConversationID,
			Body:           q.Body,
			CreatedAt:      q.CreatedAt,
			UpdatedAt:      q.UpdatedAt,
			Answered:       q.AnswerMessageID != nil,
			ServerTime:     &serverTime,
		})
	}
	return items
}

// assistantChatCap documents handoffkeep's silent 1000-question cap on the
// pending list (checker MINOR-5): when the count sits exactly at the cap the
// caller is told the list may be truncated rather than left to assume a
// complete view.
const assistantChatCap = 1000

func (s *assistantServer) toolPendingList(ctx context.Context) (any, error) {
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	items := pendingItems(pending)
	return map[string]any{
		"server_time":           pending.ServerTime,
		"items":                 items,
		"chat_questions_at_cap": len(pending.ChatQuestions) >= assistantChatCap,
		"chat_questions_cap":    assistantChatCap,
	}, nil
}

var (
	assistantRequestIDPattern  = regexp.MustCompile(`^dr-([0-9]+)-([0-9]+)$`)
	assistantQuestionIDPattern = chatQuestionIDPattern
)

func (s *assistantServer) toolPendingDetail(ctx context.Context, id string) (any, error) {
	if match := assistantRequestIDPattern.FindStringSubmatch(id); match != nil {
		taskID, _ := strconv.ParseInt(match[1], 10, 64)
		return s.pendingDetailRequest(ctx, id, taskID)
	}
	if assistantQuestionIDPattern.MatchString(id) {
		return s.pendingDetailQuestion(ctx, id)
	}
	return nil, errAssistantRequestNotFound
}

// currentRequestDetail echoes the live request id into request_not_current
// only when it has the expected dr-<task>-<revision> shape — upstream text
// that is not even an id is never reflected into a caller-visible string.
func currentRequestDetail(current *assistantDecisionRequest) string {
	if current != nil && assistantRequestIDPattern.MatchString(current.ID) {
		return current.ID
	}
	return "none"
}

func (s *assistantServer) pendingDetailRequest(ctx context.Context, id string, taskID int64) (any, error) {
	task, found, err := s.hk.task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errAssistantRequestNotFound
	}
	current := task.Refs.DecisionRequest
	if current == nil || current.ID != id {
		return nil, toolErrorDetail("request_not_current", currentRequestDetail(current))
	}
	return map[string]any{
		"item": assistantPendingItem{
			ID:          current.ID,
			Key:         assistantItemKey(current.ID, current.Revision),
			Kind:        "decision_request",
			Revision:    current.Revision,
			Status:      current.Status,
			HumanOnly:   current.HumanOnly,
			Lane:        task.Lane,
			TaskID:      task.ID,
			TaskState:   task.State,
			Title:       task.Title,
			Question:    current.Question,
			DueAt:       current.DueAt,
			RequestedAt: current.RequestedAt,
			RequestedBy: current.RequestedBy,
		},
		"pending":     current.Status == "open" && !assistantTerminalTaskStates[task.State],
		"task_state":  task.State,
		"request":     current,
		"disposition": task.Refs.Disposition != nil,
	}, nil
}

func (s *assistantServer) pendingDetailQuestion(ctx context.Context, id string) (any, error) {
	q, found, err := s.hk.chatQuestion(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errAssistantRequestNotFound
	}
	return map[string]any{
		"item": assistantPendingItem{
			ID:             q.ID,
			Key:            assistantItemKey(q.ID, q.Revision),
			Kind:           "chat_question",
			Revision:       q.Revision,
			Status:         q.State,
			Lane:           q.Lane,
			ConversationID: q.ConversationID,
			Body:           q.Body,
			CreatedAt:      q.CreatedAt,
			UpdatedAt:      q.UpdatedAt,
			Answered:       q.AnswerMessageID != nil,
		},
		"pending": q.State == "pending",
	}, nil
}

// Closed progress vocabulary. These strings are the entire contract — every
// state a subject can report, each one derived only from handoffkeep fields.
const (
	assistantStateAccepted        = "accepted"
	assistantStateDelivered       = "delivered"
	assistantStateInProgress      = "in-progress"
	assistantStateDecisionPending = "decision-pending"
	assistantStateDone            = "done"
	assistantStateFailed          = "failed"
)

// assistantRelayScanBounds cap the durable relay-row walk: a lane's history
// or the undelivered set is small in practice, and an unbounded walk would
// let one tool call pin a connection on a giant table.
const (
	assistantRelayScanPages = 8
	assistantRelayPageSize  = 500
)

func relayRowExhausted(row handoffkeepRelayEvent) bool {
	// A replay-retired row keeps delivered_at empty and carries the
	// hub/replay-retired:<reason> delivered_to stamp — still undelivered, but
	// nothing will ever inject it again, so it reports as failed.
	return row.DeliveredAt == "" && (strings.HasPrefix(row.DeliveredTo, relayReplayRetiredMarker) || row.Attempts >= relayReplayMaxAttempts)
}

func relayRowReceipt(row handoffkeepRelayEvent) map[string]any {
	return map[string]any{
		"relay_row_id": row.ID,
		"event_id":     row.EventID,
		"owner_lane":   row.OwnerLane,
		"attempts":     row.Attempts,
		"delivered_at": row.DeliveredAt,
		"delivered_to": row.DeliveredTo,
		"received_at":  row.ReceivedAt,
	}
}

func outboxReceipt(row assistantOutboxRow) map[string]any {
	return map[string]any{
		"outbox_id":   row.ID,
		"event_id":    row.EventID,
		"target_lane": row.TargetLane,
		"created_at":  row.CreatedAt,
		"sent_at":     row.SentAt,
		"hub_row_id":  row.HubRowID,
	}
}

// scanRelayLane pages one lane's durable relay rows (oldest-first) up to the
// walk bound and returns everything it saw.
func (s *assistantServer) scanRelayLane(ctx context.Context, lane string, undelivered bool) ([]handoffkeepRelayEvent, error) {
	var rows []handoffkeepRelayEvent
	var afterID int64
	for pages := 0; pages < assistantRelayScanPages; pages++ {
		page, err := s.hk.relayEvents(ctx, lane, undelivered, afterID, assistantRelayPageSize)
		if err != nil {
			return nil, err
		}
		rows = append(rows, page...)
		if len(page) < assistantRelayPageSize {
			break
		}
		afterID = page[len(page)-1].ID
	}
	return rows, nil
}

func (s *assistantServer) findRelayRow(ctx context.Context, lane, eventID string) (handoffkeepRelayEvent, bool, error) {
	rows, err := s.scanRelayLane(ctx, lane, false)
	if err != nil {
		return handoffkeepRelayEvent{}, false, err
	}
	for _, row := range rows {
		if row.EventID == eventID {
			return row, true, nil
		}
	}
	return handoffkeepRelayEvent{}, false, nil
}

func (s *assistantServer) toolProgress(ctx context.Context, target, requestID string) (any, error) {
	if (target == "") == (requestID == "") {
		return nil, errAssistantInvalidArgs
	}
	if target != "" {
		return s.progressTarget(ctx, target)
	}
	return s.progressRequest(ctx, requestID)
}

// progressRequest answers for a stable request id: dr-<task>-<revision> or a
// Q id. Every state transition comes from hk fields only and carries the
// receipts that prove it.
func (s *assistantServer) progressRequest(ctx context.Context, requestID string) (any, error) {
	if match := assistantRequestIDPattern.FindStringSubmatch(requestID); match != nil {
		taskID, _ := strconv.ParseInt(match[1], 10, 64)
		return s.progressDecisionRequest(ctx, requestID, taskID)
	}
	if assistantQuestionIDPattern.MatchString(requestID) {
		return s.progressQuestion(ctx, requestID)
	}
	return nil, errAssistantRequestNotFound
}

func decisionReceipts(task assistantTask, request *assistantDecisionRequest) map[string]any {
	receipts := map[string]any{
		"task": map[string]any{"id": task.ID, "lane": task.Lane, "state": task.State, "title": task.Title},
	}
	if request != nil {
		receipts["request"] = map[string]any{
			"id":           request.ID,
			"revision":     request.Revision,
			"status":       request.Status,
			"human_only":   request.HumanOnly,
			"due_at":       request.DueAt,
			"requested_by": request.RequestedBy,
			"requested_at": request.RequestedAt,
		}
		if request.Resolution != nil {
			receipts["resolution"] = map[string]any{
				"kind":      request.Resolution.Kind,
				"option":    request.Resolution.Option,
				"by":        request.Resolution.By,
				"responder": request.Resolution.Responder,
				"at":        request.Resolution.At,
			}
		}
	}
	return receipts
}

func (s *assistantServer) progressDecisionRequest(ctx context.Context, requestID string, taskID int64) (any, error) {
	task, found, err := s.hk.task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errAssistantRequestNotFound
	}
	current := task.Refs.DecisionRequest
	receipts := decisionReceipts(task, current)
	if current == nil || current.ID != requestID {
		return nil, toolErrorDetail("request_not_current", currentRequestDetail(current))
	}
	terminal := assistantTerminalTaskStates[task.State]
	if current.Status == "open" {
		if terminal {
			// An open request on a merged/dropped task is unanswerable work.
			receipts["reason"] = "task_terminal"
			return map[string]any{"request_id": requestID, "state": assistantStateFailed, "receipts": receipts}, nil
		}
		return map[string]any{"request_id": requestID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	// Resolved. Only the assistant path — kind=answered pinned to responder
	// operator-via-berry — owes a lane notice through the hk
	// notification_outbox; its receipt chain is outbox row → durable relay
	// row → delivered_at. Every other resolution (default_applied,
	// withdrawn, a human's own answered) settled on the operator path and the
	// resolution record is the whole receipt.
	if current.Resolution == nil || current.Resolution.Kind != "answered" || current.Resolution.Responder != assistantResponderName {
		return map[string]any{"request_id": requestID, "state": assistantStateDone, "receipts": receipts}, nil
	}
	eventID := requestID + "-answered"
	row, exists, err := s.findRelayRow(ctx, task.Lane, eventID)
	if err != nil {
		return nil, err
	}
	if exists {
		receipts["relay"] = relayRowReceipt(row)
		switch {
		case row.DeliveredAt != "":
			return map[string]any{"request_id": requestID, "state": assistantStateDone, "receipts": receipts}, nil
		case relayRowExhausted(row):
			receipts["reason"] = "relay_exhausted"
			return map[string]any{"request_id": requestID, "state": assistantStateFailed, "receipts": receipts}, nil
		default:
			// Persisted but undelivered is in flight — never done.
			return map[string]any{"request_id": requestID, "state": assistantStateInProgress, "receipts": receipts}, nil
		}
	}
	rows, err := s.hk.outbox(ctx, assistantRelayPageSize)
	if err != nil {
		return nil, err
	}
	for _, outbox := range rows {
		if outbox.EventID == eventID {
			receipts["outbox"] = outboxReceipt(outbox)
			return map[string]any{"request_id": requestID, "state": assistantStateAccepted, "receipts": receipts}, nil
		}
	}
	// The resolution is committed and no notice row is visible yet — the
	// PR-3 drain has not run or has not reached this row. That is honest
	// in-progress, not done.
	return map[string]any{"request_id": requestID, "state": assistantStateInProgress, "receipts": receipts}, nil
}

// assistantResponderName is the responder handoffkeep pins onto assistant-path
// resolutions; its presence is what makes an answered request owe a lane
// notice through the outbox.
const assistantResponderName = "operator-via-berry"

// chatAnsweredEventID mirrors handoffkeep's deterministic outbox id for one
// assistant answer: <question id>-rev<revision>-answered.
func chatAnsweredEventID(questionID string, revision int) string {
	return questionID + "-rev" + strconv.Itoa(revision) + "-answered"
}

func questionReceipt(q assistantChatQuestion) map[string]any {
	return map[string]any{
		"question": map[string]any{
			"id": q.ID, "revision": q.Revision, "state": q.State, "lane": q.Lane,
			"conversation_id": q.ConversationID, "answered": q.AnswerMessageID != nil,
			"answer_message_id": q.AnswerMessageID,
			"created_at":        q.CreatedAt, "updated_at": q.UpdatedAt, "resolved_at": q.ResolvedAt,
		},
	}
}

func (s *assistantServer) progressQuestion(ctx context.Context, questionID string) (any, error) {
	q, found, err := s.hk.chatQuestion(ctx, questionID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errAssistantRequestNotFound
	}
	receipts := questionReceipt(q)
	switch q.State {
	case "pending":
		return map[string]any{"request_id": questionID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	case "resolved":
		if q.AnswerMessageID == nil {
			// Settled on the operator path — the question record is the
			// whole receipt.
			return map[string]any{"request_id": questionID, "state": assistantStateDone, "receipts": receipts}, nil
		}
		// An assistant answer closed the question and queued an outbox row.
		// The chain outbox (unsent) → relay row → delivered_at mirrors the
		// decision-request path; a sent row has left the unsent list, so its
		// receipt is the relay row it became (same event_id).
		eventID := chatAnsweredEventID(q.ID, q.Revision)
		row, exists, err := s.findRelayRow(ctx, q.Lane, eventID)
		if err != nil {
			return nil, err
		}
		if exists {
			receipts["relay"] = relayRowReceipt(row)
			switch {
			case row.DeliveredAt != "":
				return map[string]any{"request_id": questionID, "state": assistantStateDone, "receipts": receipts}, nil
			case relayRowExhausted(row):
				receipts["reason"] = "relay_exhausted"
				return map[string]any{"request_id": questionID, "state": assistantStateFailed, "receipts": receipts}, nil
			default:
				return map[string]any{"request_id": questionID, "state": assistantStateInProgress, "receipts": receipts}, nil
			}
		}
		rows, err := s.hk.outbox(ctx, assistantRelayPageSize)
		if err != nil {
			return nil, err
		}
		for _, outbox := range rows {
			if outbox.EventID == eventID {
				receipts["outbox"] = outboxReceipt(outbox)
				return map[string]any{"request_id": questionID, "state": assistantStateAccepted, "receipts": receipts}, nil
			}
		}
		// Resolved on the assistant path but no notice row is reachable: the
		// drain may have run before this binary saw the row, or the row
		// predates the unsent window. Persisted-but-invisible is in flight,
		// never done.
		receipts["reason"] = "notice_row_not_visible"
		return map[string]any{"request_id": questionID, "state": assistantStateInProgress, "receipts": receipts}, nil
	default:
		// withdrawn (or any future terminal state): closed without an answer
		// through either path — terminal, not delivered.
		receipts["reason"] = "question_" + q.State
		return map[string]any{"request_id": questionID, "state": assistantStateFailed, "receipts": receipts}, nil
	}
}

// progressTarget resolves an opaque target id and reports the lane's (or
// conversation's) aggregate state: decision-pending > failed > in-progress >
// delivered > done.
func (s *assistantServer) progressTarget(ctx context.Context, targetID string) (any, error) {
	target, err := s.lookupTarget(targetID)
	if err != nil {
		return nil, err
	}
	if target.Kind == "conversation" {
		return s.progressConversation(ctx, target)
	}
	return s.progressLane(ctx, target)
}

func (s *assistantServer) progressLane(ctx context.Context, target assistantTarget) (any, error) {
	receipts := map[string]any{"target": map[string]any{"id": target.ID, "kind": target.Kind, "lane": target.Lane}}
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	var pendingKeys []string
	for _, item := range pendingItems(pending) {
		if item.Lane == target.Lane {
			pendingKeys = append(pendingKeys, item.Key)
		}
	}
	if len(pendingKeys) > 0 {
		receipts["pending"] = pendingKeys
		return map[string]any{"target": target.ID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	undelivered, err := s.scanRelayLane(ctx, target.Lane, true)
	if err != nil {
		return nil, err
	}
	var liveRows, exhaustedRows []map[string]any
	for _, row := range undelivered {
		if relayRowExhausted(row) {
			exhaustedRows = append(exhaustedRows, relayRowReceipt(row))
		} else {
			liveRows = append(liveRows, relayRowReceipt(row))
		}
	}
	if len(exhaustedRows) > 0 {
		receipts["relay_rows"] = exhaustedRows
		receipts["reason"] = "relay_exhausted"
		return map[string]any{"target": target.ID, "state": assistantStateFailed, "receipts": receipts}, nil
	}
	outboxRows, err := s.hk.outbox(ctx, assistantRelayPageSize)
	if err != nil {
		return nil, err
	}
	var owedOutbox []map[string]any
	for _, row := range outboxRows {
		if row.TargetLane == target.Lane {
			owedOutbox = append(owedOutbox, outboxReceipt(row))
		}
	}
	if len(liveRows) > 0 || len(owedOutbox) > 0 {
		if len(liveRows) > 0 {
			receipts["relay_rows"] = liveRows
		}
		if len(owedOutbox) > 0 {
			receipts["outbox"] = owedOutbox
		}
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	tasks, err := s.hk.tasksForLane(ctx, target.Lane)
	if err != nil {
		return nil, err
	}
	var liveTasks []map[string]any
	for _, task := range tasks {
		if !assistantTerminalTaskStates[task.State] {
			liveTasks = append(liveTasks, map[string]any{"id": task.ID, "state": task.State, "title": task.Title})
		}
	}
	if len(liveTasks) > 0 {
		receipts["tasks"] = liveTasks
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	rows, err := s.scanRelayLane(ctx, target.Lane, false)
	if err != nil {
		return nil, err
	}
	var newest *handoffkeepRelayEvent
	for i := range rows {
		if newest == nil || rows[i].ID > newest.ID {
			newest = &rows[i]
		}
	}
	if newest != nil && newest.DeliveredAt != "" {
		receipts["relay"] = relayRowReceipt(*newest)
		return map[string]any{"target": target.ID, "state": assistantStateDelivered, "receipts": receipts}, nil
	}
	// Nothing pending, nothing owed, nothing live: the target is settled.
	return map[string]any{"target": target.ID, "state": assistantStateDone, "receipts": receipts}, nil
}

func (s *assistantServer) progressConversation(ctx context.Context, target assistantTarget) (any, error) {
	receipts := map[string]any{"target": map[string]any{"id": target.ID, "kind": target.Kind, "conversation": target.Conversation}}
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	var pendingKeys []string
	for _, item := range pendingItems(pending) {
		if item.Kind == "chat_question" && item.ConversationID == target.Conversation {
			pendingKeys = append(pendingKeys, item.Key)
		}
	}
	if len(pendingKeys) > 0 {
		receipts["pending"] = pendingKeys
		return map[string]any{"target": target.ID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	return map[string]any{"target": target.ID, "state": assistantStateDone, "receipts": receipts}, nil
}

// toolPoll is the dedupe snapshot: the pending set keyed by <id>:<revision>
// with the hk server clock. The consumer, not the server, owns liveness —
// see assistantPollContract.
func (s *assistantServer) toolPoll(ctx context.Context) (any, error) {
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	items := map[string]any{}
	for _, item := range pendingItems(pending) {
		items[item.Key] = map[string]any{
			"id":         item.ID,
			"kind":       item.Kind,
			"revision":   item.Revision,
			"status":     item.Status,
			"human_only": item.HumanOnly,
			"lane":       item.Lane,
			"due_at":     item.DueAt,
		}
	}
	return map[string]any{"server_time": pending.ServerTime, "items": items}, nil
}
