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
	errAssistantUnknownTarget   = toolError("unknown_target")
	errAssistantTargetsInvalid  = toolError("targets_file_invalid")
	errAssistantInvalidArgs     = toolError("invalid_arguments")
	errAssistantRequestNotFound = toolError("request_not_found")
)

// The five read tools of PR-2 plus — gated by the writes set — the three
// write tools of PR-3. No parameter accepts a lane, URL, route or shell
// string: the only identifiers a caller may supply are opaque target ids,
// hk-shaped request ids and the bounded write inputs.
func assistantToolList(writes int) []map[string]any {
	tools := assistantReadToolList()
	for _, tool := range assistantWriteToolList() {
		if name, _ := tool["name"].(string); assistantWriteSetIncludes(writes, name) {
			tools = append(tools, tool)
		}
	}
	return tools
}

func assistantReadToolList() []map[string]any {
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
				"questions), revision, human_only flag, answered flag, current status and the handoffkeep " +
				"server_time. Merged or dropped tasks never appear. Read-only.",
			"inputSchema": noArgs,
		},
		{
			"name": "pending_detail",
			"description": "Read one open item by its stable id: dr-<task>-<revision> for a decision request " +
				"or the Q id for a chat question. Only items that are still open on an operator-allowlisted " +
				"lane or conversation answer; every other id — unknown, stale, settled, merged, dropped or " +
				"out of scope — fails closed with the same request_not_found. Read-only.",
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
				"justify it. A relay row that is only persisted is never done; a delivered_to stamp that is " +
				"not a real <machine>/<pane> is never delivered, and a sink stamp fails with reason " +
				"sink_lane. Exactly one of target or request_id is required; the id must resolve inside " +
				"the operator-allowlisted targets or it fails closed with request_not_found. Read-only.",
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

// assistantToolKnown reports whether the name is a registered tool — read
// or write — regardless of the writes flag, so a disabled write tool
// answers writes_disabled instead of unknown_tool.
func assistantToolKnown(name string) bool {
	for _, tool := range append(assistantReadToolList(), assistantWriteToolList()...) {
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

// callAssistantTool gates disabled writes first — a write call on a
// tool outside the enabled set fails closed with writes_disabled before
// the targets file is even touched — then loads and validates the targets
// file before any tool runs: the file is the root of the closed surface,
// so when it fails its mode, symlink or content checks every tool fails
// closed with targets_file_invalid.
func (s *assistantServer) callAssistantTool(ctx context.Context, identity, name string, args json.RawMessage) (any, error) {
	if assistantToolIsWrite(name) {
		// A write tool outside the enabled set fails closed before any
		// request — the flag is a set, so an answer-only binary answers
		// writes_disabled to deliver too, never unknown_tool.
		if !s.writeEnabled(name) {
			return nil, errAssistantWritesDisabled
		}
		// The per-identity write limit sits ahead of every upstream call:
		// a leaked credential cannot flood panes or the outbox faster
		// than the bucket allows (amendment 1E).
		if s.limiter != nil && !s.limiter.allow(identity) {
			return nil, errAssistantRateLimited
		}
	}
	targets, err := loadAssistantTargets(s.targetsPath)
	if err != nil {
		return nil, errAssistantTargetsInvalid
	}
	switch name {
	case "targets":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return toolTargets(targets)
	case "pending_list":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPendingList(ctx, targets)
	case "pending_detail":
		var in struct {
			ID string `json:"id"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPendingDetail(ctx, in.ID, targets)
	case "progress":
		var in struct {
			Target    string `json:"target"`
			RequestID string `json:"request_id"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolProgress(ctx, in.Target, in.RequestID, targets)
	case "poll":
		var in struct{}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolPoll(ctx, targets)
	case "answer_decision":
		var in struct {
			RequestID string `json:"request_id"`
			Option    string `json:"option,omitempty"`
			Text      string `json:"text,omitempty"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolAnswerDecision(ctx, identity, in.RequestID, in.Option, in.Text, targets)
	case "answer_question":
		var in struct {
			ID       string `json:"id"`
			Revision int    `json:"revision"`
			Text     string `json:"text"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolAnswerQuestion(ctx, identity, in.ID, in.Revision, in.Text, targets)
	case "deliver":
		var in struct {
			Target         string `json:"target"`
			IdempotencyKey string `json:"idempotency_key"`
			Text           string `json:"text"`
		}
		if err := decodeAssistantArgs(args, &in); err != nil {
			return nil, err
		}
		return s.toolDeliver(ctx, identity, in.Target, in.IdempotencyKey, in.Text, targets)
	default:
		return nil, errAssistantInvalidArgs
	}
}

// toolTargets lists the operator-verified targets the file declared this call.
func toolTargets(targets map[string]assistantTarget) (any, error) {
	list := make([]assistantTarget, 0, len(targets))
	for _, target := range targets {
		list = append(list, target)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return map[string]any{"targets": list}, nil
}

// assistantTargetScope is the set of lanes and conversations the operator has
// allowlisted through the targets file. Every by-id read is gated on it so an
// unknown or unmapped id answers the same generic request_not_found — the
// tools cannot be used as an existence oracle across lanes.
type assistantTargetScope struct {
	lanes         map[string]bool
	conversations map[string]bool
}

func scopeForTargets(targets map[string]assistantTarget) assistantTargetScope {
	scope := assistantTargetScope{
		lanes:         make(map[string]bool, len(targets)),
		conversations: make(map[string]bool, len(targets)),
	}
	for _, target := range targets {
		switch {
		case target.Kind == "lane" && target.Lane != "":
			scope.lanes[target.Lane] = true
		case target.Kind == "conversation" && target.Conversation != "":
			scope.conversations[target.Conversation] = true
		}
	}
	return scope
}

func (scope assistantTargetScope) laneAllowed(lane string) bool {
	return lane != "" && scope.lanes[lane]
}

func (scope assistantTargetScope) questionAllowed(question assistantChatQuestion) bool {
	return scope.laneAllowed(question.Lane) || (question.ConversationID != "" && scope.conversations[question.ConversationID])
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
	Answered       bool                      `json:"answered"`
	Lane           string                    `json:"lane,omitempty"`
	TaskID         int64                     `json:"task_id,omitempty"`
	TaskState      string                    `json:"task_state,omitempty"`
	Title          string                    `json:"title,omitempty"`
	Question       string                    `json:"question,omitempty"`
	Body           string                    `json:"body,omitempty"`
	ConversationID string                    `json:"conversation_id,omitempty"`
	DueAt          *time.Time                `json:"due_at,omitempty"`
	RequestedAt    time.Time                 `json:"requested_at,omitzero"`
	RequestedBy    string                    `json:"requested_by,omitempty"`
	CreatedAt      time.Time                 `json:"created_at,omitzero"`
	UpdatedAt      time.Time                 `json:"updated_at,omitzero"`
	Options        *assistantDecisionOptions `json:"options,omitempty"`
	// ServerTime is the handoffkeep clock: the pending snapshot stamps it
	// inline, and detail reads borrow the same snapshot clock after their
	// scope checks — the single-object GETs carry no server_time of their
	// own.
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
// upstream payload says. The scope filter applies the same allowlist the
// by-id tools enforce: a decision request on an unmapped lane and a
// question outside every mapped lane and conversation never appear.
func pendingItems(pending assistantPending, scope assistantTargetScope) []assistantPendingItem {
	serverTime := pending.ServerTime
	items := make([]assistantPendingItem, 0, len(pending.DecisionRequests)+len(pending.ChatQuestions))
	for _, d := range pending.DecisionRequests {
		if assistantTerminalTaskStates[d.State] || !scope.laneAllowed(d.Lane) {
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
		if !scope.questionAllowed(q) {
			continue
		}
		items = append(items, assistantChatItem(q, &serverTime))
	}
	return items
}

func assistantChatItem(q assistantChatQuestion, serverTime *time.Time) assistantPendingItem {
	return assistantPendingItem{
		ID:             q.ID,
		Key:            assistantItemKey(q.ID, q.Revision),
		Kind:           "chat_question",
		Revision:       q.Revision,
		Status:         q.State,
		Answered:       q.AnswerMessageID != nil,
		Lane:           q.Lane,
		ConversationID: q.ConversationID,
		Body:           q.Body,
		CreatedAt:      q.CreatedAt,
		UpdatedAt:      q.UpdatedAt,
		ServerTime:     serverTime,
	}
}

// assistantChatCap documents handoffkeep's silent 1000-row cap on the
// pending chat-question list (checker MINOR-5): the decision-request half
// has no LIMIT upstream — open requests on live tasks are unbounded — but
// ListChatQuestions runs with limit 1000, so at the cap the list cannot
// prove the pending set is complete.
const assistantChatCap = 1000

// pendingViewTruncated reports whether the capped chat-question half of the
// pending list sits at its cap.
func pendingViewTruncated(pending assistantPending) bool {
	return len(pending.ChatQuestions) >= assistantChatCap
}

func (s *assistantServer) toolPendingList(ctx context.Context, targets map[string]assistantTarget) (any, error) {
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	items := pendingItems(pending, scopeForTargets(targets))
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

// toolPendingDetail answers only for open items on in-scope targets. Every
// other id — unknown, stale, superseded, settled, merged, dropped or out of
// scope — returns the same generic request_not_found so the tool cannot be
// used as an existence oracle.
func (s *assistantServer) toolPendingDetail(ctx context.Context, id string, targets map[string]assistantTarget) (any, error) {
	scope := scopeForTargets(targets)
	if match := assistantRequestIDPattern.FindStringSubmatch(id); match != nil {
		taskID, _ := strconv.ParseInt(match[1], 10, 64)
		if taskID < 1 {
			// dr-0-* names no task — fail closed locally with the same
			// generic not_found every missing id answers.
			return nil, errAssistantRequestNotFound
		}
		return s.pendingDetailRequest(ctx, id, taskID, scope)
	}
	if assistantQuestionIDPattern.MatchString(id) {
		return s.pendingDetailQuestion(ctx, id, scope)
	}
	return nil, errAssistantRequestNotFound
}

// serverTime fetches the pending snapshot purely for its database clock: the
// single-object GET endpoints do not carry a server_time field.
func (s *assistantServer) serverTime(ctx context.Context) (time.Time, error) {
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return pending.ServerTime, nil
}

// detailClock is the detail answer's server time: the response's own HTTP
// Date when the upstream sent a parseable one, and only then the extra
// pending-snapshot GET the single-object endpoints otherwise cost (PR-2
// carry-forward). A missing or unparsable Date fails over, never silently
// into a zero clock.
func (s *assistantServer) detailClock(ctx context.Context, date time.Time) (time.Time, error) {
	if !date.IsZero() {
		return date, nil
	}
	return s.serverTime(ctx)
}

func (s *assistantServer) pendingDetailRequest(ctx context.Context, id string, taskID int64, scope assistantTargetScope) (any, error) {
	task, found, clock, err := s.hk.task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found || !scope.laneAllowed(task.Lane) || assistantTerminalTaskStates[task.State] {
		return nil, errAssistantRequestNotFound
	}
	current := task.Refs.DecisionRequest
	if current == nil || current.ID != id || current.Status != "open" {
		return nil, errAssistantRequestNotFound
	}
	serverTime, err := s.detailClock(ctx, clock)
	if err != nil {
		return nil, err
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
			ServerTime:  &serverTime,
		},
		"pending":     true,
		"server_time": serverTime,
		"task_state":  task.State,
		"disposition": task.Refs.Disposition != nil,
	}, nil
}

func (s *assistantServer) pendingDetailQuestion(ctx context.Context, id string, scope assistantTargetScope) (any, error) {
	q, found, clock, err := s.hk.chatQuestion(ctx, id)
	if err != nil {
		return nil, err
	}
	// A question counts as an open detail item only while it is still
	// pending — an answered-but-pending question is open (the outbox notice
	// is still owed) but a settled one is not.
	if !found || !scope.questionAllowed(q) || q.State != "pending" {
		return nil, errAssistantRequestNotFound
	}
	serverTime, err := s.detailClock(ctx, clock)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"item":        assistantChatItem(q, &serverTime),
		"pending":     true,
		"server_time": serverTime,
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
	assistantTasksPageLimit = 1000
	// assistantTasksScanPages bounds the after_id task walk: mature lanes
	// page past the single-page cap instead of declaring view_truncated,
	// but an unbounded list still cannot prove a lane quiet.
	assistantTasksScanPages = 5
	assistantOutboxLimit    = 1000
)

// relay row classes. markDelivered sets delivered_at and delivered_to
// together, so the presence of delivered_at only says the row is stamped —
// what delivered_to names decides whether the stamp is a delivery.
const (
	relayRowLive      = iota // undelivered, attempts remain
	relayRowExhausted        // undelivered, attempts spent — dead but unstamped
	relayRowDelivered        // delivered_to names a real <machine>/<pane>
	relayRowSink             // sink/* — the lane's notices go nowhere
	relayRowStamped          // every other stamp — retired, cancelled, chat-*
)

// classifyRelayRow allowlists the real delivery form (N1): a delivered_to
// stamp is a delivery only when it names <machine>/<pane> where the machine
// is a loader-accepted machine id minus the reserved pseudo-machines and
// the pane is any loader-accepted pane value minus the one non-delivery
// sentinel. Anything else — hub/replay-retired:*, hub/chat-*,
// resolve/<by>, */cancelled — is a stamp, not a delivery; sink/* gets its
// own class because a sink must never report done.
func classifyRelayRow(row handoffkeepRelayEvent) int {
	if row.DeliveredAt == "" {
		if row.Attempts >= relayReplayMaxAttempts {
			return relayRowExhausted
		}
		return relayRowLive
	}
	if !relayDeliveredToTarget(row.DeliveredTo) {
		if strings.HasPrefix(row.DeliveredTo, "sink/") {
			return relayRowSink
		}
		return relayRowStamped
	}
	return relayRowDelivered
}

// relayReservedMachines are the machine ids delivered_to uses for stamps
// that are not deliveries: the hub's own housekeeping stamps, the sink, and
// the hk resolve route's <by> stamp.
var relayReservedMachines = map[string]bool{"hub": true, "sink": true, "resolve": true}

// relayDeliveredToTarget accepts the loader's full pane set (any non-empty
// value up to 128 bytes — parseReportRelayRoutes requires nothing more),
// minus "cancelled", the one pane value a lanes file can hold that is a
// non-delivery sentinel. Judging by pane *shape* was N1: it read stamps
// like m1/workspace:pane-1 — a real pane outside lanePanePattern — as
// non-deliveries.
func relayDeliveredToTarget(deliveredTo string) bool {
	machine, pane, found := strings.Cut(deliveredTo, "/")
	if !found {
		return false
	}
	if relayReservedMachines[machine] {
		return false
	}
	return machineIDPattern.MatchString(machine) && pane != "" && len(pane) <= 128 && pane != relayCancelledPane
}

// relayRowState maps a row class onto the closed vocabulary: live and
// persisted rows are in-progress, delivered rows are done, and every
// non-delivery stamp is failed — retired/cancelled/chat-terminal as
// non_delivery_stamp, sink as sink_lane, an exhausted undelivered row as
// relay_exhausted.
func relayRowState(row handoffkeepRelayEvent) (state, reason string) {
	switch classifyRelayRow(row) {
	case relayRowDelivered:
		return assistantStateDone, ""
	case relayRowSink:
		return assistantStateFailed, "sink_lane"
	case relayRowStamped:
		return assistantStateFailed, "non_delivery_stamp"
	case relayRowExhausted:
		return assistantStateFailed, "relay_exhausted"
	default:
		return assistantStateInProgress, ""
	}
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

// scanRelayLane pages one lane's durable relay rows (oldest-first — the only
// ordering GET /v1/relay/events exposes) up to the walk bound and returns
// everything it saw. Reaching the page bound with a still-full last page
// reports truncated: an unseen tail can hide live rows, so callers must not
// treat a truncated walk as a complete view.
func (s *assistantServer) scanRelayLane(ctx context.Context, lane string, undelivered bool) ([]handoffkeepRelayEvent, bool, error) {
	var rows []handoffkeepRelayEvent
	var afterID int64
	for pages := 0; pages < assistantRelayScanPages; pages++ {
		page, err := s.hk.relayEvents(ctx, lane, undelivered, afterID, assistantRelayPageSize)
		if err != nil {
			return nil, false, err
		}
		if len(page) == 0 {
			return rows, false, nil
		}
		rows = append(rows, page...)
		if len(page) < assistantRelayPageSize {
			return rows, false, nil
		}
		afterID = page[len(page)-1].ID
	}
	return rows, true, nil
}

// findRelayRow locates one event's relay row. The undelivered scan runs first
// because live rows are the common case and that set is small; the full walk
// covers stamped rows. truncated reports whether either walk hit its page
// bound — a miss under truncation is not proof the row does not exist.
func (s *assistantServer) findRelayRow(ctx context.Context, lane, eventID string) (handoffkeepRelayEvent, bool, bool, error) {
	undelivered, undeliveredTruncated, err := s.scanRelayLane(ctx, lane, true)
	if err != nil {
		return handoffkeepRelayEvent{}, false, false, err
	}
	for _, row := range undelivered {
		if row.EventID == eventID {
			return row, true, undeliveredTruncated, nil
		}
	}
	rows, stampedTruncated, err := s.scanRelayLane(ctx, lane, false)
	if err != nil {
		return handoffkeepRelayEvent{}, false, false, err
	}
	for _, row := range rows {
		if row.EventID == eventID {
			return row, true, undeliveredTruncated || stampedTruncated, nil
		}
	}
	return handoffkeepRelayEvent{}, false, undeliveredTruncated || stampedTruncated, nil
}

func (s *assistantServer) toolProgress(ctx context.Context, target, requestID string, targets map[string]assistantTarget) (any, error) {
	if (target == "") == (requestID == "") {
		return nil, errAssistantInvalidArgs
	}
	if target != "" {
		return s.progressTarget(ctx, target, targets)
	}
	return s.progressRequest(ctx, requestID, targets)
}

// progressRequest answers for a stable request id — dr-<task>-<revision> or a
// Q id — but only when the resolved object sits inside the operator-allowed
// lanes and conversations. Missing, stale, merged, dropped and out-of-scope
// ids all answer the same generic request_not_found.
func (s *assistantServer) progressRequest(ctx context.Context, requestID string, targets map[string]assistantTarget) (any, error) {
	scope := scopeForTargets(targets)
	if match := assistantRequestIDPattern.FindStringSubmatch(requestID); match != nil {
		taskID, _ := strconv.ParseInt(match[1], 10, 64)
		if taskID < 1 {
			return nil, errAssistantRequestNotFound
		}
		return s.progressDecisionRequest(ctx, requestID, taskID, scope)
	}
	if assistantQuestionIDPattern.MatchString(requestID) {
		return s.progressQuestion(ctx, requestID, scope)
	}
	return nil, errAssistantRequestNotFound
}

// decisionReceipts exposes only provable identifiers. Resolution text,
// receipt text and resolver identity are human-channel data — the receipt
// carries only kind, responder and the resolution time.
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
				"responder": request.Resolution.Responder,
				"at":        request.Resolution.At,
			}
		}
	}
	return receipts
}

func (s *assistantServer) progressDecisionRequest(ctx context.Context, requestID string, taskID int64, scope assistantTargetScope) (any, error) {
	task, found, _, err := s.hk.task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found || !scope.laneAllowed(task.Lane) {
		return nil, errAssistantRequestNotFound
	}
	current := task.Refs.DecisionRequest
	if current == nil || current.ID != requestID {
		// One generic not_found: superseded, withdrawn and invented ids must
		// not leak which request is live.
		return nil, errAssistantRequestNotFound
	}
	receipts := decisionReceipts(task, current)
	if current.Status == "open" {
		if assistantTerminalTaskStates[task.State] {
			// An open request on a merged/dropped task is unanswerable work.
			receipts["reason"] = "task_terminal"
			return map[string]any{"request_id": requestID, "state": assistantStateFailed, "receipts": receipts}, nil
		}
		return map[string]any{"request_id": requestID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	// Resolved. Only the assistant path — kind=answered pinned to responder
	// operator-via-berry — owes a lane notice through the hk
	// notification_outbox; its receipt chain is outbox row → durable relay
	// row → delivered_at naming a real pane. Every other resolution
	// (default_applied, withdrawn, a human's own answered) settled on the
	// operator path and the resolution record is the whole receipt.
	if current.Resolution == nil || current.Resolution.Kind != "answered" || current.Resolution.Responder != assistantResponderName {
		return map[string]any{"request_id": requestID, "state": assistantStateDone, "receipts": receipts}, nil
	}
	return s.progressRelayNotice(ctx, requestID, task.Lane, noticeMatcher(requestID, "decision_request"), receipts)
}

// noticeMatcher returns the event-id matcher for one item's notice chain.
// A decision request's event is exactly dr-…-answered. A chat question's
// family is Q-…-rev<n>-answered for any n: the answer's event id is pinned
// at answer time, but an edited question keeps its answer slot while the
// revision moves (hk F6), so the current revision can name an event that
// was never written — the stored chain for this item is still that one
// answer's chain.
func noticeMatcher(itemID, kind string) func(string) bool {
	if kind == "chat_question" {
		prefix := itemID + "-rev"
		return func(eventID string) bool {
			if !strings.HasPrefix(eventID, prefix) {
				return false
			}
			rev, ok := strings.CutSuffix(eventID[len(prefix):], "-answered")
			if !ok {
				return false
			}
			n, err := strconv.Atoi(rev)
			return err == nil && n > 0
		}
	}
	eventID := itemID + "-answered"
	return func(candidate string) bool {
		return candidate == eventID
	}
}

// progressRelayNotice is the shared assistant-answer chain for resolved
// decision requests and answered chat questions: the unsent outbox row is
// the accepted receipt, the durable relay row decides done, failed or
// in-progress under the delivery rules, and a row the walk cannot see is
// honest in-progress — never done.
func (s *assistantServer) progressRelayNotice(ctx context.Context, requestID, lane string, match func(string) bool, receipts map[string]any) (any, error) {
	rows, err := s.hk.outbox(ctx, assistantOutboxLimit)
	if err != nil {
		return nil, err
	}
	state, err := s.noticeVerdict(ctx, lane, match, rows, receipts)
	if err != nil {
		return nil, err
	}
	return map[string]any{"request_id": requestID, "state": state, "receipts": receipts}, nil
}

// noticeVerdict evaluates one item's notice chain against an
// already-fetched unsent outbox view — the fold pays one outbox read per
// call instead of one per answered item (R3-4). The unsent view is checked
// first: an owed row is the honest state even when an older revision's
// relay row already delivered (the edited-question chain). A matching
// relay row then decides the verdict; no row anywhere is in-progress with
// a reason, never a silent done.
func (s *assistantServer) noticeVerdict(ctx context.Context, lane string, match func(string) bool, unsentRows []assistantOutboxRow, receipts map[string]any) (string, error) {
	var owed *assistantOutboxRow
	for i := range unsentRows {
		row := &unsentRows[i]
		if !match(row.EventID) {
			continue
		}
		if lane != "" && row.TargetLane != lane {
			continue
		}
		if owed == nil || row.EventID > owed.EventID {
			owed = row
		}
	}
	if owed != nil {
		receipts["outbox"] = outboxReceipt(*owed)
		return assistantStateAccepted, nil
	}
	if lane == "" {
		// A conversation-scoped question may carry no lane at all: the
		// outbox is the only place its notice can appear, so that is the
		// whole chain.
		if len(unsentRows) >= assistantOutboxLimit {
			receipts["reason"] = "view_truncated"
		} else {
			receipts["reason"] = "notice_row_not_visible"
		}
		return assistantStateInProgress, nil
	}
	row, exists, truncated, err := s.findRelayRowMatch(ctx, lane, match)
	if err != nil {
		return "", err
	}
	if truncated {
		receipts["truncated"] = true
	}
	if exists {
		receipts["relay"] = relayRowReceipt(row)
		state, reason := relayRowState(row)
		if reason != "" {
			receipts["reason"] = reason
		}
		return state, nil
	}
	if truncated || len(unsentRows) >= assistantOutboxLimit {
		receipts["reason"] = "view_truncated"
		return assistantStateInProgress, nil
	}
	// The resolution is committed and no notice row is visible yet — the
	// drain has not run or has not reached this row. That is honest
	// in-progress, not done.
	receipts["reason"] = "notice_row_not_visible"
	return assistantStateInProgress, nil
}

// findRelayRowMatch locates the newest durable relay row whose event id the
// matcher accepts. The undelivered scan runs first because live rows are
// the common case and that set is small; the full walk covers stamped
// rows. truncated reports whether either walk hit its page bound — a miss
// under truncation is not proof the row does not exist.
func (s *assistantServer) findRelayRowMatch(ctx context.Context, lane string, match func(string) bool) (handoffkeepRelayEvent, bool, bool, error) {
	undelivered, undeliveredTruncated, err := s.scanRelayLane(ctx, lane, true)
	if err != nil {
		return handoffkeepRelayEvent{}, false, false, err
	}
	var newest *handoffkeepRelayEvent
	for i := range undelivered {
		if !match(undelivered[i].EventID) {
			continue
		}
		if newest == nil || undelivered[i].ID > newest.ID {
			newest = &undelivered[i]
		}
	}
	if newest != nil {
		return *newest, true, undeliveredTruncated, nil
	}
	rows, stampedTruncated, err := s.scanRelayLane(ctx, lane, false)
	if err != nil {
		return handoffkeepRelayEvent{}, false, false, err
	}
	for i := range rows {
		if !match(rows[i].EventID) {
			continue
		}
		if newest == nil || rows[i].ID > newest.ID {
			newest = &rows[i]
		}
	}
	if newest != nil {
		return *newest, true, undeliveredTruncated || stampedTruncated, nil
	}
	return handoffkeepRelayEvent{}, false, undeliveredTruncated || stampedTruncated, nil
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

// progressQuestion follows the real PR-1 answer chain. The answer CAS sets
// answer_message_id and leaves the question state at "pending" — a pending
// question that already has an assistant answer is answered, not
// decision-pending, and its progress is the outbox → relay row → delivered
// chain for <question id>-rev<revision>-answered under the same delivery
// rules as a decision request.
func (s *assistantServer) progressQuestion(ctx context.Context, questionID string, scope assistantTargetScope) (any, error) {
	q, found, _, err := s.hk.chatQuestion(ctx, questionID)
	if err != nil {
		return nil, err
	}
	if !found || !scope.questionAllowed(q) {
		return nil, errAssistantRequestNotFound
	}
	receipts := questionReceipt(q)
	if q.AnswerMessageID != nil {
		return s.progressRelayNotice(ctx, questionID, q.Lane, noticeMatcher(q.ID, "chat_question"), receipts)
	}
	switch q.State {
	case "pending":
		return map[string]any{"request_id": questionID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	case "resolved":
		// Settled on the operator path — the question record is the whole
		// receipt.
		return map[string]any{"request_id": questionID, "state": assistantStateDone, "receipts": receipts}, nil
	default:
		// withdrawn (or any future terminal state): closed without an answer
		// through either path — terminal, not delivered.
		receipts["reason"] = "question_" + q.State
		return map[string]any{"request_id": questionID, "state": assistantStateFailed, "receipts": receipts}, nil
	}
}

// foldAnsweredChains runs the durable answer chain — the same outbox →
// relay → delivered evidence progress(request_id) follows — for each
// answered pending item and folds the per-item states into one aggregate.
// A failed chain wins; otherwise the least-advanced outstanding stage
// stands (accepted before in-progress before done), so the aggregate is
// done only when every chain proved delivery. The unsent outbox is fetched
// once per call, not once per item (R3-4); per-item verdicts land in
// receipts["answered"] carrying receipts only — never an answer body —
// and the winning item's reason is raised to the top level when the fold
// reports failed or in-progress.
func (s *assistantServer) foldAnsweredChains(ctx context.Context, items []assistantPendingItem, receipts map[string]any) (string, error) {
	rank := map[string]int{
		assistantStateFailed:     0,
		assistantStateAccepted:   1,
		assistantStateInProgress: 2,
		assistantStateDelivered:  3,
		assistantStateDone:       4,
	}
	unsentRows, err := s.hk.outbox(ctx, assistantOutboxLimit)
	if err != nil {
		return "", err
	}
	aggregate := assistantStateDone
	var reason string
	var verdicts []map[string]any
	for _, item := range items {
		itemReceipts := map[string]any{
			"id":              item.ID,
			"revision":        item.Revision,
			"kind":            item.Kind,
			"lane":            item.Lane,
			"conversation_id": item.ConversationID,
		}
		state, err := s.noticeVerdict(ctx, item.Lane, noticeMatcher(item.ID, item.Kind), unsentRows, itemReceipts)
		if err != nil {
			return "", err
		}
		verdicts = append(verdicts, map[string]any{"key": item.Key, "state": state, "receipts": itemReceipts})
		if r, ok := rank[state]; ok && r < rank[aggregate] {
			aggregate = state
			if itemReason, _ := itemReceipts["reason"].(string); itemReason != "" {
				reason = itemReason
			}
		}
	}
	receipts["answered"] = verdicts
	if reason != "" {
		receipts["reason"] = reason
	}
	return aggregate, nil
}

// progressTarget resolves an opaque target id and reports the lane's (or
// conversation's) aggregate state: decision-pending > failed > in-progress >
// delivered > done.
func (s *assistantServer) progressTarget(ctx context.Context, targetID string, targets map[string]assistantTarget) (any, error) {
	target, ok := targets[targetID]
	if !ok {
		return nil, errAssistantUnknownTarget
	}
	scope := scopeForTargets(targets)
	if target.Kind == "conversation" {
		return s.progressConversation(ctx, target, scope)
	}
	return s.progressLane(ctx, target, scope)
}

func (s *assistantServer) progressLane(ctx context.Context, target assistantTarget, scope assistantTargetScope) (any, error) {
	receipts := map[string]any{"target": map[string]any{"id": target.ID, "kind": target.Kind, "lane": target.Lane}}
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	var pendingKeys []string
	var answeredItems []assistantPendingItem
	for _, item := range pendingItems(pending, scope) {
		if item.Lane != target.Lane {
			continue
		}
		// Only unanswered work is decision-pending: an item that already
		// carries an assistant answer follows the outbox/relay notice
		// chain like progress(request_id) does, folded below.
		if item.Answered {
			answeredItems = append(answeredItems, item)
		} else {
			pendingKeys = append(pendingKeys, item.Key)
		}
	}
	if len(pendingKeys) > 0 {
		receipts["pending"] = pendingKeys
		return map[string]any{"target": target.ID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	if pendingViewTruncated(pending) {
		// A capped list cannot prove the lane has nothing pending.
		receipts["reason"] = "view_truncated"
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	undelivered, undeliveredTruncated, err := s.scanRelayLane(ctx, target.Lane, true)
	if err != nil {
		return nil, err
	}
	var liveRows, exhaustedRows []map[string]any
	for _, row := range undelivered {
		if classifyRelayRow(row) == relayRowExhausted {
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
	if len(answeredItems) > 0 {
		state, err := s.foldAnsweredChains(ctx, answeredItems, receipts)
		if err != nil {
			return nil, err
		}
		if state != assistantStateDone {
			// The least-advanced outstanding chain is the lane's answer —
			// provable current work preempts a settled-history verdict.
			return map[string]any{"target": target.ID, "state": state, "receipts": receipts}, nil
		}
	}
	outboxRows, err := s.hk.outbox(ctx, assistantOutboxLimit)
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
	if undeliveredTruncated || len(outboxRows) >= assistantOutboxLimit {
		// A bounded view cannot prove nothing is live or owed.
		receipts["reason"] = "view_truncated"
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	tasks, tasksTruncated, err := s.hk.tasksForLane(ctx, target.Lane)
	if err != nil {
		return nil, err
	}
	if tasksTruncated {
		// The task walk hit its page bound — live rows past the bound can
		// hide, so settled is unprovable from here.
		receipts["reason"] = "view_truncated"
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
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
	rows, relayTruncated, err := s.scanRelayLane(ctx, target.Lane, false)
	if err != nil {
		return nil, err
	}
	if relayTruncated {
		receipts["reason"] = "view_truncated"
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	var newest *handoffkeepRelayEvent
	for i := range rows {
		if newest == nil || rows[i].ID > newest.ID {
			newest = &rows[i]
		}
	}
	if newest != nil {
		receipts["relay"] = relayRowReceipt(*newest)
		switch classifyRelayRow(*newest) {
		case relayRowDelivered:
			return map[string]any{"target": target.ID, "state": assistantStateDelivered, "receipts": receipts}, nil
		case relayRowSink:
			receipts["reason"] = "sink_lane"
			return map[string]any{"target": target.ID, "state": assistantStateFailed, "receipts": receipts}, nil
		case relayRowExhausted:
			receipts["reason"] = "relay_exhausted"
			return map[string]any{"target": target.ID, "state": assistantStateFailed, "receipts": receipts}, nil
		case relayRowStamped:
			receipts["reason"] = "non_delivery_stamp"
			return map[string]any{"target": target.ID, "state": assistantStateFailed, "receipts": receipts}, nil
		default:
			// A live newest row can only appear here when the undelivered
			// scan above already proved it — defensive in-progress.
			return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
		}
	}
	// Nothing pending, nothing owed, nothing live: the target is settled.
	return map[string]any{"target": target.ID, "state": assistantStateDone, "receipts": receipts}, nil
}

func (s *assistantServer) progressConversation(ctx context.Context, target assistantTarget, scope assistantTargetScope) (any, error) {
	receipts := map[string]any{"target": map[string]any{"id": target.ID, "kind": target.Kind, "conversation": target.Conversation}}
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	var pendingKeys []string
	var answeredItems []assistantPendingItem
	for _, item := range pendingItems(pending, scope) {
		if item.Kind != "chat_question" || item.ConversationID != target.Conversation {
			continue
		}
		if item.Answered {
			answeredItems = append(answeredItems, item)
		} else {
			pendingKeys = append(pendingKeys, item.Key)
		}
	}
	if len(pendingKeys) > 0 {
		receipts["pending"] = pendingKeys
		return map[string]any{"target": target.ID, "state": assistantStateDecisionPending, "receipts": receipts}, nil
	}
	if pendingViewTruncated(pending) {
		receipts["reason"] = "view_truncated"
		return map[string]any{"target": target.ID, "state": assistantStateInProgress, "receipts": receipts}, nil
	}
	if len(answeredItems) > 0 {
		state, err := s.foldAnsweredChains(ctx, answeredItems, receipts)
		if err != nil {
			return nil, err
		}
		return map[string]any{"target": target.ID, "state": state, "receipts": receipts}, nil
	}
	return map[string]any{"target": target.ID, "state": assistantStateDone, "receipts": receipts}, nil
}

// toolPoll is the dedupe snapshot: the pending set keyed by <id>:<revision>
// with the hk server clock. The consumer, not the server, owns liveness —
// see assistantPollContract.
func (s *assistantServer) toolPoll(ctx context.Context, targets map[string]assistantTarget) (any, error) {
	pending, err := s.hk.pending(ctx)
	if err != nil {
		return nil, err
	}
	items := map[string]any{}
	for _, item := range pendingItems(pending, scopeForTargets(targets)) {
		items[item.Key] = map[string]any{
			"id":         item.ID,
			"kind":       item.Kind,
			"revision":   item.Revision,
			"status":     item.Status,
			"human_only": item.HumanOnly,
			"answered":   item.Answered,
			"lane":       item.Lane,
			"due_at":     item.DueAt,
		}
	}
	return map[string]any{
		"server_time":           pending.ServerTime,
		"items":                 items,
		"chat_questions_at_cap": len(pending.ChatQuestions) >= assistantChatCap,
		"chat_questions_cap":    assistantChatCap,
	}, nil
}
