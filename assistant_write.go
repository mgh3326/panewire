package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// The write surface (MGH-36 PR-3): three tools and the outbox drainer.
// Everything here sits behind PANEWIRE_ASSISTANT_WRITES (=answer|deliver|
// all); with a tool not in the set it is absent from tools/list and every
// call fails closed with writes_disabled before the targets file is even
// read. The caller still cannot supply a lane, URL, route, event id,
// responder or by: the only identifiers it names are opaque target ids,
// hk-shaped request ids, a bounded idempotency key and the
// answer/instruction text. Attribution is pinned server-side —
// handoffkeep records responder operator-via-berry and by=<token identity>
// itself, and the hub label is fixed in this file.

const (
	// assistantViaPrefix marks every assistant-originated lane text, the
	// same prefix handoffkeep writes into outbox notice text.
	assistantViaPrefix = "[via berry] "
	// assistantHubLabel is the fixed lane-event label the hub records as
	// the ingress reason (http_ingress:panewire-assistant). The caller can
	// never set it.
	assistantHubLabel = "panewire-assistant"
	// assistantDecisionTextMaxBytes is handoffkeep's decision text cap.
	assistantDecisionTextMaxBytes = 1000
	// assistantChatAnswerMaxBytes caps one assistant answer body (hk allows
	// MaxBytes; the lane notice only ever carries a bounded quote).
	assistantChatAnswerMaxBytes = 4096
	// assistantDeliverTextMaxBytes is the lane-event cap minus the prefix,
	// so the composed text can never trip the hub's text_too_long. Deliver
	// text is rejected, never truncated (amendment 1B).
	assistantDeliverTextMaxBytes = laneEventTextLimit - len(assistantViaPrefix)
)

var (
	// assistantIdempotencyKeyPattern is the deliver key shape: bounded,
	// printable, no whitespace, and disjoint from the event-id separator
	// so berry:<target>:<key> can never be ambiguous.
	assistantIdempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{8,64}$`)
	// assistantOptionKeyPattern bounds a decision option key locally before
	// it is ever sent (handoffkeep validates membership authoritatively).
	assistantOptionKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,32}$`)
)

var (
	errAssistantWritesDisabled      = toolError("writes_disabled")
	errAssistantUndeliverable       = toolError("target_not_deliverable")
	errAssistantTextTooLong         = toolError("text_too_long")
	errAssistantIdempotencyConflict = toolError("idempotency_conflict")
	errAssistantInFlightRetry       = toolError("in_flight_retry")
	errAssistantInvalidAnswer       = toolError("invalid_answer")
	errAssistantRateLimited         = toolError("rate_limited")
	errAssistantSlotTaken           = toolError("question_slot_taken")
)

// assistantDetailError is a named tool error carrying extra value-free
// fields for the error object — the current revision on stale_revision,
// the relay row id and retry hint on the duplicate outcomes.
type assistantDetailError struct {
	name   string
	detail map[string]any
}

func (e *assistantDetailError) Error() string { return e.name }

func assistantStaleRevision(current int) error {
	detail := map[string]any{}
	if current > 0 {
		detail["current_revision"] = current
	}
	return &assistantDetailError{name: "stale_revision", detail: detail}
}

func assistantDuplicateUnverified(hubRowID int64) error {
	return &assistantDetailError{name: "duplicate_unverified", detail: map[string]any{
		"relay_row_id": hubRowID,
		"hint":         "do not mint a new key; call progress",
	}}
}

func assistantInFlightRetry() error {
	return &assistantDetailError{name: errAssistantInFlightRetry.Error(), detail: map[string]any{
		"hint": "another send holds this key; retry the same key",
	}}
}

func assistantToolIsWrite(name string) bool {
	return name == "answer_decision" || name == "answer_question" || name == "deliver"
}

// assistantWriteSetIncludes reports whether the flag set enables one write
// tool. Both the tools/list builder and the server's per-call gate use it,
// so list membership and dispatch can never drift apart.
func assistantWriteSetIncludes(writes int, name string) bool {
	switch writes {
	case assistantWritesAll:
		return assistantToolIsWrite(name)
	case assistantWritesAnswer:
		return name == "answer_decision" || name == "answer_question"
	case assistantWritesDeliver:
		return name == "deliver"
	}
	return false
}

// assistantWriteToolList is the gated write tool surface. The schemas share
// the read tools' fail-closed property: additionalProperties false means a
// caller-supplied lane, url, route, event_id, responder or by field is an
// invalid_arguments before any upstream call.
func assistantWriteToolList() []map[string]any {
	return []map[string]any{
		{
			"name": "answer_decision",
			"description": "Apply the operator's answer to one open decision request visible to the " +
				"assistant: the stable request id (dr-<task>-<revision> exactly as pending_list shows it), " +
				"the option key, and the owner's quoted answer text. The answer is recorded by " +
				"handoffkeep with kind answered and responder operator-via-berry — the caller can never " +
				"set kind, responder or by. Missing and out-of-scope ids answer the same generic " +
				"request_not_found as the read tools; a stale revision answers decision_request_stale; a " +
				"human_only request answers decision_request_human_only; a disposition item answers " +
				"disposition_operator_only; a request already resolved differently answers " +
				"decision_request_resolved, while the identical replay is a duplicate success with no " +
				"new effect. A lane notice drains asynchronously through the outbox drainer.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"request_id": map[string]any{"type": "string"},
					"option":     map[string]any{"type": "string"},
					"text":       map[string]any{"type": "string"},
				},
				"required":             []string{"request_id"},
				"additionalProperties": false,
			},
		},
		{
			"name": "answer_question",
			"description": "Answer one pending chat question visible to the assistant: the Q id, the " +
				"expected revision and the answer text. The answer is posted through handoffkeep's " +
				"assistant chat channel (source_channel assistant, author operator) under an " +
				"expected-revision compare-and-swap — never through the hub chat route and never as " +
				"source_channel web. A question that already holds an assistant answer answers " +
				"question_slot_taken (the slot is single-use; do not retry), a moved revision answers " +
				"stale_revision with the current revision, and missing or out-of-scope ids answer the " +
				"same generic request_not_found as the read tools. A lane notice drains asynchronously " +
				"through the outbox drainer.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id":       map[string]any{"type": "string"},
					"revision": map[string]any{"type": "integer"},
					"text":     map[string]any{"type": "string"},
				},
				"required":             []string{"id", "revision", "text"},
				"additionalProperties": false,
			},
		},
		{
			"name": "deliver",
			"description": "Deliver one instruction to a target's operator-mapped lane as a hub lane.event: " +
				"the opaque target id, a caller idempotency key (^[A-Za-z0-9._-]{8,64}$) and the " +
				"instruction text — at most 2036 bytes, valid UTF-8, no control characters (no " +
				"newlines), trimmed non-empty, and never starting with '[' so a forged [tag] cannot " +
				"be built. The event id is berry:<target>:<key> — deterministic, never random, never " +
				"time-based — the text is posted prefixed [via berry], and the lane comes only from " +
				"the targets file (a conversation target posts to its deliver_lane). The same key " +
				"with the same text is a duplicate success; the same key with different text answers " +
				"idempotency_conflict and sends nothing; a duplicate the stored row cannot verify " +
				"answers duplicate_unverified — call progress, never a new key; an in-flight claim " +
				"answers in_flight_retry — retry the same key. The caller can never supply a lane, " +
				"URL, route or event id.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"target":          map[string]any{"type": "string"},
					"idempotency_key": map[string]any{"type": "string"},
					"text":            map[string]any{"type": "string"},
				},
				"required":             []string{"target", "idempotency_key", "text"},
				"additionalProperties": false,
			},
		},
	}
}

// --- Write rate limit ----------------------------------------------------

// writeRateLimiter is a per-identity token bucket (default 10/min, burst
// 5): a leaked write credential cannot flood panes or the outbox faster
// than this. Buckets for identities idle longer than the prune horizon are
// dropped so the map cannot grow without bound.
type writeRateLimiter struct {
	mu      sync.Mutex
	perSec  float64
	burst   float64
	now     func() time.Time
	buckets map[string]*writeRateBucket
}

type writeRateBucket struct {
	tokens float64
	at     time.Time
}

const writeRateBucketIdleHorizon = 10 * time.Minute

func newWriteRateLimiter(perMin, burst int) *writeRateLimiter {
	return &writeRateLimiter{
		perSec:  float64(perMin) / 60,
		burst:   float64(burst),
		now:     time.Now,
		buckets: map[string]*writeRateBucket{},
	}
}

func (l *writeRateLimiter) allow(identity string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if len(l.buckets) > 1024 {
		for id, b := range l.buckets {
			if now.Sub(b.at) > writeRateBucketIdleHorizon {
				delete(l.buckets, id)
			}
		}
	}
	b := l.buckets[identity]
	if b == nil {
		b = &writeRateBucket{tokens: l.burst, at: now}
		l.buckets[identity] = b
	}
	if elapsed := now.Sub(b.at).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.perSec
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.at = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// --- Audit ---------------------------------------------------------------

// textSHA8 is the short content hash the audit carries in place of any
// text: enough to pair a duplicate with its conflict, never the text.
func textSHA8(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:4])
}

// logAuditWrite is the write-attempt audit line (amendment E): identity,
// tool, the request or target id, the idempotency key or event id, the
// revision, the outcome class and the hub row id — plus the text's sha8.
// Never the text itself, a token or an upstream URL.
func (s *assistantServer) logAuditWrite(identity, tool, subject, key, eventID string, revision int, outcome string, hubRowID int64, sha8 string) {
	if len(subject) > 160 {
		subject = subject[:160]
	}
	if len(key) > 80 {
		key = key[:80]
	}
	if len(eventID) > 160 {
		eventID = eventID[:160]
	}
	s.audit.Info("assistant_write", "identity", identity, "tool", tool, "subject", subject, "idempotency_key", key, "event_id", eventID, "revision", revision, "outcome", outcome, "hub_row_id", hubRowID, "text_sha8", sha8)
}

// --- Shared helpers ------------------------------------------------------

// mapAssistantWriteError collapses an upstream write response into the
// tool's named vocabulary. Named upstream errors pass through verbatim
// (they are already value-free tokens); reason-suffixed validation errors —
// which can quote caller input — collapse onto invalid_answer; everything
// unmapped keeps the status-code class.
func mapAssistantWriteError(status int, payload []byte, overrides map[string]error) error {
	name := apiErrorName(payload)
	if name != "" {
		if override, ok := overrides[name]; ok {
			return override
		}
		switch name {
		case "not_found", "task_terminal":
			return errAssistantRequestNotFound
		}
		return &assistantUpstreamError{name: name}
	}
	if strings.HasPrefix(apiErrorRaw(payload), "invalid") {
		return errAssistantInvalidAnswer
	}
	return hkRejectedError(status)
}

// validAssistantWriteText is the shared answer text gate: non-empty when
// required, UTF-8, no NUL, bounded.
func validAssistantWriteText(text string, maxBytes int, required bool) error {
	if text == "" {
		if required {
			return errAssistantInvalidArgs
		}
		return nil
	}
	if len(text) > maxBytes {
		return errAssistantTextTooLong
	}
	if !utf8.ValidString(text) || strings.IndexByte(text, 0) >= 0 {
		return errAssistantInvalidArgs
	}
	return nil
}

// validDeliverText is the strict lane-injection gate (amendment 1B): reject,
// never truncate. The trimmed text must be non-empty, at most 2036 bytes,
// valid UTF-8, free of every control character the hub rejects — no
// newlines — and no Unicode format character (category Cf: ZWSP, word
// joiner, BOM, bidi overrides) that could smuggle invisible or reordered
// text into a pane. It must not start with '[' or the fullwidth '［', so a
// forged tag like "[director-1]" can never ride the [via berry] prefix
// into a pane in either glyph.
func validDeliverText(text string) error {
	if text == "" {
		return errAssistantInvalidArgs
	}
	if len(text) > assistantDeliverTextMaxBytes {
		return errAssistantTextTooLong
	}
	if !utf8.ValidString(text) {
		return errAssistantInvalidArgs
	}
	for _, r := range text {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) || unicode.Is(unicode.Cf, r) {
			return errAssistantInvalidArgs
		}
	}
	if first, _ := utf8.DecodeRuneInString(text); first == '[' || first == '［' {
		return errAssistantInvalidArgs
	}
	return nil
}

// --- answer_decision -----------------------------------------------------

// toolAnswerDecision applies the operator's answer to an open decision
// request through handoffkeep's assistant resolve route only. The caller
// names request id, option and text; attribution never crosses (the request
// struct has no field that could carry it).
func (s *assistantServer) toolAnswerDecision(ctx context.Context, identity, requestID, option, text string, targets map[string]assistantTarget) (result any, err error) {
	resultClass := "internal_error"
	eventID := requestID + "-answered"
	defer func() {
		if err != nil {
			resultClass = err.Error()
		}
		s.logAuditWrite(identity, "answer_decision", requestID, "", eventID, 0, resultClass, 0, textSHA8(text))
	}()
	match := assistantRequestIDPattern.FindStringSubmatch(requestID)
	if match == nil {
		return nil, errAssistantRequestNotFound
	}
	taskID, _ := strconv.ParseInt(match[1], 10, 64)
	if taskID < 1 {
		return nil, errAssistantRequestNotFound
	}
	if option != "" && !assistantOptionKeyPattern.MatchString(option) {
		return nil, errAssistantInvalidArgs
	}
	if err := validAssistantWriteText(text, assistantDecisionTextMaxBytes, false); err != nil {
		return nil, err
	}
	if option == "" && text == "" {
		return nil, errAssistantInvalidArgs
	}
	scope := scopeForTargets(targets)
	task, found, _, err := s.hk.task(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found || !scope.laneAllowed(task.Lane) || assistantTerminalTaskStates[task.State] {
		return nil, errAssistantRequestNotFound
	}
	status, payload, err := s.hk.resolveAssistant(ctx, requestID, option, text)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, mapAssistantWriteError(status, payload, nil)
	}
	var out struct {
		Duplicate bool `json:"duplicate"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return nil, errAssistantHKInvalid
	}
	s.kickDrain()
	resultClass = "ok"
	if out.Duplicate {
		resultClass = "duplicate"
	}
	return map[string]any{
		"request_id": requestID,
		"resolved":   true,
		"duplicate":  out.Duplicate,
		"event_id":   eventID,
	}, nil
}

// --- answer_question -----------------------------------------------------

// toolAnswerQuestion posts one assistant answer through handoffkeep's
// assistant channel — POST /v1/chat/messages with source_channel=assistant
// and author operator under the expected-revision CAS. The hub chat route
// is never touched: it hardcodes web attribution. The question row is
// pre-read (amendment 1D): an already-held answer slot is question_slot_taken
// — distinct from stale, it tells the caller the slot is single-use — a
// non-pending state is the generic not_found, and a revision mismatch is
// stale_revision carrying the current revision.
func (s *assistantServer) toolAnswerQuestion(ctx context.Context, identity, questionID string, revision int, text string, targets map[string]assistantTarget) (result any, err error) {
	resultClass := "internal_error"
	eventID := chatAnsweredEventID(questionID, revision)
	defer func() {
		if err != nil {
			resultClass = err.Error()
		}
		s.logAuditWrite(identity, "answer_question", questionID, "", eventID, revision, resultClass, 0, textSHA8(text))
	}()
	if !assistantQuestionIDPattern.MatchString(questionID) {
		return nil, errAssistantRequestNotFound
	}
	if revision < 1 || revision > 1<<20 {
		return nil, errAssistantInvalidArgs
	}
	if err := validAssistantWriteText(text, assistantChatAnswerMaxBytes, true); err != nil {
		return nil, err
	}
	scope := scopeForTargets(targets)
	q, found, _, err := s.hk.chatQuestion(ctx, questionID)
	if err != nil {
		return nil, err
	}
	if !found || !scope.questionAllowed(q) || q.State != "pending" {
		return nil, errAssistantRequestNotFound
	}
	if q.AnswerMessageID != nil {
		// The slot is single-use: an assistant answer already holds it and
		// no caller action can free it — never retry (hk gap F6; the fix is
		// hk-side in chatQuestionUpsert).
		return nil, errAssistantSlotTaken
	}
	if q.Revision != revision {
		return nil, assistantStaleRevision(q.Revision)
	}
	// The origin event id is deterministic per question+revision: a retried
	// identical answer dedupes into the same row; a retried different
	// answer is an idempotency conflict, never a second message.
	origin := "berry-answer-" + questionID + "-rev" + strconv.Itoa(revision)
	status, payload, err := s.hk.postAssistantChatAnswer(ctx, q.ConversationID, questionID, revision, text, origin)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return nil, mapAssistantWriteError(status, payload, map[string]error{
			// A 409 racing a concurrent change between the pre-read and the
			// CAS is a stale revision, same as the pre-read verdict.
			"chat_question_stale":        assistantStaleRevision(0),
			"chat_message_conflict":      errAssistantIdempotencyConflict,
			"chat_conversation_conflict": errAssistantIdempotencyConflict,
		})
	}
	var out struct {
		ID int64 `json:"id"`
	}
	if json.Unmarshal(payload, &out) != nil {
		return nil, errAssistantHKInvalid
	}
	s.kickDrain()
	resultClass = "ok"
	if status == http.StatusOK {
		resultClass = "duplicate"
	}
	return map[string]any{
		"question_id": questionID,
		"revision":    revision,
		"answered":    true,
		"duplicate":   status == http.StatusOK,
		"event_id":    eventID,
		"message_id":  out.ID,
	}, nil
}

// --- deliver -------------------------------------------------------------

// deliverEventID derives the deterministic lane-event id for one deliver
// call: berry:<target>:<key> — readable, prefix-namespaced so it can never
// collide with dr-…-answered, Q-…-rev<n>-answered or any other producer's
// ids, and at most 6+64+1+64 bytes against the hub's 512 cap. It is never
// random, never time-based, and holds no text hash — a different text under
// one key must stay one event, not become a second one.
func deliverEventID(targetID, key string) string {
	return "berry:" + targetID + ":" + key
}

// deliverLane resolves the lane a deliver call may post to — the lane
// target's own lane, or the conversation target's operator-mapped
// deliver_lane — and reports whether the hub ingress could ever accept it.
// A lane the hub's own label rule rejects is not derivable and not fixable
// by the caller: the target is simply not deliverable.
func deliverLane(target assistantTarget) (string, bool) {
	lane := target.Lane
	if target.Kind == "conversation" {
		lane = target.DeliverLane
	}
	return lane, lane != "" && hubAgentLabelPattern.MatchString(lane)
}

// toolDeliver posts one instruction to the target's mapped lane as a hub
// lane.event. The POST is the dedupe probe: a 201 is the new durable row; a
// 409 naming an id > 0 sends the caller to the stored row for a
// byte-for-byte compare — equal is the duplicate success, different is the
// idempotency conflict (the hub already refused to store anything new), and
// a row the walk cannot see is duplicate_unverified, never a fresh key. A
// 409 with id 0 is the in-flight window: in_flight_retry, retry the same
// key.
func (s *assistantServer) toolDeliver(ctx context.Context, identity, targetID, key, rawText string, targets map[string]assistantTarget) (result any, err error) {
	// eventID is only computed once the key validates — an audit line for a
	// rejected call must never record a would-be wire id the caller shaped.
	eventID := ""
	text := strings.TrimSpace(rawText)
	sha8 := textSHA8(text)
	var hubRow int64
	resultClass := "internal_error"
	defer func() {
		if err != nil {
			resultClass = err.Error()
		}
		s.logAuditWrite(identity, "deliver", targetID, key, eventID, 0, resultClass, hubRow, sha8)
	}()
	target, ok := targets[targetID]
	if !ok {
		return nil, errAssistantUnknownTarget
	}
	lane, deliverable := deliverLane(target)
	if !deliverable {
		return nil, errAssistantUndeliverable
	}
	if !assistantIdempotencyKeyPattern.MatchString(key) {
		return nil, errAssistantInvalidArgs
	}
	if err := validDeliverText(text); err != nil {
		return nil, err
	}
	eventID = deliverEventID(targetID, key)
	laneText := assistantViaPrefix + text
	hubRow, duplicate, err := s.hub.postRelayEvent(ctx, lane, eventID, laneText, assistantHubLabel)
	if err != nil {
		return nil, err
	}
	if !duplicate {
		s.kickDrain()
		resultClass = "ok"
		return map[string]any{
			"status":       assistantStateAccepted,
			"duplicate":    false,
			"event_id":     eventID,
			"target":       targetID,
			"relay_row_id": hubRow,
		}, nil
	}
	if hubRow < 1 {
		// A racing send holds the dedupe claim: the row resolves to a real
		// id on the next POST — never a success and never a new key.
		return nil, assistantInFlightRetry()
	}
	// The durable row decides: byte-for-byte equal text is the duplicate
	// success, any difference is the idempotency conflict — nothing was
	// sent (the 409 already refused the new payload). A row the walk cannot
	// see proves nothing at all.
	row, found, truncated, err := s.findRelayRow(ctx, lane, eventID)
	if err != nil {
		return nil, err
	}
	if !found || truncated {
		return nil, assistantDuplicateUnverified(hubRow)
	}
	if row.Text != laneText {
		return nil, errAssistantIdempotencyConflict
	}
	resultClass = "duplicate"
	return map[string]any{
		"status":       assistantStateAccepted,
		"duplicate":    true,
		"event_id":     eventID,
		"target":       targetID,
		"relay_row_id": row.ID,
	}, nil
}

// --- Outbox drainer ------------------------------------------------------

// drainNow runs one serialized outbox pass: the periodic loop and the
// kicked pass a write triggers funnel through passMu, so passes can never
// overlap. It returns the event ids marked sent this pass and their hub
// row ids.
func (s *assistantServer) drainNow(ctx context.Context) map[string]int64 {
	if s.hub == nil {
		return nil
	}
	s.passMu.Lock()
	defer s.passMu.Unlock()
	passCtx, cancel := context.WithTimeout(ctx, assistantDrainPassTimeout)
	defer cancel()
	return s.drainPass(passCtx)
}

// kickDrain asks the loop for one pass without blocking the write tool:
// the buffered channel holds at most one pending pass, so a burst of
// writes coalesces into a single extra drain and a drain failure never
// reaches the tool call (amendment 1C).
func (s *assistantServer) kickDrain() {
	if s.drainKick == nil {
		return
	}
	select {
	case s.drainKick <- struct{}{}:
	default:
	}
}

// drainPass is the whole stateless drain (design Q3): read the unsent hk
// outbox, post each row to the hub as a lane.event with the row's own
// event_id — never a new id — and mark it sent only on a real receipt: a
// 201 row id, or a 409 duplicate naming an existing row id. An id of 0 or
// missing is never a receipt and is never marked. hk's list has no cursor,
// so the pass re-reads the oldest ≤1000 unsent rows until it sees nothing
// new: rows marked sent leave the view, rows that failed stay, and the
// seen-set keeps one poison head from starving the rows behind it within a
// pass. A crash anywhere is recoverable: the row stays owed in hk, the
// retry reuses the same event id, and hub dedupe makes the re-post a no-op.
func (s *assistantServer) drainPass(ctx context.Context) (sent map[string]int64) {
	sent = map[string]int64{}
	defer func() {
		if recover() != nil {
			s.logAudit("-", "drain", "drain", "-", "drain_panic")
		}
	}()
	seen := map[string]bool{}
	for {
		rows, err := s.hk.outbox(ctx, assistantOutboxLimit)
		if err != nil {
			s.logAudit("-", "drain", "drain", "-", "outbox_read:"+err.Error())
			return sent
		}
		fresh := 0
		for _, row := range rows {
			if seen[row.EventID] {
				continue
			}
			seen[row.EventID] = true
			fresh++
			if ctx.Err() != nil {
				return sent
			}
			s.drainRow(ctx, row, sent)
		}
		if len(rows) < assistantOutboxLimit || fresh == 0 {
			return sent
		}
	}
}

// drainRow handles one owed notification. The row's event id and its
// transformed text go to the hub unchanged across every retry; the hub row
// id is the only thing the mark-sent records. A hub error — even a 400
// after the text was sanitized — logs once and the pass moves on: one
// failing row must never stall the rest (amendment 1C-g).
func (s *assistantServer) drainRow(ctx context.Context, row assistantOutboxRow, sent map[string]int64) {
	text := drainLaneText(row)
	sha8 := textSHA8(text)
	id, _, err := s.hub.postRelayEvent(ctx, row.TargetLane, row.EventID, text, assistantHubLabel)
	if err != nil {
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "hub_error:"+err.Error(), 0, sha8)
		return
	}
	if id < 1 {
		// The standing rule: a 409 racing an in-flight claim, or any reply
		// whose id is 0 or missing, is NEVER sent — the row stays owed and
		// the next pass retries with the same event id.
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "receipt_missing", 0, sha8)
		return
	}
	status, payload, err := s.hk.outboxMarkSent(ctx, row.EventID, id)
	if err != nil {
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "hk_error:"+err.Error(), id, sha8)
		return
	}
	switch {
	case status == http.StatusOK:
		sent[row.EventID] = id
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "sent", id, sha8)
	case status == http.StatusConflict:
		// notification_outbox_conflict: a racing drain already marked this
		// row with a different hub id — the row is sent, just not by this
		// pass. Warn and skip; never re-POST.
		sent[row.EventID] = id
		s.audit.Warn("assistant_write", "identity", "drainer", "tool", "drain", "subject", row.TargetLane, "event_id", row.EventID, "outcome", "already_marked_conflict", "hub_row_id", id, "text_sha8", sha8)
	case status == http.StatusNotFound:
		// The outbox row vanished between the list and the mark — nothing
		// remains owed.
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "row_gone", id, sha8)
	default:
		s.logAuditWrite("drainer", "drain", row.TargetLane, "", row.EventID, 0, "hk_error:"+mapAssistantWriteError(status, payload, nil).Error(), id, sha8)
	}
}

// drainLoop is the periodic drain: one pass at startup, then one per
// interval ±25% jitter or per write kick until ctx ends. Jitter keeps two
// instances that started together from racing every pass in lockstep.
func (s *assistantServer) drainLoop(ctx context.Context) {
	s.drainNow(ctx)
	for {
		half := s.drainInterval / 2
		delay := s.drainInterval - s.drainInterval/4 + time.Duration(rand.Int64N(int64(half)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.drainKick:
			timer.Stop()
			s.drainNow(ctx)
		case <-timer.C:
			s.drainNow(ctx)
		}
	}
}

// drainLaneText is the deterministic outbox-text transform, keyed by row
// kind (amendment 1C): decision rows already render dr-<task>-<rev> and
// pass through; chat rows get the revision their event id carries injected
// ("Q-… rev<n> answered:", PR-1 MINOR-2); unknown kinds relay verbatim.
// Every text then goes through the deterministic sanitizer so a row hk
// could never post — a newline in an answer body, an over-cap quote — is
// made drainable instead of failing every pass forever.
func drainLaneText(row assistantOutboxRow) string {
	text := row.Text
	if row.Kind == "chat_answer" {
		text = injectQuestionRevision(row.EventID, text)
	}
	return sanitizeLaneText(text)
}

// injectQuestionRevision parses rev<n> out of a Q-…-rev<n>-answered event
// id and rewrites the bare Q-… mention to "Q-… rev<n>". A text already
// carrying the reference, or an event id that parses to no question, is
// returned unchanged — the transform is a pure function of the row.
func injectQuestionRevision(eventID, text string) string {
	base, ok := strings.CutSuffix(eventID, "-answered")
	if !ok {
		return text
	}
	i := strings.LastIndex(base, "-rev")
	if i <= 0 {
		return text
	}
	qid, rev := base[:i], base[i+len("-rev"):]
	if n, err := strconv.Atoi(rev); err != nil || n <= 0 || !chatQuestionIDPattern.MatchString(qid) {
		return text
	}
	ref := qid + " rev" + rev
	if strings.Contains(text, ref) {
		return text
	}
	if strings.Contains(text, qid) {
		return strings.Replace(text, qid, ref, 1)
	}
	return ref + " — " + text
}

// sanitizeLaneText makes one outbox text hub-postable deterministically
// (amendment 1C-g): every rune the hub's validLaneEventText rejects
// (≤0x1f, 0x7f–0x9f) becomes a space, then the same 2048-byte
// UTF-8-boundary re-cap with "…" hk itself applies. It is a pure function —
// two drain instances produce byte-identical output for the same row, so a
// second drainer never trips the hub's payload-mismatch warning.
func sanitizeLaneText(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			b.WriteByte(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return capLaneText(b.String())
}

// capLaneText is the hk assistantLaneText re-cap shape: text over the
// lane-event byte limit is cut at a UTF-8 boundary so the result plus the
// ellipsis marker fits, then the "…" is appended — and only ever appended
// when something was actually cut.
func capLaneText(text string) string {
	if len(text) <= laneEventTextLimit {
		return text
	}
	keep := laneEventTextLimit - len("…")
	cut := text[:keep]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut + "…"
}
