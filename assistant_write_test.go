package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// fakeAssistantHub is the POST /v1/relay/events endpoint and nothing else —
// any request outside that one route fails the test, which is the wire
// assertion behind "the assistant never calls a hub chat route". It models
// the real ingress semantics: 201 with the durable row id, 409
// duplicate_event_id carrying the existing row's id, and the in-flight
// window where the dedupe claim exists but no id is known yet.
type fakeAssistantHub struct {
	t      *testing.T
	server *httptest.Server
	// hk, when set, receives a durable relay row per stored event — the
	// real hub persists through handoffkeep, so the hub row id IS the hk
	// relay row id and findRelayRow can see what the hub stored.
	hk *fakeAssistantHK

	mu       sync.Mutex
	requests []string
	posts    int
	nextID   int64
	rows     map[string]*fakeHubRow
	// Injection knobs:
	//   inFlightIDs    — these event ids answer 409 {"id":0} and store nothing
	//   rejectEventIDs — these event ids answer the given status, store nothing
	//   failNextStatus — the next POST answers this status, stores nothing
	//   killAfterStore — the next POST stores the row then drops the connection
	inFlightIDs    map[string]bool
	rejectEventIDs map[string]int
	failNextStatus int
	killAfterStore bool
}

type fakeHubRow struct {
	id      int64
	lane    string
	eventID string
	text    string
	label   string
}

func newFakeAssistantHub(t *testing.T) *fakeAssistantHub {
	h := &fakeAssistantHub{
		t:              t,
		nextID:         9000,
		rows:           map[string]*fakeHubRow{},
		inFlightIDs:    map[string]bool{},
		rejectEventIDs: map[string]int{},
	}
	h.server = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.server.Close)
	return h
}

func (h *fakeAssistantHub) serve(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests = append(h.requests, r.Method+" "+r.URL.Path)
	h.mu.Unlock()
	if r.Method != http.MethodPost || r.URL.Path != "/v1/relay/events" {
		h.t.Errorf("assistant client reached an unlisted hub route: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var input struct {
		Kind    string `json:"kind"`
		Lane    string `json:"lane"`
		EventID string `json:"event_id"`
		Text    string `json:"text"`
		Label   string `json:"label"`
		Host    string `json:"host"`
		Deliver string `json:"deliver,omitempty"`
	}
	raw, _ := io.ReadAll(r.Body)
	if json.Unmarshal(raw, &input) != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_request"}`))
		return
	}
	h.mu.Lock()
	h.requests[len(h.requests)-1] += " " + input.EventID
	h.posts++
	fail := h.failNextStatus
	if fail != 0 {
		h.failNextStatus = 0
	}
	reject, rejected := h.rejectEventIDs[input.EventID]
	inFlight := h.inFlightIDs[input.EventID]
	key := input.Lane + "\x00" + input.EventID
	old := h.rows[key]
	h.mu.Unlock()
	if fail != 0 {
		w.WriteHeader(fail)
		_, _ = w.Write([]byte(`{"error":"injected"}`))
		return
	}
	if rejected {
		w.WriteHeader(reject)
		_, _ = w.Write([]byte(`{"error":"injected"}`))
		return
	}
	if inFlight {
		// The dedupe claim exists but its POST has not learned a row id —
		// the real relayLaneEventContext's knownID==0 window.
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"duplicate_event_id","id":0}`))
		return
	}
	if old != nil {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"duplicate_event_id","id":%d}`, old.id)))
		return
	}
	// The real ingress validates kind, lane shape, event id and text before
	// storing — the fake applies the same gates so a text the hub would
	// refuse is caught here, not asserted away.
	if input.Kind != "lane.event" || !hubAgentLabelPattern.MatchString(input.Lane) || !validLaneEventID(input.EventID) || !validLaneEventText(input.Text) || !hubAgentLabelPattern.MatchString(input.Label) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_request"}`))
		return
	}
	if len(input.Text) > laneEventTextLimit {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"text_too_long"}`))
		return
	}
	h.mu.Lock()
	h.nextID++
	row := &fakeHubRow{id: h.nextID, lane: input.Lane, eventID: input.EventID, text: input.Text, label: input.Label}
	h.rows[key] = row
	kill := h.killAfterStore
	if kill {
		h.killAfterStore = false
	}
	h.mu.Unlock()
	if h.hk != nil {
		h.hk.mu.Lock()
		h.hk.relays = append(h.hk.relays, handoffkeepRelayEvent{
			ID: row.id, Kind: "lane.event", OwnerLane: row.lane,
			EventID: row.eventID, Text: row.text, Machine: "host-a",
			ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		})
		h.hk.mu.Unlock()
	}
	if kill {
		// Persisted, then the wire died before the 201 reached the client —
		// the drainer's hardest case: the row exists, the pass saw an error.
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"id":%d,"event_id":%q,"lane":%q,"routed":true,"machine":"host-a"}`, row.id, row.eventID, row.lane)))
}

func (h *fakeAssistantHub) rowCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rows)
}

func (h *fakeAssistantHub) rowFor(lane, eventID string) *fakeHubRow {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rows[lane+"\x00"+eventID]
}

func (h *fakeAssistantHub) postCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.posts
}

func (h *fakeAssistantHub) requestLog() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.requests...)
}

// assistantWriteTargets maps one lane target, one deliverable conversation
// target and one conversation with no deliver_lane.
const assistantWriteTargets = `{"targets":{"ops":{"kind":"lane","lane":"lane-a","description":"ops lane"},"desk":{"kind":"conversation","conversation":"operator-desk","deliver_lane":"desk-lane"},"quiet":{"kind":"conversation","conversation":"quiet-desk"}}}`

// assistantWriteServer builds a writes-enabled server over the two fakes.
// writes is one of the assistantWrites* constants; mutate can adjust the
// config further (rate limit, drain interval).
func assistantWriteServer(t *testing.T, hk *fakeAssistantHK, hub *fakeAssistantHub, writes int, mutate func(*assistantConfig)) (*assistantServer, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	cfg := assistantConfig{
		Listen:        "127.0.0.1:0",
		HKURL:         hk.server.URL,
		HKToken:       assistantTestHKToken,
		TokenFile:     writeMode0600(t, dir, "berry.token", assistantTestBerryToken),
		TargetsFile:   writeMode0600(t, dir, "targets.json", assistantWriteTargets),
		ClientName:    "berry-test",
		Writes:        writes,
		DrainInterval: assistantDrainIntervalMin,
		// A generous default budget: most AC tests fire a dozen writes and
		// are not the rate-limit test — AC12 sets its own tight values.
		WriteRatePerMin: 600,
		WriteBurst:      50,
	}
	if hub != nil {
		cfg.HubURL = hub.server.URL
		cfg.HubToken = "hub-token-PLANT-5c2d"
	}
	if mutate != nil {
		mutate(&cfg)
	}
	logs := &bytes.Buffer{}
	srv, err := newAssistantServer(cfg, assistantServerDeps{Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatalf("newAssistantServer: %v", err)
	}
	return srv, logs
}

// assistantListTools returns the sorted tool names the server advertises.
func assistantListTools(t *testing.T, srv *assistantServer) []string {
	t.Helper()
	writer := assistantServe(t, srv, assistantTestBerryToken, http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("tools/list status=%d", writer.Code)
	}
	var response struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("tools/list undecodable: %v", err)
	}
	var names []string
	for _, tool := range response.Result.Tools {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	return names
}

// assistantFullConfig writes a complete valid config file with the given
// PANEWIRE_ASSISTANT_WRITES value so the flag matrix exercises the real
// loader, not a struct literal.
func assistantFullConfig(t *testing.T, hk *fakeAssistantHK, hub *fakeAssistantHub, writes string) string {
	t.Helper()
	dir := t.TempDir()
	lines := []string{
		"PANEWIRE_ASSISTANT_LISTEN=127.0.0.1:0",
		"HANDOFFKEEP_URL=" + hk.server.URL,
		"HANDOFFKEEP_TOKEN=" + assistantTestHKToken,
		"PANEWIRE_ASSISTANT_TOKEN_FILE=" + writeMode0600(t, dir, "berry.token", assistantTestBerryToken),
		"PANEWIRE_ASSISTANT_TARGETS_FILE=" + writeMode0600(t, dir, "targets.json", assistantWriteTargets),
		"PANEWIRE_ASSISTANT_CLIENT_NAME=berry-test",
	}
	if writes != "" {
		lines = append(lines, "PANEWIRE_ASSISTANT_WRITES="+writes)
	}
	if hub != nil {
		lines = append(lines,
			"PANEWIRE_ASSISTANT_HUB_URL="+hub.server.URL,
			"PANEWIRE_ASSISTANT_HUB_TOKEN=hub-token-PLANT-5c2d")
	}
	return writeMode0600(t, dir, "assistant.conf", strings.Join(lines, "\n")+"\n")
}

// AC1 (amendment F): the write set is selective — off lists and answers
// nothing, answer adds only the two answer tools, deliver adds only
// deliver, all adds all three; a disabled write call fails closed with
// writes_disabled before a single upstream request; an unrecognized flag
// value refuses startup; the hub pair is required exactly when writes are
// enabled.
func TestAssistantWritesFlagAC1(t *testing.T) {
	t.Run("loader parses the set", func(t *testing.T) {
		hk := newFakeAssistantHK(t)
		hub := newFakeAssistantHub(t)
		cases := []struct {
			flag string
			want int
		}{
			{"", assistantWritesOff},
			{"0", assistantWritesOff},
			{"off", assistantWritesOff},
			{"false", assistantWritesOff},
			{"no", assistantWritesOff},
			{" ", assistantWritesOff},
			{"answer", assistantWritesAnswer},
			{"deliver", assistantWritesDeliver},
			{"all", assistantWritesAll},
			{" ALL ", assistantWritesAll},
		}
		for _, tc := range cases {
			var useHub *fakeAssistantHub
			if tc.want != assistantWritesOff {
				useHub = hub
			}
			cfg, err := loadAssistantConfig(assistantFullConfig(t, hk, useHub, tc.flag))
			if err != nil {
				t.Fatalf("flag %q: %v", tc.flag, err)
			}
			if cfg.Writes != tc.want {
				t.Fatalf("flag %q: writes=%d, want %d", tc.flag, cfg.Writes, tc.want)
			}
		}
		// An unrecognized value refuses startup even with the hub pair set.
		for _, bad := range []string{"1", "yes", "answers", "answer,deliver", "read", "on"} {
			if _, err := loadAssistantConfig(assistantFullConfig(t, hk, hub, bad)); err == nil {
				t.Fatalf("flag %q accepted, want refusal", bad)
			}
		}
	})

	readTools := []string{"pending_detail", "pending_list", "poll", "progress", "targets"}

	t.Run("off exposes no write tool and writes fail closed silently", func(t *testing.T) {
		hk := newFakeAssistantHK(t)
		srv, _ := assistantWriteServer(t, hk, nil, assistantWritesOff, nil)
		if srv.hub != nil || srv.drainKick != nil || srv.limiter != nil {
			t.Fatal("writes-off server still wired a hub client, kick or limiter")
		}
		if got := assistantListTools(t, srv); !slices.Equal(got, readTools) {
			t.Fatalf("tools/list=%v, want %v", got, readTools)
		}
		for _, tool := range []string{"answer_decision", "answer_question", "deliver"} {
			if got := assistantCallError(t, srv, assistantTestBerryToken, tool, map[string]any{}); got != "writes_disabled" {
				t.Fatalf("%s: error=%q, want writes_disabled", tool, got)
			}
		}
		if log := hk.requestLog(); len(log) != 0 {
			t.Fatalf("disabled writes reached upstream: %v", log)
		}
		// Reads behave exactly as before.
		if got := assistantCall(t, srv, assistantTestBerryToken, "targets", nil); got.isError {
			t.Fatalf("read tool failed on a writes-off binary: %s", got.text)
		}
	})

	t.Run("answer set adds answers only", func(t *testing.T) {
		hk := newFakeAssistantHK(t)
		hub := newFakeAssistantHub(t)
		hub.hk = hk
		srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAnswer, nil)
		want := append(append([]string{}, readTools...), "answer_decision", "answer_question")
		sort.Strings(want)
		if got := assistantListTools(t, srv); !slices.Equal(got, want) {
			t.Fatalf("tools/list=%v, want %v", got, want)
		}
		if got := assistantCallError(t, srv, assistantTestBerryToken, "deliver", map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "x"}); got != "writes_disabled" {
			t.Fatalf("deliver on answer-only: %q, want writes_disabled", got)
		}
	})

	t.Run("deliver set adds deliver only", func(t *testing.T) {
		hk := newFakeAssistantHK(t)
		hub := newFakeAssistantHub(t)
		hub.hk = hk
		srv, _ := assistantWriteServer(t, hk, hub, assistantWritesDeliver, nil)
		want := append(append([]string{}, readTools...), "deliver")
		sort.Strings(want)
		if got := assistantListTools(t, srv); !slices.Equal(got, want) {
			t.Fatalf("tools/list=%v, want %v", got, want)
		}
		if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision", map[string]any{"request_id": "dr-1-1"}); got != "writes_disabled" {
			t.Fatalf("answer_decision on deliver-only: %q, want writes_disabled", got)
		}
	})

	t.Run("hub pair required exactly when writes are on", func(t *testing.T) {
		hk := newFakeAssistantHK(t)
		dir := t.TempDir()
		base := assistantConfig{
			Listen:        "127.0.0.1:0",
			HKURL:         hk.server.URL,
			HKToken:       assistantTestHKToken,
			TokenFile:     writeMode0600(t, dir, "berry.token", assistantTestBerryToken),
			TargetsFile:   writeMode0600(t, dir, "targets.json", assistantWriteTargets),
			ClientName:    "berry-test",
			Writes:        assistantWritesAnswer,
			DrainInterval: assistantDrainIntervalMin,
		}
		if _, err := newAssistantServer(base, assistantServerDeps{}); err == nil || !strings.Contains(err.Error(), "hub url and token") {
			t.Fatalf("writes on without hub pair: err=%v, want refusal", err)
		}
		base.HubURL = "http://127.0.0.1:1"
		if _, err := newAssistantServer(base, assistantServerDeps{}); err == nil {
			t.Fatal("half hub pair accepted")
		}
	})
}

// seedDecisionTask registers one lane-a task carrying an open dr-<id>-<rev>
// request; the resolve route's own checks (terminal, disposition,
// human_only) come after, so every refusal class is exercised.
func seedDecisionTask(hk *fakeAssistantHK, taskID int64, revision int, mutate func(*assistantTask)) string {
	requestID := fmt.Sprintf("dr-%d-%d", taskID, revision)
	task := assistantTask{ID: taskID, Lane: "lane-a", Title: "work", State: "needs_decision"}
	task.Refs.DecisionRequest = &assistantDecisionRequest{ID: requestID, Revision: revision, Status: "open", Question: "go?", DefaultAction: "wait", RequestedBy: "wrk-a", RequestedAt: time.Now().UTC()}
	if mutate != nil {
		mutate(&task)
	}
	hk.seedTask(task)
	return requestID
}

// AC2: answer_decision — success mints the deterministic outbox row and the
// posted body carries no caller attribution; the same answer replays as a
// duplicate; every hk refusal maps to its named error; out-of-scope and
// missing ids are indistinguishable request_not_found.
func TestAssistantAnswerDecisionAC2(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, logs := assistantWriteServer(t, hk, hub, assistantWritesAnswer, nil)

	requestID := seedDecisionTask(hk, 42, 3, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": requestID, "option": "go", "text": "ship it"})
	if result.isError {
		t.Fatalf("answer_decision: %s", result.text)
	}
	if result.decoded["resolved"] != true || result.decoded["duplicate"] != false {
		t.Fatalf("answer_decision result=%v", result.decoded)
	}
	if result.decoded["event_id"] != requestID+"-answered" {
		t.Fatalf("event_id=%v, want %s-answered", result.decoded["event_id"], requestID)
	}

	// The wire body must carry exactly request_id, option and text — never
	// responder, by or kind: attribution is handoffkeep's, not the caller's.
	bodies := hk.postedBodies("POST /v1/assistant/decisions/resolve")
	if len(bodies) != 1 {
		t.Fatalf("resolve posts=%d, want 1", len(bodies))
	}
	var posted map[string]any
	if err := json.Unmarshal([]byte(bodies[0]), &posted); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"responder", "by", "kind", "lane", "url", "event_id"} {
		if _, present := posted[key]; present {
			t.Fatalf("resolve body carried caller field %q: %s", key, bodies[0])
		}
	}
	if posted["request_id"] != requestID || posted["option"] != "go" || posted["text"] != "ship it" {
		t.Fatalf("resolve body=%s", bodies[0])
	}

	// The answer produced the owed outbox row with the deterministic id.
	owed := false
	for _, row := range hk.outboxRows() {
		if row.EventID == requestID+"-answered" && row.Kind == "decision_answered" && row.SentAt == nil {
			owed = true
		}
	}
	if !owed {
		t.Fatalf("no owed outbox row for %s-answered", requestID)
	}

	// The byte-identical replay is the duplicate success — no second row.
	dup := assistantCall(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": requestID, "option": "go", "text": "ship it"})
	if dup.isError || dup.decoded["duplicate"] != true {
		t.Fatalf("duplicate answer: %+v", dup)
	}
	rows := 0
	for _, row := range hk.outboxRows() {
		if row.EventID == requestID+"-answered" {
			rows++
		}
	}
	if rows != 1 {
		t.Fatalf("duplicate minted a second outbox row (%d)", rows)
	}

	// A different answer on the resolved request is resolved-differently.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": requestID, "option": "wait"}); got != "decision_request_resolved" {
		t.Fatalf("resolved-differently: %q", got)
	}

	// Stale revision id on a live in-scope task.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": "dr-42-99", "option": "go"}); got != "decision_request_stale" {
		t.Fatalf("stale: %q", got)
	}

	// human_only refuses before any option check.
	humanID := seedDecisionTask(hk, 50, 1, func(task *assistantTask) {
		task.Refs.DecisionRequest.HumanOnly = true
	})
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": humanID, "option": "go"}); got != "decision_request_human_only" {
		t.Fatalf("human_only: %q", got)
	}

	// A disposition item is operator-only.
	dispoID := seedDecisionTask(hk, 51, 1, func(task *assistantTask) {
		task.Refs.Disposition = json.RawMessage(`[{"kind":"merge"}]`)
	})
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": dispoID, "option": "go"}); got != "disposition_operator_only" {
		t.Fatalf("disposition: %q", got)
	}

	// A terminal task's request is indistinguishable from a missing one.
	terminalID := seedDecisionTask(hk, 52, 1, func(task *assistantTask) { task.State = "merged" })
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
		map[string]any{"request_id": terminalID, "option": "go"}); got != "request_not_found" {
		t.Fatalf("terminal: %q", got)
	}

	// Missing and out-of-scope ids collapse onto the same generic error and
	// never reach the resolve route.
	outID := seedDecisionTask(hk, 53, 1, func(task *assistantTask) { task.Lane = "lane-z" })
	before := len(hk.postedBodies("POST /v1/assistant/decisions/resolve"))
	for _, id := range []string{"dr-999-1", outID, "nonsense", ""} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision",
			map[string]any{"request_id": id, "option": "go"}); got != "request_not_found" {
			t.Fatalf("id %q: %q, want request_not_found", id, got)
		}
	}
	if got := len(hk.postedBodies("POST /v1/assistant/decisions/resolve")); got != before {
		t.Fatalf("scoped-out ids reached resolve (%d posts)", got)
	}

	// Caller attribution fields are rejected at the schema, not forwarded.
	for _, extra := range []map[string]any{
		{"request_id": requestID, "option": "go", "responder": "operator"},
		{"request_id": requestID, "option": "go", "by": "operator"},
	} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_decision", extra); got != "invalid_arguments" {
			t.Fatalf("caller attribution %v: %q, want invalid_arguments", extra, got)
		}
	}

	// Audit names the tool, subject, event id and outcome — never the text.
	audit := logs.String()
	for _, needle := range []string{`"tool":"answer_decision"`, `"subject":"dr-42-3"`, `"event_id":"dr-42-3-answered"`, `"outcome":"ok"`} {
		if !strings.Contains(audit, needle) {
			t.Fatalf("audit missing %s:\n%s", needle, audit)
		}
	}
	if strings.Contains(audit, "ship it") {
		t.Fatal("audit leaked the answer text")
	}
}

// AC3: answer_question — the pre-read drives the verdict: a held answer
// slot is question_slot_taken (not stale), a revision gap is stale_revision
// carrying the current revision, the post runs hk's assistant-channel CAS
// shape only, and the hub is never touched — not even for the relay (the
// drainer owns that).
func TestAssistantAnswerQuestionAC3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAnswer, nil)
	now := time.Now().UTC()

	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-40", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "which pane?", State: "pending", Revision: 2, CreatedAt: now, UpdatedAt: now})

	result := assistantCall(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-40", "revision": 2, "text": "pane 7"})
	if result.isError {
		t.Fatalf("answer_question: %s", result.text)
	}
	if result.decoded["answered"] != true || result.decoded["duplicate"] != false {
		t.Fatalf("answer result=%v", result.decoded)
	}
	if result.decoded["event_id"] != "Q-20261010-40-rev2-answered" {
		t.Fatalf("event_id=%v", result.decoded["event_id"])
	}
	if mid, _ := result.decoded["message_id"].(float64); mid < 1 {
		t.Fatalf("message_id=%v", result.decoded["message_id"])
	}

	// The post is the assistant-channel CAS shape and nothing else.
	bodies := hk.postedBodies("POST /v1/chat/messages")
	if len(bodies) != 1 {
		t.Fatalf("chat posts=%d, want 1", len(bodies))
	}
	var posted struct {
		ConversationID string `json:"conversation_id"`
		Author         string `json:"author"`
		SourceChannel  string `json:"source_channel"`
		OriginEventID  string `json:"origin_event_id"`
		Body           string `json:"body"`
		Answers        []struct {
			QuestionID string `json:"question_id"`
			Revision   int    `json:"revision"`
		} `json:"answers"`
		Responder string `json:"responder"`
		By        string `json:"by"`
	}
	if err := json.Unmarshal([]byte(bodies[0]), &posted); err != nil {
		t.Fatal(err)
	}
	if posted.Author != "operator" || posted.SourceChannel != "assistant" ||
		posted.OriginEventID != "berry-answer-Q-20261010-40-rev2" ||
		posted.ConversationID != "operator-desk" || posted.Body != "pane 7" ||
		len(posted.Answers) != 1 || posted.Answers[0].QuestionID != "Q-20261010-40" || posted.Answers[0].Revision != 2 {
		t.Fatalf("chat post body=%s", bodies[0])
	}
	if posted.Responder != "" || posted.By != "" {
		t.Fatalf("caller attribution reached the wire: %s", bodies[0])
	}
	if posts := hub.postCount(); posts != 0 {
		t.Fatalf("answer_question touched the hub (%d posts) — the drainer owns relay", posts)
	}

	// The identical replay hits the pre-read verdict: the assistant's own
	// answer now holds the slot, which is question_slot_taken — the wire
	// dedupe only fires when the pre-read raced the write.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-40", "revision": 2, "text": "pane 7"}); got != "question_slot_taken" {
		t.Fatalf("replay verdict: %q, want question_slot_taken", got)
	}

	// The wire-level dedupe — a pre-read that raced the real write and saw
	// a free slot — still counts as handled: an existing row under the same
	// origin event id is the duplicate success, not an error.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-46", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "?", State: "pending", Revision: 1, CreatedAt: now, UpdatedAt: now})
	hk.mu.Lock()
	hk.bodyFor["/v1/chat/messages"] = `{"id":999}`
	hk.mu.Unlock()
	wireDup := assistantCall(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-46", "revision": 1, "text": "x"})
	if wireDup.isError || wireDup.decoded["duplicate"] != true || wireDup.decoded["message_id"] != float64(999) {
		t.Fatalf("wire dedupe: %+v", wireDup)
	}
	hk.mu.Lock()
	delete(hk.bodyFor, "/v1/chat/messages")
	hk.mu.Unlock()

	// A question already answered by any channel reports the occupied slot.
	held := int64(777)
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-41", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "?", State: "pending", Revision: 1, AnswerMessageID: &held, CreatedAt: now, UpdatedAt: now})
	before := len(hk.postedBodies("POST /v1/chat/messages"))
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-41", "revision": 1, "text": "x"}); got != "question_slot_taken" {
		t.Fatalf("occupied slot: %q", got)
	}
	if got := len(hk.postedBodies("POST /v1/chat/messages")); got != before {
		t.Fatalf("slot-taken still posted (%d)", got)
	}

	// A revision gap is stale_revision with the current revision in detail.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-42", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "?", State: "pending", Revision: 5, CreatedAt: now, UpdatedAt: now})
	stale := assistantCall(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-42", "revision": 3, "text": "x"})
	if !stale.isError {
		t.Fatal("stale revision answered")
	}
	var staleErr struct {
		Error           string `json:"error"`
		CurrentRevision int    `json:"current_revision"`
	}
	if err := json.Unmarshal([]byte(stale.text), &staleErr); err != nil {
		t.Fatal(err)
	}
	if staleErr.Error != "stale_revision" || staleErr.CurrentRevision != 5 {
		t.Fatalf("stale detail=%s", stale.text)
	}

	// The raced CAS — slot free at the pre-read, gone at the post — is the
	// same stale_revision verdict, never a retry-worthy error.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-43", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "?", State: "pending", Revision: 1, CreatedAt: now, UpdatedAt: now})
	hk.mu.Lock()
	hk.conflictOnceFor["/v1/chat/messages"] = "chat_question_stale"
	hk.mu.Unlock()
	if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_question",
		map[string]any{"id": "Q-20261010-43", "revision": 1, "text": "x"}); got != "stale_revision" {
		t.Fatalf("raced CAS: %q, want stale_revision", got)
	}

	// Resolved, missing and out-of-scope questions all collapse onto the
	// generic request_not_found.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-44", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "?", State: "resolved", Revision: 1, CreatedAt: now, UpdatedAt: now})
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-45", ConversationID: "rogue-desk", Lane: "lane-z",
		Body: "?", State: "pending", Revision: 1, CreatedAt: now, UpdatedAt: now})
	for _, id := range []string{"Q-20261010-44", "Q-20261010-45", "Q-20261010-99", "dr-1-1"} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, "answer_question",
			map[string]any{"id": id, "revision": 1, "text": "x"}); got != "request_not_found" {
			t.Fatalf("question %q: %q, want request_not_found", id, got)
		}
	}
}

// AC4: deliver — deterministic berry:<target>:<key> event ids, lane-only
// routing from the targets file, the POST-as-probe idempotency flow, strict
// argument and text validation, and the three duplicate verdicts.
func TestAssistantDeliverAC4(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, logs := assistantWriteServer(t, hk, hub, assistantWritesDeliver, nil)

	result := assistantCall(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "restart the lane"})
	if result.isError {
		t.Fatalf("deliver: %s", result.text)
	}
	if result.decoded["event_id"] != "berry:ops:k12345678" || result.decoded["status"] != "accepted" || result.decoded["duplicate"] != false {
		t.Fatalf("deliver result=%v", result.decoded)
	}
	rowID, _ := result.decoded["relay_row_id"].(float64)
	if rowID < 1 {
		t.Fatalf("relay_row_id=%v", result.decoded["relay_row_id"])
	}
	row := hub.rowFor("lane-a", "berry:ops:k12345678")
	if row == nil || row.text != "[via berry] restart the lane" || row.label != "panewire-assistant" {
		t.Fatalf("hub row=%+v", row)
	}

	// Same key+text is the durable duplicate success — POST, 409, walk,
	// byte-compare — never a fresh store.
	dup := assistantCall(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "restart the lane"})
	if dup.isError || dup.decoded["duplicate"] != true || dup.decoded["relay_row_id"] != rowID {
		t.Fatalf("duplicate: %+v", dup)
	}
	if hub.rowCount() != 1 || hub.postCount() != 2 {
		t.Fatalf("duplicate stored or skipped the probe: rows=%d posts=%d", hub.rowCount(), hub.postCount())
	}

	// Same key, different text: the stored row proves the conflict and the
	// hub stored nothing new.
	if got := assistantCallError(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "different instruction"}); got != "idempotency_conflict" {
		t.Fatalf("conflict: %q", got)
	}
	if hub.rowCount() != 1 {
		t.Fatalf("conflict stored a row (%d)", hub.rowCount())
	}

	// A fresh instance over the same fakes derives the identical event id —
	// determinism survives restart, which is what makes the key idempotent.
	srv2, _ := assistantWriteServer(t, hk, hub, assistantWritesDeliver, nil)
	restart := assistantCall(t, srv2, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "restart the lane"})
	if restart.isError || restart.decoded["event_id"] != "berry:ops:k12345678" || restart.decoded["duplicate"] != true {
		t.Fatalf("restart duplicate: %+v", restart)
	}

	// Conversation targets route to their operator-mapped deliver_lane.
	desk := assistantCall(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "desk", "idempotency_key": "k23456789", "text": "heads up"})
	if desk.isError {
		t.Fatalf("conversation deliver: %s", desk.text)
	}
	if hub.rowFor("desk-lane", "berry:desk:k23456789") == nil {
		t.Fatal("conversation deliver did not land on deliver_lane")
	}

	// Fail-closed targets and fields — none may reach the hub.
	postsBefore := hub.postCount()
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"unknown target", map[string]any{"target": "ghost", "idempotency_key": "k12345678", "text": "x"}, "unknown_target"},
		{"conversation without deliver_lane", map[string]any{"target": "quiet", "idempotency_key": "k12345678", "text": "x"}, "target_not_deliverable"},
		{"caller lane", map[string]any{"target": "ops", "idempotency_key": "k34567890", "text": "x", "lane": "lane-b"}, "invalid_arguments"},
		{"caller url", map[string]any{"target": "ops", "idempotency_key": "k34567890", "text": "x", "url": "http://x"}, "invalid_arguments"},
		{"caller event id", map[string]any{"target": "ops", "idempotency_key": "k34567890", "text": "x", "event_id": "berry:ops:x"}, "invalid_arguments"},
		{"caller route", map[string]any{"target": "ops", "idempotency_key": "k34567890", "text": "x", "route": "w1:p1"}, "invalid_arguments"},
		{"short key", map[string]any{"target": "ops", "idempotency_key": "short", "text": "x"}, "invalid_arguments"},
		{"spaced key", map[string]any{"target": "ops", "idempotency_key": "has space here", "text": "x"}, "invalid_arguments"},
		{"long key", map[string]any{"target": "ops", "idempotency_key": strings.Repeat("k", 65), "text": "x"}, "invalid_arguments"},
		{"colon key", map[string]any{"target": "ops", "idempotency_key": "key:123456", "text": "x"}, "invalid_arguments"},
		{"leading bracket text", map[string]any{"target": "ops", "idempotency_key": "k45678901", "text": "[director-1] do x"}, "invalid_arguments"},
		{"newline text", map[string]any{"target": "ops", "idempotency_key": "k45678901", "text": "line1\nline2"}, "invalid_arguments"},
		{"control text", map[string]any{"target": "ops", "idempotency_key": "k45678901", "text": "a\x01b"}, "invalid_arguments"},
		{"empty text", map[string]any{"target": "ops", "idempotency_key": "k45678901", "text": "   "}, "invalid_arguments"},
		{"overlong text", map[string]any{"target": "ops", "idempotency_key": "k45678901", "text": strings.Repeat("x", assistantDeliverTextMaxBytes+1)}, "text_too_long"},
	} {
		if got := assistantCallError(t, srv, assistantTestBerryToken, "deliver", tc.args); got != tc.want {
			t.Fatalf("%s: %q, want %s", tc.name, got, tc.want)
		}
	}
	if hub.postCount() != postsBefore {
		t.Fatalf("rejected deliver calls reached the hub: posts=%d want %d", hub.postCount(), postsBefore)
	}

	// Exactly at the cap is accepted — the boundary is 2036 bytes of text.
	atCap := assistantCall(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k56789012", "text": strings.Repeat("y", assistantDeliverTextMaxBytes)})
	if atCap.isError {
		t.Fatalf("at-cap text rejected: %s", atCap.text)
	}

	// The 409-with-id-0 in-flight window.
	hub.inFlightIDs["berry:ops:k67890123"] = true
	if got := assistantCallError(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k67890123", "text": "queued"}); got != "in_flight_retry" {
		t.Fatalf("in-flight: %q", got)
	}
	delete(hub.inFlightIDs, "berry:ops:k67890123")

	// A duplicate row the hk walk cannot find is duplicate_unverified —
	// the caller gets the row id and the no-new-key hint.
	hub.mu.Lock()
	hub.nextID++
	hub.rows["lane-a\x00berry:ops:k78901234"] = &fakeHubRow{id: hub.nextID, lane: "lane-a", eventID: "berry:ops:k78901234", text: "[via berry] foreign", label: "panewire-assistant"}
	unverifiedID := hub.nextID
	hub.mu.Unlock()
	unv := assistantCall(t, srv, assistantTestBerryToken, "deliver",
		map[string]any{"target": "ops", "idempotency_key": "k78901234", "text": "foreign"})
	if !unv.isError {
		t.Fatal("foreign duplicate accepted")
	}
	var unvErr struct {
		Error      string `json:"error"`
		RelayRowID int64  `json:"relay_row_id"`
		Hint       string `json:"hint"`
	}
	if err := json.Unmarshal([]byte(unv.text), &unvErr); err != nil {
		t.Fatal(err)
	}
	if unvErr.Error != "duplicate_unverified" || unvErr.RelayRowID != unverifiedID || unvErr.Hint == "" {
		t.Fatalf("unverified detail=%s", unv.text)
	}

	// Audit: identity, tool, subject, key, event id, outcome — no text.
	audit := logs.String()
	for _, needle := range []string{`"tool":"deliver"`, `"subject":"ops"`, `"idempotency_key":"k12345678"`, `"event_id":"berry:ops:k12345678"`, `"outcome":"idempotency_conflict"`} {
		if !strings.Contains(audit, needle) {
			t.Fatalf("audit missing %s:\n%s", needle, audit)
		}
	}
	if strings.Contains(audit, "restart the lane") {
		t.Fatal("audit leaked delivered text")
	}
}

// AC5: the drainer — every receipt class, the crash/restart recoveries and
// the two-drainer race all leave each row exactly-once effectively sent.
func TestAssistantDrainerAC5(t *testing.T) {
	setup := func(t *testing.T) (*fakeAssistantHK, *fakeAssistantHub, *assistantServer) {
		hk := newFakeAssistantHK(t)
		hub := newFakeAssistantHub(t)
		hub.hk = hk
		srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, nil)
		return hk, hub, srv
	}
	unsent := func(hk *fakeAssistantHK) int {
		n := 0
		for _, row := range hk.outboxRows() {
			if row.SentAt == nil {
				n++
			}
		}
		return n
	}

	t.Run("a failing row never stalls the rest (g)", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-1-1-answered", "[via berry] dr-1-1 answered go: x")
		hub.rejectEventIDs["dr-2-1-answered"] = http.StatusBadRequest
		hk.seedOutbox("decision_answered", "lane-a", "dr-2-1-answered", "[via berry] dr-2-1 answered go: x")
		hk.seedOutbox("decision_answered", "lane-a", "dr-3-1-answered", "[via berry] dr-3-1 answered go: x")
		sent := srv.drainNow(context.Background())
		if len(sent) != 2 {
			t.Fatalf("sent=%v, want the two healthy rows", sent)
		}
		if unsent(hk) != 1 || hub.rowFor("lane-a", "dr-2-1-answered") != nil {
			t.Fatal("the rejected row must stay owed and unposted-as-stored")
		}
		marks, _ := hk.markCounts()
		if marks != 2 {
			t.Fatalf("marks=%d, want 2", marks)
		}
	})

	t.Run("409 id 0 is never a receipt (h)", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-10-1-answered", "[via berry] dr-10-1 answered go: x")
		hub.inFlightIDs["dr-10-1-answered"] = true
		if sent := srv.drainNow(context.Background()); len(sent) != 0 || unsent(hk) != 1 {
			t.Fatalf("id-0 receipt marked or claimed: sent=%v", sent)
		}
		// The binary-layer guard, not just hk's id>0 validation: a mark is
		// never even attempted on a receipt that proves nothing.
		if marks := hk.postedBodies("POST /v1/assistant/outbox/sent"); len(marks) != 0 {
			t.Fatalf("mark-sent attempted on an id-0 receipt: %v", marks)
		}
		delete(hub.inFlightIDs, "dr-10-1-answered")
		if sent := srv.drainNow(context.Background()); len(sent) != 1 || unsent(hk) != 0 {
			t.Fatalf("retry after the claim cleared: sent=%v unsent=%d", sent, unsent(hk))
		}
		if hub.rowCount() != 1 {
			t.Fatalf("rows=%d, want 1", hub.rowCount())
		}
	})

	t.Run("hub stores then the wire dies — dedupe carries the retry (i)", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-11-1-answered", "[via berry] dr-11-1 answered go: x")
		hub.killAfterStore = true
		if sent := srv.drainNow(context.Background()); len(sent) != 0 || unsent(hk) != 1 {
			t.Fatalf("dead-wire receipt claimed sent: %v", sent)
		}
		if hub.rowCount() != 1 {
			t.Fatal("the row was stored before the wire died")
		}
		if sent := srv.drainNow(context.Background()); len(sent) != 1 || unsent(hk) != 0 || hub.rowCount() != 1 {
			t.Fatalf("dedupe retry: sent=%v unsent=%d rows=%d", sent, unsent(hk), hub.rowCount())
		}
	})

	t.Run("mark-sent transport failure retries idempotently (j)", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-12-1-answered", "[via berry] dr-12-1 answered go: x")
		hk.killMarkSent = true
		if sent := srv.drainNow(context.Background()); len(sent) != 0 || unsent(hk) != 1 {
			t.Fatalf("dropped mark claimed sent: %v", sent)
		}
		if sent := srv.drainNow(context.Background()); len(sent) != 1 || unsent(hk) != 0 {
			t.Fatalf("mark retry: sent=%v unsent=%d", sent, unsent(hk))
		}
		if hub.rowCount() != 1 {
			t.Fatalf("rows=%d, want 1 — the retry deduped on the event id", hub.rowCount())
		}
	})

	t.Run("mark-sent conflict warns and never re-posts", func(t *testing.T) {
		hk, hub, srv := setup(t)
		// hk has TWO rows under one event id: one already sent by a racing
		// drain against another hub (row id 555), one still owed. The owed
		// row lists, posts, and its mark hits the recorded-555 conflict —
		// which logs the class and skips; the row stays owed for the next
		// pass but this pass is done with it.
		hk.seedOutboxSent("decision_answered", "lane-a", "dr-13-1-answered", "[via berry] dr-13-1 answered go: x", 555)
		hk.seedOutbox("decision_answered", "lane-a", "dr-13-1-answered", "[via berry] dr-13-1 answered go: x")
		sent := srv.drainNow(context.Background())
		_, conflicts := hk.markCounts()
		if conflicts != 1 {
			t.Fatalf("conflicts=%d, want 1", conflicts)
		}
		if _, handled := sent["dr-13-1-answered"]; !handled {
			t.Fatalf("conflicted row not accounted: sent=%v", sent)
		}
		if hub.postCount() != 1 {
			t.Fatalf("posts=%d — a conflict must never re-POST", hub.postCount())
		}
	})

	t.Run("row gone between list and mark logs and skips", func(t *testing.T) {
		hk, _, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-15-1-answered", "[via berry] dr-15-1 answered go: x")
		hk.statusOnceFor["/v1/assistant/outbox/sent"] = http.StatusNotFound
		if sent := srv.drainNow(context.Background()); len(sent) != 0 {
			t.Fatalf("gone row claimed sent: %v", sent)
		}
		// The hub row exists — the receipt was real — the mark just could
		// not land; the next pass dedupes and marks cleanly.
		if sent := srv.drainNow(context.Background()); len(sent) != 1 || unsent(hk) != 0 {
			t.Fatalf("recovery pass: sent=%v unsent=%d", sent, unsent(hk))
		}
	})

	t.Run("two drainers race — one hub row, idempotent marks (race)", func(t *testing.T) {
		hk, hub, srv := setup(t)
		srv2, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, nil)
		hk.seedOutbox("decision_answered", "lane-a", "dr-16-1-answered", "[via berry] dr-16-1 answered go: x")
		var wg sync.WaitGroup
		var s1, s2 map[string]int64
		wg.Add(2)
		go func() { defer wg.Done(); s1 = srv.drainNow(context.Background()) }()
		go func() { defer wg.Done(); s2 = srv2.drainNow(context.Background()) }()
		wg.Wait()
		if hub.rowCount() != 1 {
			t.Fatalf("racing drainers stored %d hub rows, want exactly 1", hub.rowCount())
		}
		marks, conflicts := hk.markCounts()
		if marks < 1 || conflicts != 0 {
			t.Fatalf("marks=%d conflicts=%d, want marks>=1 conflicts=0", marks, conflicts)
		}
		if unsent(hk) != 0 || len(s1)+len(s2) != 2 {
			t.Fatalf("race outcome: s1=%v s2=%v unsent=%d", s1, s2, unsent(hk))
		}
	})

	t.Run("restart mid-drain inherits the owed row", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hk.seedOutbox("decision_answered", "lane-a", "dr-17-1-answered", "[via berry] dr-17-1 answered go: x")
		hub.killAfterStore = true
		srv.drainNow(context.Background())
		// The first instance dies with the row stored-but-unmarked; a fresh
		// binary over the same fakes finishes it with no second hub row.
		srv2, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, nil)
		if sent := srv2.drainNow(context.Background()); len(sent) != 1 || hub.rowCount() != 1 {
			t.Fatalf("restart drain: sent=%v rows=%d", sent, hub.rowCount())
		}
	})

	t.Run("the seen-set drains past a poison head beyond one page", func(t *testing.T) {
		hk, hub, srv := setup(t)
		hub.rejectEventIDs["poison-head"] = http.StatusBadRequest
		hk.seedOutbox("decision_answered", "lane-a", "poison-head", "[via berry] bad")
		for i := 0; i < assistantOutboxLimit+5; i++ {
			hk.seedOutbox("decision_answered", "lane-a", fmt.Sprintf("bulk-%04d", i), "[via berry] bulk")
		}
		// drainPass, not drainNow: coverage of >1000 rows costs two
		// round-trips each, which exceeds the production 30s pass bound
		// under -race — that bound guards a wedged production pass, not
		// this paging assertion.
		sent := srv.drainPass(context.Background())
		if len(sent) != assistantOutboxLimit+5 || unsent(hk) != 1 {
			t.Fatalf("paging drain: sent=%d unsent=%d", len(sent), unsent(hk))
		}
	})
}

// AC6: the drain text — decision rows carry their already-rendered id+rev,
// chat rows get the revision injected from the event id, unknown kinds pass
// verbatim, and every row goes through the deterministic sanitizer.
func TestAssistantDrainerTextAC6(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, nil)

	hk.seedOutbox("decision_answered", "lane-a", "dr-9-2-answered", "[via berry] dr-9-2 answered go: ship it")
	hk.seedOutbox("chat_answer", "lane-a", "Q-20261010-07-rev4-answered", "[via berry] Q-20261010-07 answered: hello")
	hk.seedOutbox("weird_future_kind", "lane-a", "future-1", "[via berry] raw\nnotice")
	hk.seedOutbox("chat_answer", "lane-a", "Q-20261010-08-rev2-answered", "[via berry] Q-20261010-08 rev2 answered: already")

	srv.drainNow(context.Background())

	if row := hub.rowFor("lane-a", "dr-9-2-answered"); row == nil || row.text != "[via berry] dr-9-2 answered go: ship it" {
		t.Fatalf("decision text=%+v — must pass through verbatim", row)
	}
	if row := hub.rowFor("lane-a", "Q-20261010-07-rev4-answered"); row == nil || row.text != "[via berry] Q-20261010-07 rev4 answered: hello" {
		t.Fatalf("chat text=%+v — revision must be injected", row)
	}
	if row := hub.rowFor("lane-a", "future-1"); row == nil || row.text != "[via berry] raw notice" {
		t.Fatalf("unknown kind=%+v — verbatim through the sanitizer", row)
	}
	if row := hub.rowFor("lane-a", "Q-20261010-08-rev2-answered"); row == nil || row.text != "[via berry] Q-20261010-08 rev2 answered: already" {
		t.Fatalf("already-revisioned text=%+v — must not double-inject", row)
	}

	// The sanitizer is a pure function: same input, same bytes, always.
	messy := "a\nb\x01\x7f\u0080c"
	first, second := sanitizeLaneText(messy), sanitizeLaneText(messy)
	if first != second || first != "a b   c" {
		t.Fatalf("sanitizer nondeterministic or wrong: %q vs %q", first, second)
	}
	if !validLaneEventText(first) {
		t.Fatalf("sanitized text the hub would reject: %q", first)
	}

	// An over-cap row re-caps at the byte limit on a UTF-8 boundary.
	longText := strings.Repeat("x", laneEventTextLimit-10) + "€" + strings.Repeat("y", 100)
	capped := sanitizeLaneText(longText)
	if len(capped) > laneEventTextLimit || !strings.HasSuffix(capped, "…") || !utf8.ValidString(capped) {
		t.Fatalf("recap: len=%d suffix=%q", len(capped), capped[len(capped)-6:])
	}
	// Exactly at the limit keeps its tail — the ellipsis only ever marks a
	// real cut.
	exact := strings.Repeat("z", laneEventTextLimit)
	if got := sanitizeLaneText(exact); got != exact {
		t.Fatalf("exact-limit text rewritten: len=%d", len(got))
	}
}

// AC7: the endpoint allowlist — every tool plus a drain pass exercised
// against fakes that fail any route outside the fixed set; afterwards the
// recorded request paths are checked against the list verbatim.
func TestAssistantEndpointAllowlistAC7(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, nil)
	now := time.Now().UTC()

	hk.pending = assistantFixturePending(now)
	hk.seedTask(assistantTask{ID: 42, Lane: "lane-a", Title: "t", State: "needs_decision", Refs: struct {
		DecisionRequest *assistantDecisionRequest `json:"decision_request,omitempty"`
		Disposition     json.RawMessage           `json:"disposition,omitempty"`
	}{DecisionRequest: &assistantDecisionRequest{ID: "dr-42-3", Revision: 3, Status: "open", Question: "?", DefaultAction: "wait", RequestedBy: "w", RequestedAt: now}}})
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-01", ConversationID: "operator-desk", Lane: "lane-a", Body: "?", State: "pending", Revision: 2, CreatedAt: now, UpdatedAt: now})

	assistantCall(t, srv, assistantTestBerryToken, "targets", nil)
	assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil)
	assistantCall(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "dr-42-3"})
	assistantCall(t, srv, assistantTestBerryToken, "pending_detail", map[string]any{"id": "Q-20261010-01"})
	assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "dr-42-3"})
	assistantCall(t, srv, assistantTestBerryToken, "poll", nil)
	assistantCall(t, srv, assistantTestBerryToken, "answer_decision", map[string]any{"request_id": "dr-42-3", "option": "go", "text": "x"})
	assistantCall(t, srv, assistantTestBerryToken, "answer_question", map[string]any{"id": "Q-20261010-01", "revision": 2, "text": "x"})
	assistantCall(t, srv, assistantTestBerryToken, "deliver", map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "x"})
	srv.drainNow(context.Background())

	hkAllowed := map[string]bool{
		"GET /v1/assistant/pending": true, "GET /v1/assistant/outbox": true,
		"GET /v1/tasks": true, "GET /v1/chat/questions/Q-20261010-01": true,
		"GET /v1/relay/events":                 true,
		"POST /v1/assistant/decisions/resolve": true, "POST /v1/chat/messages": true,
		"POST /v1/assistant/outbox/sent": true,
	}
	for _, req := range hk.requestLog() {
		path := req
		if strings.HasPrefix(path, "GET /v1/tasks/") {
			path = "GET /v1/tasks" // task id is a path segment, not a route
		}
		if !hkAllowed[path] {
			t.Fatalf("assistant client called an unlisted hk route: %s", req)
		}
	}
	for _, req := range hub.requestLog() {
		if !strings.HasPrefix(req, "POST /v1/relay/events") {
			t.Fatalf("assistant client called an unlisted hub route: %s", req)
		}
	}
}

// AC8: no write path can leak a credential — planted tokens, the CF pair,
// the upstream Location header and upstream bodies are swept from tool
// output and audit across forced failure paths.
func TestAssistantWriteSecretsAC8(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, logs := assistantWriteServer(t, hk, hub, assistantWritesAll, func(c *assistantConfig) {
		c.HKCFID, c.HKCFSecret = assistantTestCFID, assistantTestCFSecret
	})
	seedDecisionTask(hk, 42, 3, nil)
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261010-01", ConversationID: "operator-desk", Lane: "lane-a", Body: "?", State: "pending", Revision: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()})
	hk.seedOutbox("decision_answered", "lane-a", "dr-50-1-answered", "[via berry] dr-50-1 answered go: x")

	var outputs []string
	collect := func(r assistantToolResult) { outputs = append(outputs, r.text) }

	// Forced failures on every write path: upstream rejects with a planted
	// body and a planted Location, then a dead transport.
	hk.statusFor["/v1/assistant/decisions/resolve"] = http.StatusInternalServerError
	hk.statusFor["/v1/chat/messages"] = http.StatusBadGateway
	collect(assistantCall(t, srv, assistantTestBerryToken, "answer_decision", map[string]any{"request_id": "dr-42-3", "option": "go"}))
	collect(assistantCall(t, srv, assistantTestBerryToken, "answer_question", map[string]any{"id": "Q-20261010-01", "revision": 1, "text": "x"}))
	hub.failNextStatus = http.StatusInternalServerError
	collect(assistantCall(t, srv, assistantTestBerryToken, "deliver", map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "x"}))
	// A drain that succeeds, a drain that hits a hub error, and a drain
	// whose mark-sent fails — every failure class writes its audit line.
	srv.drainNow(context.Background())
	hk.seedOutbox("decision_answered", "lane-a", "dr-51-1-answered", "[via berry] dr-51-1 answered go: x")
	hub.failNextStatus = http.StatusBadGateway
	srv.drainNow(context.Background())
	hk.seedOutbox("decision_answered", "lane-a", "dr-52-1-answered", "[via berry] dr-52-1 answered go: x")
	hk.statusOnceFor["/v1/assistant/outbox/sent"] = http.StatusInternalServerError
	srv.drainNow(context.Background())
	hk.mu.Lock()
	delete(hk.statusFor, "/v1/assistant/decisions/resolve")
	hk.mu.Unlock()

	sweep := strings.Join(outputs, "\n") + "\n" + logs.String()
	for _, secret := range []string{
		assistantTestHKToken, assistantTestBerryToken, assistantTestCFSecret, assistantTestCFID,
		"hub-token-PLANT-5c2d", assistantTestLocation, "attacker.example",
		"upstream planted body", hk.server.URL, hub.server.URL,
	} {
		if strings.Contains(sweep, secret) {
			t.Fatalf("secret %q leaked into tool output or audit", secret)
		}
	}
}

// AC12: the per-identity write limiter gates ahead of every upstream call.
func TestAssistantRateLimitAC12(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	hub.hk = hk
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, func(c *assistantConfig) {
		c.WriteRatePerMin = 60
		c.WriteBurst = 1
	})
	seedDecisionTask(hk, 42, 3, nil)

	first := assistantCall(t, srv, assistantTestBerryToken, "answer_decision", map[string]any{"request_id": "dr-42-3", "option": "go"})
	if first.isError {
		t.Fatalf("burst-allowing first write: %s", first.text)
	}
	before := len(hk.requestLog())
	if got := assistantCallError(t, srv, assistantTestBerryToken, "deliver", map[string]any{"target": "ops", "idempotency_key": "k12345678", "text": "x"}); got != "rate_limited" {
		t.Fatalf("limited write: %q", got)
	}
	if got := len(hk.requestLog()); got != before {
		t.Fatalf("rate-limited call reached upstream: %d new requests", got-before)
	}

	// The bucket is per-identity, not global.
	limiter := newWriteRateLimiter(60, 1)
	if !limiter.allow("a") || limiter.allow("a") || !limiter.allow("b") {
		t.Fatal("limiter not per-identity")
	}
}

// MINOR-7: the orphan sweep cursor advances over the settled prefix — an
// assistant row behind a graced web row is exempt without pinning the
// cursor forever, and once the prefix settles the assistant row is never
// re-read.
func TestHubChatSweepSettledPrefixAC9(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, store)
	ctx := context.Background()

	// A web row still inside its grace window (unsettled — could still be
	// delivered), then an assistant row behind it (settled: the hk outbox
	// owns its notice).
	web, _, err := store.CreateChatMessageExtended(ctx, ChatMessageCreate{
		ConversationID: hubChatConversationID, Author: "operator", Body: "graced web message",
		SourceChannel: "web", OriginEventID: "web-ev-1", OriginTimestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	assistant := &ChatMessage{ID: store.nextMsgID, ConversationID: hubChatConversationID, Author: "operator", Body: "assistant answer",
		SourceChannel: "assistant", RelayState: "stored", CreatedAt: time.Now().UTC().Add(-time.Hour)}
	store.nextMsgID++
	store.messages[assistant.ID] = assistant
	store.order = append(store.order, assistant.ID)
	store.mu.Unlock()

	hub.drainChatOutbox(ctx)
	hub.chatMu.Lock()
	cursor := hub.chatSweepCursor
	hub.chatMu.Unlock()
	if cursor >= web.ID {
		t.Fatalf("cursor=%d advanced past the graced web row %d", cursor, web.ID)
	}

	// The grace expires: the web row fails and the assistant row behind it
	// becomes the consumed settled prefix.
	store.mu.Lock()
	store.messages[web.ID].CreatedAt = time.Now().UTC().Add(-2 * time.Hour)
	store.mu.Unlock()
	hub.drainChatOutbox(ctx)

	hub.chatMu.Lock()
	cursor = hub.chatSweepCursor
	hub.chatMu.Unlock()
	if got := store.messageState(web.ID); got != "failed" {
		t.Fatalf("graced-out web row state=%q, want failed", got)
	}
	if got := store.messageState(assistant.ID); got != "stored" {
		t.Fatalf("assistant row state=%q — it must stay stored (hk owns it)", got)
	}
	if cursor < assistant.ID {
		t.Fatalf("cursor=%d did not consume the settled prefix through %d", cursor, assistant.ID)
	}

	// The next sweep never re-reads the consumed rows.
	store.mu.Lock()
	before := len(store.listAfters)
	store.mu.Unlock()
	hub.drainChatOutbox(ctx)
	store.mu.Lock()
	afters := append([]int64{}, store.listAfters[before:]...)
	store.mu.Unlock()
	if len(afters) != 1 || afters[0] != cursor {
		t.Fatalf("sweep re-scanned below the cursor: afters=%v cursor=%d", afters, cursor)
	}
}

// answeredQuestion seeds a lane-a question whose assistant answer already
// holds the slot — the pending item the fold turns into a chain.
func answeredQuestion(id string, rev int, msgID int64) assistantChatQuestion {
	now := time.Now().UTC()
	return assistantChatQuestion{ID: id, ConversationID: "operator-desk", Lane: "lane-a", Body: "?",
		State: "pending", Revision: rev, AnswerMessageID: &msgID, CreatedAt: now, UpdatedAt: now}
}

func deliveredRelayRow(id int64, eventID, deliveredTo string) handoffkeepRelayEvent {
	now := time.Now().UTC()
	return handoffkeepRelayEvent{ID: id, Kind: "lane.event", OwnerLane: "lane-a", EventID: eventID,
		Attempts: 1, DeliveredAt: now.Format(time.RFC3339Nano), DeliveredTo: deliveredTo,
		ReceivedAt: now.Format(time.RFC3339)}
}

// R3-2: the answered-chain fold is min-rank — failed wins, then accepted,
// then in-progress, then done — and the winning item's reason lifts to the
// top-level receipts. Per-item verdicts carry receipts only: no answer
// body, no by, ever.
func TestAssistantFoldPrecedencePR3(t *testing.T) {
	chain := func(kind string) string {
		switch kind {
		case "done":
			return "delivered"
		case "accepted":
			return "unsent outbox"
		case "failed":
			return "sink stamp"
		case "in-progress":
			return "live relay"
		}
		return ""
	}
	cases := []struct {
		name       string
		chains     []string
		wantState  string
		wantReason string
	}{
		{"done and accepted folds to accepted", []string{"done", "accepted"}, "accepted", ""},
		{"failed beats accepted", []string{"failed", "accepted"}, "failed", "sink_lane"},
		{"done and done is delivered", []string{"done", "done"}, "delivered", ""},
		{"accepted beats in-progress", []string{"in-progress", "accepted"}, "accepted", ""},
		{"in-progress beats done", []string{"done", "in-progress"}, "in-progress", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hk := newFakeAssistantHK(t)
			pending := assistantPending{ServerTime: time.Now().UTC()}
			for i, kind := range tc.chains {
				qid := fmt.Sprintf("Q-20261111-%02d", 30+i)
				pending.ChatQuestions = append(pending.ChatQuestions, answeredQuestion(qid, 1, int64(700+i)))
				eventID := qid + "-rev1-answered"
				switch chain(kind) {
				case "delivered":
					hk.relays = append(hk.relays, deliveredRelayRow(int64(900+i), eventID, "host-a/w1:p1"))
				case "unsent outbox":
					hk.seedOutbox("chat_answer", "lane-a", eventID, "[via berry] "+qid+" answered: x")
				case "sink stamp":
					hk.relays = append(hk.relays, deliveredRelayRow(int64(900+i), eventID, "sink/sink:lane-a"))
				case "live relay":
					hk.relays = append(hk.relays, handoffkeepRelayEvent{ID: int64(900 + i), Kind: "lane.event",
						OwnerLane: "lane-a", EventID: eventID, Attempts: 1})
				}
			}
			hk.pending = pending
			srv, _ := assistantTestServer(t, hk, nil)
			result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
			if result.isError {
				t.Fatalf("progress: %s", result.text)
			}
			if result.decoded["state"] != tc.wantState {
				t.Fatalf("folded state=%v, want %s; receipts=%v", result.decoded["state"], tc.wantState, result.decoded["receipts"])
			}
			receipts, _ := result.decoded["receipts"].(map[string]any)
			if reason, _ := receipts["reason"].(string); reason != tc.wantReason {
				t.Fatalf("top-level reason=%q, want %q", reason, tc.wantReason)
			}
			verdicts, _ := receipts["answered"].([]any)
			if len(verdicts) != len(tc.chains) {
				t.Fatalf("verdicts=%v, want %d", verdicts, len(tc.chains))
			}
			// No verdict may carry the answer body or a by field — the fold
			// exposes chain state, never human-channel content.
			raw, _ := json.Marshal(verdicts)
			for _, banned := range []string{`"body"`, `"by"`, `"text"`, `"answer_text"`} {
				if strings.Contains(string(raw), banned) {
					t.Fatalf("verdict receipts carry %s: %s", banned, raw)
				}
			}
		})
	}
}

// R3-3: the one-megabyte read cap names its error — a body that fits is
// accepted, one byte over is hk_response_truncated, and a stream that dies
// mid-body is the same named error, never a partial payload.
func TestAssistantReadCapPR3(t *testing.T) {
	// Exactly at the cap is a valid answer.
	hk := newFakeAssistantHK(t)
	pad := strings.Repeat(" ", 1<<20-120)
	hk.bodyFor["/v1/assistant/pending"] = `{"server_time":"` + time.Now().UTC().Format(time.RFC3339Nano) +
		`","decision_requests":[],"chat_questions":[],"pad":"` + pad + `"}`
	if len(hk.bodyFor["/v1/assistant/pending"]) > 1<<20 {
		t.Fatalf("fixture oversize: %d", len(hk.bodyFor["/v1/assistant/pending"]))
	}
	srv, _ := assistantTestServer(t, hk, nil)
	if got := assistantCall(t, srv, assistantTestBerryToken, "pending_list", nil); got.isError {
		t.Fatalf("exact-cap body rejected: %s", got.text)
	}

	// One byte over is the named truncation error.
	hk = newFakeAssistantHK(t)
	hk.bodyFor["/v1/assistant/pending"] = `{"server_time":"` + time.Now().UTC().Format(time.RFC3339Nano) +
		`","decision_requests":[],"chat_questions":[],"pad":"` + strings.Repeat(" ", 1<<20) + `"}`
	srv, _ = assistantTestServer(t, hk, nil)
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil); got != "hk_response_truncated" {
		t.Fatalf("over-cap body: %q, want hk_response_truncated", got)
	}

	// A stream that ends mid-body — the client asked for 1 MiB + 1 and got
	// an unexpected EOF — is the same named error, not a decode attempt.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("no hijacker")
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		// Declare a full body then close after a few bytes.
		fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", 1<<20)
		_, _ = buf.WriteString(`{"server_time":"2026`)
		_ = buf.Flush()
		_ = conn.Close()
	}))
	t.Cleanup(dead.Close)
	hk = newFakeAssistantHK(t)
	dir := t.TempDir()
	cfg := assistantConfig{
		Listen: "127.0.0.1:0", HKURL: dead.URL, HKToken: assistantTestHKToken,
		TokenFile:   writeMode0600(t, dir, "tok", assistantTestBerryToken),
		TargetsFile: writeMode0600(t, dir, "tgt", assistantTestTargets), ClientName: "berry-test",
	}
	srv, err := newAssistantServer(cfg, assistantServerDeps{})
	if err != nil {
		t.Fatal(err)
	}
	if got := assistantCallError(t, srv, assistantTestBerryToken, "pending_list", nil); got != "hk_response_truncated" {
		t.Fatalf("mid-body cut: %q, want hk_response_truncated", got)
	}
}

// R3-4: one progress(target) call reads the unsent outbox exactly once no
// matter how many answered items fold — the per-item refetch is gone.
func TestAssistantOutboxOncePR3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	pending := assistantPending{ServerTime: time.Now().UTC()}
	for i := 0; i < 4; i++ {
		qid := fmt.Sprintf("Q-20261111-%02d", 50+i)
		pending.ChatQuestions = append(pending.ChatQuestions, answeredQuestion(qid, 1, int64(800+i)))
		hk.seedOutbox("chat_answer", "lane-a", qid+"-rev1-answered", "[via berry] "+qid+" answered: x")
	}
	hk.pending = pending
	srv, _ := assistantTestServer(t, hk, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.isError || result.decoded["state"] != "accepted" {
		t.Fatalf("fold: %+v", result)
	}
	reads := 0
	for _, call := range hk.requestLog() {
		if call == "GET /v1/assistant/outbox" {
			reads++
		}
	}
	if reads != 1 {
		t.Fatalf("outbox reads=%d, want exactly 1: %v", reads, hk.requestLog())
	}
}

// The fold's poll side: pending_list and poll both carry the pre-filter
// chat_questions_at_cap bit (R3-1 keeps it pre-filter on purpose — hk's
// oldest-1000 ordering means a post-filter flag would lie).
func TestAssistantPollAtCapPR3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	pending := assistantPending{ServerTime: time.Now().UTC()}
	for i := 0; i < assistantChatCap; i++ {
		pending.ChatQuestions = append(pending.ChatQuestions, assistantChatQuestion{
			ID: fmt.Sprintf("Q-20261111-%04d", i), ConversationID: "other-desk",
			Lane: "other-lane", State: "pending", Revision: 1})
	}
	hk.pending = pending
	srv, _ := assistantTestServer(t, hk, nil)
	for _, tool := range []string{"pending_list", "poll"} {
		result := assistantCall(t, srv, assistantTestBerryToken, tool, nil)
		if result.isError {
			t.Fatalf("%s: %s", tool, result.text)
		}
		if result.decoded["chat_questions_at_cap"] != true {
			t.Fatalf("%s at_cap=%v, want true", tool, result.decoded["chat_questions_at_cap"])
		}
	}
}

// The edited-question chain (hk F6): an answer written at rev2 keeps its
// slot while the question's revision moves to 3 — the notice matcher
// accepts Q-…-rev<n>-answered for any n, so progress still follows the one
// chain that exists instead of hunting a rev3 event that was never minted.
func TestAssistantEditedQuestionChainPR3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	msgID := int64(910)
	// The question now reads revision 3, slot taken by the rev2 answer.
	hk.seedQuestion(assistantChatQuestion{ID: "Q-20261111-60", ConversationID: "operator-desk", Lane: "lane-a",
		Body: "edited", State: "pending", Revision: 3, AnswerMessageID: &msgID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()})
	hk.pending = assistantPending{ServerTime: time.Now().UTC(), ChatQuestions: []assistantChatQuestion{
		hk.questions["Q-20261111-60"],
	}}
	srv, _ := assistantTestServer(t, hk, nil)

	// An unsent rev2 outbox row is the accepted receipt.
	hk.seedOutbox("chat_answer", "lane-a", "Q-20261111-60-rev2-answered", "[via berry] Q-20261111-60 answered: x")
	for _, args := range []map[string]any{
		{"request_id": "Q-20261111-60"},
		{"target": "ops"},
	} {
		result := assistantCall(t, srv, assistantTestBerryToken, "progress", args)
		if result.isError || result.decoded["state"] != "accepted" {
			t.Fatalf("edited-question progress %v: %+v", args, result)
		}
	}

	// Once the rev2 row delivered, the chain reports done — the current
	// revision's missing rev3 event id never stalls it.
	hk.mu.Lock()
	for i := range hk.outbox {
		if hk.outbox[i].EventID == "Q-20261111-60-rev2-answered" {
			now := time.Now().UTC()
			hubID := int64(4321)
			hk.outbox[i].SentAt = &now
			hk.outbox[i].HubRowID = &hubID
		}
	}
	hk.relays = append(hk.relays, deliveredRelayRow(950, "Q-20261111-60-rev2-answered", "host-a/w1:p1"))
	hk.mu.Unlock()
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"request_id": "Q-20261111-60"})
	if result.isError || result.decoded["state"] != "done" {
		t.Fatalf("delivered edited-question chain: %+v", result)
	}
}

// tasksForLane pages by after_id: a lane with more live tasks than one page
// can hold is still walked, and a lane past the scan bound reports
// view_truncated rather than a quiet lane it cannot prove.
func TestAssistantTasksPagingPR3(t *testing.T) {
	hk := newFakeAssistantHK(t)
	// One page plus one — every task is terminal except the very last, so
	// the walk must reach the second page to find live work.
	for i := int64(1); i <= assistantTasksPageLimit; i++ {
		hk.seedTask(assistantTask{ID: i, Lane: "lane-a", State: "merged"})
	}
	hk.seedTask(assistantTask{ID: assistantTasksPageLimit + 1, Lane: "lane-a", State: "queued"})
	srv, _ := assistantTestServer(t, hk, nil)
	result := assistantCall(t, srv, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.isError {
		t.Fatalf("progress: %s", result.text)
	}
	if result.decoded["state"] != "in-progress" {
		t.Fatalf("paged lane state=%v, want in-progress", result.decoded["state"])
	}
	taskReads := 0
	for _, call := range hk.requestLog() {
		if call == "GET /v1/tasks" {
			taskReads++
		}
	}
	if taskReads != 2 {
		t.Fatalf("task reads=%d, want 2 pages: %v", taskReads, hk.requestLog())
	}
	receipts, _ := result.decoded["receipts"].(map[string]any)
	tasks, _ := receipts["tasks"].([]any)
	if len(tasks) != 1 {
		t.Fatalf("live tasks=%v, want the one queued row", tasks)
	}

	// A lane past the scan bound is honest: full pages to the cap means an
	// unseen tail, which reports view_truncated — never done.
	hk2 := newFakeAssistantHK(t)
	for i := int64(1); i <= assistantTasksPageLimit*assistantTasksScanPages; i++ {
		hk2.seedTask(assistantTask{ID: i, Lane: "lane-a", State: "merged"})
	}
	srv2, _ := assistantTestServer(t, hk2, nil)
	result = assistantCall(t, srv2, assistantTestBerryToken, "progress", map[string]any{"target": "ops"})
	if result.isError || result.decoded["state"] != "in-progress" {
		t.Fatalf("scan-bound lane: %+v", result)
	}
	receipts, _ = result.decoded["receipts"].(map[string]any)
	if receipts["reason"] != "view_truncated" {
		t.Fatalf("scan-bound reason=%v, want view_truncated", receipts["reason"])
	}
}
