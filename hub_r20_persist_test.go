package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHandoffkeep records every request in arrival order so a test can assert
// the persist/inject/deliver sequence rather than only its effects.
type fakeHandoffkeep struct {
	mu     sync.Mutex
	calls  []fakeHandoffkeepCall
	nextID int64
	status int
	stored []handoffkeepRelayEvent
	// rows models handoffkeep's idempotency index, keyed by the documented
	// five fields. A POST that collides updates only attempts and replies 200.
	rows       map[string]*handoffkeepRelayEvent
	ownerLane  string
	undelivere []handoffkeepRelayEvent
	// observe runs at the start of every request, before any reply, so a test
	// can inspect hub state at the exact moment handoffkeep is called.
	observe func(method, path string)
}

type fakeHandoffkeepCall struct {
	Method string
	Path   string
	Query  string
	Body   map[string]any
}

func newFakeHandoffkeep(t *testing.T) (*fakeHandoffkeep, *handoffkeepRelayClient, func()) {
	t.Helper()
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	server := httptest.NewServer(fake)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return fake, client, server.Close
}

func (f *fakeHandoffkeep) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{}
	if raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20)); len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	f.mu.Lock()
	f.calls = append(f.calls, fakeHandoffkeepCall{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Body: body})
	status := f.status
	observe := f.observe
	f.mu.Unlock()
	if observe != nil {
		observe(r.Method, r.URL.Path)
	}
	switch {
	case r.Method == http.MethodGet:
		f.mu.Lock()
		pending := append([]handoffkeepRelayEvent(nil), f.undelivere...)
		if len(pending) == 0 {
			for _, row := range f.rows {
				if row.DeliveredAt == "" {
					pending = append(pending, *row)
				}
			}
		}
		query := r.URL.Query()
		kind := query.Get("kind")
		lane := query.Get("lane")
		afterID, _ := strconv.ParseInt(query.Get("after_id"), 10, 64)
		limit, _ := strconv.Atoi(query.Get("limit"))
		if limit <= 0 {
			limit = handoffkeepReplayLimit
		}
		filtered := pending[:0]
		for _, record := range pending {
			if (kind != "" && record.Kind != kind) || (lane != "" && record.OwnerLane != lane) || record.ID <= afterID {
				continue
			}
			filtered = append(filtered, record)
		}
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })
		if len(filtered) > limit {
			filtered = filtered[:limit]
		}
		pending = filtered
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"events": pending})
	case strings.HasSuffix(r.URL.Path, "/delivered"):
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	default:
		if status != http.StatusCreated && status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":"boom"}`)
			return
		}
		f.mu.Lock()
		key := fakeHandoffkeepBodyKey(body)
		row, duplicate := f.rows[key]
		if duplicate {
			// The documented contract: a duplicate POST updates only attempts
			// and returns the original row with 200.
			row.Attempts++
			status = http.StatusOK
		} else {
			f.nextID++
			row = &handoffkeepRelayEvent{ID: f.nextID, Kind: asString(body["kind"]), JobID: asString(body["job_id"]),
				Epoch: asInt(body["epoch"]), OwnerLane: asString(body["owner_lane"]), ReportPath: asString(body["report_path"]),
				Reason: asString(body["reason"]), EventID: asString(body["event_id"]), Text: asString(body["text"]), Attempts: 1}
			if f.ownerLane != "" {
				row.OwnerLane = f.ownerLane
			}
			f.rows[key] = row
			f.stored = append(f.stored, *row)
		}
		stored := *row
		f.mu.Unlock()
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(stored)
	}
}

// fakeHandoffkeepRowKey is handoffkeep's idempotency key, in the field order
// the node outbox and the hub dedupe key both use.
func fakeHandoffkeepRowKey(kind, jobID string, epoch int, reportPath, reason string) string {
	return kind + "\x00" + jobID + "\x00" + strconv.Itoa(epoch) + "\x00" + reportPath + "\x00" + reason
}

func fakeHandoffkeepLaneEventKey(lane, eventID string) string {
	return "lane.event\x00" + lane + "\x00" + eventID
}

func fakeHandoffkeepBodyKey(body map[string]any) string {
	if asString(body["kind"]) == "lane.event" {
		return fakeHandoffkeepLaneEventKey(asString(body["owner_lane"]), asString(body["event_id"]))
	}
	return fakeHandoffkeepRowKey(asString(body["kind"]), asString(body["job_id"]), asInt(body["epoch"]), asString(body["report_path"]), asString(body["reason"]))
}

func fakeHandoffkeepStoredKey(row handoffkeepRelayEvent) string {
	if row.Kind == "lane.event" {
		return fakeHandoffkeepLaneEventKey(row.OwnerLane, row.EventID)
	}
	return fakeHandoffkeepRowKey(row.Kind, row.JobID, row.Epoch, row.ReportPath, row.Reason)
}

// seedUndelivered puts rows handoffkeep already holds behind both the
// undelivered listing and the idempotency index, so a hub that re-POSTs one of
// them gets 200 with attempts+1 rather than a new row.
func (f *fakeHandoffkeep) seedUndelivered(records ...handoffkeepRelayEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, record := range records {
		row := record
		f.rows[fakeHandoffkeepStoredKey(row)] = &row
		f.undelivere = append(f.undelivere, row)
		if row.ID > f.nextID {
			f.nextID = row.ID
		}
	}
}

// attemptsFor reports the durable attempt counter for one row.
func (f *fakeHandoffkeep) attemptsFor(kind, jobID string, epoch int, reportPath, reason string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, exists := f.rows[fakeHandoffkeepRowKey(kind, jobID, epoch, reportPath, reason)]
	if !exists {
		return -1
	}
	return row.Attempts
}

func (f *fakeHandoffkeep) laneAttemptsFor(lane, eventID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, exists := f.rows[fakeHandoffkeepLaneEventKey(lane, eventID)]
	if !exists {
		return -1
	}
	return row.Attempts
}

func (f *fakeHandoffkeep) queries(path string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for _, call := range f.calls {
		if call.Method == http.MethodGet && call.Path == path {
			out = append(out, call.Query)
		}
	}
	return out
}

// rowCount is how many distinct rows handoffkeep holds. A resend or an attempt
// bump must never change it.
func (f *fakeHandoffkeep) rowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}

func asInt(value any) int {
	number, _ := value.(float64)
	return int(number)
}

func (f *fakeHandoffkeep) sequence() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, call := range f.calls {
		out = append(out, call.Method+" "+call.Path)
	}
	return out
}

func (f *fakeHandoffkeep) count(method, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, call := range f.calls {
		if call.Method == method && call.Path == path {
			total++
		}
	}
	return total
}

func r20LanesFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func r20Hub(t *testing.T, lanes string, handoffkeep *handoffkeepRelayClient, logs io.Writer) *HubServer {
	t.Helper()
	logger := slog.Default()
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath: r20LanesFile(t, lanes),
		Logger:          logger,
		handoffkeep:     handoffkeep,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

// TE1 walks the whole cursor: emit file, node socket push, hub receipt,
// persistence, injection, node acknowledgement, delivery marking.
func TestR20EmitToDeliveredCursorInOrder(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}

	// A worker emits: the file lands first, then the daemon queue carries it.
	inbox := t.TempDir()
	if code := runEmitCLI([]string{"--kind", "job.completed", "--job", "r20-cursor", "--report", "report.md", "--owner-lane", "lane-a", "--label", "wrk-a", "--host", "host-a", "--report-last-line", "VERDICT: DONE", "--inbox-root", inbox}, io.Discard, io.Discard, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("emit code=%d", code)
	}
	node := &HubClient{jobsInboxRoot: inbox, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 4)}
	queued := node.jobCompletionEvents()
	if len(queued) != 1 {
		t.Fatalf("node produced %d relay events", len(queued))
	}
	completion, ok := decodeHubJobCompletionPayload(queued[0].Payload)
	if !ok {
		t.Fatalf("payload=%s", queued[0].Payload)
	}

	// The relay path is synchronous, so an injection queued before the POST is
	// visible on the agent channel at the moment handoffkeep is called.
	injectedBeforePersist := -1
	fake.mu.Lock()
	fake.observe = func(method, path string) {
		if method == http.MethodPost && path == "/v1/relay/events" {
			injectedBeforePersist = len(agent.relays)
		}
	}
	fake.mu.Unlock()

	hub.relayJobCompletion(completion)

	if injectedBeforePersist != 0 {
		t.Fatalf("relay.inject queued before POST /v1/relay/events (relays pending at persist time = %d, want 0)", injectedBeforePersist)
	}
	select {
	case directive := <-agent.relays:
		if directive.Pane != "w1:p1" {
			t.Fatalf("directive=%+v", directive)
		}
	case <-time.After(time.Second):
		t.Fatal("relay was not injected")
	}
	select {
	case persisted := <-agent.persisted:
		if persisted.EventID == 0 || persisted.Kind != "job.completed" {
			t.Fatalf("persisted=%+v", persisted)
		}
	case <-time.After(time.Second):
		t.Fatal("node was not told the record is persisted")
	}
	acknowledged := hub.acknowledgeRelayPendingForTest(t, "host-a", relayAckPayload{JobID: "r20-cursor", Pane: "w1:p1"})
	hub.markRelayEventDelivered(acknowledged)

	got := fake.sequence()
	want := []string{"POST /v1/relay/events", "POST /v1/relay/events/101/delivered"}
	if len(got) != len(want) {
		t.Fatalf("handoffkeep calls=%v want=%v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("handoffkeep call %d = %q, want %q (full sequence %v)", index, got[index], want[index], got)
		}
	}
	// The POST must precede the injection, not merely accompany it.
	fake.mu.Lock()
	body := fake.calls[0].Body
	fake.mu.Unlock()
	if body["owner_lane"] != "lane-a" || body["machine"] != "host-a" || body["pane_id"] != "w1:p1" {
		t.Fatalf("persisted body=%v", body)
	}
}

func (h *HubServer) acknowledgeRelayPendingForTest(t *testing.T, machine string, ack relayAckPayload) relayPending {
	t.Helper()
	pending, ok := h.acknowledgeRelayPending(machine, ack)
	if !ok {
		t.Fatal("relay acknowledgement was not matched to a pending injection")
	}
	return pending
}

// TE2: a hub that could not persist must not inject.
func TestR20PersistFailureSkipsInject(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.status = http.StatusInternalServerError
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 4)}
	hub.subscribers[subscriber] = struct{}{}

	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20-fail", Epoch: 1, OwnerLane: "lane-a", Label: "wrk-a", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done"})

	select {
	case directive := <-agent.relays:
		t.Fatalf("unpersisted relay was injected anyway: %+v", directive)
	default:
	}
	unpersisted := 0
	for len(subscriber.messages) > 0 {
		message := <-subscriber.messages
		if message.event != nil && message.event.Kind == "relay.unpersisted" {
			unpersisted++
			var payload struct {
				JobID  string `json:"job_id"`
				Reason string `json:"reason"`
			}
			if json.Unmarshal(message.event.Payload, &payload) != nil || payload.JobID != "r20-fail" || payload.Reason != "persist_failed" {
				t.Fatalf("payload=%s", message.event.Payload)
			}
		}
	}
	if unpersisted != 1 {
		t.Fatalf("relay.unpersisted broadcasts=%d, want 1", unpersisted)
	}
	if hub.UnpersistedRelayEventCount() != 1 {
		t.Fatalf("counter=%d", hub.UnpersistedRelayEventCount())
	}
	if fake.count(http.MethodPost, "/v1/relay/events") != 1 {
		t.Fatalf("persist attempts=%d", fake.count(http.MethodPost, "/v1/relay/events"))
	}
	// The record stays resendable: the node still owns it.
	if _, blocked := hub.relayDedupe[relayEventDedupeKey("job.completed", hubJobEventPayload{JobID: "r20-fail", Epoch: 1, ReportPath: "report.md"})]; blocked {
		t.Fatal("a failed persist left the record blocked by the hub dedupe")
	}
}

// TE3: a restarted hub re-injects what Postgres still holds as undelivered.
func TestR20StartupReinjectsUndelivered(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.seedUndelivered(
		handoffkeepRelayEvent{ID: 7, Kind: "job.completed", JobID: "r20-one", Epoch: 1, OwnerLane: "lane-a", ReportPath: "one.md", ReportLastLine: "done one"},
		handoffkeepRelayEvent{ID: 9, Kind: "job.completed", JobID: "r20-two", Epoch: 1, OwnerLane: "lane-b", ReportPath: "two.md", ReportLastLine: "done two"},
	)
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-a","pane":"w1:p2"}}}`, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}

	hub.replayUndeliveredRelayEvents(context.Background())

	panes := map[string]string{}
	for i := 0; i < 2; i++ {
		select {
		case directive := <-agent.relays:
			panes[directive.JobID] = directive.Pane
		case <-time.After(time.Second):
			t.Fatalf("only %d undelivered events were re-injected", i)
		}
	}
	if panes["r20-one"] != "w1:p1" || panes["r20-two"] != "w1:p2" {
		t.Fatalf("replayed panes=%v", panes)
	}
	select {
	case directive := <-agent.relays:
		t.Fatalf("a delivered event was re-injected: %+v", directive)
	default:
	}
	// Startup replay must never re-persist a row that already exists. R20T5
	// re-POSTs each replayed row, but only as the attempt bump the contract
	// has no other endpoint for: the row count is what "not re-persisted"
	// means, and it must not move.
	if fake.rowCount() != 2 {
		t.Fatalf("startup replay created rows: handoffkeep holds %d, want the 2 it started with", fake.rowCount())
	}
	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 2 {
		t.Fatalf("startup replay POSTed %d times, want one attempt bump per replayed row", posts)
	}
	for _, row := range []struct {
		job  string
		path string
	}{{"r20-one", "one.md"}, {"r20-two", "two.md"}} {
		if got := fake.attemptsFor("job.completed", row.job, 1, row.path, ""); got != 1 {
			t.Fatalf("%s attempts=%d after replay, want 1", row.job, got)
		}
	}
}

func TestR20StartupReplayAdvancesDurableCursor(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	seed := make([]handoffkeepRelayEvent, 0, handoffkeepReplayLimit+1)
	for id := 1; id <= handoffkeepReplayLimit+1; id++ {
		seed = append(seed, handoffkeepRelayEvent{ID: int64(id), Kind: "job.completed", JobID: "cursor-job-" + strconv.Itoa(id), Epoch: 1, OwnerLane: "lane-a", ReportPath: "report.md"})
	}
	fake.seedUndelivered(seed...)
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	hub.r19a.relayAckTimeout = time.Hour
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, handoffkeepReplayLimit+2), persisted: make(chan hubRelayPersistedEvent, handoffkeepReplayLimit+2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.replayUndeliveredRelayEvents(context.Background())
	if injections := drainRelays(agent); injections != handoffkeepReplayLimit+1 {
		t.Fatalf("startup replay injections=%d", injections)
	}
	queries := fake.queries("/v1/relay/events")
	if len(queries) != 2 {
		t.Fatalf("startup replay pages=%d queries=%v", len(queries), queries)
	}
	first, err := url.ParseQuery(queries[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := url.ParseQuery(queries[1])
	if err != nil {
		t.Fatal(err)
	}
	if first.Get("after_id") != "" || second.Get("after_id") != strconv.Itoa(handoffkeepReplayLimit) {
		t.Fatalf("startup cursor did not advance: first=%q second=%q", queries[0], queries[1])
	}
}

// TE9: without --handoffkeep-env the hub keeps its pre-R20 behavior exactly.
func TestR20WithoutHandoffkeepEnvNoCallsAndStillInjects(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	_ = client
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20-compat", Epoch: 1, OwnerLane: "lane-a", Label: "wrk-a", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done"})
	select {
	case <-agent.relays:
	case <-time.After(time.Second):
		t.Fatal("a hub without handoffkeep stopped injecting")
	}
	select {
	case <-agent.persisted:
		t.Fatal("a hub without handoffkeep sent relay.persisted")
	default:
	}
	hub.replayUndeliveredRelayEvents(context.Background())
	if len(fake.sequence()) != 0 {
		t.Fatalf("handoffkeep was called %v times without the flag", fake.sequence())
	}
}

// AC24: the idempotency key excludes owner_lane, so a 200 can name another
// lane. That is one warning line, never a routing change.
func TestR20OwnerLaneMismatchOnDuplicateWarnsOnce(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.status = http.StatusOK
	fake.ownerLane = "lane-b"
	var logs bytes.Buffer
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-a","pane":"w1:p2"}}}`, client, &logs)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}

	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20-lane", Epoch: 1, OwnerLane: "lane-a", Label: "wrk-a", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done"})

	select {
	case directive := <-agent.relays:
		if directive.Pane != "w1:p1" {
			t.Fatalf("the stored owner_lane changed routing: %+v", directive)
		}
	case <-time.After(time.Second):
		t.Fatal("relay was not injected")
	}
	if got := strings.Count(logs.String(), "persisted relay event reports a different owner lane"); got != 1 {
		t.Fatalf("owner lane mismatch warnings=%d\n%s", got, logs.String())
	}
}

// AC8: the flag reads a mode-0600 file and no error may echo a token.
func TestR20HandoffkeepEnvIsMode0600AndErrorsCarryNoToken(t *testing.T) {
	const secret = "hk-secret-value-do-not-log"
	dir := t.TempDir()
	loose := filepath.Join(dir, "loose.env")
	if err := os.WriteFile(loose, []byte("HANDOFFKEEP_URL=https://example.invalid\nHANDOFFKEEP_TOKEN="+secret+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubHandoffkeepEnv(loose); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("mode-0644 env err=%v", err)
	}
	partial := filepath.Join(dir, "partial.env")
	if err := os.WriteFile(partial, []byte("HANDOFFKEEP_TOKEN="+secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubHandoffkeepEnv(partial); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("missing URL err=%v", err)
	}
	good := filepath.Join(dir, "good.env")
	if err := os.WriteFile(good, []byte("HANDOFFKEEP_URL=https://example.invalid\nHANDOFFKEEP_TOKEN="+secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := loadHubHandoffkeepEnv(good)
	if err != nil || env.Token != secret {
		t.Fatalf("env=%+v err=%v", hubHandoffkeepEnv{URL: env.URL}, err)
	}
	authFile := filepath.Join(dir, "auth.env")
	if err := os.WriteFile(authFile, []byte("HUB_TOKEN_operator=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authFile, "--handoffkeep-env", good}, slog.Default())
	if hub == nil || code != ExitOK || err != nil || hub.handoffkeep == nil {
		t.Fatalf("hub=%v code=%d err=%v", hub != nil, code, err)
	}
	if _, _, code, err := newHubServerForCLI([]string{"--hub-auth", authFile, "--handoffkeep-env", loose}, slog.Default()); code != ExitConditionInvalid || err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("code=%d err=%v", code, err)
	}
}
