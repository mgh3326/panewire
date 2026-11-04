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
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	ackStateOperatorToken = "ack-state-fixture-operator-token"
	ackStateNodeAToken    = "ack-state-fixture-node-a-token"
	ackStateNodeBToken    = "ack-state-fixture-node-b-token"
	ackStateServiceToken  = "ack-state-fixture-svc-ack-token"
	ackStateOtherToken    = "ack-state-fixture-svc-read-token"
)

// ackStateHub is a hub whose inbox holds events relayed by two nodes, with the
// scoped services the lookup distinguishes.
func ackStateHub(t *testing.T, storage hubInboxStorage) (*HubServer, *httptest.Server, *time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 4, 3, 0, 0, 0, time.UTC)
	h, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": ackStateOperatorToken, "node-a": ackStateNodeAToken, "node-b": ackStateNodeBToken,
			"svc-ack": ackStateServiceToken, "svc-read": ackStateOtherToken,
		},
		Scopes:             map[string]string{"svc-ack": "inbox.ack_state", "svc-read": "inbox.read:lane-a,inbox.ack:lane-a"},
		ReportRelayPath:    r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"w1:p1"},"lane-b":{"machine":"node-b","pane":"w2:p1"}}}`),
		DeliveryPolicyPath: "default", InboxStorePath: storage.path, InboxDatabaseURL: storage.dsn,
		Now:    func() time.Time { return now },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	// Each lane's pane reports as busy, so inbox-class events are held in the
	// inbox instead of being injected at once.
	for node, pane := range map[string]string{"node-a": "w1:p1", "node-b": "w2:p1"} {
		snapshot := &hubSessionSnapshotRecord{sessions: []HubSession{{PaneID: pane, Status: "working", Harness: "claude"}}, snapshotStatus: hubSnapshotStatusOK, receivedAt: now}
		h.nodes[node] = &hubNodeRecord{machineID: node, state: "connected", sessionSnapshot: snapshot}
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	return h, server, &now
}

// relayFrom relays an inbox-class lane.event over a node's socket identity.
func relayFrom(t *testing.T, h *HubServer, node, lane, eventID string) {
	t.Helper()
	agent := &hubAgent{machineID: node, relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: "FYI secret body of " + eventID}
	if result := h.relayLaneEvent(event, agent); result.ID == 0 || result.PersistFailed {
		t.Fatalf("relay %s from %s: %+v", eventID, node, result)
	}
}

func ackStateGet(t *testing.T, server *httptest.Server, caller pw2Caller, eventIDs ...string) (int, string) {
	t.Helper()
	query := url.Values{}
	for _, id := range eventIDs {
		query.Add("event_id", id)
	}
	return caller.do(t, server, http.MethodGet, "/v1/inbox/ack-state?"+query.Encode(), "")
}

type ackStateAnswer struct {
	Events   []map[string]any `json:"events"`
	NotFound []string         `json:"not_found"`
}

func decodeAckState(t *testing.T, body string) ackStateAnswer {
	t.Helper()
	var answer ackStateAnswer
	if err := json.Unmarshal([]byte(body), &answer); err != nil {
		t.Fatalf("answer %q: %v", body, err)
	}
	return answer
}

func TestInboxAckStateAuthz(t *testing.T) {
	forEachHubInboxStorage(t, testInboxAckStateAuthz)
}

func testInboxAckStateAuthz(t *testing.T, storage hubInboxStorage) {
	h, server, now := ackStateHub(t, storage)
	relayFrom(t, h, "node-a", "lane-a", "ev-a")
	relayFrom(t, h, "node-b", "lane-a", "ev-b")
	relayFrom(t, h, "node-a", "lane-b", "ev-a2")
	operator := pw2Caller{"operator", "", ackStateOperatorToken}
	nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
	nodeB := pw2Caller{"node-b", "node-b", ackStateNodeBToken}
	service := pw2Caller{"svc-ack", "svc-ack", ackStateServiceToken}
	lacking := pw2Caller{"svc-read", "svc-read", ackStateOtherToken}

	// Ack ev-a on the hub exactly as its receiver would.
	items, err := h.inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Event.EventID == "ev-a" {
			if n, err := h.inbox.store.ack(item.ID, "lane-a", false, *now); err != nil || n != 1 {
				t.Fatalf("ack: %d %v", n, err)
			}
		}
	}

	// The sender node sees its own event, acked, with the ack time.
	status, body := ackStateGet(t, server, nodeA, "ev-a")
	answer := decodeAckState(t, body)
	if status != http.StatusOK || len(answer.Events) != 1 || answer.Events[0]["event_id"] != "ev-a" || answer.Events[0]["acked"] != true || answer.Events[0]["acked_at"] != "2026-11-04T03:00:00Z" {
		t.Fatalf("sender node: %d %s", status, body)
	}
	// An unacked event is reported unacked, with no ack time.
	status, body = ackStateGet(t, server, nodeA, "ev-a2")
	answer = decodeAckState(t, body)
	if status != http.StatusOK || len(answer.Events) != 1 || answer.Events[0]["acked"] != false || answer.Events[0]["acked_at"] != nil {
		t.Fatalf("unacked event: %d %s", status, body)
	}

	// Another node, or a node asking for an unknown id, gets 401 and nothing.
	for _, tc := range []struct {
		caller pw2Caller
		ids    []string
	}{{nodeB, []string{"ev-a"}}, {nodeA, []string{"ev-b"}}, {nodeA, []string{"no-such-event"}}, {nodeA, []string{"ev-b", "no-such-event"}}} {
		status, body = ackStateGet(t, server, tc.caller, tc.ids...)
		if status != http.StatusUnauthorized || strings.Contains(body, "ev-") {
			t.Fatalf("%s %v: %d %s", tc.caller.name, tc.ids, status, body)
		}
	}
	// A batch answers for the ids a node may see; the rest look unknown.
	status, body = ackStateGet(t, server, nodeA, "ev-a", "ev-b", "ev-a2")
	answer = decodeAckState(t, body)
	if status != http.StatusOK || len(answer.Events) != 2 || !slices.Equal(answer.NotFound, []string{"ev-b"}) {
		t.Fatalf("node batch: %d %s", status, body)
	}

	// A service needs inbox.ack_state: a lane scope, or none, is a 403.
	for _, caller := range []pw2Caller{lacking} {
		if status, body = ackStateGet(t, server, caller, "ev-a"); status != http.StatusForbidden || !strings.Contains(body, "scope_forbidden") {
			t.Fatalf("%s: %d %s", caller.name, status, body)
		}
	}
	status, body = ackStateGet(t, server, service, "ev-a", "ev-b", "ev-a2", "ev-a")
	answer = decodeAckState(t, body)
	if status != http.StatusOK || len(answer.Events) != 3 || len(answer.NotFound) != 0 {
		t.Fatalf("service batch with a repeated id: %d %s", status, body)
	}
	if got := []any{answer.Events[0]["event_id"], answer.Events[1]["event_id"], answer.Events[2]["event_id"]}; !slices.Equal(got, []any{"ev-a", "ev-b", "ev-a2"}) {
		t.Fatalf("answers not in request order: %v", got)
	}
	// The operator is unrestricted. An unknown id is not found; with a known
	// one beside it the batch still answers.
	status, body = ackStateGet(t, server, operator, "ev-b", "no-such-event")
	answer = decodeAckState(t, body)
	if status != http.StatusOK || len(answer.Events) != 1 || !slices.Equal(answer.NotFound, []string{"no-such-event"}) {
		t.Fatalf("operator mixed batch: %d %s", status, body)
	}
	for _, caller := range []pw2Caller{operator, service} {
		status, body = ackStateGet(t, server, caller, "no-such-event")
		answer = decodeAckState(t, body)
		if status != http.StatusNotFound || len(answer.Events) != 0 || !slices.Equal(answer.NotFound, []string{"no-such-event"}) {
			t.Fatalf("%s unknown id: %d %s", caller.name, status, body)
		}
	}

	// Unauthenticated callers and a service with a wrong token are 401.
	for _, caller := range []pw2Caller{{"anonymous", "", ""}, {"wrong-token", "svc-ack", ackStateNodeAToken}, {"unknown-node", "node-z", ackStateNodeAToken}} {
		if status, _ = ackStateGet(t, server, caller, "ev-a"); status != http.StatusUnauthorized {
			t.Fatalf("%s: %d", caller.name, status)
		}
	}

	// No row content leaks: only the three fields, nothing of the body or lane.
	for _, caller := range []pw2Caller{operator, service, nodeA} {
		_, body = ackStateGet(t, server, caller, "ev-a", "ev-a2")
		for _, leaked := range []string{"secret", "FYI", "lane", "payload", "text", "sender", "node-a", "kind"} {
			if strings.Contains(body, leaked) {
				t.Fatalf("%s answer leaks %q: %s", caller.name, leaked, body)
			}
		}
		for _, event := range decodeAckState(t, body).Events {
			for key := range event {
				if key != "event_id" && key != "acked" && key != "acked_at" {
					t.Fatalf("%s answer carries %q: %s", caller.name, key, body)
				}
			}
		}
	}

	// Malformed requests: no id, too many, an id the inbox can never hold.
	if status, _ = operator.do(t, server, http.MethodGet, "/v1/inbox/ack-state", ""); status != http.StatusBadRequest {
		t.Fatalf("no ids: %d", status)
	}
	many := make([]string, hubInboxAckStateMaxIDs+1)
	for i := range many {
		many[i] = fmt.Sprintf("ev-%d", i)
	}
	if status, _ = ackStateGet(t, server, operator, many...); status != http.StatusBadRequest {
		t.Fatalf("too many ids: %d", status)
	}
	if status, _ = ackStateGet(t, server, operator, "bad\x01id"); status != http.StatusBadRequest {
		t.Fatalf("control character id: %d", status)
	}
	if status, _ = ackStateGet(t, server, operator, ""); status != http.StatusBadRequest {
		t.Fatalf("empty id: %d", status)
	}
}

// One event id relayed to two lanes is acked only once both rows are.
func TestInboxAckStateEventOnSeveralLanes(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		first, err := s.putFrom("node-a", "k-a", "lane-a", "lane.event", "inbox", quietEvent("shared", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		second, err := s.putFrom("node-a", "k-b", "lane-b", "lane.event", "inbox", quietEvent("shared", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		state := func() laneInboxAckState {
			states, err := s.ackStates([]string{"shared"}, "node-a")
			if err != nil {
				t.Fatal(err)
			}
			return states["shared"]
		}
		if got := state(); got.Acked || got.AckedAt != "" {
			t.Fatalf("nothing acked: %+v", got)
		}
		if _, err := s.ack(first.ID, "", false, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := state(); got.Acked {
			t.Fatalf("one of two rows acked: %+v", got)
		}
		if _, err := s.ack(second.ID, "", false, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := state(); !got.Acked || got.AckedAt != now.Add(2*time.Second).Format(time.RFC3339) {
			t.Fatalf("both rows acked: %+v", got)
		}
		// Another sender sees neither row; a retry from it does not take the row over.
		if _, err := s.putFrom("node-b", "k-a", "lane-a", "lane.event", "inbox", quietEvent("shared", "FYI"), now, 0, 0); err != nil {
			t.Fatal(err)
		}
		if states, err := s.ackStates([]string{"shared"}, "node-b"); err != nil || len(states) != 0 {
			t.Fatalf("other sender: %+v %v", states, err)
		}
	})
}

// A store from before the columns gains them, event_id read back from the
// payload; the old row has no sender, so no node sees it.
func TestLaneInboxAckStateMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`DROP TABLE lane_inbox_event_id_backfill`, `DROP INDEX lane_inbox_event_id`, `ALTER TABLE lane_inbox DROP COLUMN event_id`, `ALTER TABLE lane_inbox DROP COLUMN sender`,
		`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,acked_at) VALUES('old','lane-a','lane.event','inbox','{"job_id":"j","event_id":"old-ev"}',1,5)`} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	s.db.Close()
	s, err = openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	if states, err := s.ackStates([]string{"old-ev"}, ""); err != nil || !states["old-ev"].Acked {
		t.Fatalf("operator view of a pre-column row: %+v %v", states, err)
	}
	if states, err := s.ackStates([]string{"old-ev"}, "node-a"); err != nil || len(states) != 0 {
		t.Fatalf("node view of a pre-column row: %+v %v", states, err)
	}
}

// The backfill commits with its marker: a hub that stopped before the marker
// was written backfills on the next open, and one that finished never rescans.
func TestLaneInboxAckStateBackfillIsOnceAndResumable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at) VALUES('old','lane-a','lane.event','inbox','{"job_id":"j","event_id":"old-ev"}',1),('none','lane-a','lane.event','inbox','{"job_id":"j2"}',1)`); err != nil {
		t.Fatal(err)
	}
	reopen := func() {
		s.db.Close()
		var err error
		if s, err = openLaneInboxStore(path); err != nil {
			t.Fatal(err)
		}
	}
	// The first open already wrote the marker, so these rows are not rescanned.
	reopen()
	if states, err := s.ackStates([]string{"old-ev"}, ""); err != nil || len(states) != 0 {
		t.Fatalf("finished backfill rescanned the table: %+v %v", states, err)
	}
	// Without the marker (a hub stopped before commit) the next open fills them.
	if _, err := s.db.Exec(`DELETE FROM lane_inbox_event_id_backfill`); err != nil {
		t.Fatal(err)
	}
	reopen()
	defer s.db.Close()
	if states, err := s.ackStates([]string{"old-ev"}, ""); err != nil || len(states) != 1 {
		t.Fatalf("row left unfilled after reopen: %+v %v", states, err)
	}
}

// Migration 0005 fills event_id for rows stored before the column. It runs on
// Postgres only (CI): the UPDATE is replayed over rows left empty, as a store
// from before the migration would hold them.
func TestPostgresAckStateMigrationBackfillsExistingRows(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	store, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	rows := map[string]struct{ payload, want string }{
		"plain":    {`{"job_id":"j","event_id":"ev-plain"}`, "ev-plain"},
		"indented": {"  \n{\"job_id\":\"j\",\"event_id\":\"ev-indented\"}", "ev-indented"},
		"nul-body": {`{"text":"a\u0000b","event_id":"ev-nul-body"}`, "ev-nul-body"},
		"nul-id":   {`{"event_id":"a\u0000b"}`, ""},
		"no-id":    {`{"job_id":"j"}`, ""},
		"array":    {`[1]`, ""},
		"scalar":   {`"str"`, ""},
		"not-json": {`nonsense`, ""},
	}
	for key, row := range rows {
		if _, err := store.db.Exec(`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at) VALUES($1,'lane-a','lane.event','inbox',$2,1)`, key, row.payload); err != nil {
			t.Fatal(key, err)
		}
	}
	migration, err := hubPostgresMigrations.ReadFile("hub_postgres_migrations/0005_inbox_ack_state.sql")
	if err != nil {
		t.Fatal(err)
	}
	var update string
	for _, line := range strings.Split(string(migration), "\n") {
		if strings.HasPrefix(line, "UPDATE lane_inbox") {
			update = line
		}
	}
	if update == "" {
		t.Fatal("0005 has no backfill UPDATE")
	}
	if _, err := store.db.Exec(update); err != nil {
		t.Fatalf("backfill aborted on a legacy row: %v", err)
	}
	for key, row := range rows {
		var got string
		if err := store.db.QueryRow(`SELECT event_id FROM lane_inbox WHERE event_key=$1`, key).Scan(&got); err != nil || got != row.want {
			t.Errorf("%s: event_id %q, want %q (%v)", key, got, row.want, err)
		}
	}
}

func TestInboxStatusCLI(t *testing.T) {
	h, server, _ := ackStateHub(t, hubInboxStorage{path: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	relayFrom(t, h, "node-a", "lane-a", "ev-a")
	envFile := func(machine, token string) string {
		path := filepath.Join(t.TempDir(), machine+".env")
		if err := os.WriteFile(path, []byte("HUB_MACHINE_ID="+machine+"\nHUB_TOKEN="+token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	run := func(machine, token string, args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		args = append([]string{"status"}, args...)
		args = append(args, "--hub-url", server.URL, "--hub-token-env", envFile(machine, token))
		code := runInboxCLI(args, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
		return code, out.String(), errOut.String()
	}

	code, out, errText := run("node-a", ackStateNodeAToken, "--event-id", "ev-a", "--json")
	if code != ExitOK || decodeAckState(t, out).Events[0]["event_id"] != "ev-a" {
		t.Fatalf("node status --json: %d %q %q", code, out, errText)
	}
	code, out, _ = run("operator", ackStateOperatorToken, "--event-id", "ev-a")
	if code != ExitOK || out != "ev-a\tunacked\n" {
		t.Fatalf("status text: %d %q", code, out)
	}
	// An id with no row is printed and exits 7, so a script can tell it from an unacked one.
	code, out, _ = run("operator", ackStateOperatorToken, "--event-id", "ev-a", "--event-id", "nope")
	if code != ExitPartial || out != "ev-a\tunacked\nnope\tnot found\n" {
		t.Fatalf("partial status: %d %q", code, out)
	}
	code, out, _ = run("operator", ackStateOperatorToken, "--event-id", "nope", "--json")
	if code != ExitPartial || !slices.Equal(decodeAckState(t, out).NotFound, []string{"nope"}) {
		t.Fatalf("unknown status --json: %d %q", code, out)
	}
	// A node asking for what it did not send is refused like any other route.
	if code, _, stderr := run("node-b", ackStateNodeBToken, "--event-id", "ev-a"); code != ExitDeliveryFailure || !strings.Contains(stderr, "HTTP 401") {
		t.Fatalf("foreign node: %d %q", code, stderr)
	}
	if code, _, _ = run("service", ackStateServiceToken); code != ExitUsage {
		t.Fatalf("no --event-id: %d", code)
	}
	if code, _, _ = run("operator", ackStateOperatorToken, "--event-id", "ev-a", "--lane", "lane-a"); code != ExitUsage {
		t.Fatalf("--lane on status: %d", code)
	}
}
