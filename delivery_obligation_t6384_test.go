package panewire

// hk 6384 (hk 4289 T1): durable delivery obligations. The hub creates one for
// each row the relay path stores, adopts the ones it missed, and closes them
// from stored facts. These tests drive the real relay writers and Sweep, and
// read the obligation tables, which are what this step produces.

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const t6384Lanes = `{"lanes":{
 "lane-a":{"machine":"host-a","pane":"w1:p1"},
 "worker-a":{"machine":"host-a","pane":"w1:worker","parent":"lane-a"},
 "lane-gone":{"machine":"host-z","pane":"w9:p9"},
 "quiet-design":{"machine":"host-a","pane":"w1:design"},
 "lane-sink":{"sink":true}
}}`

type t6384Rig struct {
	t      *testing.T
	fake   *fakeHandoffkeep
	client *handoffkeepRelayClient
	path   string
	clock  *t3602Clock
	logs   *t1002Log
	hub    *HubServer
	agent  *hubAgent
}

func newT6384Rig(t *testing.T) *t6384Rig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	rig := &t6384Rig{t: t, fake: fake, client: client, path: freshLaneInboxFile(t), logs: &t1002Log{},
		clock: &t3602Clock{now: time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)}}
	rig.start()
	return rig
}

// start runs a hub over the rig's lanes, store file and handoffkeep: calling it
// again after a Close is a restart. Its one node serves every routed pane as a
// busy claude session.
func (r *t6384Rig) start() {
	r.t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(r.t, t6384Lanes),
		DeliveryPolicyPath: "default",
		InboxStorePath:     r.path,
		handoffkeep:        r.client,
		Now:                r.clock.Now,
		Logger:             slog.New(slog.NewTextHandler(r.logs, nil)),
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { _ = hub.Close() })
	hub.relayControl.relayAckTimeout = time.Hour
	// Sweep is driven under a moving clock: the fixture node has no socket to
	// ping and must not go stale.
	hub.keepaliveInterval, hub.staleAfter = 1000*time.Hour, 1000*time.Hour
	r.agent = &hubAgent{machineID: "host-a", relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	var sessions []HubSession
	for _, pane := range []string{"w1:p1", "w1:design"} {
		sessions = append(sessions, HubSession{PaneID: pane, Status: "working", Harness: "claude"})
	}
	now := r.clock.Now()
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: r.agent, lastPing: now, lastKeepaliveSent: now,
		sessionSnapshot: &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: now}}
	hub.mu.Unlock()
	r.hub = hub
}

// pass advances the clock by wait, runs one Sweep and waits out the obligation
// pass it started.
func (r *t6384Rig) pass(wait time.Duration) {
	r.t.Helper()
	r.clock.advance(wait)
	r.hub.Sweep()
	r.hub.obligations.mu.Lock()
	defer r.hub.obligations.mu.Unlock()
}

func t6384Event(lane, id, text string) hubJobEventPayload {
	return hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, Text: text}
}

// send relays a lane.event from the node and returns the injection it queued.
func (r *t6384Rig) send(lane, id string) hubRelayInjectEvent {
	r.t.Helper()
	r.hub.relayLaneEvent(t6384Event(lane, id, "please look at "+id), r.agent)
	return r.injected()
}

func (r *t6384Rig) injected() hubRelayInjectEvent {
	r.t.Helper()
	got := t3426Injected(r.agent)
	if len(got) != 1 || got[0].EventID < 1 {
		r.t.Fatalf("injections=%+v, want one with a durable id", got)
	}
	return got[0]
}

func (r *t6384Rig) delivered(directive hubRelayInjectEvent) {
	r.t.Helper()
	t650Send(r.t, r.hub, "host-a", r.agent, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
}

func (r *t6384Rig) row(id int64) handoffkeepRelayEvent {
	r.t.Helper()
	r.fake.mu.Lock()
	defer r.fake.mu.Unlock()
	for _, row := range r.fake.rows {
		if row.ID == id {
			return *row
		}
	}
	r.t.Fatalf("no durable row %d", id)
	return handoffkeepRelayEvent{}
}

type t6384Obligation struct {
	RelayID                              int64
	Kind, Lane, Machine, Pane            string
	State, SettleReason, Fault           string
	Spent, AttemptsSeen                  int
	ResultID                             sql.NullString
	CreatedAt, UpdatedAt, NextAt, ItemID int64
}

// t6384Obligations reads every obligation in id order. ItemID is the relay_id
// of the lane_inbox item that shares its event key, or 0 with none.
func t6384Obligations(t *testing.T, s *laneInboxStore) []t6384Obligation {
	t.Helper()
	rows, err := s.db.QueryContext(t.Context(), `SELECT o.relay_id,o.kind,o.lane,o.machine,o.pane,o.state,o.settle_reason,o.fault,o.spent,o.attempts_seen,o.result_id,o.created_at,o.updated_at,o.next_at,COALESCE((SELECT i.relay_id FROM lane_inbox i WHERE i.event_key=o.event_key),0) FROM delivery_obligations o ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []t6384Obligation
	for rows.Next() {
		var o t6384Obligation
		if err := rows.Scan(&o.RelayID, &o.Kind, &o.Lane, &o.Machine, &o.Pane, &o.State, &o.SettleReason, &o.Fault, &o.Spent, &o.AttemptsSeen, &o.ResultID, &o.CreatedAt, &o.UpdatedAt, &o.NextAt, &o.ItemID); err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (r *t6384Rig) obligations() []t6384Obligation {
	r.t.Helper()
	return t6384Obligations(r.t, r.hub.inbox.store)
}

// obligation is the one obligation of the durable row id.
func (r *t6384Rig) obligation(id int64) t6384Obligation {
	r.t.Helper()
	var found []t6384Obligation
	for _, o := range r.obligations() {
		if o.RelayID == id {
			found = append(found, o)
		}
	}
	if len(found) != 1 {
		r.t.Fatalf("row %d has %d obligations: %+v", id, len(found), r.obligations())
	}
	return found[0]
}

func (r *t6384Rig) want(id int64, state, reason string) {
	r.t.Helper()
	if o := r.obligation(id); o.State != state || o.SettleReason != reason {
		r.t.Fatalf("row %d obligation = %s/%q, want %s/%q (row %+v)", id, o.State, o.SettleReason, state, reason, r.row(id))
	}
}

func (r *t6384Rig) exec(query string, args ...any) {
	r.t.Helper()
	if _, err := r.hub.inbox.store.db.ExecContext(r.t.Context(), query, args...); err != nil {
		r.t.Fatal(err)
	}
}

// blockInserts makes every obligation insert fail until the returned func runs.
func (r *t6384Rig) blockInserts() (unblock func()) {
	r.t.Helper()
	r.exec(`CREATE TRIGGER t6384_block BEFORE INSERT ON delivery_obligations BEGIN SELECT RAISE(ABORT,'blocked'); END`)
	return func() { r.exec(`DROP TRIGGER t6384_block`) }
}

// designLane turns lane into a design lane, which is inbox-only, from now on:
// the shape of a lane that became inbox-only after its rows were stored.
func (r *t6384Rig) designLane(lane string) {
	r.t.Helper()
	raw := strings.Replace(string(defaultDeliveryPolicy), `design_lanes: ["*-design"]`, `design_lanes: ["*-design", "`+lane+`"]`, 1)
	path := filepath.Join(r.t.TempDir(), "delivery.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		r.t.Fatal(err)
	}
	policy, err := loadDeliveryPolicy(path)
	if err != nil || !policy.designLane(lane) {
		r.t.Fatalf("policy with %s as a design lane: %v", lane, err)
	}
	r.hub.inbox.policy = policy
}

// t6384Row is an undelivered lane.event row handoffkeep holds and this hub
// never relayed.
func t6384Row(id int64, lane, eventID string, attempts int) handoffkeepRelayEvent {
	return handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: "seeded " + eventID, Attempts: attempts}
}

// A 201 or 200 on the relay path creates exactly one obligation, linked to the
// event's inbox item. Killed by: dropping either recordObligation call.
func TestObligationCreatedOnPersist(t *testing.T) {
	rig := newT6384Rig(t)
	lane := rig.send("lane-a", "ev-create")
	if got := rig.obligations(); len(got) != 1 {
		t.Fatalf("obligations after one lane.event = %+v", got)
	}
	o := rig.obligation(lane.EventID)
	now := rig.clock.Now().UnixMilli()
	// The obligation starts from the row's attempts. The fixture counts the
	// first write there; handoffkeep itself starts a row at 0.
	want := t6384Obligation{RelayID: lane.EventID, Kind: "lane.event", Lane: "lane-a", Machine: "host-a", Pane: "w1:p1", State: obligationOwed,
		AttemptsSeen: 1, Spent: 1, CreatedAt: now, UpdatedAt: now, NextAt: now + obligationFirstCheck.Milliseconds(), ItemID: lane.EventID}
	if o != want {
		t.Fatalf("obligation = %+v\nwant         %+v", o, want)
	}
	// A producer resend is the same event.
	rig.hub.relayLaneEvent(t6384Event("lane-a", "ev-create", "please look at ev-create"), rig.agent)
	if got := rig.obligations(); len(got) != 1 {
		t.Fatalf("a resend added an obligation: %+v", got)
	}

	// An escalation is typed into the owner lane's parent.
	rig.hub.relayJobEventFrom("host-a", "job.escalate", hubJobEventPayload{JobID: "job-6384", Epoch: 1, OwnerLane: "worker-a", Question: "which way?", ReportPath: "r.md", EventID: "esc-1"})
	job := rig.injected()
	if o := rig.obligation(job.EventID); o.Kind != "job.escalate" || o.State != obligationOwed || o.Lane != "worker-a" || o.Pane != "w1:p1" || o.ItemID != job.EventID {
		t.Fatalf("job obligation = %+v", o)
	}

	// HTTP ingress, a 200 for a row an earlier hub stored, and a row with no
	// route are the relay path too.
	ingress := rig.hub.relayLaneEvent(t6384Event("lane-a", "ev-ingress", "please look at ev-ingress"), nil)
	rig.obligation(ingress.ID)
	rig.fake.seedUndelivered(t6384Row(900, "lane-a", "ev-earlier", 2))
	rig.fake.mu.Lock()
	rig.fake.status = http.StatusOK
	rig.fake.mu.Unlock()
	rig.hub.relayLaneEvent(t6384Event("lane-a", "ev-earlier", "seeded ev-earlier"), nil)
	// The re-POST spent an attempt in handoffkeep: 3.
	if o := rig.obligation(900); o.Spent != 3 || o.AttemptsSeen != 3 {
		t.Fatalf("a row the hub never tracked starts at its attempts: %+v", o)
	}
	rig.fake.mu.Lock()
	rig.fake.status = http.StatusCreated
	rig.fake.mu.Unlock()
	unrouted := rig.hub.relayLaneEvent(t6384Event("lane-gone", "ev-unrouted", "please look"), rig.agent)
	if o := rig.obligation(unrouted.ID); o.Machine != "" || o.State != obligationOwed {
		t.Fatalf("unrouted obligation = %+v", o)
	}

	before := len(rig.obligations())
	rig.fake.mu.Lock()
	rig.fake.status = http.StatusInternalServerError
	rig.fake.mu.Unlock()
	if result := rig.hub.relayLaneEvent(t6384Event("lane-a", "ev-refused", "please look"), rig.agent); !result.PersistFailed {
		t.Fatalf("result = %+v, want a refused write", result)
	}
	if got := len(rig.obligations()); got != before {
		t.Fatalf("a row handoffkeep refused has an obligation: %d, want %d", got, before)
	}
}

// An event the policy files in an inbox never reaches handoffkeep and owes
// nothing there. Killed by: recording an obligation where receiveInbox takes
// the event.
func TestInboxClassEventHasNoObligation(t *testing.T) {
	rig := newT6384Rig(t)
	info := t6384Event("lane-a", "ev-info", "nightly numbers")
	info.NotifyType = "info"
	for _, event := range []hubJobEventPayload{info, t6384Event("quiet-design", "ev-design", "please look")} {
		if result := rig.hub.relayLaneEvent(event, rig.agent); result.Machine != "inbox" {
			t.Fatalf("%s relayed as %+v, want an inbox item", event.EventID, result)
		}
	}
	rig.hub.relayJobEventFrom("host-a", "job.completed", hubJobEventPayload{JobID: "job-quiet", Epoch: 1, OwnerLane: "quiet-design", ReportLastLine: "DONE", ReportPath: "r.md", EventID: "done-quiet"})
	if items, err := rig.hub.inbox.store.list("quiet-design", false); err != nil || len(items) != 2 {
		t.Fatalf("design inbox = %+v, %v; want the lane.event and the completion", items, err)
	}
	if rows := rig.fake.rowCount(); rows != 0 {
		t.Fatalf("handoffkeep rows = %d, want none for inbox-class events", rows)
	}
	rig.pass(time.Hour)
	if got := rig.obligations(); len(got) != 0 {
		t.Fatalf("inbox-class events have obligations: %+v", got)
	}
}

// A real relay.delivered closes the obligation confirmed on the next due pass,
// and a closed one is not read again. Killed by: a classify step without the
// confirmedInHK rule, or Sweep not starting the pass.
func TestObligationClosedOnDelivered(t *testing.T) {
	rig := newT6384Rig(t)
	lane := rig.send("lane-a", "ev-delivered")
	rig.hub.relayJobEventFrom("host-a", "job.completed", hubJobEventPayload{JobID: "job-6384", Epoch: 1, OwnerLane: "lane-a", ReportLastLine: "DONE", ReportPath: "r.md", EventID: "done-1"})
	job := rig.injected()

	rig.pass(time.Minute)
	rig.want(lane.EventID, obligationOwed, "")
	rig.want(job.EventID, obligationOwed, "")
	if o := rig.obligation(lane.EventID); o.NextAt != rig.clock.Now().Add(obligationRecheckMin).UnixMilli() {
		t.Fatalf("an owed row is read again at %d: %+v", o.NextAt, o)
	}

	rig.delivered(lane)
	rig.delivered(job)
	rig.pass(2 * time.Minute)
	rig.want(lane.EventID, obligationConfirmed, "")
	rig.want(job.EventID, obligationConfirmed, "")

	closed := rig.obligations()
	reads := len(rig.fake.queries("/v1/relay/events"))
	rig.pass(48 * time.Hour)
	// One listing page for adopt, and no row read.
	if got := len(rig.fake.queries("/v1/relay/events")) - reads; got != 1 {
		t.Fatalf("a pass over closed obligations sent %d reads, want the adopt page only", got)
	}
	after := rig.obligations()
	if len(after) != len(closed) || after[0] != closed[0] || after[1] != closed[1] {
		t.Fatalf("closed obligations changed:\n%+v\n%+v", closed, after)
	}
}

// Every synthetic delivered_to writer settles its obligation with its own
// reason, and none confirms. Killed by: confirmedInHK reading
// delivered_at != "" alone.
func TestSyntheticDestinationNeverConfirms(t *testing.T) {
	rig := newT6384Rig(t)
	ctx := context.Background()
	want := map[int64]string{}

	// sink/sink:<lane>: the sink branch of the relay path.
	sink := rig.hub.relayLaneEvent(t6384Event("lane-sink", "ev-sink", "record this"), rig.agent)
	want[sink.ID] = "sink"

	// hub/inbox from refuseInboxOnlyBody: the lane turned inbox-only between
	// the write and the typing.
	typed := rig.send("lane-a", "ev-refused-typing")
	// hub/inbox from replayInbox, for a row with no inbox item yet.
	rig.fake.seedUndelivered(t6384Row(901, "quiet-design", "ev-replayed", 1))
	// hub/filtered:<reason> and hub/chat-terminal.
	rig.fake.seedUndelivered(t6384Row(902, "lane-a", "ev-filtered", 1), t6384Row(903, "lane-a", "chat-42", 1))
	// hub/replay-retired:<reason>, every reason that settles.
	reasons := []string{"stale", "lane_gone", relayGapRetireReason, relayUnconfirmedInboxRetireReason, relayDigestNotReplayedRetireReason}
	for i, reason := range reasons {
		id := int64(910 + i)
		rig.fake.seedUndelivered(t6384Row(id, "lane-a", "ev-retired-"+reason, 1))
		want[id] = reason
	}
	rig.pass(time.Second) // adopt tracks the seeded rows

	rig.designLane("lane-a")
	route, agent, routed := rig.hub.resolveRelayRoute("lane.event", t6384Event("lane-a", "ev-refused-typing", ""))
	if !routed {
		t.Fatal("lane-a is not routed")
	}
	event := relayEventFromRecord(rig.row(typed.EventID))
	rig.hub.injectLaneRelayEvent(event, route, agent, typed.EventID, relayEventDedupeKey("lane.event", event))
	want[typed.EventID] = "inbox"
	rig.hub.replayRelayEvent(rig.row(901), relayReplaySourceStartup)
	want[901] = "inbox"
	if _, filed, ok := rig.hub.fileFilteredInHK(ctx, "lane.event", relayEventFromRecord(rig.row(902)), "host-a", filteredReasonReviewerLifecycle); !filed || !ok {
		t.Fatalf("filtered row filed=%v ok=%v", filed, ok)
	}
	want[902] = "filtered"
	rig.hub.replayRelayEvent(rig.row(903), relayReplaySourceStartup)
	want[903] = "chat-terminal"
	for i, reason := range reasons {
		rig.hub.retireRelayReplay(rig.row(int64(910+i)), reason)
	}
	for id, reason := range want {
		row := rig.row(id)
		if row.DeliveredAt == "" || !strings.HasPrefix(row.DeliveredTo, "hub/") && !strings.HasPrefix(row.DeliveredTo, "sink/") {
			t.Fatalf("fixture: row %d (%s) is not closed synthetically: %+v", id, reason, row)
		}
	}

	rig.pass(time.Hour)
	if got := rig.obligations(); len(got) != len(want) {
		t.Fatalf("obligations = %d, want %d: %+v", len(got), len(want), got)
	}
	for id, reason := range want {
		rig.want(id, obligationSettled, reason)
	}
}

// A node's applied report closes a row as <machine>/applied-report, a real
// delivery. Killed by: treating that pane marker as a synthetic destination.
func TestAppliedReportPaneConfirms(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-applied")
	rig.hub.settleNodeApplied(context.Background(), "host-a", map[string][]int64{"lane-a": {directive.EventID}})
	if row := rig.row(directive.EventID); row.DeliveredTo != "host-a/"+relayAppliedReportPane {
		t.Fatalf("row delivered_to = %q", row.DeliveredTo)
	}
	rig.pass(time.Hour)
	rig.want(directive.EventID, obligationConfirmed, "")
}

// A row the replay gate retired is settled, never confirmed. Killed by:
// confirmedInHK reading delivered_at != "" alone.
func TestRetiredRowIsNotConfirmed(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-stale")
	rig.fake.mu.Lock()
	for _, row := range rig.fake.rows {
		row.ReceivedAt = rig.clock.Now().Format(time.RFC3339Nano)
	}
	rig.fake.mu.Unlock()
	rig.clock.advance(relayReplayMaxAge + time.Minute)
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	if row := rig.row(directive.EventID); row.DeliveredTo != relayReplayRetiredMarker+"stale" || row.DeliveredAt == "" {
		t.Fatalf("fixture: the replay did not retire the row: %+v", row)
	}
	rig.pass(time.Minute)
	rig.want(directive.EventID, obligationSettled, "stale")
}

// A pre-upgrade labelled replay retired its row before queueing it. That
// retirement proves nothing and the obligation stays open for confirmation.
func TestLegacySpentCopyBecomesExhausted(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-spent")
	if err := rig.client.markDelivered(t.Context(), directive.EventID, relayReplayRetiredMachine, relayReplayRetiredPane+relayUnconfirmedRetireReason); err != nil {
		t.Fatal(err)
	}
	for _, wait := range []time.Duration{time.Minute, obligationParkRecheck + time.Minute} {
		rig.pass(wait)
		o := rig.obligation(directive.EventID)
		if o.State != obligationExhausted || o.Fault != obligationFaultUnconfirmed || o.SettleReason != "" {
			t.Fatalf("obligation = %+v, want exhausted for unconfirmed", o)
		}
		if o.NextAt != rig.clock.Now().Add(obligationParkRecheck).UnixMilli() {
			t.Fatalf("an exhausted row is parked: %+v", o)
		}
	}
	// The marker row is the hub's bookkeeping and owes nothing.
	if got := rig.obligations(); len(got) != 1 {
		t.Fatalf("obligations = %+v, want the marked row's only", got)
	}
}

// A restarted hub reads the obligation from its store and closes it from the
// late acknowledgement. Killed by: keeping obligations in memory, or a schema
// step that clears the table at open.
func TestObligationSurvivesRestart(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-restart")
	before := rig.obligation(directive.EventID)
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	if got := rig.obligation(directive.EventID); got != before {
		t.Fatalf("after the restart %+v, want %+v", got, before)
	}
	rig.delivered(directive)
	rig.pass(time.Minute)
	rig.want(directive.EventID, obligationConfirmed, "")
}

// An insert that fails leaves the relay untouched and is healed by adopt.
// Killed by: no adopt step.
func TestAdoptFillsMissingObligation(t *testing.T) {
	rig := newT6384Rig(t)
	unblock := rig.blockInserts()
	directive := rig.send("lane-a", "ev-missed")
	if got := rig.obligations(); len(got) != 0 {
		t.Fatalf("obligations with inserts blocked = %+v", got)
	}
	if want := "event_id=" + strconv.FormatInt(directive.EventID, 10); !strings.Contains(rig.logs.String(), "delivery obligation was not recorded") || !strings.Contains(rig.logs.String(), want) {
		t.Fatalf("the failed insert was not logged with %s:\n%s", want, rig.logs.String())
	}
	unblock()
	rig.pass(time.Minute)
	o := rig.obligation(directive.EventID)
	if o.State != obligationOwed || o.Kind != "lane.event" || o.Lane != "lane-a" || o.Spent != 1 || o.AttemptsSeen != 1 || o.ItemID != directive.EventID {
		t.Fatalf("adopted obligation = %+v", o)
	}
	rig.delivered(directive)
	rig.pass(time.Minute)
	rig.want(directive.EventID, obligationConfirmed, "")
}

// t6384Gate serves the fake handoffkeep, failing every read while down and
// counting the undelivered listing requests.
type t6384Gate struct {
	down     atomic.Bool
	listings atomic.Int64
}

func (r *t6384Rig) gate() *t6384Gate {
	r.t.Helper()
	gate := &t6384Gate{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Query().Get("undelivered") != "" {
			gate.listings.Add(1)
		}
		if req.Method == http.MethodGet && gate.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.fake.ServeHTTP(w, req)
	}))
	r.t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		r.t.Fatal(err)
	}
	r.hub.handoffkeep.baseURL = target
	return gate
}

// Adopt runs on every pass, so rows missed during an outage, of handoffkeep or
// of the store, are tracked on the first pass after it. An outage changes no
// obligation. Killed by: adopting only at startup, or backing off after a
// failure.
func TestAdoptRetriesEveryPassDuringOutage(t *testing.T) {
	rig := newT6384Rig(t)
	tracked := rig.send("lane-a", "ev-tracked")
	gate := rig.gate()
	rig.fake.seedUndelivered(t6384Row(900, "lane-a", "ev-outage", 2))

	gate.down.Store(true)
	for i := 0; i < 3; i++ {
		rig.pass(time.Hour)
	}
	if got := gate.listings.Load(); got != 3 {
		t.Fatalf("listings during the outage = %d, want one per pass", got)
	}
	if got := rig.obligations(); len(got) != 1 || got[0].State != obligationOwed || got[0].RelayID != tracked.EventID {
		t.Fatalf("an outage changed obligations: %+v", got)
	}

	gate.down.Store(false)
	unblock := rig.blockInserts()
	rig.pass(time.Second)
	rig.pass(time.Second)
	if got := rig.obligations(); len(got) != 1 {
		t.Fatalf("obligations with inserts blocked = %+v", got)
	}
	unblock()
	rig.pass(time.Second)
	if o := rig.obligation(900); o.State != obligationOwed || o.Spent != 2 || o.AttemptsSeen != 2 {
		t.Fatalf("adopted obligation = %+v", o)
	}
}

// t6384Listing points the hub at a handoffkeep whose undelivered listing holds
// exactly rows, whatever their state.
func (r *t6384Rig) listing(rows ...handoffkeepRelayEvent) {
	r.t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		after, _ := strconv.ParseInt(req.URL.Query().Get("after_id"), 10, 64)
		page := []handoffkeepRelayEvent{}
		for _, row := range rows {
			if row.ID > after && req.URL.Query().Get("undelivered") != "" {
				page = append(page, row)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": page})
	}))
	r.t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		r.t.Fatal(err)
	}
	r.hub.handoffkeep.baseURL = target
}

// Adopt tracks only rows still owed to a pane, whatever the listing returns.
// Killed by: adopting every listed row.
func TestAdoptNeverAdoptsDeliveredOrRetired(t *testing.T) {
	rig := newT6384Rig(t)
	delivered := t6384Row(1, "lane-a", "ev-delivered", 1)
	delivered.DeliveredAt, delivered.DeliveredTo = "2026-11-25T08:00:00Z", "host-a/w1:p1"
	retired := t6384Row(2, "lane-a", "ev-retired", 1)
	retired.DeliveredAt, retired.DeliveredTo = "2026-11-25T08:00:00Z", relayReplayRetiredMarker+"stale"
	marked := t6384Row(3, "lane-a", "ev-retired-open", 1)
	marked.DeliveredTo = relayReplayRetiredMarker + relayUnconfirmedRetireReason
	marker := t6384Row(4, relayUnconfirmedMarkLane, unconfirmedMarkEventID(1), 1)
	foreign := t6384Row(5, "lane-a", "ev-foreign", 1)
	foreign.Kind = "audit.note"
	orphan := t6384Row(6, "", "ev-orphan", 1)
	owed := t6384Row(7, "lane-a", "ev-owed", 1)
	job := handoffkeepRelayEvent{ID: 8, Kind: "job.lost", JobID: "job-8", Epoch: 1, OwnerLane: "lane-a", Reason: "pane_gone", ReportPath: "r.md", EventID: "lost-8", Attempts: 1}
	rig.listing(delivered, retired, marked, marker, foreign, orphan, owed, job)

	rig.pass(time.Second)
	got := rig.obligations()
	if len(got) != 2 || got[0].RelayID != 7 || got[1].RelayID != 8 || got[1].Kind != "job.lost" {
		t.Fatalf("adopted = %+v, want the owed rows 7 and 8 only", got)
	}
}

// One pass reads at most three pages and the next resumes after them, wrapping
// at the end. Killed by: an unbounded walk, or a walk that restarts at 0.
func TestAdoptIsBoundedAndResumes(t *testing.T) {
	rig := newT6384Rig(t)
	const pageCap = obligationAdoptPages * obligationAdoptPageSize
	rows := make([]handoffkeepRelayEvent, 0, pageCap+50)
	for i := 1; i <= pageCap+50; i++ {
		rows = append(rows, t6384Row(int64(1000+i), "lane-a", fmt.Sprintf("ev-%04d", i), 1))
	}
	rig.fake.seedUndelivered(rows...)
	listings := func() []string {
		var after []string
		for _, query := range rig.fake.queries("/v1/relay/events") {
			values, err := url.ParseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			if values.Get("undelivered") != "" {
				after = append(after, values.Get("after_id"))
			}
		}
		return after
	}

	// No wait: nothing is due, so the listing requests are adopt's alone.
	rig.pass(0)
	if got := len(rig.obligations()); got != pageCap {
		t.Fatalf("obligations after one pass = %d, want the cap %d", got, pageCap)
	}
	if got, want := strings.Join(listings(), ","), ",1200,1400"; got != want {
		t.Fatalf("first pass after_id = %q, want %q", got, want)
	}
	rig.pass(0)
	if got := len(rig.obligations()); got != pageCap+50 {
		t.Fatalf("obligations after the second pass = %d, want %d", got, pageCap+50)
	}
	if got, want := strings.Join(listings(), ","), ",1200,1400,1600"; got != want {
		t.Fatalf("second pass after_id = %q, want %q: it resumes", got, want)
	}
	var cursor, wrapped int64
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT adopt_after_id,adopt_wrapped_at FROM delivery_reconciler_state`).Scan(&cursor, &wrapped); err != nil || cursor != 0 || wrapped != rig.clock.Now().UnixMilli() {
		t.Fatalf("after a short page cursor=%d wrapped=%d, %v", cursor, wrapped, err)
	}
	rig.pass(0)
	if got, want := strings.Join(listings(), ","), ",1200,1400,1600,,1200,1400"; got != want {
		t.Fatalf("third pass after_id = %q, want %q: it starts over", got, want)
	}
}

// One pass reads a bounded number of due obligations. Killed by: dropping the
// limit.
func TestObligationClassifyIsBounded(t *testing.T) {
	rig := newT6384Rig(t)
	const total = obligationClassifyLimit + 7
	rows := make([]handoffkeepRelayEvent, 0, total)
	for i := 1; i <= total; i++ {
		rows = append(rows, t6384Row(int64(2000+i), "lane-a", fmt.Sprintf("ev-due-%02d", i), 1))
	}
	rig.fake.seedUndelivered(rows...)
	rig.pass(0)
	reads := len(rig.fake.queries("/v1/relay/events"))
	rig.pass(time.Hour)
	// The adopt page, then one read per classified row.
	if got := len(rig.fake.queries("/v1/relay/events")) - reads; got != 1+obligationClassifyLimit {
		t.Fatalf("reads in one pass = %d, want %d", got, 1+obligationClassifyLimit)
	}
}

// A receiver that has the inbox item settles the obligation, and a delivery
// the hub could not yet record in handoffkeep waits out the grace first.
// Killed by: no inbox rule, or an inbox rule with no grace.
func TestObligationSettledByInboxItem(t *testing.T) {
	rig := newT6384Rig(t)
	acked := rig.send("lane-a", "ev-acked")
	typed := rig.send("lane-a", "ev-typed")
	rig.pass(time.Minute)
	rig.want(acked.EventID, obligationOwed, "")

	item := t3426Item(t, rig.hub, "lane-a", "lane.event", acked.JobID)
	if n, err := rig.hub.inbox.store.ack(item.ID, "lane-a", false, rig.clock.Now()); err != nil || n != 1 {
		t.Fatalf("ack = %d, %v", n, err)
	}
	rig.fake.mu.Lock()
	rig.fake.deliveredStatus = http.StatusInternalServerError
	rig.fake.mu.Unlock()
	rig.clock.advance(2 * time.Minute)
	rig.delivered(typed)
	rig.pass(0)
	rig.want(acked.EventID, obligationSettled, obligationSettledInbox)
	rig.want(typed.EventID, obligationOwed, "")
	rig.pass(obligationRecheckMin + obligationInboxGrace)
	rig.want(typed.EventID, obligationSettled, obligationSettledInbox)
}

// A digest batch's row is settled once nothing claims the batch any more. The
// pass releases nothing itself. Killed by: settling every digest batch, or a
// pass that releases claims.
func TestObligationSettledByDigestRelease(t *testing.T) {
	rig := newT6384Rig(t)
	info := t6384Event("lane-a", "ev-digested", "nightly numbers")
	info.NotifyType = "info"
	rig.hub.relayLaneEvent(info, rig.agent)
	rig.clock.advance(31 * time.Minute) // past the inbox deadline
	rig.hub.flushInbox()
	batch := rig.injected()
	rig.want(batch.EventID, obligationOwed, "")
	claims := func() (n int) {
		if err := rig.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM lane_inbox WHERE escalation_batch<>''`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	rig.pass(time.Minute)
	rig.want(batch.EventID, obligationOwed, "")
	if got := claims(); got != 1 {
		t.Fatalf("claims after a pass = %d, want the batch's one", got)
	}
	if err := rig.hub.inbox.store.releaseBatchClaims(rig.row(batch.EventID).EventID); err != nil {
		t.Fatal(err)
	}
	rig.pass(2 * time.Minute)
	rig.want(batch.EventID, obligationSettled, obligationSettledDigest)
}

// handoffkeep folds a job's later round into the first round's row. That row's
// delivery is the first round's and confirms nothing about the later one.
// Killed by: confirming from whatever row relay_id names.
func TestObligationFoldedRowDoesNotAnswerForALaterEvent(t *testing.T) {
	rig := newT6384Rig(t)
	round := hubJobEventPayload{JobID: "job-folded", Epoch: 1, OwnerLane: "worker-a", Question: "round one?", ReportPath: "r.md", EventID: "round-1"}
	rig.hub.relayJobEventFrom("host-a", "job.escalate", round)
	first := rig.injected()
	rig.delivered(first)
	rig.fake.mu.Lock()
	rig.fake.status = http.StatusOK
	rig.fake.mu.Unlock()
	round.Question, round.EventID = "round two?", "round-2"
	rig.hub.relayJobEventFrom("host-a", "job.escalate", round)
	if second := rig.injected(); second.EventID != first.EventID {
		t.Fatalf("fixture: round two got its own row %d", second.EventID)
	}
	rig.pass(time.Hour)
	got := rig.obligations()
	if len(got) != 2 || got[0].State != obligationConfirmed || got[1].State != obligationOwed || got[1].RelayID != first.EventID {
		t.Fatalf("obligations = %+v, want round one confirmed and round two owed", got)
	}
}

// A row handoffkeep no longer lists stays open. Killed by: closing it.
func TestObligationMissingRowStaysOpen(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-vanished")
	rig.fake.mu.Lock()
	clear(rig.fake.rows)
	rig.fake.mu.Unlock()
	rig.pass(time.Minute)
	o := rig.obligation(directive.EventID)
	if o.State != obligationOwed || o.NextAt != rig.clock.Now().Add(obligationParkRecheck).UnixMilli() {
		t.Fatalf("obligation = %+v, want owed and parked", o)
	}
}

// A hub with no inbox store keeps no obligations and relays as before.
func TestObligationNoneWithoutInboxStore(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": "op"}, ReportRelayPath: r20LanesFile(t, t6384Lanes),
		handoffkeep: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	if hub.inbox != nil {
		t.Fatal("fixture: the hub has an inbox store")
	}
	if result := hub.relayLaneEvent(t6384Event("lane-gone", "ev-off", "please look"), nil); result.ID == 0 {
		t.Fatalf("result = %+v", result)
	}
	hub.Sweep()
	hub.obligations.mu.Lock()
	defer hub.obligations.mu.Unlock()
}

// Close ends a pass blocked on a handoffkeep read, returns once it is over and
// does not wait out the read's timeout. Killed by: a pass context that Close
// does not cancel.
func TestObligationCloseCancelsABlockedPass(t *testing.T) {
	rig := newT6384Rig(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	rig.fake.mu.Lock()
	rig.fake.observeQuery = func(method, _, query string) {
		if method == http.MethodGet && strings.Contains(query, "undelivered") && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	rig.fake.mu.Unlock()
	t.Cleanup(func() { close(release) })
	rig.hub.startObligationReconcile()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the pass never listed")
	}
	closed := make(chan error, 1)
	go func() { closed <- rig.hub.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(handoffkeepTimeout / 2):
		t.Fatal("Close waited out a handoffkeep read it should have canceled")
	}
	if !rig.hub.obligations.mu.TryLock() {
		t.Fatal("the pass is still running after Close")
	}
	rig.hub.obligations.mu.Unlock()
}
