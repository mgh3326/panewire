package panewire

// hk 3431, rewritten for the durable incidents of hk 6821: an unconfirmed relay
// row tells the job's owner lane only when someone can still act on it, and one
// notice per host fault per incident. A worker lane whose jobs have all ended,
// or whose live job's hk task has ended, is logged instead; the rows that fail
// while a notice speaks for the host fault are folded into the next notice. The
// obligation pass posts the notices, from the inbox store. Contract:
// docs/relay-harness-delivery.md. A hub with no store keeps the in-memory window
// (relay_unconfirmed_quiet_legacy_t3431_test.go).

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	// t6821Second is a second worker lane on the first worker's host, and
	// t6821Remote one on another host.
	t6821Second = "wk-t6821-second"
	t6821Remote = "wk-t6821-remote"
)

// t6821Rig is the hk 3431 rig over a hub with an inbox store.
type t6821Rig struct {
	*t3431Rig
	// copies are the retry copies queued to worker lanes, oldest first.
	copies []hubRelayInjectEvent
}

func t6821Routes() map[string]reportRelayRoute {
	return map[string]reportRelayRoute{
		t3431Worker: {Machine: "host-a", Pane: "w1:pW", Parent: t3431Design},
		t6821Second: {Machine: "host-a", Pane: "w1:pS", Parent: t3431Design},
		t6821Remote: {Machine: "host-b", Pane: "w2:pR", Parent: t3431Design},
		t3431Design: {Machine: "host-a", Pane: "w1:pD"},
	}
}

func t6821Start(t *testing.T, jobs fleet.JobStore) *t6821Rig {
	t.Helper()
	return &t6821Rig{t3431Rig: t3431StartWith(t, jobs, t6821Routes(), freshLaneInboxFile(t))}
}

// t6821Live is a rig whose worker lane runs a live job with a task in
// progress, so every row that fails is told.
func t6821Live(t *testing.T) *t6821Rig {
	t.Helper()
	rig := t6821Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)
	return rig
}

// send relays one findings lane.event to lane and returns the directive the hub
// queued for it.
func (rig *t6821Rig) send(lane string) hubRelayInjectEvent {
	rig.t.Helper()
	rig.collect()
	rig.seq++
	eventID := fmt.Sprintf("mergewake-findings-%d", rig.seq)
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, NotifyType: "findings", Text: "[mergewake] findings for the worker: fix the failing check"}
	rig.hub.relayLaneEvent(event, rig.src)
	_, agent := rig.machineOf(lane)
	for {
		select {
		case directive := <-agent.relays:
			if directive.Lane == lane && directive.EventID > 0 {
				return directive
			}
			if directive.Lane == t3431Design {
				rig.pending = append(rig.pending, directive)
			}
		case <-time.After(2 * time.Second):
			rig.t.Fatalf("the hub queued no relay.inject for %s", lane)
		}
	}
}

// machineOf is the node that serves lane.
func (rig *t6821Rig) machineOf(lane string) (string, *hubAgent) {
	if t6821Routes()[lane].Machine == "host-b" {
		return "host-b", rig.src
	}
	return "host-a", rig.dest
}

// report sends the node's two frames for a row it gave up on.
func (rig *t6821Rig) report(d hubRelayInjectEvent, how t6820Drop) {
	rig.t.Helper()
	machine, agent := rig.machineOf(d.Lane)
	if how.report != "" {
		t650Send(rig.t, rig.hub, machine, agent, "relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: how.report})
	}
	if how.drop != "" {
		t650Send(rig.t, rig.hub, machine, agent, "relay.dropped", relayDroppedPayload{JobID: d.JobID, Pane: d.Pane, Lane: d.Lane, OriginalEventID: d.EventID, Reason: how.drop, Harness: how.harness})
	}
}

// failOn relays one row to lane, which its node reports maybe typed.
func (rig *t6821Rig) failOn(lane string) hubRelayInjectEvent {
	rig.t.Helper()
	d := rig.send(lane)
	rig.report(d, t6820Maybe)
	return d
}

// fail is failOn for the worker lane.
func (rig *t6821Rig) fail() int64 {
	rig.t.Helper()
	return rig.failOn(t3431Worker).EventID
}

// collect sorts what the hub queued into the design lane's notices and the
// copies for worker lanes.
func (rig *t6821Rig) collect() {
	for _, agent := range []*hubAgent{rig.dest, rig.src} {
		for _, directive := range t3426Injected(agent) {
			if directive.Lane == t3431Design {
				rig.pending = append(rig.pending, directive)
			} else {
				rig.copies = append(rig.copies, directive)
			}
		}
	}
}

// pass advances the clock by wait and runs one obligation pass.
func (rig *t6821Rig) pass(wait time.Duration) {
	rig.t.Helper()
	rig.clock.advance(wait)
	rig.hub.obligations.mu.Lock()
	defer rig.hub.obligations.mu.Unlock()
	rig.hub.reconcileObligations(rig.t.Context())
	rig.collect()
}

// told runs one pass after wait and returns the notices it queued.
func (rig *t6821Rig) told(wait time.Duration) []hubRelayInjectEvent {
	rig.t.Helper()
	rig.pass(wait)
	return rig.notices()
}

// one is told, failing unless exactly one notice was queued.
func (rig *t6821Rig) one(wait time.Duration) string {
	rig.t.Helper()
	got := rig.told(wait)
	if len(got) != 1 {
		rig.t.Fatalf("the pass told the design lane %d notices, want 1: %+v\nlogs:\n%s", len(got), got, rig.logs.String())
	}
	return got[0].Text
}

// none is told, failing if any notice was queued.
func (rig *t6821Rig) none(wait time.Duration) {
	rig.t.Helper()
	if got := rig.told(wait); len(got) != 0 {
		rig.t.Fatalf("the design lane was told: %+v", got)
	}
}

// t6821Text is the notice of a maybe-typed worker row, as the relay ends it.
func t6821Text(row int64, eventID, lane, pane string) string {
	return fmt.Sprintf("[delivery unconfirmed] event %d (lane.event, producer id %s) for lane %s was not confirmed on host-a pane %s: maybe_in_pane. Check the pane before resending; it may already contain the message. Reported once.", row, eventID, lane, pane)
}

func t6821Folded(count int, since time.Time) string {
	return fmt.Sprintf(" (+%d more unconfirmed for this host since %s UTC)", count, since.UTC().Format("15:04"))
}

// notice stores: the row the fake holds under the notice id.
func (rig *t6821Rig) noticeRows() []handoffkeepRelayEvent {
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	var out []handoffkeepRelayEvent
	for _, row := range rig.fake.rows {
		if strings.HasPrefix(row.EventID, unconfirmedEscalationPrefix) {
			out = append(out, *row)
		}
	}
	return out
}

// Acceptance 1 of hk 3431: the worker lane's only job has ended, so nobody is
// there to read or re-send: no owner-lane notice, one INFO line instead. An
// orphaned job has not ended: the lane is not named while its node is away, and
// the row waits for it.
func TestUnconfirmedNoticeSkippedWhenWorkerJobEnded_t3431(t *testing.T) {
	for _, tc := range []struct {
		name                string
		completed, orphaned bool
		message, why        string
	}{
		{name: "completed", completed: true, message: "skipped; nobody can act on it", why: "why=job_ended"},
		{name: "orphaned", orphaned: true, message: "held; the lane's job is orphaned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := t6821Start(t, t3431LiveJobWithTask())
			rig.task(t3431Task, "in_progress", "started")
			rig.job(t3431Job, tc.completed, tc.orphaned)

			row := rig.fail()
			rig.none(0)

			lines := rig.infoLines(tc.message, fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "reason=maybe_in_pane", tc.why)
			if len(lines) != 1 {
				t.Fatalf("want exactly one INFO line naming the event, lane and reason; got %d in:\n%s", len(lines), rig.logs.String())
			}
			rig.none(time.Hour)
			if r := rig.row(row); (r.DueAt != 0) != tc.orphaned || (r.ToldAt != 0) != tc.completed {
				t.Fatalf("row = %+v", r)
			}
		})
	}
}

// Acceptance 2: the live job's hk task has ended (merged, dropped, deployed or
// canceled), judged by hk's category, or by the state when hk sends none.
func TestUnconfirmedNoticeSkippedWhenTaskEnded_t3431(t *testing.T) {
	for _, tc := range []struct{ state, category string }{
		{"merged", "completed"}, {"dropped", "canceled"}, {"deployed", "completed"},
		{"merged", ""}, {"dropped", ""}, {"canceled", ""},
	} {
		t.Run(tc.state+"/"+tc.category, func(t *testing.T) {
			rig := t6821Start(t, t3431LiveJobWithTask())
			rig.task(t3431Task, tc.state, tc.category)
			rig.job(t3431Job, false, false)

			row := rig.fail()
			rig.none(0)

			if lines := rig.infoLines(fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "reason=maybe_in_pane", "why=task_ended"); len(lines) != 1 {
				t.Fatalf("want exactly one INFO line for the skipped notice; logs:\n%s", rig.logs.String())
			}
		})
	}
}

// Acceptance 3: a live job whose task is in progress gets the notice.
func TestUnconfirmedNoticeSentForTaskInProgress_t3431(t *testing.T) {
	rig := t6821Live(t)

	row := rig.fail()

	got := rig.one(0)
	if want := t6821Text(row, "mergewake-findings-1", t3431Worker, "w1:pW"); !strings.HasSuffix(got, want) {
		t.Fatalf("notice text changed:\n got %q\nwant suffix %q", got, want)
	}
	rig.tasks.mu.Lock()
	reads := rig.tasks.reads
	rig.tasks.mu.Unlock()
	if reads == 0 {
		t.Fatal("the hub never read the job's hk task")
	}
}

// Acceptance 4: seven rows inside the window send one notice; once the window
// ends the next one names the six folded into it, from the earliest.
func TestUnconfirmedNoticeOncePerWindow_t3431(t *testing.T) {
	rig := t6821Live(t)
	first := rig.clock.Now()

	var sent []hubRelayInjectEvent
	var rows []int64
	for i := 0; i < 7; i++ {
		rows = append(rows, rig.fail())
		sent = append(sent, rig.told(0)...)
		rig.clock.advance(4 * time.Minute) // seven rows over 24 minutes
	}
	if len(sent) != 1 {
		t.Fatalf("seven rows inside 30 minutes sent %d notices, want 1: %+v", len(sent), sent)
	}
	if want := t6821Text(rows[0], "mergewake-findings-1", t3431Worker, "w1:pW"); !strings.HasSuffix(sent[0].Text, want) {
		t.Fatalf("the first notice:\n got %q\nwant suffix %q", sent[0].Text, want)
	}
	for _, row := range rows[1:] {
		if lines := rig.infoLines(fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "why=folded"); len(lines) != 1 {
			t.Fatalf("folded row %d logged %d INFO lines, want 1; logs:\n%s", row, len(lines), rig.logs.String())
		}
	}

	rig.clock.set(first.Add(31 * time.Minute))
	rows = append(rows, rig.fail())
	got := rig.one(0)
	// Seven rows wait: the one named and six more, since the earliest of them,
	// the second row, 4 minutes after the first.
	want := t6821Text(rows[1], "mergewake-findings-2", t3431Worker, "w1:pW") + t6821Folded(6, first.Add(4*time.Minute))
	if !strings.HasSuffix(got, want) {
		t.Fatalf("the notice after the window does not name the folded rows:\n got %q\nwant suffix %q", got, want)
	}
	rig.none(31 * time.Minute)
}

// Acceptance 5: a job record or task the hub cannot read fails open: the notice
// goes out.
func TestUnconfirmedNoticeFailsOpenWhenUnreadable_t3431(t *testing.T) {
	cases := map[string]struct {
		jobs     t3431Jobs
		withTask bool
	}{
		"job record unreadable": {jobs: t3431Jobs{err: fmt.Errorf("hk unavailable")}, withTask: true},
		"job record missing":    {jobs: t3431Jobs{records: map[string]map[string]any{}}, withTask: true},
		"task unreadable":       {jobs: t3431LiveJobWithTask(), withTask: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rig := t6821Start(t, tc.jobs)
			if tc.withTask {
				rig.task(t3431Task, "merged", "completed")
			}
			rig.job(t3431Job, false, false)

			row := rig.fail()

			if got := rig.one(0); !strings.HasSuffix(got, t6821Text(row, "mergewake-findings-1", t3431Worker, "w1:pW")) {
				t.Fatalf("an unreadable record must fail open with the notice; got %q", got)
			}
		})
	}
}

// A worker lane the hub has no job for at all is not known to have ended (a hub
// restart forgets every ended job; a lane may run work the hub never placed),
// so it keeps the notice.
func TestUnconfirmedNoticeSentWhenHubKnowsNoJob_t3431(t *testing.T) {
	rig := t6821Start(t, t3431LiveJobWithTask())

	row := rig.fail()

	if got := rig.one(0); !strings.HasSuffix(got, t6821Text(row, "mergewake-findings-1", t3431Worker, "w1:pW")) {
		t.Fatalf("want the notice for a lane the hub has no job for; got %q", got)
	}
}

// A row dropped again is not folded again, and a row a notice named is never
// named or counted again.
func TestUnconfirmedNoticeRedropIsNotFolded_t3431(t *testing.T) {
	rig := t6821Live(t)

	named := rig.failOn(t3431Worker)
	rig.one(0)
	rig.report(named, t6820Maybe)
	rig.none(0)
	folded := rig.failOn(t3431Worker)
	rig.pass(0)
	rig.report(folded, t6820Maybe)
	rig.report(named, t6820Maybe)
	got := rig.one(31 * time.Minute)
	want := t6821Text(folded.EventID, "mergewake-findings-2", t3431Worker, "w1:pW")
	if !strings.HasSuffix(got, want) {
		t.Fatalf("a re-dropped row was counted:\n got %q\nwant suffix %q", got, want)
	}
	rig.report(folded, t6820Maybe)
	rig.none(31 * time.Minute)
}

// A notice handoffkeep fails to store is posted again until it is stored, with
// every row that failed meanwhile: nothing gives up, and nothing is spent.
func TestUnconfirmedNoticeGiveUpLeavesWindowOpen_t3431(t *testing.T) {
	rig := t6821Live(t)
	first := rig.clock.Now()

	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3431Design
	rig.fake.mu.Unlock()
	lost := rig.fail()
	rig.none(0)
	// Past every wait of the retired ladder, the notice is still owed.
	for _, wait := range []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, 15 * time.Minute} {
		rig.none(wait)
	}
	if got := rig.logs.String(); strings.Count(got, `msg="relay unconfirmed escalation will retry"`) < 6 {
		t.Fatalf("the failing notice was not retried:\n%s", got)
	}
	row := rig.fail()
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()

	got := rig.one(15 * time.Minute)
	want := t6821Text(lost, "mergewake-findings-1", t3431Worker, "w1:pW") + t6821Folded(1, first)
	if !strings.HasSuffix(got, want) || row == lost {
		t.Fatalf("the stored notice does not name both rows:\n got %q\nwant suffix %q", got, want)
	}
}

// A post answered as a duplicate stored an earlier text, which may not name the
// rows now waiting. It takes the window and tells no row: the next notice names
// them under its own id.
func TestUnconfirmedNoticeDuplicateRecordsNothing_t3431(t *testing.T) {
	rig := t6821Live(t)
	// The incident opens at the pass's clock, which is its id.
	incident := rig.clock.Now().UnixMilli()
	told := handoffkeepRelayEvent{ID: 9001, Kind: "lane.event", OwnerLane: t3431Design, EventID: fmt.Sprintf("%si%d-n0", unconfirmedEscalationPrefix, incident),
		JobID: laneEventTransportID(t3431Design, fmt.Sprintf("%si%d-n0", unconfirmedEscalationPrefix, incident)), Epoch: 1, Reason: relayDroppedMaybeInPane, Text: "an earlier text"}
	rig.fake.mu.Lock()
	rig.fake.rows[fakeHandoffkeepStoredKey(told)] = &told
	rig.fake.mu.Unlock()

	row := rig.fail()
	rig.none(0)
	rig.none(29 * time.Minute)
	got := rig.one(2 * time.Minute)
	if !strings.HasSuffix(got, t6821Text(row, "mergewake-findings-1", t3431Worker, "w1:pW")) {
		t.Fatalf("the row was lost behind a duplicate: %q", got)
	}
}

// A folded row whose window ended is told once, by the pass, without another
// drop; dropping it again says nothing.
func TestUnconfirmedNoticeRedropAfterExpirySends_t3431(t *testing.T) {
	rig := t6821Live(t)

	rig.fail()
	rig.one(0)
	rig.clock.advance(time.Minute)
	folded := rig.failOn(t3431Worker)
	rig.none(0)

	got := rig.one(30 * time.Minute)
	if !strings.HasSuffix(got, t6821Text(folded.EventID, "mergewake-findings-2", t3431Worker, "w1:pW")) {
		t.Fatalf("the folded row counted itself, or was not named: %q", got)
	}
	rig.report(folded, t6820Maybe)
	rig.report(folded, t6820Maybe)
	rig.none(31 * time.Minute)
}

// F1: R1's post fails and R1 is confirmed before it is posted again. R1's notice
// was never stored, so it holds no window: R2, which fails 5 minutes later, is
// told at once.
func TestUnconfirmedNoticeEarlyRetryExitLeavesWindowOpen_t3431(t *testing.T) {
	rig := t6821Live(t)

	r1 := rig.failOn(t3431Worker)
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3431Design
	rig.fake.mu.Unlock()
	rig.none(0)
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: r1.JobID, Pane: r1.Pane, OriginalEventID: r1.EventID})
	rig.none(0)

	rig.clock.advance(5 * time.Minute)
	row := rig.fail()
	if got := rig.one(0); !strings.HasSuffix(got, t6821Text(row, "mergewake-findings-2", t3431Worker, "w1:pW")) {
		t.Fatalf("the row after a confirmed one: %q", got)
	}
}

// F2: R2 fails while R1's post is failing. The stored notice names both.
func TestUnconfirmedNoticeFoldDuringFailedRetriesIsNamed_t3431(t *testing.T) {
	rig := t6821Live(t)
	first := rig.clock.Now()

	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3431Design
	rig.fake.mu.Unlock()
	r1 := rig.fail()
	rig.none(0)
	rig.clock.advance(time.Second)
	rig.fail()
	rig.none(0)
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()

	got := rig.one(10 * time.Second)
	if want := t6821Text(r1, "mergewake-findings-1", t3431Worker, "w1:pW") + t6821Folded(1, first); !strings.HasSuffix(got, want) {
		t.Fatalf("the notice does not name R2:\n got %q\nwant suffix %q", got, want)
	}
}

// t6821LostReply stores the first notice post and loses its reply.
type t6821LostReply struct {
	next http.RoundTripper
	once *sync.Once
	lost *atomic.Int32
}

func (rt t6821LostReply) RoundTrip(req *http.Request) (*http.Response, error) {
	notice := false
	if req.Method == http.MethodPost && req.URL.Path == "/v1/relay/events" && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(body)
		notice = strings.Contains(string(raw), unconfirmedEscalationPrefix)
	}
	response, err := rt.next.RoundTrip(req)
	if !notice || err != nil {
		return response, err
	}
	lose := false
	rt.once.Do(func() { lose = true })
	if !lose {
		return response, nil
	}
	_ = response.Body.Close()
	rt.lost.Add(1)
	return nil, fmt.Errorf("the reply was lost")
}

// A post that was stored though its reply was lost is answered as a duplicate
// under the same id: one stored notice, and a row folded behind it is named by
// the next one.
func TestUnconfirmedNoticeDuplicateRetryPreservesLaterFolds_t3431(t *testing.T) {
	rig := t6821Live(t)
	client := rig.hub.handoffkeep.client
	var lost atomic.Int32
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	client.Transport = t6821LostReply{next: previous, once: &sync.Once{}, lost: &lost}
	t.Cleanup(func() { client.Transport = previous })

	rig.fail()
	rig.none(0)
	later := rig.fail()
	rig.none(10 * time.Second) // the same id, answered as a duplicate
	if lost.Load() != 1 || len(rig.noticeRows()) != 1 {
		t.Fatalf("lost replies=%d stored notices=%+v, want one of each", lost.Load(), rig.noticeRows())
	}
	got := rig.one(31 * time.Minute)
	if !strings.Contains(got, "(+1 more unconfirmed for this host") {
		t.Fatalf("the later fold was cleared by a duplicate: %q (row %d)", got, later)
	}
	if rows := rig.noticeRows(); len(rows) != 2 || rows[0].EventID == rows[1].EventID {
		t.Fatalf("stored notices=%+v, want two ids", rows)
	}
}

// A concurrent claim of the notice's id with no durable row proves nothing was
// stored: no window is taken, and the notice is posted again.
func TestUnconfirmedNoticeZeroIDDuplicateLeavesWindowOpen_t3431(t *testing.T) {
	rig := t6821Live(t)
	id := fmt.Sprintf("%si%d-n0", unconfirmedEscalationPrefix, rig.clock.Now().UnixMilli())
	key := relayEventDedupeKey("lane.event", unconfirmedNoticeEvent(t3431Design, id, relayDroppedMaybeInPane, "x"))
	rig.hub.mu.Lock()
	rig.hub.relayDedupe[key] = 0
	rig.hub.mu.Unlock()

	row := rig.fail()
	rig.none(0)
	rig.hub.mu.Lock()
	delete(rig.hub.relayDedupe, key)
	rig.hub.mu.Unlock()
	if got := rig.one(10 * time.Second); !strings.HasSuffix(got, t6821Text(row, "mergewake-findings-1", t3431Worker, "w1:pW")) {
		t.Fatalf("a claim with no durable row took the window: %q", got)
	}
}

// Every row waiting for a notice is counted, once each, however many there are
// and however often they drop again.
func TestUnconfirmedNoticeOverflowRedropCountedOnce_t3431(t *testing.T) {
	rig := t6821Live(t)
	rig.fail()
	rig.one(0)
	var folded []hubRelayInjectEvent
	for i := 0; i < 70; i++ {
		folded = append(folded, rig.failOn(t3431Worker))
	}
	rig.pass(0)
	rig.pass(0)
	for _, d := range folded {
		rig.report(d, t6820Maybe)
	}
	got := rig.one(31 * time.Minute)
	if t3431FoldCount(got) != len(folded)-1 {
		t.Fatalf("a notice for %d waiting rows counts %d more: %q", len(folded), t3431FoldCount(got), got)
	}
}

// A row whose read failed when it became due is joined later and named.
func TestUnconfirmedNoticeGiveUpAfterRowReadFailureFolds_t3431(t *testing.T) {
	rig := t6821Live(t)
	r1 := rig.failOn(t3431Worker)
	client := rig.hub.handoffkeep.client
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	client.Transport = t3431FailReadTransport{next: previous, afterID: r1.EventID - 1}
	t.Cleanup(func() { client.Transport = previous })
	rig.none(0)
	r2 := rig.fail()
	first := rig.one(time.Second)
	if !strings.Contains(first, fmt.Sprintf("event %d ", r2)) {
		t.Fatalf("an unreadable row held back the others: %q", first)
	}
	client.Transport = previous
	if got := rig.one(31 * time.Minute); !strings.Contains(got, fmt.Sprintf("event %d ", r1.EventID)) {
		t.Fatalf("the row whose read failed was never told: %q", got)
	}
}

// Rows dropped at the same moment make one incident and are all named or
// counted in its one notice. Killed by: an incident per drop; posting from the
// drop.
func TestUnconfirmedNoticeConcurrentDropsNeverLost_t3431(t *testing.T) {
	rig := t6821Live(t)
	var rows []hubRelayInjectEvent
	for i := 0; i < 3; i++ {
		rows = append(rows, rig.send(t3431Worker))
	}
	frame := func(kind string, payload any) []byte {
		raw, _ := json.Marshal(payload)
		wire, _ := json.Marshal(map[string]any{"type": "event", "kind": kind, "payload": json.RawMessage(raw)})
		return wire
	}
	var wg sync.WaitGroup
	for _, d := range rows {
		report := frame("relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: t6820Maybe.report})
		drop := frame("relay.dropped", relayDroppedPayload{JobID: d.JobID, Pane: d.Pane, Lane: d.Lane, OriginalEventID: d.EventID, Reason: t6820Maybe.drop, Harness: t6820Maybe.harness})
		wg.Add(1)
		go func() {
			defer wg.Done()
			rig.hub.handleAgentMessage("host-a", "fixture", rig.dest, report)
			rig.hub.handleAgentMessage("host-a", "fixture", rig.dest, drop)
		}()
	}
	wg.Wait()

	got := rig.one(0)
	if t3431FoldCount(got) != len(rows)-1 || len(rig.incidents()) != 1 {
		t.Fatalf("%d concurrent rows, the notice counts %d more, incidents %+v: %q", len(rows), t3431FoldCount(got), rig.incidents(), got)
	}
	rig.none(31 * time.Minute)
}

// Under a persistent 4xx nothing is told and every row waits; the first stored
// notice names them all.
func TestUnconfirmedNoticeFoldsBoundedUnderRefusal_t3431(t *testing.T) {
	rig := t6821Live(t)
	first := rig.clock.Now()
	const drops = 70
	var rows []hubRelayInjectEvent
	for i := 0; i < drops; i++ {
		rows = append(rows, rig.send(t3431Worker))
	}
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3431Design
	rig.fake.mu.Unlock()
	for _, d := range rows {
		rig.report(d, t6820Maybe)
	}
	rig.none(0)
	rig.none(0)
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()

	got := rig.one(15 * time.Minute)
	if !strings.Contains(got, fmt.Sprintf("(+%d more unconfirmed for this host since %s UTC)", drops-1, first.Format("15:04"))) {
		t.Fatalf("the notice after the refusals does not name all %d: %q", drops, got)
	}
}
