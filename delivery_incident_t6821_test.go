package panewire

// hk 6821 (hk 4289 T4): durable delivery incidents. The design lane is told
// once per host fault per incident, from rows the inbox store holds, and a
// notice slot per (design lane, machine, fault) spaces the notices across
// incidents. These tests drive the node's real frames and the obligation pass,
// and read the notices queued and the incident tables.

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func t6821Num(id int64) string { return strconv.FormatInt(id, 10) }

type t6821Incident struct {
	ID                 int64
	Machine, Fault     string
	State, CloseReason string
	Members, Announced int
	NoticeEventID      string
}

func (rig *t6821Rig) incidents() []t6821Incident {
	rig.t.Helper()
	rows, err := rig.hub.inbox.store.db.QueryContext(rig.t.Context(), `SELECT id,machine,fault,state,close_reason,members,announced,notice_event_id FROM delivery_incidents ORDER BY id`)
	if err != nil {
		rig.t.Fatal(err)
	}
	defer rows.Close()
	var out []t6821Incident
	for rows.Next() {
		var i t6821Incident
		if err := rows.Scan(&i.ID, &i.Machine, &i.Fault, &i.State, &i.CloseReason, &i.Members, &i.Announced, &i.NoticeEventID); err != nil {
			rig.t.Fatal(err)
		}
		out = append(out, i)
	}
	if err := rows.Err(); err != nil {
		rig.t.Fatal(err)
	}
	return out
}

// open is the one open incident.
func (rig *t6821Rig) open() t6821Incident {
	rig.t.Helper()
	var open []t6821Incident
	for _, i := range rig.incidents() {
		if i.State == incidentStateOpen {
			open = append(open, i)
		}
	}
	if len(open) != 1 {
		rig.t.Fatalf("open incidents=%+v, want one (all: %+v)", open, rig.incidents())
	}
	return open[0]
}

type t6821Row struct {
	State           string
	Incident        int64
	DueAt, ToldAt   int64
	Fault, NoticeAs string
}

func (rig *t6821Rig) row(relayID int64) t6821Row {
	rig.t.Helper()
	var r t6821Row
	if err := rig.hub.inbox.store.db.QueryRowContext(rig.t.Context(), `SELECT state,incident_id,notice_due_at,told_at,fault,fault_reason FROM delivery_obligations WHERE relay_id=?`, relayID).
		Scan(&r.State, &r.Incident, &r.DueAt, &r.ToldAt, &r.Fault, &r.NoticeAs); err != nil {
		rig.t.Fatalf("row %d has no obligation: %v", relayID, err)
	}
	return r
}

func (rig *t6821Rig) count(query string, args ...any) int {
	rig.t.Helper()
	var n int
	if err := rig.hub.inbox.store.db.QueryRowContext(rig.t.Context(), query, args...).Scan(&n); err != nil {
		rig.t.Fatal(err)
	}
	return n
}

// delivered reports the node's late proof of the directive.
func (rig *t6821Rig) delivered(d hubRelayInjectEvent) {
	rig.t.Helper()
	machine, agent := rig.machineOf(d.Lane)
	t650Send(rig.t, rig.hub, machine, agent, "relay.delivered", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID})
}

// copyOf passes after wait and returns the one copy queued for d's row.
func (rig *t6821Rig) copyOf(d hubRelayInjectEvent, wait time.Duration) hubRelayInjectEvent {
	rig.t.Helper()
	rig.copies = nil
	rig.pass(wait)
	var found []hubRelayInjectEvent
	for _, c := range rig.copies {
		if c.EventID == d.EventID {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		rig.t.Fatalf("copies of row %d = %+v, want one", d.EventID, rig.copies)
	}
	return found[0]
}

// exhaust relays one row to the worker lane that fails as how on every copy
// until its budget is spent, and runs the pass that exhausts it. The returned
// directive is its last copy.
func (rig *t6821Rig) exhaust(how t6820Drop) hubRelayInjectEvent {
	rig.t.Helper()
	d := rig.send(t3431Worker)
	for _, wait := range []time.Duration{0, time.Minute} {
		rig.report(d, how)
		d = rig.copyOf(d, wait)
		if got := rig.notices(); len(got) != 0 {
			rig.t.Fatalf("told before the budget was spent: %+v", got)
		}
	}
	rig.report(d, how)
	rig.pass(5 * time.Minute)
	if r := rig.row(d.EventID); r.State == obligationOwed {
		rig.t.Fatalf("row %d was not exhausted: %+v", d.EventID, r)
	}
	return d
}

// restart closes the hub and starts another over the same store and
// handoffkeep. The jobs the hub knew are forgotten, as a restart forgets them.
func (rig *t6821Rig) restart() {
	rig.t.Helper()
	if err := rig.hub.Close(); err != nil {
		rig.t.Fatal(err)
	}
	spawnApp := rig.hub.spawnNewApp
	hub, err := NewHubServer(rig.config)
	if err != nil {
		rig.t.Fatal(err)
	}
	rig.t.Cleanup(func() { _ = hub.Close() })
	hub.relayControl.relayAckTimeout = time.Hour
	hub.spawnNewApp = spawnApp
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.src = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	hub.nodes["host-b"] = &hubNodeRecord{agent: rig.src}
	hub.mu.Unlock()
	rig.hub = hub
}

// Rows that fail the same way on one host, from two worker lanes and at once,
// make one incident and one notice that counts the others. Another host, and
// another fault, are other incidents. Killed by: keying the incident by lane or
// row; posting per drop.
func TestIncidentOnePerHostFaultFoldsMembers(t *testing.T) {
	rig := t6821Live(t)
	first := rig.failOn(t3431Worker)
	rig.clock.advance(time.Second)
	rig.failOn(t6821Second)
	rig.failOn(t3431Worker)
	rig.failOn(t6821Remote)
	other := rig.send(t3431Worker)
	rig.report(other, t6820Drop{report: "composer_foreign"})

	got := rig.told(0)
	if len(got) != 3 {
		t.Fatalf("notices=%+v, want one per (host, fault): host-a/maybe_in_pane, host-b/maybe_in_pane, host-a/unproven", got)
	}
	incidents := rig.incidents()
	members := map[string]int{}
	for _, i := range incidents {
		members[i.Machine+"/"+i.Fault] = i.Members
		if i.Announced != i.Members {
			t.Errorf("incident %+v has unannounced members after its notice", i)
		}
	}
	if len(incidents) != 3 || members["host-a/maybe_in_pane"] != 3 || members["host-b/maybe_in_pane"] != 1 || members["host-a/unproven"] != 1 {
		t.Fatalf("incidents=%+v", incidents)
	}
	var folded []string
	for _, notice := range got {
		if strings.Contains(notice.Text, "more unconfirmed for this host") {
			folded = append(folded, notice.Text)
		}
	}
	if len(folded) != 1 || !strings.Contains(folded[0], "event "+t6821Num(first.EventID)+" ") || t3431FoldCount(folded[0]) != 2 {
		t.Fatalf("folded notices=%q, want one naming the earliest of three rows", folded)
	}
	rig.none(31 * time.Minute)
}

// A restart keeps the incident and its window: a row that fails after it is
// folded, and told once the window ends. Killed by: keeping the window in
// memory; opening a second incident for the key.
func TestIncidentSurvivesRestart(t *testing.T) {
	rig := t6821Start(t, t3431LiveJobWithTask())
	rig.fail()
	rig.one(0)
	before := rig.open()

	rig.restart()
	rig.clock.advance(time.Minute)
	later := rig.fail()
	rig.none(0)
	if after := rig.open(); after.ID != before.ID || after.Members != 2 || after.Announced != 1 {
		t.Fatalf("incident after the restart = %+v, before it %+v", after, before)
	}
	got := rig.one(29 * time.Minute)
	if !strings.Contains(got, "event "+t6821Num(later)+" ") || t3431FoldCount(got) != 0 {
		t.Fatalf("the notice after the window = %q, want row %d alone", got, later)
	}
}

// A new member after the window is told in a new notice under a new id; without
// one nothing is repeated. Killed by: folding forever; re-posting an announced
// member.
func TestIncidentReNoticesOnNewMemberAfterSpacing(t *testing.T) {
	rig := t6821Live(t)
	rig.fail()
	rig.one(0)
	first := rig.open().NoticeEventID
	rig.none(31 * time.Minute)
	later := rig.fail()
	got := rig.one(0)
	if !strings.Contains(got, "event "+t6821Num(later)+" ") {
		t.Fatalf("the second notice = %q, want row %d", got, later)
	}
	if second := rig.open(); second.NoticeEventID == first || second.Announced != 2 || second.Members != 2 {
		t.Fatalf("incident=%+v, first notice %q", second, first)
	}
}

// A member's delivery closes the incident, but the key's slot stays: a new
// fault inside the window opens an incident and folds silently, and the notice
// after the window names the folded count. Killed by: spacing on the incident.
func TestReopenedIncidentRespectsSpacing(t *testing.T) {
	rig := t6821Live(t)
	member := rig.failOn(t3431Worker)
	rig.one(0)
	first := rig.open().ID
	rig.clock.advance(time.Minute)
	rig.delivered(member)
	rig.none(0)
	if got := rig.incidents(); got[0].State != incidentStateClosed || got[0].CloseReason != incidentRecovered {
		t.Fatalf("incidents=%+v, want the first recovered", got)
	}

	rig.clock.advance(4 * time.Minute)
	a := rig.fail()
	rig.none(0)
	rig.fail()
	rig.none(20 * time.Minute)
	if again := rig.open(); again.ID == first || again.Members != 2 || again.Announced != 0 {
		t.Fatalf("reopened incident=%+v", again)
	}
	got := rig.one(5 * time.Minute)
	if !strings.Contains(got, "event "+t6821Num(a)+" ") || t3431FoldCount(got) != 1 {
		t.Fatalf("the notice after the window = %q, want row %d and one more", got, a)
	}
}

// A delivery on the member's pane and machine that is not a member's says
// nothing about the fault. Killed by: closing on any delivery to the pane.
func TestRecoveredOnlyOnMemberDelivery(t *testing.T) {
	rig := t6821Live(t)
	member := rig.failOn(t3431Worker)
	rig.one(0)
	rig.clock.advance(time.Minute)
	other := rig.send(t3431Worker)
	rig.delivered(other)
	rig.pass(time.Minute)
	rig.pass(time.Minute)
	if got := rig.open(); got.Members != 1 {
		t.Fatalf("incident=%+v, want the one member", got)
	}
	if r := rig.row(other.EventID); r.State != obligationConfirmed || r.Incident != 0 {
		t.Fatalf("the other row = %+v", r)
	}
	rig.delivered(member)
	rig.pass(0)
	if got := rig.incidents(); len(got) != 1 || got[0].CloseReason != incidentRecovered {
		t.Fatalf("incidents=%+v, want recovered", got)
	}
}

// One confirmed member recovers the incident at the next pass, without waiting
// for the row's parked read, and the other members detach. Killed by: closing
// only when every member is closed; reading a member only every four hours.
func TestIncidentRecoversOnMemberConfirmed(t *testing.T) {
	rig := t6821Live(t)
	a := rig.exhaust(t6820ReadFailed)
	rig.notices()
	b := rig.exhaust(t6820ReadFailed)
	rig.notices()
	id := rig.open().ID
	if ra, rb := rig.row(a.EventID), rig.row(b.EventID); ra.Incident != id || rb.Incident != id || ra.State != obligationNoticed {
		t.Fatalf("members=%+v %+v, incident %d", ra, rb, id)
	}

	rig.delivered(b)
	rig.pass(0)

	if got := rig.incidents(); len(got) != 1 || got[0].State != incidentStateClosed || got[0].CloseReason != incidentRecovered {
		t.Fatalf("incidents=%+v, want recovered", got)
	}
	if r := rig.row(a.EventID); r.Incident != 0 || r.State != obligationExhausted {
		t.Fatalf("the other member did not detach: %+v", r)
	}
	if r := rig.row(b.EventID); r.State != obligationConfirmed || r.Incident != 0 {
		t.Fatalf("the confirmed member = %+v", r)
	}
}

// An incident no member has faulted in for twice the spacing closes as quiet,
// and no sooner. Killed by: never closing; closing at the spacing.
func TestIncidentQuietClose(t *testing.T) {
	rig := t6821Live(t)
	rig.fail()
	rig.one(0)
	rig.none(59 * time.Minute)
	rig.open()
	rig.none(time.Minute)
	if got := rig.incidents(); len(got) != 1 || got[0].State != incidentStateClosed || got[0].CloseReason != incidentQuiet {
		t.Fatalf("incidents=%+v, want quiet", got)
	}
	rig.none(5 * time.Hour)
	if got := rig.incidents(); len(got) != 1 {
		t.Fatalf("a told row opened a new incident: %+v", got)
	}
}

// A closed incident's members detach: a noticed row is exhausted again, and no
// later read puts it back or tells it again. Killed by: keeping incident_id;
// leaving the state noticed; joining on every read.
func TestExhaustedDetachesFromClosedIncident(t *testing.T) {
	rig := t6821Live(t)
	d := rig.exhaust(t6820ReadFailed)
	if got := rig.notices(); len(got) != 1 || !strings.Contains(got[0].Text, "pane_read_failed. The pane could not be read before sending") {
		t.Fatalf("notices at exhaustion = %+v", got)
	}
	if r := rig.row(d.EventID); r.State != obligationNoticed || r.Incident == 0 {
		t.Fatalf("exhausted row = %+v", r)
	}
	rig.none(time.Hour)
	if r := rig.row(d.EventID); r.State != obligationExhausted || r.Incident != 0 || r.DueAt != 0 || r.ToldAt == 0 {
		t.Fatalf("row after the close = %+v", r)
	}
	for range 3 {
		rig.none(obligationParkRecheck)
	}
	if got := rig.incidents(); len(got) != 1 {
		t.Fatalf("the detached row joined again: %+v", got)
	}
}

// A row whose lane's job has ended is not named or counted, and an incident
// left with only such rows closes as moot. Killed by: naming an ended lane;
// never closing moot.
func TestIncidentMootMembersNotNamed(t *testing.T) {
	// The lane's job ends before the row joins, or while it waits in the
	// incident for the next notice.
	for _, waited := range []bool{false, true} {
		name := "an ended lane is not named"
		if waited {
			name = "a lane that ends while waiting is not named"
		}
		t.Run(name, func(t *testing.T) {
			rig := t6821Live(t)
			if waited {
				rig.failOn(t6821Second)
				rig.one(0)
			}
			ended := rig.fail()
			if waited {
				rig.none(0)
				if r := rig.row(ended); r.Incident == 0 || r.DueAt == 0 {
					t.Fatalf("the row did not wait in the incident: %+v", r)
				}
				rig.clock.advance(31 * time.Minute)
			}
			live := rig.failOn(t6821Second)
			rig.job(t3431Job, true, false)

			got := rig.one(0)
			if !strings.Contains(got, "event "+t6821Num(live.EventID)+" ") || strings.Contains(got, "event "+t6821Num(ended)+" ") || t3431FoldCount(got) != 0 {
				t.Fatalf("notice=%q, want only the live lane's row %d", got, live.EventID)
			}
			if r := rig.row(ended); r.DueAt != 0 || r.ToldAt != 0 {
				t.Fatalf("the ended lane's row = %+v", r)
			}
			rig.open()
		})
	}
	t.Run("only ended lanes close it", func(t *testing.T) {
		rig := t6821Live(t)
		rig.fail()
		rig.one(0)
		waiting := rig.fail()
		rig.none(0)
		rig.job(t3431Job, true, false)
		rig.none(0)
		if got := rig.incidents(); len(got) != 1 || got[0].CloseReason != incidentMoot {
			t.Fatalf("incidents=%+v, want moot", got)
		}
		if r := rig.row(waiting); r.DueAt != 0 || r.Incident != 0 {
			t.Fatalf("a moot member waits on: %+v", r)
		}
		rig.none(31 * time.Minute)
		if got := rig.incidents(); len(got) != 1 {
			t.Fatalf("a moot row opened a new incident: %+v", got)
		}
	})
}

// A notice handoffkeep did not store commits nothing: the incident stays open,
// unannounced, with no slot, and the same notice is posted until it is stored.
// Killed by: committing before the post; closing the incident; giving up.
func TestIncidentNoticeWriteFailureKeepsOpen(t *testing.T) {
	rig := t6821Live(t)
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3431Design
	rig.fake.mu.Unlock()
	row := rig.fail()
	rig.none(0)
	rig.none(time.Hour * 3) // past the quiet close: a notice still owed holds it open

	i := rig.open()
	if i.Announced != 0 || i.NoticeEventID != "" || rig.count(`SELECT count(*) FROM delivery_notice_slots`) != 0 {
		t.Fatalf("a refused notice was committed: incident=%+v", i)
	}
	if r := rig.row(row); r.DueAt == 0 || r.ToldAt != 0 {
		t.Fatalf("the row of a refused notice = %+v", r)
	}
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()
	rig.one(15 * time.Minute)
	// Told, and long quiet: the same pass closes it.
	if got := rig.incidents(); len(got) != 1 || got[0].ID != i.ID || got[0].Announced != 1 || !strings.HasSuffix(got[0].NoticeEventID, "-n0") || got[0].CloseReason != incidentQuiet {
		t.Fatalf("incidents=%+v", got)
	}
	if rig.count(`SELECT count(*) FROM delivery_notice_slots WHERE notice_at>0 AND notices=1`) != 1 || rig.row(row).ToldAt == 0 {
		t.Fatal("the stored notice did not commit its slot and rows")
	}
}

// A notice the spawn's --owner-notify withholds is counted once and never told,
// however often the row drops again, and takes no slot: another spawn's row is
// told at once. The row's job id is the one the withholding rule reads (hk
// 5360). Killed by: not recording the withholding; telling the row later;
// spacing the next notice behind it.
func TestWithheldNoticeIsRecordedNotRepeated(t *testing.T) {
	rig := t6821Live(t)
	job := laneEventTransportID(t3431Worker, "mergewake-findings-1")
	req := t4270Request(job, t3431Design, "", "implementer")
	req.Args = append(req.Args, fleet.OwnerNotifyArg, fleet.OwnerNotifyNever)
	if _, _, err := rig.hub.inbox.store.claimSpawn(t.Context(), req, job, t3431Design, rig.clock.Now()); err != nil {
		t.Fatal(err)
	}

	d := rig.failOn(t3431Worker)
	rig.none(0)
	decisions := func() int {
		return rig.count(`SELECT count(*) FROM owner_notice_decisions WHERE event_key=? AND decision=?`, "unconfirmed:"+t6821Num(d.EventID), ownerDecisionCount)
	}
	if got := decisions(); got != 1 {
		t.Fatalf("decisions=%d, want the withheld notice counted once", got)
	}
	if got := rig.open(); got.Members != 1 || got.Announced != 1 || rig.count(`SELECT count(*) FROM delivery_notice_slots`) != 0 {
		t.Fatalf("withheld incident = %+v", got)
	}
	rig.clock.advance(time.Minute)
	other := rig.fail()
	if got := rig.one(0); !strings.Contains(got, "event "+t6821Num(other)+" ") || t3431FoldCount(got) != 0 {
		t.Fatalf("the next spawn's notice = %q, want row %d alone", got, other)
	}
	for range 3 {
		rig.report(d, t6820Maybe)
		rig.none(31 * time.Minute)
	}
	if got := rig.count(`SELECT count(*) FROM owner_notice_decisions`); got != 1 {
		t.Fatalf("decisions=%d after re-drops, want 1", got)
	}
	if r := rig.row(d.EventID); r.ToldAt == 0 || r.DueAt != 0 {
		t.Fatalf("withheld row = %+v", r)
	}
}

// A safe fault is told when its row is exhausted, never at its first report, and
// an unrouted or ended-job row is never told. A rejected prompt waits for the
// drop: claude's proves nothing was typed, devin's may have typed part.
// Killed by: telling every drop; telling a maybe-typed row only at exhaustion.
func TestNoticeTimingByFault(t *testing.T) {
	t.Run("ack timeout is told at exhaustion", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		expire := func() {
			rig.hub.expireRelayAckWindow(relayPendingKey(d.EventID, d.JobID), nil)
		}
		expire()
		d = rig.copyOf(d, 0)
		expire()
		d = rig.copyOf(d, time.Minute)
		if got := rig.notices(); len(got) != 0 {
			t.Fatalf("told before exhaustion: %+v", got)
		}
		expire()
		if got := rig.one(5 * time.Minute); !strings.Contains(got, "ack_timeout. The worker may not have it") {
			t.Fatalf("notice=%q", got)
		}
	})
	t.Run("working expiry is told at exhaustion", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.report(d, t6820Working)
		if got := rig.one(0); !strings.Contains(got, "working_wait_expired. The worker may not have it") {
			t.Fatalf("notice=%q", got)
		}
	})
	t.Run("job ended is never told", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.report(d, t6820Drop{report: relayUnconfirmedPromptFailed, drop: relayDroppedJobEnded, harness: "claude"})
		for range 3 {
			rig.none(obligationParkRecheck)
		}
		if len(rig.incidents()) != 0 {
			t.Fatalf("incidents=%+v", rig.incidents())
		}
	})
	t.Run("claude prompt rejection waits for exhaustion", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.report(d, t6820PromptFailed)
		rig.pass(0)
		if got := rig.notices(); len(got) != 0 || len(rig.incidents()) != 0 {
			t.Fatalf("a safe rejection was told at once: %+v", got)
		}
	})
	t.Run("devin prompt rejection is told at the drop", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.report(d, t6820Drop{report: relayUnconfirmedPromptFailed, drop: relayDroppedRetriesExhausted, harness: "devin"})
		if got := rig.one(0); !strings.Contains(got, "maybe_in_pane. Check the pane before resending") {
			t.Fatalf("notice=%q", got)
		}
	})
	t.Run("a notice that is itself lost starts no other", func(t *testing.T) {
		// The design lane has a lane above it, which a lost notice could name.
		routes := t6821Routes()
		routes[t3431Design] = reportRelayRoute{Machine: "host-a", Pane: "w1:pD", Parent: "desk-t6821"}
		routes["desk-t6821"] = reportRelayRoute{Machine: "host-a", Pane: "w1:pK"}
		rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}
		rig.task(t3431Task, "in_progress", "started")
		rig.job(t3431Job, false, false)
		rig.fail()
		rig.pass(0)
		got := rig.notices()
		if len(got) != 1 {
			t.Fatalf("notices=%+v", got)
		}
		notice := got[0]
		rig.report(notice, t6820Maybe)
		// The notice's own row is retried to the design lane, as any row is.
		rig.pass(0)
		rig.pass(31 * time.Minute)
		if r := rig.row(notice.EventID); r.DueAt != 0 || r.Incident != 0 {
			t.Fatalf("the notice's own row = %+v", r)
		}
		if rows := rig.noticeRows(); len(rows) != 1 || len(rig.incidents()) != 1 {
			t.Fatalf("a lost notice started another: rows=%+v incidents=%+v", rows, rig.incidents())
		}
	})
	t.Run("unrouted is never told", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.hub.expireRelayAckWindow(relayPendingKey(d.EventID, d.JobID), nil)
		rig.hub.mu.Lock()
		delete(rig.hub.nodes, "host-a")
		rig.hub.mu.Unlock()
		for range 3 {
			rig.pass(time.Hour)
		}
		if r := rig.row(d.EventID); r.Fault != obligationFaultUnrouted || r.DueAt != 0 {
			t.Fatalf("row=%+v", r)
		}
		if len(rig.incidents()) != 0 {
			t.Fatalf("incidents=%+v", rig.incidents())
		}
	})
}

// A row the node held across a restart has no ack window and no recorded
// fault when its drop arrives; the design lane is still told, once, and the
// row's retry schedule is left alone. Killed by: noting drops only for rows
// with a fault.
func TestDropAfterRestartIsStillTold(t *testing.T) {
	for _, how := range []t6820Drop{t6820Maybe, t6820Working, t6820ReadFailed} {
		t.Run(how.drop, func(t *testing.T) {
			rig := t6821Live(t)
			d := rig.send(t3431Worker)
			rig.restart()
			rig.report(d, t6820Drop{drop: how.drop, harness: how.harness})
			got := rig.one(0)
			if !strings.Contains(got, "event "+t6821Num(d.EventID)+" ") || !strings.Contains(got, ": "+how.drop+". ") {
				t.Fatalf("notice=%q", got)
			}
			if r := rig.row(d.EventID); r.Fault != "" || r.State != obligationOwed {
				t.Fatalf("the drop scheduled a retry: %+v", r)
			}
			rig.report(d, t6820Drop{drop: how.drop, harness: how.harness})
			rig.none(31 * time.Minute)
		})
	}
}

// A row an older build retired with its labelled copy spent is exhausted by the
// pass and told, as the design's converter table asks. Killed by: skipping every
// row handoffkeep shows delivered.
func TestRetiredSpentCopyIsTold(t *testing.T) {
	rig := t6821Live(t)
	d := rig.send(t3431Worker)
	rig.fake.mu.Lock()
	for _, row := range rig.fake.rows {
		if row.ID == d.EventID {
			row.DeliveredAt, row.DeliveredTo = "2026-11-07T10:42:00Z", relayReplayRetiredMarker+relayUnconfirmedRetireReason
		}
	}
	rig.fake.mu.Unlock()
	if got := rig.one(obligationFirstCheck); !strings.Contains(got, "event "+t6821Num(d.EventID)+" ") || !strings.Contains(got, "unconfirmed. Check the pane") {
		t.Fatalf("notice=%q", got)
	}
}

// A notice to a design lane that is inbox-only is filed in its inbox; the filed
// item is durable, so it takes the slot and tells the row. Killed by: waiting
// for a handoffkeep row the inbox never writes.
func TestNoticeFiledInInboxOnlyDesignLane(t *testing.T) {
	routes := t6821Routes()
	routes["pw-design"] = reportRelayRoute{Machine: "host-a", Pane: "w1:pD"}
	routes[t3431Worker] = reportRelayRoute{Machine: "host-a", Pane: "w1:pW", Parent: "pw-design"}
	rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}

	row := rig.fail()
	rig.pass(0)
	rig.pass(31 * time.Minute)

	items := t4270Items(t, rig.hub, "pw-design")
	if len(items) != 1 || !strings.Contains(items[0].Event.Text, "event "+t6821Num(row)+" ") {
		t.Fatalf("design inbox = %+v", items)
	}
	if r := rig.row(row); r.ToldAt == 0 || r.DueAt != 0 {
		t.Fatalf("row=%+v", r)
	}
	if got := rig.open(); got.Announced != 1 {
		t.Fatalf("incident=%+v", got)
	}
}

// A waiting row whose read fails stays due; the notice names the next one, so
// one unreadable row does not hold back the host fault. Killed by: deferring the
// notice at the first failed read.
func TestUnreadableMemberDoesNotHoldNotice(t *testing.T) {
	rig := t6821Live(t)
	rig.fail()
	rig.one(0)
	broken := rig.fail()
	rig.pass(0)
	rig.clock.advance(time.Second)
	readable := rig.fail()
	rig.pass(0)
	client := rig.hub.handoffkeep.client
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	client.Transport = t3431FailReadTransport{next: previous, afterID: broken - 1}
	t.Cleanup(func() { client.Transport = previous })

	got := rig.one(31 * time.Minute)

	if !strings.Contains(got, "event "+t6821Num(readable)+" ") || t3431FoldCount(got) != 0 {
		t.Fatalf("notice=%q, want row %d alone", got, readable)
	}
	if r := rig.row(broken); r.DueAt == 0 || r.ToldAt != 0 {
		t.Fatalf("the unreadable row = %+v", r)
	}
	client.Transport = previous
	if got := rig.one(31 * time.Minute); !strings.Contains(got, "event "+t6821Num(broken)+" ") {
		t.Fatalf("the unreadable row was never told: %q", got)
	}
}

// A row of a lane with no design lane is only logged, at its report and again
// if it is exhausted, and waits for nothing. Killed by: opening an incident for
// it; leaving it due.
func TestRowWithoutDesignLaneIsOnlyLogged_t6821(t *testing.T) {
	routes := t6821Routes()
	routes[t3431Worker] = reportRelayRoute{Machine: "host-a", Pane: "w1:pW"}
	rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}

	row := rig.fail()
	lines := func() int {
		return strings.Count(rig.logs.String(), `msg="relay unconfirmed escalation has no design lane" event_id=`+t6821Num(row)+" ")
	}
	rig.none(0)
	if got := lines(); got != 1 {
		t.Fatalf("no-design-lane lines=%d, want 1:\n%s", got, rig.logs.String())
	}
	if r := rig.row(row); r.DueAt != 0 || r.Incident != 0 {
		t.Fatalf("row=%+v", r)
	}
	for range 3 {
		rig.none(31 * time.Minute)
	}
	if got := lines(); got > 2 || len(rig.incidents()) != 0 || rig.row(row).DueAt != 0 {
		t.Fatalf("lines=%d row=%+v incidents=%+v", got, rig.row(row), rig.incidents())
	}
}
