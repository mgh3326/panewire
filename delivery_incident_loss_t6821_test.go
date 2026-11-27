package panewire

// hk 6821: a failure the design lane heard of from the node's drop before
// there were incidents is still told when the row's route, its age, a restart or
// a failed read would otherwise end it silently.

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// t6821Outage fails the by-id read of one row while on is set. It is put in
// place once, so a retry on another goroutine can read the client meanwhile.
type t6821Outage struct {
	failing t3431FailReadTransport
	on      *atomic.Bool
}

func (rt t6821Outage) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.on.Load() {
		return rt.failing.RoundTrip(req)
	}
	return rt.failing.next.RoundTrip(req)
}

// unreadable starts an outage of the row's by-id read and returns its end.
func (rig *t6821Rig) unreadable(relayID int64) (restore func()) {
	client := rig.hub.handoffkeep.client
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	on := &atomic.Bool{}
	on.Store(true)
	client.Transport = t6821Outage{failing: t3431FailReadTransport{next: previous, afterID: relayID - 1}, on: on}
	return func() { on.Store(false) }
}

// dropNode forgets the node of the remote worker lane, which leaves its rows no
// route.
func (rig *t6821Rig) dropNode(machine string) {
	rig.hub.mu.Lock()
	defer rig.hub.mu.Unlock()
	delete(rig.hub.nodes, machine)
}

// age makes the stored row older than the replay window.
func (rig *t6821Rig) age(relayID int64) {
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	for _, row := range rig.fake.rows {
		if row.ID == relayID {
			row.ReceivedAt = rig.clock.Now().Add(-relayReplayMaxAge - time.Hour).Format(time.RFC3339Nano)
		}
	}
}

// A node drops a row having typed nothing and then goes away with budget left:
// no copy can be queued and nothing will exhaust the row, so it is told when the
// route is found missing. Killed by: renaming the row unrouted without making it
// due.
func TestDroppedRowThatLosesItsRouteIsTold(t *testing.T) {
	for _, how := range []t6820Drop{t6820ReadFailed, {report: relayUnconfirmedPromptFailed, drop: relayDroppedRetriesExhausted, harness: "claude"}} {
		t.Run(how.drop, func(t *testing.T) {
			rig := t6821Live(t)
			d := rig.send(t6821Remote)
			rig.report(d, how)
			rig.dropNode("host-b")

			got := rig.one(0)

			if !strings.Contains(got, "event "+t6821Num(d.EventID)+" ") || !strings.Contains(got, ": "+how.drop+". ") || !strings.Contains(got, "on host-b pane w2:pR") {
				t.Fatalf("notice=%q", got)
			}
			for range 3 {
				rig.none(31 * time.Minute)
			}
		})
	}
}

// A row whose retries end by age is told of the failure it had, whether the
// pass finds it aged or the replay walk retires it first. Killed by: waiting at
// the age gate without a notice; clearing the notice at a stale retirement.
func TestAgedFailedRowIsTold(t *testing.T) {
	for name, retire := range map[string]bool{"the pass finds it aged": false, "the replay walk retires it": true} {
		t.Run(name, func(t *testing.T) {
			rig := t6821Live(t)
			d := rig.send(t6821Remote)
			rig.hub.expireRelayAckWindow(relayPendingKey(d.EventID, d.JobID), nil)
			rig.dropNode("host-b")
			rig.none(0)
			rig.none(time.Hour)
			rig.age(d.EventID)
			if retire {
				rig.hub.replayUndeliveredLaneEvents(context.Background())
				if row := rig.fake.deliveredToFor(d.EventID); row != relayReplayRetiredMarker+"stale" {
					t.Fatalf("the walk did not retire the row: %q", row)
				}
			}

			got := rig.one(time.Hour)

			if !strings.Contains(got, "event "+t6821Num(d.EventID)+" ") || !strings.Contains(got, ": ack_timeout. ") {
				t.Fatalf("notice=%q", got)
			}
			rig.none(5 * time.Hour)
		})
	}
	t.Run("a hub that was down past the window", func(t *testing.T) {
		rig := t6821Live(t)
		d := rig.send(t3431Worker)
		rig.report(d, t6820ReadFailed)
		rig.age(d.EventID)
		rig.restart()
		rig.hub.replayUndeliveredLaneEvents(context.Background())

		if got := rig.one(25 * time.Hour); !strings.Contains(got, ": pane_read_failed. ") {
			t.Fatalf("notice=%q", got)
		}
	})
}

// The design lane of a lane with no parent comes from the job record, which a
// restart forgets: a waiting row is kept until the node reports the job again.
// Killed by: dropping the row at the first pass that finds no design lane.
func TestDueRowWaitsForItsDesignLaneAfterRestart(t *testing.T) {
	routes := t6821Routes()
	routes[t3431Worker] = reportRelayRoute{Machine: "host-a", Pane: "w1:pW"}
	rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)
	row := rig.fail()

	// The hub stops before any pass, and comes back two hours later.
	rig.clock.advance(2 * time.Hour)
	rig.restart()
	rig.none(0)
	rig.none(5 * time.Minute)

	rig.job(t3431Job, false, false)
	if got := rig.one(0); !strings.Contains(got, "event "+t6821Num(row)+" ") {
		t.Fatalf("notice=%q", got)
	}
}

// A row withheld from its owner joins while a notice whose reply was lost waits
// to be posted again: the repeat keeps its id and is stored once. Killed by:
// advancing the id's counter on a withheld join.
func TestWithheldJoinKeepsThePendingNoticeID(t *testing.T) {
	rig := t6821Live(t)
	job := laneEventTransportID(t3431Worker, "mergewake-findings-2")
	req := t4270Request(job, t3431Design, "", "implementer")
	req.Args = append(req.Args, "--owner-notify", "never")
	if _, _, err := rig.hub.inbox.store.claimSpawn(t.Context(), req, job, t3431Design, rig.clock.Now()); err != nil {
		t.Fatal(err)
	}
	client := rig.hub.handoffkeep.client
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	var lost atomic.Int32
	client.Transport = t6821LostReply{next: previous, once: &sync.Once{}, lost: &lost}
	t.Cleanup(func() { client.Transport = previous })

	rig.fail()
	rig.none(0)
	withheld := rig.fail()
	rig.none(10 * time.Second)

	if r := rig.row(withheld); r.ToldAt == 0 {
		t.Fatalf("the second row was not withheld: %+v", r)
	}
	if rows := rig.noticeRows(); lost.Load() != 1 || len(rows) != 1 {
		t.Fatalf("lost replies=%d stored notices=%+v, want one", lost.Load(), rows)
	}
}

// A drop after a restart for a row whose obligation was never written, while
// handoffkeep cannot be read, is retried until it is recorded. Killed by:
// giving up at the first failed read.
func TestOrphanedDropIsRetriedAfterAFailedRead(t *testing.T) {
	rig := t6821Live(t)
	d := rig.send(t3431Worker)
	if _, err := rig.hub.inbox.store.db.ExecContext(t.Context(), `DELETE FROM delivery_obligations WHERE relay_id=?`, d.EventID); err != nil {
		t.Fatal(err)
	}
	rig.restart()
	restore := rig.unreadable(d.EventID)

	rig.report(d, t6820Drop{drop: relayDroppedCheckPane, harness: "claude"})
	restore()

	eventually(t, 10*time.Second, "the retried drop to make the row due", func() bool {
		return rig.count(`SELECT count(*) FROM delivery_obligations WHERE relay_id=`+strconv.FormatInt(d.EventID, 10)+` AND notice_due_at>0`) == 1
	})
	if got := rig.one(0); !strings.Contains(got, "event "+t6821Num(d.EventID)+" ") {
		t.Fatalf("notice=%q", got)
	}
}

// A notice stored whose reply was lost leaves the incident unresolved: a member
// confirmed meanwhile does not close it, so the repeat keeps its id and is
// stored once. Killed by: closing the incident and posting the other rows under
// a new one.
func TestUnresolvedNoticeKeepsItsIncidentOpen(t *testing.T) {
	rig := t6821Live(t)
	client := rig.hub.handoffkeep.client
	previous := client.Transport
	if previous == nil {
		previous = http.DefaultTransport
	}
	var lost atomic.Int32
	client.Transport = t6821LostReply{next: previous, once: &sync.Once{}, lost: &lost}
	t.Cleanup(func() { client.Transport = previous })

	member := rig.failOn(t3431Worker)
	rig.failOn(t3431Worker)
	rig.pass(0)
	first := rig.open().ID
	rig.delivered(member)
	rig.pass(0)
	if got := rig.open(); got.ID != first {
		t.Fatalf("an unresolved incident was replaced: %+v", got)
	}
	rig.pass(10 * time.Second)
	rig.pass(time.Second)

	if rows := rig.noticeRows(); lost.Load() != 1 || len(rows) != 1 {
		t.Fatalf("lost replies=%d stored notices=%+v, want one", lost.Load(), rows)
	}
	// Resolved as a duplicate, it closes for the confirmed member.
	if got := rig.incidents()[0]; got.ID != first || got.CloseReason != incidentRecovered || got.Announced != 1 {
		t.Fatalf("incident=%+v", got)
	}
}

// A pass settles the row between the writes of a report: the notice a
// maybe-typed report owes is already due. The trigger stands for that pass; it
// settles the row as a released digest batch as soon as its fault is written.
// Killed by: marking the row due in a second write.
func TestReportMarksItsNoticeInTheSameWrite(t *testing.T) {
	for name, how := range map[string]struct {
		before, report t6820Drop
		fault          string
	}{
		"a report": {report: t6820Drop{report: t6820Maybe.report}, fault: relayDroppedMaybeInPane},
		"a drop": {before: t6820Drop{report: relayUnconfirmedPromptFailed},
			report: t6820Drop{drop: relayDroppedRetriesExhausted, harness: "devin"}, fault: obligationFaultPromptUnproven},
	} {
		t.Run(name, func(t *testing.T) {
			rig := t6821Live(t)
			d := rig.send(t3431Worker)
			rig.report(d, how.before)
			if _, err := rig.hub.inbox.store.db.ExecContext(t.Context(), `CREATE TRIGGER settle_between AFTER UPDATE OF fault ON delivery_obligations
				WHEN NEW.fault='`+how.fault+`' AND NEW.state='owed' BEGIN
				UPDATE delivery_obligations SET state='settled',settle_reason='digest-not-replayed' WHERE id=NEW.id; END`); err != nil {
				t.Fatal(err)
			}

			rig.report(d, how.report)

			if r := rig.row(d.EventID); r.State != obligationSettled || r.DueAt == 0 || r.NoticeAs != relayDroppedMaybeInPane {
				t.Fatalf("row=%+v", r)
			}
		})
	}
}

// The hub holds an orphaned drop past every wait of its schedule: it is
// recorded when handoffkeep answers, however late. Killed by: giving up after
// the last wait.
func TestOrphanedDropIsHeldUntilRecorded(t *testing.T) {
	previousBackoff := orphanedDropBackoff
	orphanedDropBackoff = [4]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { orphanedDropBackoff = previousBackoff })
	rig := t6821Live(t)
	d := rig.send(t3431Worker)
	if _, err := rig.hub.inbox.store.db.ExecContext(t.Context(), `DELETE FROM delivery_obligations WHERE relay_id=?`, d.EventID); err != nil {
		t.Fatal(err)
	}
	rig.restart()
	restore := rig.unreadable(d.EventID)

	rig.report(d, t6820Drop{drop: relayDroppedCheckPane, harness: "claude"})
	eventually(t, 5*time.Second, "more retries than the schedule has waits", func() bool {
		return strings.Count(rig.logs.String(), `msg="orphaned relay drop was not recorded; it is retried"`) > 2*len(orphanedDropBackoff)
	})
	restore()

	eventually(t, 5*time.Second, "the held drop to make the row due", func() bool {
		return rig.count(`SELECT count(*) FROM delivery_obligations WHERE relay_id=`+strconv.FormatInt(d.EventID, 10)+` AND notice_due_at>0`) == 1
	})
}
