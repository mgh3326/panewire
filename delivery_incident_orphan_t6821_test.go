package panewire

// hk 6821: a row whose lane's job is only orphaned -- its node away -- waits
// for the node. A job that comes back while an obligation pass runs must not
// find its rows settled by what the pass read before.

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// returnAfterJoin brings the worker lane's orphaned job back while a pass runs:
// at the first handoffkeep request after the pass has joined row to an
// incident, which is after the pass read the row's lane. Each of also brings
// back something else that went away with the job.
func (rig *t6821Rig) returnAfterJoin(row int64, also ...func()) (returned *atomic.Bool) {
	returned = new(atomic.Bool)
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	rig.fake.observe = func(string, string) {
		var incident int64
		if err := rig.hub.inbox.store.db.QueryRow(`SELECT incident_id FROM delivery_obligations WHERE relay_id=?`, row).Scan(&incident); err != nil || incident == 0 {
			return
		}
		if returned.CompareAndSwap(false, true) {
			for _, bringBack := range also {
				bringBack()
			}
			rig.job(t3431Job, false, false)
		}
	}
	return returned
}

// keptAndTold fails unless the pass that saw the job return left row waiting in
// an open incident without naming it, and the next notice names it.
func (rig *t6821Rig) keptAndTold(row int64, returned *atomic.Bool) {
	rig.t.Helper()
	for _, notice := range rig.told(0) {
		if strings.Contains(notice.Text, "event "+t6821Num(row)+" ") {
			rig.t.Fatalf("the pass that read the job orphaned named its row: %s", notice.Text)
		}
	}
	if !returned.Load() {
		rig.t.Fatal("the job did not return during the pass")
	}
	r := rig.row(row)
	if r.ToldAt != 0 || r.DueAt == 0 {
		rig.t.Fatalf("a returned job's row no longer waits: %+v\n%s", r, rig.logs.String())
	}
	if open := rig.count(`SELECT COUNT(*) FROM delivery_incidents WHERE id=? AND state=?`, r.Incident, incidentStateOpen); open != 1 {
		rig.t.Fatalf("the row's incident is not open: %+v", rig.incidents())
	}
	named := 0
	for _, notice := range rig.told(31 * time.Minute) {
		if strings.Contains(notice.Text, "event "+t6821Num(row)+" ") {
			named++
		}
	}
	if named != 1 {
		rig.t.Fatalf("notices naming row %d after the return = %d, want 1\n%s", row, named, rig.logs.String())
	}
}

// The key's slot is held, so the pass goes from joining the row straight to
// closing the incident, and the job returns in between.
func TestJobReturningBeforeTheCloseKeepsItsRow(t *testing.T) {
	rig := t6821Live(t)
	rig.fail()
	rig.one(0)
	rig.job(t3431Job, false, true)
	row := rig.fail()
	// Another host's row, joined after it: reading it is when the job returns.
	rig.failOn(t6821Remote)
	rig.keptAndTold(row, rig.returnAfterJoin(row))
}

// A node drops the row with its job live, then goes away: the row, left with no
// route, is due, and its job orphaned. The node comes back during the pass,
// after the pass read the lane and before it posts the row's notice.
func TestJobReturningBeforeTheNoticeKeepsItsRow(t *testing.T) {
	rig := t6821Live(t)
	d := rig.send(t3431Worker)
	rig.report(d, t6820ReadFailed)
	// Another host's row: reading it for a join or a notice is a request between.
	rig.failOn(t6821Remote)
	rig.hub.mu.Lock()
	node := rig.hub.nodes["host-a"]
	rig.hub.mu.Unlock()
	rig.dropNode("host-a")
	rig.job(t3431Job, false, true)
	rig.keptAndTold(d.EventID, rig.returnAfterJoin(d.EventID, func() {
		rig.hub.mu.Lock()
		defer rig.hub.mu.Unlock()
		rig.hub.nodes["host-a"] = node
	}))
}

// A node's own report ends the wait: with the job, the row is told; without it,
// the job completed and the row is settled.
func TestOrphanedNoticeFollowsTheNodeReport(t *testing.T) {
	unconfirmed := func(rig *t6821Rig, wait time.Duration) (texts []string) {
		for _, directive := range rig.told(wait) {
			if strings.Contains(directive.Text, "[delivery unconfirmed]") {
				texts = append(texts, directive.Text)
			}
		}
		return texts
	}
	for _, parentless := range []bool{false, true} {
		for _, back := range []bool{true, false} {
			name := map[bool]string{false: "a parent", true: "no parent"}[parentless] + map[bool]string{false: ", without the job", true: ", with the job"}[back]
			t.Run(name, func(t *testing.T) {
				routes := t6821Routes()
				if parentless {
					routes[t3431Worker] = reportRelayRoute{Machine: "host-a", Pane: "w1:pW"}
				}
				rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}
				rig.task(t3431Task, "in_progress", "started")
				rig.job(t3431Job, false, true)
				row := rig.fail()
				// Well past noticeDesignWait.
				for range 3 {
					if got := unconfirmed(rig, noticeDesignWait); len(got) != 0 {
						t.Fatalf("told while the node was away: %q", got)
					}
				}
				if r := rig.row(row); r.DueAt == 0 || r.ToldAt != 0 || r.Incident == 0 {
					t.Fatalf("an orphaned job's row does not wait in an incident: %+v\n%s", r, rig.logs.String())
				}
				var active []HubActiveJob
				if back {
					active = []HubActiveJob{{JobID: t3431Job, AgentLabel: t3431Worker, Epoch: 1, OwnerLane: t3431Design}}
				}
				rig.hub.observeActiveJobs("host-b", active, rig.clock.Now())
				got := unconfirmed(rig, time.Minute)
				r := rig.row(row)
				if back {
					if len(got) != 1 || !strings.Contains(got[0], "event "+t6821Num(row)+" ") {
						t.Fatalf("notices=%q, want one naming row %d\n%s", got, row, rig.logs.String())
					}
				} else if len(got) != 0 || r.DueAt != 0 || r.ToldAt == 0 {
					t.Fatalf("a completed job's row: notices=%q row=%+v", got, r)
				}
			})
		}
	}
}

// An orphaned job that names an owner with no route holds the row too: the
// route may be back by the time the node is.
func TestOrphanedJobWithAnUnroutedOwnerHoldsItsRow(t *testing.T) {
	routes := t6821Routes()
	routes[t3431Worker] = reportRelayRoute{Machine: "host-a", Pane: "w1:pW"}
	rig := &t6821Rig{t3431Rig: t3431StartWith(t, t3431LiveJobWithTask(), routes, freshLaneInboxFile(t))}
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, true)
	rig.hub.mu.Lock()
	rig.hub.jobs[t3431Job].OwnerLane = "wk-t6821-unrouted"
	rig.hub.mu.Unlock()
	row := rig.fail()
	for range 3 {
		rig.none(noticeDesignWait)
	}
	if r := rig.row(row); r.DueAt == 0 || r.ToldAt != 0 {
		t.Fatalf("the row no longer waits: %+v\n%s", r, rig.logs.String())
	}
	rig.job(t3431Job, false, false) // back, and named by a routed owner
	if got := rig.one(time.Minute); !strings.Contains(got, "event "+t6821Num(row)+" ") {
		t.Fatalf("notice=%q, want row %d", got, row)
	}
}

// An orphaned job whose task has ended has nobody to come back for; one whose
// task cannot be read is told, as any unreadable job is.
func TestOrphanedJobIsHeldOnlyForAnOpenTask(t *testing.T) {
	t.Run("an ended task", func(t *testing.T) {
		rig := t6821Live(t)
		rig.task(t3431Task, "done", "completed")
		rig.job(t3431Job, false, true)
		row := rig.fail()
		rig.none(0)
		if r := rig.row(row); r.DueAt != 0 || r.ToldAt == 0 {
			t.Fatalf("an orphaned job's row with an ended task = %+v\n%s", r, rig.logs.String())
		}
	})
	t.Run("an unreadable task", func(t *testing.T) {
		rig := t6821Start(t, t3431LiveJobWithTask())
		rig.job(t3431Job, false, true)
		row := rig.fail()
		if got := rig.one(0); !strings.Contains(got, "event "+t6821Num(row)+" ") {
			t.Fatalf("notice=%q, want row %d", got, row)
		}
	})
}

// A moot close needs every lane's answer to be final: one completed lane and
// one orphaned lane keep the incident open.
func TestMootCloseWaitsForAnOrphanedLane(t *testing.T) {
	const second = "hk6821-second"
	rig := t6821Live(t)
	job := func(completed, orphaned bool) {
		rig.hub.mu.Lock()
		defer rig.hub.mu.Unlock()
		rig.hub.jobs[second] = &hubJobRecord{
			HubActiveJob: HubActiveJob{JobID: second, AgentLabel: t6821Second, Epoch: 1, OwnerLane: t3431Design},
			Node:         "host-b", LastSeen: rig.clock.Now(), Completed: completed, Orphaned: orphaned, FencedNodes: map[string]uint64{},
		}
	}
	job(false, true)
	rig.fail()
	rig.one(0)
	row := rig.failOn(t6821Second).EventID
	rig.none(0)
	rig.job(t3431Job, true, false)
	for range 3 {
		rig.none(31 * time.Minute)
	}
	if got := rig.open(); got.Members != 2 {
		t.Fatalf("open incident = %+v, want both rows", got)
	}
	job(true, false)
	rig.none(31 * time.Minute)
	if got := rig.incidents(); len(got) != 1 || got[0].CloseReason != incidentMoot {
		t.Fatalf("incidents=%+v, want one moot close", got)
	}
	if r := rig.row(row); r.DueAt != 0 || r.ToldAt == 0 {
		t.Fatalf("the completed lanes' row = %+v", r)
	}
}
