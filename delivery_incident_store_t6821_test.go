package panewire

// hk 6821 (hk 4289 T4): the incident store on every backend, and the notice
// columns on a store from before them.

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// The due marks, the join, the notice commit and the closes hold on SQLite,
// Postgres and MySQL alike.
func TestObligationStoreContractIncidents(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		ctx := t.Context()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		key := func(relay int64) string { return "incident\x00" + t6821Num(relay) }
		row := func(relay int64, kind, machine string) newDeliveryObligation {
			return newDeliveryObligation{key: key(relay), relayID: relay, kind: kind, lane: "wk", machine: machine, pane: "w1:p1", attempts: 1}
		}
		for relay := int64(1); relay <= 9; relay++ {
			kind := "lane.event"
			if relay == 9 {
				kind = "job.completed"
			}
			if err := s.createObligation(ctx, row(relay, kind, ""), now, now); err != nil {
				t.Fatal(err)
			}
		}
		member := func(relay int64) (r struct {
			id, incident, due, told, next int64
			state, machine, pane, as      string
		}) {
			t.Helper()
			if err := s.db.QueryRowContext(ctx, s.q(`SELECT id,incident_id,notice_due_at,told_at,next_at,state,machine,pane,fault_reason FROM delivery_obligations WHERE relay_id=?`), relay).
				Scan(&r.id, &r.incident, &r.due, &r.told, &r.next, &r.state, &r.machine, &r.pane, &r.as); err != nil {
				t.Fatal(err)
			}
			return r
		}
		flag := func(relay int64, kind, machine, fault string, at time.Time) {
			t.Helper()
			if err := s.flagObligationRetry(ctx, row(relay, kind, machine), fault, "", at); err != nil {
				t.Fatal(err)
			}
		}
		observed := func(relay int64) openObligation {
			t.Helper()
			o, err := s.retryObligationForKey(ctx, key(relay))
			if err != nil {
				t.Fatal(err)
			}
			return o
		}

		// A maybe-typed report is due at once and records where it happened; a
		// safe one, and a job.* row, are not due.
		flag(1, "lane.event", "host-a", relayDroppedMaybeInPane, now)
		flag(2, "lane.event", "host-a", obligationAckTimeout, now)
		flag(9, "job.completed", "host-a", relayDroppedMaybeInPane, now)
		if r := member(1); r.due != now.UnixMilli() || r.as != relayDroppedMaybeInPane || r.machine != "host-a" || r.pane != "w1:p1" {
			t.Fatalf("maybe-typed report = %+v", r)
		}
		// A report that changes no fault moves no destination, and one that
		// names none leaves it.
		flag(1, "lane.event", "host-b", obligationAckTimeout, now.Add(time.Second))
		flag(2, "lane.event", "", relayDroppedPaneReadFailed, now.Add(time.Second))
		if r := member(1); r.machine != "host-a" || r.due != now.UnixMilli() {
			t.Fatalf("an unrecorded fault moved the row: %+v", r)
		}
		if r := member(2); r.machine != "host-a" || r.due != 0 {
			t.Fatalf("safe report = %+v", r)
		}
		if r := member(9); r.due != 0 {
			t.Fatalf("a job row is due: %+v", r)
		}

		// Exhaustion makes a safe row due under its fault, once.
		if err := s.exhaustObservedObligation(ctx, observed(2), now.Add(time.Minute), now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if r := member(2); r.state != obligationExhausted || r.due != now.Add(time.Minute).UnixMilli() || r.as != relayDroppedPaneReadFailed {
			t.Fatalf("exhausted safe row = %+v", r)
		}
		if err := s.exhaustObligation(ctx, member(2).id, obligationFaultUnconfirmed, now.Add(time.Hour), now); err != nil {
			t.Fatal(err)
		}
		if r := member(2); r.due != now.Add(time.Minute).UnixMilli() || r.as != relayDroppedPaneReadFailed {
			t.Fatalf("a second exhaustion moved the notice: %+v", r)
		}
		// An unrouted row is never due; a synthetic retirement is.
		if err := s.exhaustObligation(ctx, member(3).id, obligationFaultUnrouted, now, now); err != nil {
			t.Fatal(err)
		}
		if err := s.exhaustObligation(ctx, member(4).id, obligationFaultUnconfirmed, now.Add(2*time.Minute), now); err != nil {
			t.Fatal(err)
		}
		if member(3).due != 0 || member(4).due == 0 || member(4).as != obligationFaultUnconfirmed {
			t.Fatalf("unrouted=%+v retired=%+v", member(3), member(4))
		}

		// A drop settles a prompt rejection by harness, ends a notice with the
		// job, and speaks for a row with no fault only as an orphan.
		flag(5, "lane.event", "host-a", obligationFaultPromptUnproven, now)
		if member(5).due != 0 {
			t.Fatal("a prompt report is due before its drop")
		}
		if _, err := s.noteObligationDrop(ctx, 5, "wk", "host-a", true, relayDroppedRetriesExhausted, "devin", now); err != nil {
			t.Fatal(err)
		}
		if r := member(5); r.due == 0 || r.as != relayDroppedMaybeInPane {
			t.Fatalf("devin drop = %+v", r)
		}
		if _, err := s.noteObligationDrop(ctx, 5, "wk", "host-a", true, relayDroppedJobEnded, "", now); err != nil {
			t.Fatal(err)
		}
		if member(5).due != 0 {
			t.Fatal("an ended job's row is still due")
		}
		for _, orphan := range []int64{1, 6} {
			if err := s.markOrphanedDropDue(ctx, orphan, "wk", "host-a", true, relayDroppedCheckPane, now.Add(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.markOrphanedDropDue(ctx, 7, "wk", "host-z", false, relayDroppedCheckPane, now); err != nil {
			t.Fatal(err)
		}
		if member(1).as != relayDroppedMaybeInPane || member(6).as != relayDroppedCheckPane || member(7).due != 0 {
			t.Fatalf("orphans: 1=%+v 6=%+v 7=%+v", member(1), member(6), member(7))
		}
		for relay, want := range map[int64]bool{1: true, 100: false} {
			if got, err := s.trackedRow(ctx, relay); err != nil || got != want {
				t.Fatalf("tracked %d = %v, %v", relay, got, err)
			}
		}

		// Joins come longest untouched first.
		relays := func(rows []noticeMember) (out []int64) {
			for _, r := range rows {
				out = append(out, r.relayID)
			}
			return out
		}
		if err := s.deferNoticeJoin(ctx, member(1).id, now.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		joins, err := s.noticeJoins(ctx, 10)
		if got := relays(joins); err != nil || len(got) != 4 || got[len(got)-1] != 1 {
			t.Fatalf("joins = %v, %v; want rows 1,2,4,6 with the deferred one last", got, err)
		}

		// The key's open incident is reused; another fault opens its own.
		k := incidentKey{design: "design", machine: "host-a", fault: relayDroppedMaybeInPane}
		opened := now.Add(time.Hour)
		first, err := s.joinIncident(ctx, k, "claude", member(1).id, false, opened)
		if err != nil || first != opened.UnixMilli() {
			t.Fatalf("incident id = %d, %v; want the clock", first, err)
		}
		if _, err := s.joinIncident(ctx, k, "claude", member(1).id, false, opened); !errors.Is(err, errNoticeMemberChanged) {
			t.Fatalf("a member joined twice: %v", err)
		}
		if second, err := s.joinIncident(ctx, k, "codex", member(2).id, false, opened.Add(time.Second)); err != nil || second != first {
			t.Fatalf("second member joined %d, %v; want %d", second, err, first)
		}
		other := incidentKey{design: "design", machine: "host-a", fault: relayDroppedCheckPane}
		// Two keys opened in one millisecond keep ids of their own.
		next, err := s.joinIncident(ctx, other, "", member(6).id, true, opened)
		if err != nil || next <= first {
			t.Fatalf("other incident = %d, %v", next, err)
		}
		if r := member(6); r.told != opened.UnixMilli() || r.due != 0 || r.incident != next {
			t.Fatalf("withheld member = %+v", r)
		}
		if r := member(2); r.state != obligationNoticed || r.incident != first {
			t.Fatalf("an exhausted member = %+v", r)
		}
		if member(1).state != obligationOwed {
			t.Fatal("an owed member changed state")
		}
		incidents, err := s.openIncidents(ctx)
		// A withheld row is told without advancing announced.
		if err != nil || len(incidents) != 2 || incidents[0].key != k || incidents[0].announced != 0 || incidents[1].announced != 0 ||
			incidents[0].openedAt != opened.UnixMilli() || incidents[0].lastMemberAt != opened.Add(time.Second).UnixMilli() {
			t.Fatalf("open incidents = %+v, %v", incidents, err)
		}
		members, err := s.incidentMembers(ctx, first)
		if got := relays(members); err != nil || !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("members = %v, %v; want the longest due first", got, err)
		}

		// A posted id is unresolved until its notice is committed.
		if err := s.markNoticePosted(ctx, first, incidents[0].noticeID()); err != nil {
			t.Fatal(err)
		}
		if posted, err := s.openIncidents(ctx); err != nil || !posted[0].unresolved() || posted[1].unresolved() {
			t.Fatalf("posted = %+v, %v", posted, err)
		}

		// A stored notice commits the incident, the slot and the rows once.
		if at, err := s.noticeSlot(ctx, k); err != nil || at != 0 {
			t.Fatalf("slot = %d, %v", at, err)
		}
		told := opened.Add(2 * time.Second)
		notice := storedNotice{eventID: "n0", relayID: 77, told: []int64{member(1).id}}
		if err := s.recordNotice(ctx, incidents[0], notice, told); err != nil {
			t.Fatal(err)
		}
		if err := s.recordNotice(ctx, incidents[0], notice, told.Add(time.Hour)); !errors.Is(err, errNoticeMemberChanged) {
			t.Fatalf("a stale notice committed: %v", err)
		}
		if at, err := s.noticeSlot(ctx, k); err != nil || at != told.UnixMilli() {
			t.Fatalf("slot = %d, %v", at, err)
		}
		if r := member(1); r.told != told.UnixMilli() || r.due != 0 {
			t.Fatalf("told row = %+v", r)
		}
		flag(1, "lane.event", "host-a", relayDroppedMaybeInPane, told.Add(time.Hour))
		if member(1).due != 0 {
			t.Fatal("a told row became due again")
		}
		incidents, err = s.openIncidents(ctx)
		if err != nil || incidents[0].announced != 1 || incidents[0].unresolved() {
			t.Fatalf("after the notice = %+v, %v", incidents, err)
		}
		if err := s.recordNotice(ctx, incidents[0], storedNotice{eventID: "n1"}, told.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		var announced, notices int
		if err := s.db.QueryRowContext(ctx, s.q(`SELECT i.announced,n.notices FROM delivery_incidents i JOIN delivery_notice_slots n ON n.design_lane=i.design_lane AND n.machine=i.machine AND n.fault=i.fault WHERE i.id=?`), first).Scan(&announced, &notices); err != nil || announced != 2 || notices != 2 {
			t.Fatalf("announced=%d notices=%d, %v", announced, notices, err)
		}
		if member(2).due == 0 {
			t.Fatal("a duplicate told a row")
		}

		// A confirmed member is read at once.
		if err := s.deferObligation(ctx, member(2).id, told, told.Add(4*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := s.deferObligation(ctx, member(7).id, told, told.Add(4*time.Hour)); err != nil {
			t.Fatal(err)
		}
		for _, relay := range []int64{2, 7} {
			if err := s.readIncidentMemberNext(ctx, relay); err != nil {
				t.Fatal(err)
			}
		}
		if member(2).next != 0 || member(7).next == 0 {
			t.Fatalf("next: member=%d other=%d", member(2).next, member(7).next)
		}

		// Quiet and all-terminal wait for the due; a changed incident is left.
		incidents, _ = s.openIncidents(ctx)
		stale := incidents[0]
		stale.lastMemberAt--
		for reason, incident := range map[string]deliveryIncident{incidentQuiet: incidents[0], incidentAllTerminal: incidents[0], incidentRecovered: stale} {
			if closed, err := s.closeIncident(ctx, incident, reason, told); err != nil || closed {
				t.Fatalf("%s closed = %v, %v", reason, closed, err)
			}
		}
		if closed, err := s.closeIncident(ctx, incidents[0], incidentRecovered, told); err != nil || !closed {
			t.Fatalf("recovered = %v, %v", closed, err)
		}
		if r := member(2); r.incident != 0 || r.state != obligationExhausted || r.due == 0 {
			t.Fatalf("a detached untold member = %+v", r)
		}
		again, err := s.joinIncident(ctx, k, "claude", member(2).id, false, told.Add(time.Second))
		if err != nil || again == first {
			t.Fatalf("the closed key was not reopened: %d, %v", again, err)
		}
		incidents, _ = s.openIncidents(ctx)
		at := slices.IndexFunc(incidents, func(i deliveryIncident) bool { return i.id == again })
		if closed, err := s.closeIncident(ctx, incidents[at], incidentMoot, told); err != nil || !closed {
			t.Fatalf("moot = %v, %v", closed, err)
		}
		if r := member(2); r.incident != 0 || r.state != obligationExhausted || r.due != 0 {
			t.Fatalf("a moot member = %+v", r)
		}
		var closedRows int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_incidents WHERE state='closed' AND open_slot IS NULL AND closed_at>0`).Scan(&closedRows); err != nil || closedRows != 2 {
			t.Fatalf("closed incidents = %d, %v", closedRows, err)
		}

		// A key wider than the column is one incident too.
		wide := incidentKey{design: strings.Repeat("d", 300), machine: strings.Repeat("m", 300), fault: relayDroppedMaybeInPane}
		a, err := s.joinIncident(ctx, wide, "", member(4).id, false, told)
		if err != nil {
			t.Fatal(err)
		}
		flag(8, "lane.event", "host-a", relayDroppedMaybeInPane, told)
		if b, err := s.joinIncident(ctx, wide, "", member(8).id, false, told); err != nil || b != a {
			t.Fatalf("wide key: %d then %d, %v", a, b, err)
		}

		// A route loss keeps the fault it replaces: a node's drop is due at once,
		// an ack timeout when its retries end by exhaustion, age or a stale
		// retirement.
		for relay := int64(20); relay <= 28; relay++ {
			if err := s.createObligation(ctx, row(relay, "lane.event", "host-a"), now, now); err != nil {
				t.Fatal(err)
			}
		}
		for relay, fault := range map[int64]string{20: relayDroppedPaneReadFailed, 21: obligationAckTimeout, 22: obligationAckTimeout, 23: obligationAckTimeout, 24: relayDroppedRetriesExhausted, 25: obligationAckTimeout} {
			flag(relay, "lane.event", "host-a", fault, told)
		}
		for relay := int64(20); relay <= 23; relay++ {
			for range 2 { // a second rename changes nothing
				if err := s.markObligationUnrouted(ctx, observed(relay), told, told); err != nil {
					t.Fatal(err)
				}
			}
		}
		if r := member(20); r.due != told.UnixMilli() || r.as != relayDroppedPaneReadFailed {
			t.Fatalf("a dropped row that lost its route = %+v", r)
		}
		if r := member(21); r.due != 0 {
			t.Fatalf("an ack timeout is due at its route loss: %+v", r)
		}
		if err := s.exhaustObservedObligation(ctx, observed(21), told.Add(time.Second), told); err != nil {
			t.Fatal(err)
		}
		if err := s.endUnprovenNotice(ctx, member(22).id, told.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := s.closeObligation(ctx, member(23).id, obligationSettled, obligationSettledStale, told.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		for relay, after := range map[int64]time.Duration{21: time.Second, 22: 2 * time.Second, 23: 3 * time.Second} {
			if r := member(relay); r.due != told.Add(after).UnixMilli() || r.as != obligationAckTimeout {
				t.Fatalf("row %d at the end of its retries = %+v", relay, r)
			}
		}
		// A safe fault on a released digest batch, a row with no failure, a
		// receiver's read, and a row exhausted earlier tell nothing.
		for relay, reason := range map[int64]string{24: obligationSettledDigest, 25: obligationSettledDigest, 26: obligationSettledStale} {
			if err := s.closeObligation(ctx, member(relay).id, obligationSettled, reason, told); err != nil {
				t.Fatal(err)
			}
		}
		flag(27, "lane.event", "host-a", relayDroppedPaneReadFailed, told)
		if err := s.closeObligation(ctx, member(27).id, obligationSettled, obligationSettledInbox, told); err != nil {
			t.Fatal(err)
		}
		flag(28, "lane.event", "host-a", relayDroppedPaneReadFailed, told)
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET state=? WHERE relay_id=28`), obligationExhausted); err != nil {
			t.Fatal(err)
		}
		if err := s.endUnprovenNotice(ctx, member(28).id, told); err != nil {
			t.Fatal(err)
		}
		if err := s.closeObligation(ctx, member(28).id, obligationSettled, obligationSettledStale, told); err != nil {
			t.Fatal(err)
		}
		for _, relay := range []int64{24, 25, 26, 27, 28} {
			if r := member(relay); r.due != 0 {
				t.Fatalf("row %d is due a notice: %+v", relay, r)
			}
		}

		// A closed row needs no notice, except a released digest batch.
		if err := s.closeObligation(ctx, member(4).id, obligationSettled, obligationSettledDigest, told); err != nil {
			t.Fatal(err)
		}
		if err := s.closeObligation(ctx, member(8).id, obligationConfirmed, "", told); err != nil {
			t.Fatal(err)
		}
		if member(4).due == 0 || member(8).due != 0 {
			t.Fatalf("digest=%+v confirmed=%+v", member(4), member(8))
		}
		if err := s.forgetNotice(ctx, member(4).id); err != nil || member(4).due != 0 {
			t.Fatalf("forgotten = %+v, %v", member(4), err)
		}
	})
}

// A SQLite store from before the notice columns gains them at open, and its rows
// are due nothing.
func TestDeliveryNoticeColumnsAddedToOlderStore_t6821(t *testing.T) {
	path := freshLaneInboxFile(t)
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_700_000_000_000).UTC()
	if err := s.createObligation(t.Context(), newDeliveryObligation{key: "k", relayID: 1, kind: "lane.event", lane: "wk"}, now, now); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`DROP INDEX delivery_obligations_incident`, `ALTER TABLE delivery_obligations DROP COLUMN notice_due_at`, `ALTER TABLE delivery_obligations DROP COLUMN told_at`} {
		if _, err := s.db.ExecContext(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	for range 2 { // the second open finds them
		s, err = openLaneInboxStore(path)
		if err != nil {
			t.Fatal(err)
		}
		var rows, indexes int
		if err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM delivery_obligations WHERE notice_due_at=0 AND told_at=0`).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("rows = %d, %v", rows, err)
		}
		if err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_master WHERE type='index' AND name='delivery_obligations_incident'`).Scan(&indexes); err != nil || indexes != 1 {
			t.Fatalf("indexes = %d, %v", indexes, err)
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A copy from a source without the notice columns copies the others; any other
// difference in columns still refuses it.
func TestStoreCopyColumnsFromOlderSource_t6821(t *testing.T) {
	target := []string{"id", "lane", "notice_due_at", "told_at"}
	for _, tc := range []struct {
		table  string
		source []string
		same   bool
	}{
		{"delivery_obligations", []string{"id", "lane"}, true},
		{"delivery_obligations", target, true},
		{"delivery_obligations", []string{"id", "notice_due_at", "told_at"}, false},
		{"delivery_obligations", []string{"id", "lane", "extra"}, false},
		{"delivery_incidents", []string{"id", "lane"}, false},
	} {
		got := storeCopyColumnsFrom(tc.table, target, tc.source)
		if storeCopySameColumns(got, tc.source) != tc.same {
			t.Errorf("%s from %v: columns %v, same=%v", tc.table, tc.source, got, !tc.same)
		}
	}
	if !slices.Equal(target, []string{"id", "lane", "notice_due_at", "told_at"}) {
		t.Fatalf("the target's columns were changed: %v", target)
	}
}

// CI runs this against both database services.
func TestHubStoreCopyFromOlderSourceWithoutNoticeColumns_t6821(t *testing.T) {
	mysqlConfig, mysqlURL := hubMySQLTestDatabase(t)
	_, pgURL := hubPostgresTestSchema(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	// The source's hub has started on a build with hk 6433.
	recordSpawnBoundBackfill(t, source)
	now := time.UnixMilli(1_700_000_000_000).UTC()
	if err := source.createObligation(t.Context(), newDeliveryObligation{key: "k", relayID: 1, kind: "lane.event", lane: "wk"}, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(t.Context(), `ALTER TABLE delivery_obligations DROP KEY delivery_obligations_incident, DROP COLUMN notice_due_at, DROP COLUMN told_at`); err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := copyHubStore(t.Context(), mysqlURL, pgURL, "", false, false, &report); err != nil {
		t.Fatalf("copy from an older source: %v\n%s", err, report.String())
	}
	if !strings.Contains(report.String(), "delivery_obligations source=1 ") {
		t.Fatalf("report:\n%s", report.String())
	}
}
