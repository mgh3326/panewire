package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// These tests pin the event-identity repair for job.* relay rows: the durable
// event file name is the event's identity, so the same job's separate
// completion rounds are separate outbox rows and separate notifications,
// while a resend of one event still folds into its own row.

func relaySentRowCount(t *testing.T, store *Store, jobID string) int {
	t.Helper()
	var rows int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM relay_sent WHERE job_id=?`, jobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func relaySentEventIDs(t *testing.T, store *Store, jobID string) []string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), `SELECT event_id FROM relay_sent WHERE job_id=? ORDER BY event_id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func relaySentUnpersistedCount(t *testing.T, store *Store, jobID string) int {
	t.Helper()
	var rows int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM relay_sent WHERE job_id=? AND persisted_at IS NULL`, jobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

const relayEventIDLanes = `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`

const relayEventIDBody = `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"report.md","report_last_line":"done"}`

// Three completion rounds for one job share the five historic key fields. Each
// must still produce its own outbox row and its own pane notification, and a
// resend of any of them must fold into that event's own row.
func TestJobCompletionRoundsProduceDistinctOutboxRowsAndNotifications(t *testing.T) {
	inbox := t.TempDir()
	for _, name := range []string{"00001-job.completed.json", "00002-job.completed.json", "00003-job.completed.json"} {
		r20WriteEvent(t, inbox, "job-rounds", name, relayEventIDBody, time.Time{})
	}
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, relayEventIDLanes, client, 8)
	store := NewMemoryStore(t)
	defer store.Close()
	node := r20Node(inbox, store)

	events := node.jobCompletionEvents()
	if len(events) != 3 {
		t.Fatalf("the scan offered %d events for three completion rounds, want 3", len(events))
	}
	seen := map[string]bool{}
	for _, event := range events {
		if event.relayKey.EventID == "" {
			t.Fatalf("a job.* outbox key carried no event identity: %+v", event.relayKey)
		}
		if seen[event.relayKey.EventID] {
			t.Fatalf("two rounds collapsed onto event_id %q", event.relayKey.EventID)
		}
		seen[event.relayKey.EventID] = true
	}

	injections := 0
	for _, event := range events {
		injected, acknowledgements := r20t7Deliver(t, hub, agent, node, event)
		injections += injected
		if len(acknowledgements) != 1 || acknowledgements[0].ProducerEventID != event.relayKey.EventID {
			t.Fatalf("event %q acknowledgements=%+v, want one naming its producer event id", event.relayKey.EventID, acknowledgements)
		}
		node.commitRelaySent(event)
	}
	if injections != 3 {
		t.Fatalf("three completion rounds injected %d notes, want 3", injections)
	}
	if rows := relaySentRowCount(t, store, "job-rounds"); rows != 3 {
		t.Fatalf("relay_sent rows=%d, want 3 (one per event, not one per job)", rows)
	}
	ids := relaySentEventIDs(t, store, "job-rounds")
	if len(ids) != 3 || ids[0] != "00001-job.completed.json" || ids[1] != "00002-job.completed.json" || ids[2] != "00003-job.completed.json" {
		t.Fatalf("event_id values=%v, want the three event file names", ids)
	}
	if unpersisted := relaySentUnpersistedCount(t, store, "job-rounds"); unpersisted != 0 {
		t.Fatalf("persisted_at NULL rows=%d, want 0 after the acknowledgements", unpersisted)
	}
	// handoffkeep's five-field idempotency index still folds the three rounds
	// into one durable row; per-event identity lives in the hub dedupe key and
	// the producer_event_id it echoes back.
	if fake.rowCount() != 1 {
		t.Fatalf("handoffkeep rows=%d, want 1 folded row", fake.rowCount())
	}

	// A restart must not resurrect or duplicate any of the three.
	restarted := r20Node(inbox, store)
	if events := restarted.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a restart re-offered %d persisted events, want 0", len(events))
	}
	if rows := relaySentRowCount(t, store, "job-rounds"); rows != 3 {
		t.Fatalf("relay_sent rows=%d after restart, want 3", rows)
	}
}

// A resend of an event already sent folds into its own row: no extra rows, no
// extra notifications, and the row is retired by exactly its own
// acknowledgement. This is the R20 restart-retransmission protection kept
// intact while separate rounds stay distinct.
func TestJobEventResendKeepsOneRowAndOneNote(t *testing.T) {
	inbox := t.TempDir()
	r20WriteEvent(t, inbox, "job-resend", "00001-job.completed.json", relayEventIDBody, time.Time{})
	r20WriteEvent(t, inbox, "job-resend", "00002-job.completed.json", relayEventIDBody, time.Time{})
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, relayEventIDLanes, client, 8)
	store := NewMemoryStore(t)
	defer store.Close()

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 2 {
		t.Fatalf("the scan offered %d events, want 2", len(events))
	}
	// The sender acknowledgements are lost in flight - the hub answers them
	// but the node never applies them, which is what makes a restart resend.
	for _, event := range events {
		wire, err := json.Marshal(hubClientWireEvent(event))
		if err != nil {
			t.Fatal(err)
		}
		hub.handleAgentMessage("host-a", "fixture", agent, wire)
		if injected := drainRelays(agent); injected != 1 {
			t.Fatalf("first send of %q injected %d times, want 1", event.relayKey.EventID, injected)
		}
		drainPersisted(agent)
		node.commitRelaySent(event)
	}

	// A restarted node re-offers exactly the two unpersisted events, flagged
	// as replays, under the same event identities.
	aged := time.Now().Add(-2 * relayOutboxBackoff)
	for _, event := range events {
		if err := store.RecordRelaySent(context.Background(), event.relayKey, aged); err != nil {
			t.Fatal(err)
		}
	}
	restarted := r20Node(inbox, store)
	resends := restarted.jobCompletionEvents()
	if len(resends) != 2 {
		t.Fatalf("the restart offered %d events, want the 2 unpersisted rows", len(resends))
	}
	for index, event := range resends {
		if event.relayKey.EventID != events[index].relayKey.EventID {
			t.Fatalf("resend %d re-keyed the file to %q, want %q", index, event.relayKey.EventID, events[index].relayKey.EventID)
		}
		var payload struct {
			Replay bool `json:"replay"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || !payload.Replay {
			t.Fatalf("a resent record is missing replay:true: %s", event.Payload)
		}
		injected, acknowledgements := r20t7Deliver(t, hub, agent, restarted, event)
		if injected != 0 {
			t.Fatalf("a resend re-injected the pane %d times, want 0", injected)
		}
		if len(acknowledgements) != 1 || acknowledgements[0].ProducerEventID != event.relayKey.EventID {
			t.Fatalf("resend %q acknowledgements=%+v", event.relayKey.EventID, acknowledgements)
		}
		restarted.commitRelaySent(event)
	}
	if rows := relaySentRowCount(t, store, "job-resend"); rows != 2 {
		t.Fatalf("relay_sent rows=%d after resends, want 2", rows)
	}
	if unpersisted := relaySentUnpersistedCount(t, store, "job-resend"); unpersisted != 0 {
		t.Fatalf("persisted_at NULL rows=%d after re-acknowledgement, want 0", unpersisted)
	}

	settled := r20Node(inbox, store)
	if events := settled.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a settled restart re-offered %d events, want 0", len(events))
	}
}

// An acknowledgement names one event. Retiring round one's row must leave
// round two outstanding and resendable.
func TestJobEventAcknowledgementRetiresOnlyItsOwnRound(t *testing.T) {
	inbox := t.TempDir()
	r20WriteEvent(t, inbox, "job-acks", "00001-job.completed.json", relayEventIDBody, time.Time{})
	r20WriteEvent(t, inbox, "job-acks", "00002-job.completed.json", relayEventIDBody, time.Time{})
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, relayEventIDLanes, client, 8)
	store := NewMemoryStore(t)
	defer store.Close()

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 2 {
		t.Fatalf("the scan offered %d events, want 2", len(events))
	}
	for index, event := range events {
		wire, err := json.Marshal(hubClientWireEvent(event))
		if err != nil {
			t.Fatal(err)
		}
		hub.handleAgentMessage("host-a", "fixture", agent, wire)
		drainRelays(agent)
		acknowledgements := drainPersisted(agent)
		// Only round one's acknowledgement is applied; round two's is lost.
		if index == 0 {
			for _, ack := range acknowledgements {
				node.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID, ProducerEventID: ack.ProducerEventID})
			}
		}
		node.commitRelaySent(event)
	}
	if unpersisted := relaySentUnpersistedCount(t, store, "job-acks"); unpersisted != 1 {
		t.Fatalf("persisted_at NULL rows=%d, want exactly the unacknowledged round", unpersisted)
	}

	aged := time.Now().Add(-2 * relayOutboxBackoff)
	for _, event := range events {
		if err := store.RecordRelaySent(context.Background(), event.relayKey, aged); err != nil {
			t.Fatal(err)
		}
	}
	restarted := r20Node(inbox, store)
	resends := restarted.jobCompletionEvents()
	if len(resends) != 1 || resends[0].relayKey.EventID != "00002-job.completed.json" {
		t.Fatalf("the restart offered %+v, want only the unacknowledged second round", resends)
	}
}

func relaySentSuppressedIDs(t *testing.T, store *Store, jobID string) []string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), `SELECT event_id FROM relay_sent WHERE job_id=? AND suppressed_at IS NOT NULL ORDER BY event_id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A relay_sent row written before event_id existed records a send the file's
// timestamp can no longer prove - and cannot prove which file it recorded,
// since the five shared fields are identical across rounds. Rather than
// guessing, the per-node migration cutoff suppresses every pre-cutoff file:
// the event is never sent, the file gets a suppressed audit row under its own
// identity, and the legacy row stays inert - adopted by no one, resent by no
// one.
func TestJobEventLegacyRowIsSuppressedNotAdopted(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-time.Hour)
	r20WriteEvent(t, inbox, "job-legacy", "00001-job.completed.json", relayEventIDBody, old)
	legacyKey := relayOutboxKey{Kind: "job.completed", JobID: "job-legacy", Epoch: 1, ReportPath: "report.md"}
	store := r20MigratedStore(t, []relayOutboxKey{legacyKey})
	defer store.Close()

	node := r20Node(inbox, store)
	if events := node.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a pre-cutoff event was offered %d times, want 0", len(events))
	}
	if got := relaySentSuppressedIDs(t, store, "job-legacy"); len(got) != 1 || got[0] != "00001-job.completed.json" {
		t.Fatalf("the pre-cutoff file was not recorded as suppressed: %v", got)
	}
	legacy, err := store.RelayOutboxState(context.Background(), legacyKey)
	if err != nil || !legacy.Found || legacy.Suppressed {
		t.Fatalf("the legacy row was touched: state=%+v err=%v", legacy, err)
	}
	var legacyEventID string
	if err := store.db.QueryRowContext(context.Background(), `SELECT event_id FROM relay_sent WHERE kind=? AND job_id=? AND event_id=''`, "job.completed", "job-legacy").Scan(&legacyEventID); err != nil {
		t.Fatalf("the legacy row was adopted or removed: %v", err)
	}
	if rows := relaySentRowCount(t, store, "job-legacy"); rows != 2 {
		t.Fatalf("relay_sent rows=%d, want the inert legacy row plus the suppressed audit row", rows)
	}

	// Restarting changes nothing: the suppressed row is terminal.
	if events := r20Node(inbox, store).jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a restart re-offered %d suppressed events, want 0", len(events))
	}
}

// Deployment must not replay the retained backlog as fresh notifications:
// every event file older than this node's migration cutoff is suppressed,
// regardless of how many there are.
func TestJobEventPreMigrationBacklogEmitsZero(t *testing.T) {
	for _, backlog := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("N=%d", backlog), func(t *testing.T) {
			inbox := t.TempDir()
			old := time.Now().Add(-time.Hour)
			for index := 1; index <= backlog; index++ {
				r20WriteEvent(t, inbox, "job-backlog", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, old.Add(time.Duration(index)*time.Minute))
			}
			// A file older than the 24h scan window is dropped before the
			// cutoff even sees it; it must not count as a send either.
			r20WriteEvent(t, inbox, "job-backlog", "00999-job.completed.json", relayEventIDBody, time.Now().Add(-25*time.Hour))

			fake, client, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			hub, agent := r20t5Hub(t, relayEventIDLanes, client, 8)
			// The node genuinely migrated: its database carried a legacy row
			// the old binary wrote, so the rekey stamped the cutoff.
			store := r20MigratedStore(t, []relayOutboxKey{{Kind: "job.completed", JobID: "job-backlog", Epoch: 1, ReportPath: "report.md"}})
			defer store.Close()

			events := r20Node(inbox, store).jobCompletionEvents()
			if len(events) != 0 {
				t.Fatalf("a pre-cutoff backlog of %d offered %d events, want 0", backlog, len(events))
			}
			if got := relaySentSuppressedIDs(t, store, "job-backlog"); len(got) != backlog {
				t.Fatalf("suppressed rows=%v, want %d audit rows (one per in-window file)", got, backlog)
			}
			injections := 0
			for _, event := range events {
				injected, _ := r20t7Deliver(t, hub, agent, r20Node(inbox, store), event)
				injections += injected
			}
			if injections != 0 || fake.rowCount() != 0 {
				t.Fatalf("suppressed backlog injected=%d handoffkeep=%d, want 0", injections, fake.rowCount())
			}
		})
	}
}

// The cutoff's near edge is a grace window: an event emitted minutes before
// this node's migration is fresh news from the old binary's last seconds, and
// suppressing it would lose the one notification that job ever gets. Five of
// them send, because the window bounds the blast radius by time, not count.
func TestJobEventWithinMigrationGraceStillSends(t *testing.T) {
	inbox := t.TempDir()
	recent := time.Now().Add(-time.Minute)
	for index := 1; index <= 5; index++ {
		r20WriteEvent(t, inbox, "job-grace", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, recent.Add(time.Duration(index)*time.Second))
	}
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, relayEventIDLanes, client, 8)
	store := r20MigratedStore(t, []relayOutboxKey{{Kind: "job.completed", JobID: "job-grace", Epoch: 1, ReportPath: "report.md"}})
	defer store.Close()

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 5 {
		t.Fatalf("the grace window offered %d events, want all 5", len(events))
	}
	injections := 0
	for _, event := range events {
		injected, _ := r20t7Deliver(t, hub, agent, node, event)
		injections += injected
		node.commitRelaySent(event)
	}
	if injections != 5 {
		t.Fatalf("grace-window events injected %d notes, want 5", injections)
	}
	if got := relaySentSuppressedIDs(t, store, "job-grace"); len(got) != 0 {
		t.Fatalf("in-window events were suppressed: %v", got)
	}
}

// The boundary is exact: one second inside the window sends, one second
// outside suppresses.
func TestJobEventMigrationGraceBoundary(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	migrated := time.Now().Truncate(time.Second)
	r20AgeMigrationStamp(t, store, migrated)
	cutoff := migrated.Add(-relayMigrationGrace)

	r20WriteEvent(t, inbox, "job-edge", "00001-job.completed.json", relayEventIDBody, cutoff.Add(-time.Second))
	r20WriteEvent(t, inbox, "job-edge", "00002-job.completed.json", relayEventIDBody, cutoff.Add(time.Second))

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 1 || events[0].relayKey.EventID != "00002-job.completed.json" {
		t.Fatalf("the boundary offered %+v, want only the in-window event", events)
	}
	if got := relaySentSuppressedIDs(t, store, "job-edge"); len(got) != 1 || got[0] != "00001-job.completed.json" {
		t.Fatalf("the out-of-window event was not suppressed: %v", got)
	}
}

// The cutoff is per-node: it is the instant THAT node's store gained event
// identity, minus the grace. Two stores migrating an hour apart disagree on
// the same file - the early node sees a post-migration event to send, the
// late node sees its own pre-migration backlog to suppress.
func TestJobEventMigrationCutoffIsPerNode(t *testing.T) {
	inbox := t.TempDir()
	// The event predates the late node's migration but postdates the early
	// node's cutoff.
	r20WriteEvent(t, inbox, "job-pernode", "00001-job.completed.json", relayEventIDBody, time.Now().Add(-30*time.Minute))

	seed := []relayOutboxKey{{Kind: "job.completed", JobID: "job-pernode", Epoch: 1, ReportPath: "report.md"}}
	early := r20MigratedStore(t, seed)
	defer early.Close()
	r20AgeMigrationStamp(t, early, time.Now().Add(-time.Hour))
	late := r20MigratedStore(t, seed)
	defer late.Close()

	migratedEarly, okEarly, errEarly := early.RelayEventIDMigrationAt(context.Background())
	migratedLate, okLate, errLate := late.RelayEventIDMigrationAt(context.Background())
	if errEarly != nil || errLate != nil || !okEarly || !okLate {
		t.Fatalf("migration stamps: early=%s(%v,%v) late=%s(%v,%v)", migratedEarly, okEarly, errEarly, migratedLate, okLate, errLate)
	}
	if !migratedEarly.Before(migratedLate) {
		t.Fatalf("the fixture needs distinct migration times: early=%s late=%s", migratedEarly, migratedLate)
	}

	if events := r20Node(inbox, early).jobCompletionEvents(); len(events) != 1 {
		t.Fatalf("the early-migrated node offered %d events, want 1 (the file postdates its cutoff)", len(events))
	}
	if events := r20Node(inbox, late).jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("the late-migrated node offered %d events, want 0 (the file is its own backlog)", len(events))
	}
	if got := relaySentSuppressedIDs(t, late, "job-pernode"); len(got) != 1 {
		t.Fatalf("the late node recorded %v suppressed rows, want the one file", got)
	}
}

// A store that never ran the rekey migration has no stamp and therefore no
// cutoff: a fresh database - a new node, a recreated state.db, a relocated
// --db path - must send every retained completion rather than suppressing it.
// This is the inverse of the relaytest2 A2 reproduction
// (TestR3_A2_DBRecreateSuppressesOwedCompletions): same fixture shape,
// opposite assertion.
func TestJobEventUnmigratedStoreNeverSuppresses(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-time.Hour)
	for index := 1; index <= 5; index++ {
		r20WriteEvent(t, inbox, "job-fresh", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, old.Add(time.Duration(index)*time.Minute))
	}
	store := NewMemoryStore(t)
	defer store.Close()

	if migratedAt, ok, err := store.RelayEventIDMigrationAt(context.Background()); err != nil || ok {
		t.Fatalf("a fresh store reported a migration stamp %s(%v) err=%v, want none", migratedAt, ok, err)
	}
	events := r20Node(inbox, store).jobCompletionEvents()
	if len(events) != 5 {
		t.Fatalf("an unmigrated store offered %d of the backlog, want all 5", len(events))
	}
	if got := relaySentSuppressedIDs(t, store, "job-fresh"); len(got) != 0 {
		t.Fatalf("an unmigrated store suppressed events: %v", got)
	}
}

// Deleting state.db and letting the daemon recreate it - or pointing --db at
// a new path - must not mint a migration stamp out of thin air. The recreated
// database carries no relay history, so the rekey has nothing to move and the
// cutoff never exists; every retained file is still owed and goes out.
func TestJobEventRecreatedDatabaseDoesNotSuppress(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-time.Hour)
	for index := 1; index <= 3; index++ {
		r20WriteEvent(t, inbox, "job-recreated", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, old)
	}
	path := filepath.Join(t.TempDir(), "state.db")

	first, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	recreated, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recreated.Close()

	if migratedAt, ok, err := recreated.RelayEventIDMigrationAt(context.Background()); err != nil || ok {
		t.Fatalf("a recreated database reported a migration stamp %s(%v) err=%v, want none", migratedAt, ok, err)
	}
	events := r20Node(inbox, recreated).jobCompletionEvents()
	if len(events) != 3 {
		t.Fatalf("a recreated database offered %d of the backlog, want all 3", len(events))
	}
	if got := relaySentSuppressedIDs(t, recreated, "job-recreated"); len(got) != 0 {
		t.Fatalf("a recreated database suppressed events: %v", got)
	}
}

// An old-schema database with no job.* rows to move still goes through the
// rekey - the primary key changes - but earns no migration stamp: nothing the
// old binary sent exists in it, so there is no backlog the cutoff must hold
// back. Without the stamp, pre-"migration" files are simply offered.
func TestJobEventRowlessRekeyEarnsNoStamp(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-time.Hour)
	for index := 1; index <= 2; index++ {
		r20WriteEvent(t, inbox, "job-rowless", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, old)
	}
	store := r20MigratedStore(t, nil)
	defer store.Close()

	var eventIDInPrimaryKey int
	if err := store.db.QueryRow(`SELECT pk FROM pragma_table_info('relay_sent') WHERE name='event_id'`).Scan(&eventIDInPrimaryKey); err != nil {
		t.Fatal(err)
	}
	if eventIDInPrimaryKey == 0 {
		t.Fatal("the rekey did not run on the legacy-schema database")
	}
	if migratedAt, ok, err := store.RelayEventIDMigrationAt(context.Background()); err != nil || ok {
		t.Fatalf("a rowless rekey reported a migration stamp %s(%v) err=%v, want none", migratedAt, ok, err)
	}
	if events := r20Node(inbox, store).jobCompletionEvents(); len(events) != 2 {
		t.Fatalf("a rowless-migrated store offered %d events, want both backlog files", len(events))
	}
}

// A real migration - a database carrying rows the old binary wrote - still
// stamps event_id_since once, and that stamp is the suppression cutoff: the
// pre-cutoff file is retired while the in-window one sends. Reopening the
// same database keeps the original stamp instead of re-minting it.
func TestJobEventRealMigrationStampsOnce(t *testing.T) {
	inbox := t.TempDir()
	r20WriteEvent(t, inbox, "job-migrated", "00001-job.completed.json", relayEventIDBody, time.Now().Add(-time.Hour))
	r20WriteEvent(t, inbox, "job-migrated", "00002-job.completed.json", relayEventIDBody, time.Now().Add(-time.Minute))
	seed := []relayOutboxKey{{Kind: "job.completed", JobID: "job-migrated", Epoch: 1, ReportPath: "report.md"}}
	store := r20MigratedStore(t, seed)

	stamp, ok, err := store.RelayEventIDMigrationAt(context.Background())
	if err != nil || !ok {
		t.Fatalf("a real migration left no stamp: %s(%v) err=%v", stamp, ok, err)
	}
	if since := time.Since(stamp); since > time.Minute {
		t.Fatalf("the migration stamp is %s old, want approximately now", since)
	}
	events := r20Node(inbox, store).jobCompletionEvents()
	if len(events) != 1 || events[0].relayKey.EventID != "00002-job.completed.json" {
		t.Fatalf("a migrated node offered %+v, want only the in-window event", events)
	}
	if got := relaySentSuppressedIDs(t, store, "job-migrated"); len(got) != 1 || got[0] != "00001-job.completed.json" {
		t.Fatalf("the migrated node suppressed %v, want the pre-cutoff file", got)
	}

	// Reopening must not move the stamp: the rekey branch no longer runs, so
	// the recorded instant survives restarts verbatim.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, ok, err := reopened.RelayEventIDMigrationAt(context.Background())
	if err != nil || !ok || !again.Equal(stamp) {
		t.Fatalf("reopen changed the stamp: %s(%v) err=%v, want %s", again, ok, err, stamp)
	}
}

// Every suppression lands as a warn line outside the database - one per file,
// naming the event and the cutoff it fell behind - so a held-back completion
// is never silent. A repeat scan warns nothing more: the audit row already
// exists, and the file is done.
func TestJobEventSuppressionWarnsOncePerFile(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-time.Hour)
	r20WriteEvent(t, inbox, "job-warn", "00001-job.completed.json", relayEventIDBody, old)
	r20WriteEvent(t, inbox, "job-warn", "00002-job.completed.json", relayEventIDBody, old.Add(time.Minute))
	store := r20MigratedStore(t, []relayOutboxKey{{Kind: "job.completed", JobID: "job-warn", Epoch: 1, ReportPath: "report.md"}})
	defer store.Close()

	var warns []string
	node := r20Node(inbox, store)
	node.warn = func(message string) { warns = append(warns, message) }

	if events := node.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a pre-cutoff backlog offered %d events, want 0", len(events))
	}
	if len(warns) != 2 {
		t.Fatalf("suppression produced %d warn lines, want one per file: %v", len(warns), warns)
	}
	for index, name := range []string{"00001-job.completed.json", "00002-job.completed.json"} {
		if !strings.Contains(warns[index], name) || !strings.Contains(warns[index], "migration cutoff") {
			t.Fatalf("warn %d does not name the suppressed file and reason: %q", index, warns[index])
		}
	}

	// The second scan re-judges the files but re-warns nothing: the audit
	// rows are already written, so suppression is loud exactly once.
	if events := r20Node(inbox, store).jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("a rescan offered %d suppressed events, want 0", len(events))
	}
	if len(warns) != 2 {
		t.Fatalf("a rescan raised the warn count to %d, want it steady at 2", len(warns))
	}
}

// The emit push path must derive the same file-name identity the scanner uses,
// or the immediate send and the later scan would name different outbox rows.
func TestJobEventEmitPushDerivesTheEventFileIdentity(t *testing.T) {
	inbox := t.TempDir()
	node := &HubClient{jobsInboxRoot: inbox, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	node.SetRelayOutbox(store)
	daemon := NewDaemon(Config{InboxRoot: inbox, Hub: HubDaemonConfig{Enabled: true, Client: node}})

	r20WriteEvent(t, inbox, "job-emit", "00001-job.completed.json", relayEventIDBody, time.Time{})
	err := daemon.emitRelayEvent(localRequest{Op: "emit", Kind: "job.completed", JobID: "job-emit", Epoch: 1, OwnerLane: "lane-a", Label: "wrk-a", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done", InboxRoot: inbox, TimeoutMS: 1000})
	if err != nil {
		t.Fatalf("emitRelayEvent err=%v", err)
	}
	select {
	case event := <-node.events:
		if event.relayKey.EventID != "00001-job.completed.json" {
			t.Fatalf("the emit path keyed the event %q, want its file name", event.relayKey.EventID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the emit push queued nothing")
	}
	// The same file must therefore be the scanned event's identity too.
	scanned := scanHubRelayEventsWithin(inbox, 0)
	if len(scanned) != 1 || scanned[0].EventID != "00001-job.completed.json" {
		t.Fatalf("the scanner keyed the file %+v, want event_id=00001-job.completed.json", scanned)
	}
}

// The hub dedupe key separates two rounds of the same job once the producer's
// event identity arrives; a payload without one keeps the historic key.
func TestJobRelayEventDedupeKeyCountsProducerEventID(t *testing.T) {
	base := hubJobEventPayload{JobID: "job-dedupe", Epoch: 1, ReportPath: "report.md"}
	roundOne := base
	roundOne.EventID = "00001-job.completed.json"
	roundTwo := base
	roundTwo.EventID = "00002-job.completed.json"
	if relayEventDedupeKey("job.completed", roundOne) == relayEventDedupeKey("job.completed", roundTwo) {
		t.Fatal("two distinct completion rounds share one hub dedupe key")
	}
	if relayEventDedupeKey("job.completed", base) != "job.completed\x00"+relayDedupeKey(base)+"\x00" {
		t.Fatalf("a legacy payload's dedupe key changed: %q", relayEventDedupeKey("job.completed", base))
	}
}

// The old five-field primary key cannot hold two rounds. The rebuilt primary
// key and the job.* event index must both accept them, which is exactly the
// "notify three times, store one row" failure mode the fix rules out.
func TestJobEventRowsSurviveTheRekeyedPrimaryKey(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	when := time.Now()
	for index, name := range []string{"00001-job.completed.json", "00002-job.completed.json", "00003-job.completed.json"} {
		key := relayOutboxKey{Kind: "job.completed", JobID: "job-pk", Epoch: 1, ReportPath: "report.md", EventID: name}
		if err := store.RecordRelaySent(context.Background(), key, when.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatalf("round %d was rejected by the relay_sent key: %v", index+1, err)
		}
	}
	if rows := relaySentRowCount(t, store, "job-pk"); rows != 3 {
		t.Fatalf("relay_sent rows=%d, want 3", rows)
	}
	// The authoritative event key also folds a resend onto its own row.
	again := relayOutboxKey{Kind: "job.completed", JobID: "job-pk", Epoch: 1, ReportPath: "report.md", EventID: "00001-job.completed.json"}
	if err := store.RecordRelaySent(context.Background(), again, when.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if rows := relaySentRowCount(t, store, "job-pk"); rows != 3 {
		t.Fatalf("a resend minted a fourth row: relay_sent rows=%d", rows)
	}
	state, err := store.RelayOutboxState(context.Background(), again)
	if err != nil || !state.Found || state.SentAt.IsZero() {
		t.Fatalf("the resent row's state=%+v err=%v", state, err)
	}
	if got := state.SentAt.Sub(when.Add(time.Hour)); got < -time.Millisecond || got > time.Millisecond {
		t.Fatalf("the resend did not refresh sent_at on its own row: sent_at=%s", state.SentAt)
	}
}

// The file-name identity is unique per emission and monotone with it, so two
// events for one job can never alias each other.
func TestJobEventFileIdentityIsUniquePerEmission(t *testing.T) {
	inbox := t.TempDir()
	for index := 1; index <= 3; index++ {
		r20WriteEvent(t, inbox, "job-names", fmt.Sprintf("%05d-job.completed.json", index), relayEventIDBody, time.Time{})
	}
	scanned := scanHubRelayEventsWithin(inbox, 0)
	if len(scanned) != 3 {
		t.Fatalf("the scan produced %d events, want 3", len(scanned))
	}
	ids := []string{scanned[0].EventID, scanned[1].EventID, scanned[2].EventID}
	sort.Strings(ids)
	if ids[0] == ids[1] || ids[1] == ids[2] {
		t.Fatalf("event files share an identity: %v", ids)
	}
	for index, name := range ids {
		want := fmt.Sprintf("%05d-job.completed.json", index+1)
		if name != want {
			t.Fatalf("event_id=%q, want the file name %q", name, want)
		}
	}
}
