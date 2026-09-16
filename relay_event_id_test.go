package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

// A relay_sent row written before event_id existed is adopted by the event
// file it recorded, so an already-sent event is neither re-notified nor
// duplicated, and a later round still gets its own row.
func TestJobEventIdentityAdoptsLegacyRow(t *testing.T) {
	inbox := t.TempDir()
	r20WriteEvent(t, inbox, "job-legacy", "00001-job.completed.json", relayEventIDBody, time.Time{})
	store := NewMemoryStore(t)
	defer store.Close()
	legacyKey := relayOutboxKey{Kind: "job.completed", JobID: "job-legacy", Epoch: 1, ReportPath: "report.md"}
	if err := store.RecordRelaySent(context.Background(), legacyKey, time.Now().Add(-2*relayOutboxBackoff)); err != nil {
		t.Fatal(err)
	}

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("the scan offered %d events for one adopted row, want 1", len(events))
	}
	var payload struct {
		Replay bool `json:"replay"`
	}
	if json.Unmarshal(events[0].Payload, &payload) != nil || !payload.Replay {
		t.Fatalf("the adopted row was not flagged as a replay: %s", events[0].Payload)
	}
	if got := relaySentEventIDs(t, store, "job-legacy"); len(got) != 1 || got[0] != "00001-job.completed.json" {
		t.Fatalf("the legacy row was not adopted: event_id values=%v", got)
	}
	if rows := relaySentRowCount(t, store, "job-legacy"); rows != 1 {
		t.Fatalf("adoption minted a duplicate row: relay_sent rows=%d, want 1", rows)
	}

	// A second round is a new event, adopted or not. Committing round one's
	// replayed send puts it inside the retry backoff, so only the new file is
	// offered next.
	node.commitRelaySent(events[0])
	r20WriteEvent(t, inbox, "job-legacy", "00002-job.completed.json", relayEventIDBody, time.Time{})
	second := r20Node(inbox, store).jobCompletionEvents()
	if len(second) != 1 || second[0].relayKey.EventID != "00002-job.completed.json" {
		t.Fatalf("the second round offered %+v, want only its own event", second)
	}
	r20Node(inbox, store).commitRelaySent(second[0])
	if rows := relaySentRowCount(t, store, "job-legacy"); rows != 2 {
		t.Fatalf("a sent second round produced %d rows, want 2", rows)
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
