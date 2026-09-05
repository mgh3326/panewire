package panewire

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// r20t8SnapshotPath is the 32-row journal copy used for the T8 incident. Job
// IDs and paths are placeholders; row count, NULL count, and send cadence are
// preserved from the immutable source copy.
const r20t8SnapshotPath = "testdata/t8-relay-sent-snapshot.tsv"

type r20t8SnapshotRow struct {
	Kind       string
	JobID      string
	Epoch      uint64
	ReportPath string
	Reason     string
	SentAt     time.Time
	Persisted  bool
}

func (row r20t8SnapshotRow) key() relayOutboxKey {
	return relayOutboxKey{Kind: row.Kind, JobID: row.JobID, Epoch: row.Epoch, ReportPath: row.ReportPath, Reason: row.Reason}
}

func r20t8LoadSnapshot(t *testing.T) []r20t8SnapshotRow {
	t.Helper()
	contents, err := os.ReadFile(r20t8SnapshotPath)
	if err != nil {
		t.Fatalf("read sanitized snapshot: %v", err)
	}
	var rows []r20t8SnapshotRow
	for _, line := range strings.Split(strings.TrimRight(string(contents), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			t.Fatalf("snapshot row has %d columns, want 7: %q", len(fields), line)
		}
		epoch, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		sent, err := strconv.ParseInt(fields[5], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r20t8SnapshotRow{Kind: fields[0], JobID: fields[1], Epoch: epoch, ReportPath: fields[3], Reason: fields[4], SentAt: time.UnixMilli(sent).UTC(), Persisted: fields[6] != "None"})
	}
	return rows
}

const r20t8Lanes = `{"lanes":{` +
	`"lane-source":{"machine":"host-a","pane":"w1:p1","parent":"lane-destination"},` +
	`"lane-destination":{"machine":"host-b","pane":"w1:p2"}}}`

func r20t8OwnerLane(row r20t8SnapshotRow) string {
	if row.Kind == "job.escalate" || row.Kind == "job.joined" {
		return "lane-source"
	}
	return "lane-destination"
}

func r20t8SeedInbox(t *testing.T, inbox string, rows []r20t8SnapshotRow) {
	t.Helper()
	sequence := map[string]int{}
	for _, row := range rows {
		sequence[row.JobID]++
		record := map[string]any{
			"type": row.Kind, "epoch": row.Epoch, "owner_lane": r20t8OwnerLane(row),
			"agent_label": "lane-source", "label": "lane-source", "host": "host-a",
			"report_path": row.ReportPath, "report_last_line": "done",
		}
		if row.Reason != "" {
			record["reason"] = row.Reason
		}
		contents, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		r20WriteEvent(t, inbox, row.JobID, strconv.Itoa(sequence[row.JobID])+"-"+row.Kind+".json", string(contents), time.Time{})
	}
}

func r20t8SeedOutbox(t *testing.T, store *Store, rows []r20t8SnapshotRow) {
	t.Helper()
	for _, row := range rows {
		if err := store.RecordRelaySent(context.Background(), row.key(), row.SentAt); err != nil {
			t.Fatal(err)
		}
		if row.Persisted {
			if err := store.RecordRelayPersisted(context.Background(), row.key(), row.SentAt.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func r20t8OutboxRows(t *testing.T, store *Store) int {
	t.Helper()
	var rows int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM relay_sent").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func r20t8OutboxNullRows(t *testing.T, store *Store) int {
	t.Helper()
	var rows int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM relay_sent WHERE persisted_at IS NULL").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestR20T8RestartReusesUnpersistedOutboxEpoch is the independent RED
// reproduction for T8/AC4. An assignment fences the first send at epoch 2;
// after a process restart the scanner reads epoch 1 from the same event file.
// It must re-present the outstanding epoch-2 row instead of minting epoch 1.
func TestR20T8RestartReusesUnpersistedOutboxEpoch(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "t8-epoch-reuse", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-source","label":"lane-source","host":"host-a","report_path":"report.md","report_last_line":"done"}`, time.Time{})

	first := r20Node(inbox, store)
	first.assignedJobs["t8-epoch-reuse"] = 2
	firstBatch := first.jobCompletionEvents()
	if len(firstBatch) != 1 || firstBatch[0].relayKey.Epoch != 2 {
		t.Fatalf("first scan=%+v, want one epoch-2 event", firstBatch)
	}
	first.commitRelaySent(firstBatch[0])
	if err := store.RecordRelaySent(context.Background(), firstBatch[0].relayKey, time.Now().Add(-2*relayOutboxBackoff)); err != nil {
		t.Fatal(err)
	}

	restarted := r20Node(inbox, store)
	replay := restarted.jobCompletionEvents()
	if len(replay) != 1 {
		t.Fatalf("restart offered %d events, want the outstanding row", len(replay))
	}
	if replay[0].relayKey.Epoch != 2 {
		t.Fatalf("restart re-keyed the same event file at epoch %d, want outstanding epoch 2", replay[0].relayKey.Epoch)
	}
	restarted.commitRelaySent(replay[0])

	var rows int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM relay_sent").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("restart minted %d relay_sent rows, want 1", rows)
	}
}

// TestR20T8LiveEpochDoesNotReuseOlderUnpersistedRow is the P5 regression:
// after an epoch-1 completion was persisted by the hub but lost its sender ACK,
// a live assignment at epoch 2 using the same report must remain epoch 2. The
// second completion is a new logical event and must reach its destination.
func TestR20T8LiveEpochDoesNotReuseOlderUnpersistedRow(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	const jobID = "t8-live-epoch"
	const first = `{"type":"job.completed","epoch":1,"owner_lane":"lane-destination","label":"lane-source","host":"host-a","report_path":"report.md","report_last_line":"done"}`
	r20WriteEvent(t, inbox, jobID, "00001-job.completed.json", first, time.Time{})

	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, r20t8Lanes, client, nil)
	sourceAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	destinationAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: sourceAgent}
	hub.nodes["host-b"] = &hubNodeRecord{agent: destinationAgent}

	firstNode := r20Node(inbox, store)
	firstBatch := firstNode.jobCompletionEvents()
	if len(firstBatch) != 1 || firstBatch[0].relayKey.Epoch != 1 {
		t.Fatalf("first batch=%+v, want one epoch-1 event", firstBatch)
	}
	firstWire, err := json.Marshal(hubClientWireEvent(firstBatch[0]))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "fixture", sourceAgent, firstWire)
	firstNode.commitRelaySent(firstBatch[0])
	if injected := drainRelays(destinationAgent); injected != 1 {
		t.Fatalf("epoch-1 injections=%d, want one", injected)
	}
	_ = drainPersisted(sourceAgent) // The sender's acknowledgement was lost.

	// A redispatched run writes another completion for the same report path.
	r20WriteEvent(t, inbox, jobID, "00002-job.completed.json", first, time.Time{})
	liveNode := r20Node(inbox, store)
	liveNode.assignedJobs[jobID] = 2
	liveBatch := liveNode.jobCompletionEvents()
	if len(liveBatch) != 1 || liveBatch[0].relayKey.Epoch != 2 {
		t.Fatalf("live epoch-2 batch=%+v, want one epoch-2 event", liveBatch)
	}
	liveWire, err := json.Marshal(hubClientWireEvent(liveBatch[0]))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "fixture", sourceAgent, liveWire)
	liveNode.commitRelaySent(liveBatch[0])
	if injected := drainRelays(destinationAgent); injected != 1 {
		t.Fatalf("epoch-2 injections=%d, want one distinct completion", injected)
	}
	var rows int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM relay_sent WHERE job_id=?", jobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || fake.rowCount() != 2 {
		t.Fatalf("epoch split rows=%d handoffkeep=%d, want two each", rows, fake.rowCount())
	}
}

// TestR20T8DisconnectedSenderDoesNotAckDestination covers the no-fallback
// branch. A durable job relay can still inject into the destination pane, but
// an offline sender must not cause that destination to retire a twin outbox.
func TestR20T8DisconnectedSenderDoesNotAckDestination(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, r20t8Lanes, client, nil)
	destinationAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	hub.nodes["host-b"] = &hubNodeRecord{agent: destinationAgent}

	event := hubJobEventPayload{JobID: "t8-offline-sender", Epoch: 1, OwnerLane: "lane-destination", ReportPath: "report.md"}
	hub.relayJobEventFrom("host-a", "job.completed", event) // host-a is disconnected.
	if injected := drainRelays(destinationAgent); injected != 1 {
		t.Fatalf("destination injections=%d, want one", injected)
	}
	if acks := drainPersisted(destinationAgent); len(acks) != 0 {
		t.Fatalf("offline sender acknowledged destination: %+v", acks)
	}
	if fake.rowCount() != 1 {
		t.Fatalf("handoffkeep rows=%d, want one durable relay", fake.rowCount())
	}
}

// TestR20T8TwoNodeSnapshotAcknowledgesOnlySender reproduces the incident from
// its sanitized journal using distinct sender and destination stores. It pins
// AC1 through AC5, including real twin rows on the destination node.
func TestR20T8TwoNodeSnapshotAcknowledgesOnlySender(t *testing.T) {
	rows := r20t8LoadSnapshot(t)
	stuck := make([]r20t8SnapshotRow, 0, len(rows))
	latest := time.Time{}
	for _, row := range rows {
		if !row.Persisted {
			stuck = append(stuck, row)
		}
		if row.SentAt.After(latest) {
			latest = row.SentAt
		}
	}
	if len(rows) != 32 || len(stuck) != 12 {
		t.Fatalf("sanitized journal rows=%d null=%d, want 32 and 12", len(rows), len(stuck))
	}
	routes := loadReportRelayRoutes(r20LanesFile(t, r20t8Lanes))
	if routes["lane-source"].Machine == routes["lane-destination"].Machine {
		t.Fatalf("fixture requires distinct sender and destination machines, got %q", routes["lane-source"].Machine)
	}
	if routes["lane-source"].Machine != "host-a" || routes["lane-destination"].Machine != "host-b" {
		t.Fatalf("fixture routes=%+v, want source host-a and destination host-b", routes)
	}

	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, r20t8Lanes, client, nil)
	sourceAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 32), persisted: make(chan hubRelayPersistedEvent, 32)}
	destinationAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 32), persisted: make(chan hubRelayPersistedEvent, 32)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: sourceAgent}
	hub.nodes["host-b"] = &hubNodeRecord{agent: destinationAgent}

	inbox := t.TempDir()
	r20t8SeedInbox(t, inbox, rows)
	sourceStore := NewMemoryStore(t)
	defer sourceStore.Close()
	r20t8SeedOutbox(t, sourceStore, rows)
	destinationStore := NewMemoryStore(t)
	defer destinationStore.Close()
	r20t8SeedOutbox(t, destinationStore, stuck) // Actual destination twins.
	destination := r20t7Node(inbox, destinationStore, latest.Add(time.Hour))

	sourceRowsBefore := r20t8OutboxRows(t, sourceStore)
	destinationRowsBefore := r20t8OutboxRows(t, destinationStore)
	source := r20t7Node(inbox, sourceStore, latest.Add(time.Hour))
	events := source.jobCompletionEvents()
	if len(events) != len(stuck) {
		t.Fatalf("first restart offered %d events, want 12 outstanding source rows", len(events))
	}
	injections := 0
	for _, event := range events {
		wire, err := json.Marshal(hubClientWireEvent(event))
		if err != nil {
			t.Fatal(err)
		}
		hub.handleAgentMessage("host-a", "fixture", sourceAgent, wire)
		injections += drainRelays(destinationAgent)
		destinationAcks := drainPersisted(destinationAgent)
		for _, ack := range destinationAcks {
			destination.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID})
		}
		if len(destinationAcks) != 0 {
			t.Fatalf("destination received relay.persisted for sender event: %+v", destinationAcks)
		}
		acks := drainPersisted(sourceAgent)
		if len(acks) != 1 || acks[0].JobID != event.relayKey.JobID || acks[0].Epoch != event.relayKey.Epoch {
			t.Fatalf("source relay.persisted=%+v for key=%+v", acks, event.relayKey)
		}
		for _, ack := range acks {
			source.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID})
		}
		source.commitRelaySent(event)
	}
	if injections != len(stuck) {
		t.Fatalf("destination relay.inject count=%d, want one per logical event (%d)", injections, len(stuck))
	}
	if r20t8OutboxNullRows(t, sourceStore) != 0 {
		t.Fatalf("source persisted_at NULL rows=%d, want 0", r20t8OutboxNullRows(t, sourceStore))
	}
	if got := r20t8OutboxRows(t, destinationStore); got != destinationRowsBefore {
		t.Fatalf("destination relay_sent rows grew from %d to %d", destinationRowsBefore, got)
	}
	for _, row := range stuck {
		state, err := destinationStore.RelayOutboxState(context.Background(), row.key())
		if err != nil || !state.Found || state.Persisted {
			t.Fatalf("destination twin for %s/%s state=%+v err=%v, want unpersisted", row.Kind, row.JobID, state, err)
		}
	}

	restarted := r20t7Node(inbox, sourceStore, latest.Add(2*time.Hour))
	if events := restarted.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("second restart re-offered %d events after sender acknowledgements", len(events))
	}
	if injected := drainRelays(destinationAgent); injected != 0 {
		t.Fatalf("restart re-injected %d destination panes, want 0", injected)
	}
	if got := r20t8OutboxRows(t, sourceStore); got != sourceRowsBefore {
		t.Fatalf("source relay_sent rows changed across restart from %d to %d", sourceRowsBefore, got)
	}
	if fake.rowCount() != len(stuck) {
		t.Fatalf("handoffkeep rows=%d, want %d logical events", fake.rowCount(), len(stuck))
	}
}

// TestR20T8HubReplayDefersAckUntilSenderResends proves the no-sender replay
// rule: handoffkeep names only the destination, so replay never retires that
// node's twin. Once the original sender reconnects and resends, it alone gets
// relay.persisted and the pane is not injected again.
func TestR20T8HubReplayDefersAckUntilSenderResends(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 901, Kind: "job.completed", JobID: "t8-replay", Epoch: 1, OwnerLane: "lane-destination", ReportPath: "replay.md"})
	hub := r20Hub(t, r20t8Lanes, client, nil)
	sourceAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	destinationAgent := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: sourceAgent}
	hub.nodes["host-b"] = &hubNodeRecord{agent: destinationAgent}

	hub.replayUndeliveredRelayEvents(context.Background())
	if injected := drainRelays(destinationAgent); injected != 1 {
		t.Fatalf("startup replay injections=%d, want 1", injected)
	}
	if acks := drainPersisted(destinationAgent); len(acks) != 0 {
		t.Fatalf("startup replay acknowledged destination twin: %+v", acks)
	}
	if acks := drainPersisted(sourceAgent); len(acks) != 0 {
		t.Fatalf("startup replay acknowledged unidentified sender: %+v", acks)
	}

	event := hubClientEvent{Kind: "job.completed", Payload: json.RawMessage(`{"job_id":"t8-replay","epoch":1,"owner_lane":"lane-destination","report_path":"replay.md"}`)}
	wire, err := json.Marshal(hubClientWireEvent(event))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "fixture", sourceAgent, wire)
	if injected := drainRelays(destinationAgent); injected != 0 {
		t.Fatalf("sender resend re-injected %d panes", injected)
	}
	if acks := drainPersisted(destinationAgent); len(acks) != 0 {
		t.Fatalf("sender resend acknowledged destination: %+v", acks)
	}
	if acks := drainPersisted(sourceAgent); len(acks) != 1 || acks[0].JobID != "t8-replay" {
		t.Fatalf("sender resend acknowledgements=%+v, want one source ack", acks)
	}
}
