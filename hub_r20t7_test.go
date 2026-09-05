package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// r20t7SnapshotPath is the operator's own relay_sent journal, sanitized: the
// real inbox prefix, job ids, and scratch directory are placeholders, and
// nothing else about the 29 rows was touched. AC13 keeps the real paths out of
// the repository; the 16-millisecond burst of unacknowledged sends that the
// fault produced is the part the fixture has to preserve.
const r20t7SnapshotPath = "testdata/relay-sent-snapshot.tsv"

type r20t7SnapshotRow struct {
	Kind        string
	JobID       string
	Epoch       uint64
	ReportPath  string
	Reason      string
	SentAt      time.Time
	PersistedAt time.Time
	Persisted   bool
}

func (row r20t7SnapshotRow) key() relayOutboxKey {
	return relayOutboxKey{Kind: row.Kind, JobID: row.JobID, Epoch: row.Epoch, ReportPath: row.ReportPath, Reason: row.Reason}
}

// r20t7LoadSnapshot parses the seven tab-separated columns the operator
// exported: kind, job_id, epoch, report_path, reason, sent_at, persisted_at.
func r20t7LoadSnapshot(t *testing.T) []r20t7SnapshotRow {
	t.Helper()
	contents, err := os.ReadFile(r20t7SnapshotPath)
	if err != nil {
		t.Fatalf("the snapshot fixture must be read from disk, not inlined: %v", err)
	}
	var rows []r20t7SnapshotRow
	for _, line := range strings.Split(strings.TrimRight(string(contents), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			t.Fatalf("snapshot row has %d columns, want 7: %q", len(fields), line)
		}
		epoch, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			t.Fatalf("snapshot epoch %q: %v", fields[2], err)
		}
		sent, err := strconv.ParseInt(fields[5], 10, 64)
		if err != nil {
			t.Fatalf("snapshot sent_at %q: %v", fields[5], err)
		}
		row := r20t7SnapshotRow{Kind: fields[0], JobID: fields[1], Epoch: epoch, ReportPath: fields[3], Reason: fields[4], SentAt: time.UnixMilli(sent).UTC()}
		if fields[6] != "None" {
			persisted, err := strconv.ParseInt(fields[6], 10, 64)
			if err != nil {
				t.Fatalf("snapshot persisted_at %q: %v", fields[6], err)
			}
			row.PersistedAt, row.Persisted = time.UnixMilli(persisted).UTC(), true
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		t.Fatal("the snapshot fixture is empty")
	}
	return rows
}

// r20t7Lanes routes worker records to lane-w and both captains to their parent,
// which is where the escalate/joined records in the snapshot actually went.
const r20t7Lanes = `{"lanes":{` +
	`"lane-w":{"machine":"host-a","pane":"w1:p1"},` +
	`"lane-cap-a":{"machine":"host-a","pane":"w1:p2","parent":"lane-w"},` +
	`"lane-cap-b":{"machine":"host-a","pane":"w1:p3","parent":"lane-w"}}}`

func r20t7Lane(jobID string) string {
	switch {
	case strings.HasPrefix(jobID, "captain-a"):
		return "lane-cap-a"
	case strings.HasPrefix(jobID, "captain-b"):
		return "lane-cap-b"
	default:
		return "lane-w"
	}
}

// r20t7SeedInbox writes one local event file per snapshot row, so the node
// scanner produces exactly the records the journal says it produced.
func r20t7SeedInbox(t *testing.T, inbox string, rows []r20t7SnapshotRow) {
	t.Helper()
	sequence := map[string]int{}
	for _, row := range rows {
		sequence[row.JobID]++
		record := map[string]any{
			"type": row.Kind, "epoch": row.Epoch, "owner_lane": r20t7Lane(row.JobID),
			"agent_label": r20t7Lane(row.JobID), "label": r20t7Lane(row.JobID), "host": "host-a",
			"report_path": row.ReportPath, "report_last_line": "VERDICT: DONE",
		}
		if row.Reason != "" {
			record["reason"] = row.Reason
		}
		contents, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		r20t7WriteEvent(t, inbox, row.JobID, fmt.Sprintf("%05d-%s.json", sequence[row.JobID], row.Kind), string(contents))
	}
}

func r20t7WriteEvent(t *testing.T, inbox, jobID, name, contents string) {
	t.Helper()
	r20WriteEvent(t, inbox, jobID, name, contents, time.Time{})
}

// r20t7SeedOutbox recreates the operator's relay_sent table row for row.
func r20t7SeedOutbox(t *testing.T, store *Store, rows []r20t7SnapshotRow) {
	t.Helper()
	for _, row := range rows {
		if err := store.RecordRelaySent(context.Background(), row.key(), row.SentAt); err != nil {
			t.Fatal(err)
		}
		if row.Persisted {
			if err := store.RecordRelayPersisted(context.Background(), row.key(), row.PersistedAt); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// seedDelivered puts rows handoffkeep already holds *and* has marked delivered
// behind its idempotency index. A hub that re-POSTs one gets 200 with a
// delivered_at, which is the only authority the inject gate consults.
func (f *fakeHandoffkeep) seedDelivered(deliveredAt string, records ...handoffkeepRelayEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, record := range records {
		row := record
		row.DeliveredAt = deliveredAt
		f.rows[fakeHandoffkeepRowKey(row.Kind, row.JobID, row.Epoch, row.ReportPath, row.Reason)] = &row
		if row.ID > f.nextID {
			f.nextID = row.ID
		}
	}
}

// r20t7Node is a node restart: a fresh client over the same SQLite file, with
// the retry clock pinned so the sixty-second backoff is not a race against the
// wall clock.
func r20t7Node(inbox string, store *Store, now time.Time) *HubClient {
	client := r20Node(inbox, store)
	client.now = func() time.Time { return now }
	return client
}

// r20t7Deliver feeds one node event through the hub over the inbound message
// path the websocket actually uses - not the relay entry points underneath it,
// which would leave the node-facing half of the contract untested - and applies
// whatever the hub answers back to the node's outbox. It reports the pane
// injections and the acknowledgements the event produced.
func r20t7Deliver(t *testing.T, hub *HubServer, agent *hubAgent, node *HubClient, event hubClientEvent) (int, []hubRelayPersistedEvent) {
	t.Helper()
	wire, err := json.Marshal(hubClientWireEvent(event))
	if err != nil {
		t.Fatal(err)
	}
	unknown := hub.UnknownMessageCount()
	hub.handleAgentMessage("host-a", "remote-a", agent, wire)
	if hub.UnknownMessageCount() != unknown {
		t.Fatalf("the hub rejected a %s payload as unknown: %s", event.Kind, event.Payload)
	}
	injected := drainRelays(agent)
	acknowledgements := drainPersisted(agent)
	for _, ack := range acknowledgements {
		node.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID})
	}
	return injected, acknowledgements
}

// TA1/AC9 is the operator's incident replayed from their own journal: 29 rows,
// 11 of them stuck with persisted_at NULL, resent on every node restart and
// re-injected into the parent pane every time.
//
// "no resends" is read here as what it can mean without a time machine: the
// node cannot know a row is settled until the hub answers one more time, so the
// first restart offers the eleven stuck rows once and the storm has to be over
// by the second. Zero re-injections and eleven filled persisted_at columns are
// literal.
func TestR20T7SnapshotRestartsStopResendingAndReinjecting(t *testing.T) {
	rows := r20t7LoadSnapshot(t)
	stuck := make([]r20t7SnapshotRow, 0, len(rows))
	latest := time.Time{}
	for _, row := range rows {
		if !row.Persisted {
			stuck = append(stuck, row)
		}
		if row.SentAt.After(latest) {
			latest = row.SentAt
		}
	}
	if len(rows) != 29 || len(stuck) != 11 {
		t.Fatalf("the fixture holds %d rows with %d unpersisted, want 29 and 11", len(rows), len(stuck))
	}

	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t7Lanes, client, 64)
	// Every stuck row had already been injected and delivered before the
	// restart: that is why the hub kept logging a replay and the pane kept
	// getting the same note. handoffkeep therefore answers 200 + delivered_at.
	for index, row := range stuck {
		fake.seedDelivered("2026-09-05T13:35:00Z", handoffkeepRelayEvent{
			ID: int64(500 + index), Kind: row.Kind, JobID: row.JobID, Epoch: int(row.Epoch),
			OwnerLane: r20t7Lane(row.JobID), ReportPath: row.ReportPath, Reason: row.Reason, Attempts: 1,
		})
	}

	inbox := t.TempDir()
	r20t7SeedInbox(t, inbox, rows)
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7SeedOutbox(t, store, rows)

	// The clock is pinned past the whole journal, so nothing is held back by
	// the retry backoff and only persisted_at can retire a row.
	now := latest.Add(time.Hour)
	sends, injections := make([]int, 0, 3), 0
	for restart := 1; restart <= 3; restart++ {
		node := r20t7Node(inbox, store, now)
		events := node.jobCompletionEvents()
		for _, event := range events {
			injected, _ := r20t7Deliver(t, hub, agent, node, event)
			injections += injected
			node.commitRelaySent(event)
		}
		sends = append(sends, len(events))
	}

	if sends[0] != len(stuck) {
		t.Fatalf("the first restart offered %d records, want the %d unpersisted ones", sends[0], len(stuck))
	}
	if sends[1] != 0 || sends[2] != 0 {
		t.Fatalf("restarts 2 and 3 resent %d and %d records, want 0: the storm did not stop", sends[1], sends[2])
	}
	if injections != 0 {
		t.Fatalf("the parent pane was re-injected %d times across three restarts, want 0", injections)
	}
	for _, row := range stuck {
		state, err := store.RelayOutboxState(context.Background(), row.key())
		if err != nil {
			t.Fatal(err)
		}
		if !state.Persisted {
			t.Fatalf("persisted_at is still NULL for %s/%s: this row is resent forever", row.Kind, row.JobID)
		}
	}
	if fake.rowCount() != len(stuck) {
		t.Fatalf("handoffkeep holds %d rows, want %d: a resend minted a duplicate", fake.rowCount(), len(stuck))
	}
}

// TA1 guard: the eleven stuck rows are the ones the node offers, not some
// arbitrary eleven. It also pins that the already-settled rows stay silent.
func TestR20T7SnapshotResendsExactlyTheUnpersistedRows(t *testing.T) {
	rows := r20t7LoadSnapshot(t)
	inbox := t.TempDir()
	r20t7SeedInbox(t, inbox, rows)
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7SeedOutbox(t, store, rows)

	latest := time.Time{}
	want := map[string]struct{}{}
	for _, row := range rows {
		if row.SentAt.After(latest) {
			latest = row.SentAt
		}
		if !row.Persisted {
			want[row.key().String()] = struct{}{}
		}
	}
	node := r20t7Node(inbox, store, latest.Add(time.Hour))
	got := map[string]struct{}{}
	for _, event := range node.jobCompletionEvents() {
		got[event.relayKey.String()] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("the node offered %d records, want %d", len(got), len(want))
	}
	for key := range want {
		if _, offered := got[key]; !offered {
			t.Fatalf("an unpersisted row was not re-offered: %q", strings.ReplaceAll(key, "\x00", " | "))
		}
	}
}

// TA2/AC1/AC2: a batch whose third write fails must stamp the two that went out
// and nothing else. Stamping at claim time is what buried the remaining seven
// behind the retry backoff with no send to show for it.
func TestR20T7MidBatchWriteFailureStampsOnlyWhatWentOut(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	for index := 1; index <= 9; index++ {
		job := fmt.Sprintf("r20t7-batch-%02d", index)
		r20t7WriteEvent(t, inbox, job, "00001-job.completed.json",
			fmt.Sprintf(`{"type":"job.completed","epoch":1,"owner_lane":"lane-w","label":"lane-w","host":"host-a","report_path":%q,"report_last_line":"done"}`, job+".md"))
	}
	now := time.Now().UTC()
	node := r20t7Node(inbox, store, now)
	batch := node.jobCompletionEvents()
	if len(batch) != 9 {
		t.Fatalf("the scan produced %d records, want 9", len(batch))
	}

	written := 0
	failure := errors.New("connection reset")
	err := node.writeRelayEvents(batch, func(hubClientEvent) error {
		written++
		if written == 3 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("writeRelayEvents err=%v, want the write failure", err)
	}
	if written != 3 {
		t.Fatalf("the batch kept writing after a failure: %d writes", written)
	}
	for index, event := range batch {
		state, stateErr := store.RelayOutboxState(context.Background(), event.relayKey)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if index < 2 && state.SentAt.IsZero() {
			t.Fatalf("record %d went out but carries no sent_at", index+1)
		}
		if index >= 2 && !state.SentAt.IsZero() {
			t.Fatalf("record %d never reached the wire but is stamped sent_at=%s", index+1, state.SentAt)
		}
	}

	// The reconnect: everything from the failed write onwards is offered again,
	// immediately, because nothing marked it sent.
	retry := r20t7Node(inbox, store, now)
	resent := retry.jobCompletionEvents()
	if len(resent) != 7 {
		t.Fatalf("the reconnect re-offered %d records, want the 7 that never went out", len(resent))
	}
	if err := retry.writeRelayEvents(resent, func(hubClientEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, event := range resent {
		state, stateErr := store.RelayOutboxState(context.Background(), event.relayKey)
		if stateErr != nil || state.SentAt.IsZero() {
			t.Fatalf("a re-sent record was not stamped: %+v err=%v", state, stateErr)
		}
	}
}

// TA3/AC1: a record the immediate-send queue had no room for was never sent, so
// it must not carry a send stamp and must be offered again at once.
func TestR20T7QueueFullDropDoesNotStamp(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	const body = `{"type":"job.completed","epoch":1,"owner_lane":"lane-w","label":"lane-w","host":"host-a","report_path":"full.md","report_last_line":"done"}`
	r20t7WriteEvent(t, inbox, "r20t7-full", "00001-job.completed.json", body)

	node := r20t7Node(inbox, store, time.Now().UTC())
	// A queue with no room at all: the enqueue takes the default branch.
	node.events = make(chan hubClientEvent)
	job := hubScannedRelayEvent{Kind: "job.completed", HubActiveJob: HubActiveJob{JobID: "r20t7-full", Epoch: 1, AgentLabel: "lane-w", OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: "full.md", ReportLastLine: "done"}}
	if node.EnqueueRelayEvent(job) {
		t.Fatal("EnqueueRelayEvent reported a send on a full queue")
	}
	key := relayOutboxKey{Kind: "job.completed", JobID: "r20t7-full", Epoch: 1, ReportPath: "full.md"}
	state, err := store.RelayOutboxState(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if state.Found || !state.SentAt.IsZero() {
		t.Fatalf("a dropped record was stamped as sent: %+v", state)
	}
	if events := node.jobCompletionEvents(); len(events) != 1 {
		t.Fatalf("the next scan offered %d records, want the dropped one", len(events))
	}
}

// r20t7AckKey is the outbox key the hub's acknowledgement names.
func r20t7AckKey(ack hubRelayPersistedEvent) relayOutboxKey {
	return relayOutboxKey{Kind: ack.Kind, JobID: ack.JobID, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason}
}

// TA4/AC3/AC4: the node's outbox key and the five fields the hub echoes must be
// the same strings on the scan path, the emit path, and the restart replay
// path - including values long enough or multi-line enough to be compacted,
// which is where the two used to diverge silently.
func TestR20T7OutboxKeyMatchesAcknowledgementOnEveryPath(t *testing.T) {
	longReason := "captain escalation " + strings.Repeat("x", 260) + "\nsecond line"
	longPath := "/inbox/jobs/r20t7-key/" + strings.Repeat("d", 260) + "/report.md"

	for _, testCase := range []struct {
		name   string
		kind   string
		job    string
		reason string
		path   string
	}{
		{name: "long_and_multiline_reason", kind: "job.escalate", job: "r20t7-key-reason", reason: longReason, path: "/inbox/jobs/r20t7-key-reason/report.md"},
		{name: "long_report_path", kind: "job.joined", job: "r20t7-key-path", reason: "captain joined PR", path: longPath},
		{name: "completed_with_a_reason", kind: "job.completed", job: "r20t7-key-completed", reason: "operator note", path: "/inbox/jobs/r20t7-key-completed/report.md"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			job := hubScannedRelayEvent{
				Kind:         testCase.kind,
				HubActiveJob: HubActiveJob{JobID: testCase.job, Epoch: 1, AgentLabel: "lane-cap-a", OwnerLane: "lane-cap-a", Label: "lane-cap-a", Host: "host-a", ReportPath: testCase.path, ReportLastLine: "VERDICT: DONE"},
				Reason:       testCase.reason,
				Question:     "which branch?",
			}
			if testCase.kind == "job.completed" {
				job.OwnerLane, job.AgentLabel, job.Label = "lane-w", "lane-w", "lane-w"
			}

			for _, path := range []string{"scan", "emit", "replay"} {
				_, client, closeServer := newFakeHandoffkeep(t)
				defer closeServer()
				hub, agent := r20t5Hub(t, r20t7Lanes, client, 8)
				inbox := t.TempDir()
				store := NewMemoryStore(t)
				defer store.Close()
				node := r20t7Node(inbox, store, time.Now().UTC())

				if path == "replay" {
					// A row a previous process stamped and never got answered.
					wire := relayEventWireForm(job)
					if err := store.RecordRelaySent(context.Background(), relayEventOutboxKeyFor(wire), time.Now().Add(-2*relayOutboxBackoff)); err != nil {
						t.Fatal(err)
					}
				}
				var event hubClientEvent
				var ok bool
				if path == "emit" {
					if !node.EnqueueRelayEvent(job) {
						t.Fatal("the emit path queued nothing")
					}
					event = <-node.events
				} else {
					if event, ok = node.relayEventForSend(job); !ok {
						t.Fatalf("the %s path offered nothing", path)
					}
				}
				if path == "replay" {
					var payload struct {
						Replay bool `json:"replay"`
					}
					if json.Unmarshal(event.Payload, &payload) != nil || !payload.Replay {
						t.Fatalf("the replay path did not flag the record: %s", event.Payload)
					}
				}
				_, acknowledgements := r20t7Deliver(t, hub, agent, node, event)
				if len(acknowledgements) != 1 {
					t.Fatalf("%s path: relay.persisted count=%d, want 1", path, len(acknowledgements))
				}
				if got, want := r20t7AckKey(acknowledgements[0]).String(), event.relayKey.String(); got != want {
					t.Fatalf("%s path: the acknowledgement names a different row\n ack: %q\n key: %q", path, strings.ReplaceAll(got, "\x00", " | "), strings.ReplaceAll(want, "\x00", " | "))
				}
				state, err := store.RelayOutboxState(context.Background(), event.relayKey)
				if err != nil {
					t.Fatal(err)
				}
				if !state.Persisted {
					t.Fatalf("%s path: the acknowledgement retired no row, so this record is resent forever", path)
				}
			}
		})
	}
}

// TA4 guard: the acknowledgement is only matchable because the node normalizes
// once and keys by what it actually put on the wire.
func TestR20T7WireFormIsWhatTheOutboxIsKeyedBy(t *testing.T) {
	job := hubScannedRelayEvent{
		Kind:         "job.escalate",
		HubActiveJob: HubActiveJob{JobID: "r20t7-form", Epoch: 1, OwnerLane: "lane-cap-a", ReportPath: "/inbox/" + strings.Repeat("p", 300) + ".md", ReportLastLine: "one\ntwo"},
		Reason:       strings.Repeat("r", 300) + "\ntail",
	}
	wire := relayEventWireForm(job)
	for name, value := range map[string]string{"report_path": wire.ReportPath, "reason": wire.Reason, "report_last_line": wire.ReportLastLine} {
		if strings.ContainsAny(value, "\r\n") {
			t.Fatalf("%s still carries a newline, which the hub rejects outright: %q", name, value)
		}
		if len([]rune(value)) > hubRelayPayloadTextLimit {
			t.Fatalf("%s is %d runes, past the %d the hub truncates to", name, len([]rune(value)), hubRelayPayloadTextLimit)
		}
	}
	key := relayEventOutboxKeyFor(wire)
	if key.ReportPath != wire.ReportPath || key.Reason != wire.Reason {
		t.Fatalf("the outbox key is not the wire form: key=%+v wire report_path=%q reason=%q", key, wire.ReportPath, wire.Reason)
	}
	// A job.completed payload has no reason field, so the hub can only echo an
	// empty one. Keying by the record's own reason there never matches.
	completed := relayEventOutboxKeyFor(hubScannedRelayEvent{Kind: "job.completed", HubActiveJob: HubActiveJob{JobID: "r20t7-form", Epoch: 1, ReportPath: "a.md"}, Reason: "operator note"})
	if completed.Reason != "" {
		t.Fatalf("a job.completed outbox key carries reason %q, which no acknowledgement can name", completed.Reason)
	}
}

// TA5/AC5: a record flagged replay:true is acknowledged like any other. The
// flag is observability; it must never cost the node its acknowledgement.
func TestR20T7ReplayFlaggedRecordIsAcknowledged(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t7Lanes, client, 8)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, "r20t7-replay", "00001-job.completed.json",
		`{"type":"job.completed","epoch":1,"owner_lane":"lane-w","label":"lane-w","host":"host-a","report_path":"replay.md","report_last_line":"done"}`)
	key := relayOutboxKey{Kind: "job.completed", JobID: "r20t7-replay", Epoch: 1, ReportPath: "replay.md"}
	if err := store.RecordRelaySent(context.Background(), key, time.Now().Add(-2*relayOutboxBackoff)); err != nil {
		t.Fatal(err)
	}

	node := r20t7Node(inbox, store, time.Now().UTC())
	events := node.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("the restart offered %d records, want 1", len(events))
	}
	var payload struct {
		Replay bool `json:"replay"`
	}
	if json.Unmarshal(events[0].Payload, &payload) != nil || !payload.Replay {
		t.Fatalf("a record stamped by a previous process is missing replay:true: %s", events[0].Payload)
	}
	r20t7Deliver(t, hub, agent, node, events[0])
	state, err := store.RelayOutboxState(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Persisted {
		t.Fatal("a replay-flagged record got no acknowledgement, so it is resent after every restart")
	}
}

func r20t7DeliveredRow(jobID string) handoffkeepRelayEvent {
	return handoffkeepRelayEvent{ID: 900, Kind: "job.completed", JobID: jobID, Epoch: 1, OwnerLane: "lane-w", ReportPath: "report.md", Attempts: 1}
}

// TA6/AC6/AC8: handoffkeep says this row was already delivered, so the note is
// in the pane. Injecting again is the storm; the node still gets its
// acknowledgement, and the operator feed gets the suppressed injection.
func TestR20T7DeliveredRowIsAcknowledgedWithoutReinjection(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t7Lanes, client, 8)
	events := r20t5Subscribe(t, hub)
	fake.seedDelivered("2026-09-05T13:35:00Z", r20t7DeliveredRow("r20t7-delivered"))

	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20t7-delivered", Epoch: 1, OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done"})

	if injected := drainRelays(agent); injected != 0 {
		t.Fatalf("a row handoffkeep holds as delivered was injected %d times, want 0", injected)
	}
	acknowledgements := drainPersisted(agent)
	if len(acknowledgements) != 1 || acknowledgements[0].EventID != 900 {
		t.Fatalf("relay.persisted=%+v, want exactly one naming row 900: without it the node resends forever", acknowledgements)
	}
	suppressed := events("relay.already_delivered")
	if len(suppressed) != 1 {
		t.Fatalf("relay.already_delivered broadcasts=%d, want 1", len(suppressed))
	}
	if !bytes.Contains(suppressed[0], []byte(`"delivered_at":"2026-09-05T13:35:00Z"`)) {
		t.Fatalf("the broadcast does not carry handoffkeep's delivered_at: %s", suppressed[0])
	}
	if hub.AlreadyDeliveredRelayEventCount() != 1 {
		t.Fatalf("already-delivered counter=%d, want 1", hub.AlreadyDeliveredRelayEventCount())
	}
}

// AC8: the gate reads handoffkeep, not the node. A record the node flags as a
// replay is still injected when handoffkeep has no delivery for it, and a
// record with no flag at all is still suppressed when handoffkeep does.
func TestR20T7InjectGateFollowsDeliveredAtNotTheReplayFlag(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		replay    bool
		delivered bool
		inject    int
	}{
		{name: "replay_flag_without_delivery_still_injects", replay: true, delivered: false, inject: 1},
		{name: "no_flag_with_delivery_does_not_inject", replay: false, delivered: true, inject: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			hub, agent := r20t5Hub(t, r20t7Lanes, client, 8)
			job := "r20t7-gate-" + strconv.FormatBool(testCase.replay)
			if testCase.delivered {
				fake.seedDelivered("2026-09-05T13:35:00Z", r20t7DeliveredRow(job))
			}
			hub.relayJobCompletion(hubJobEventPayload{JobID: job, Epoch: 1, OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done", Replay: testCase.replay})
			if injected := drainRelays(agent); injected != testCase.inject {
				t.Fatalf("injections=%d, want %d", injected, testCase.inject)
			}
			if acknowledgements := drainPersisted(agent); len(acknowledgements) != 1 {
				t.Fatalf("relay.persisted=%d, want 1 either way", len(acknowledgements))
			}
		})
	}
}

// TA7/AC7: an existing row handoffkeep has not marked delivered still reaches
// the pane. This is the regression the suppression must not cause.
func TestR20T7UndeliveredRowStillInjects(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t7Lanes, client, 8)
	events := r20t5Subscribe(t, hub)
	// Seeded as an existing row, so the POST replies 200 - but delivered_at is
	// empty, which is the whole difference.
	fake.seedUndelivered(r20t7DeliveredRow("r20t7-undelivered"))

	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20t7-undelivered", Epoch: 1, OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done"})

	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("an undelivered row was injected %d times, want 1", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 1 {
		t.Fatalf("relay.persisted=%d, want 1", len(acknowledgements))
	}
	if suppressed := events("relay.already_delivered"); len(suppressed) != 0 {
		t.Fatalf("an undelivered row was reported as already delivered: %s", suppressed)
	}
	if hub.AlreadyDeliveredRelayEventCount() != 0 {
		t.Fatalf("already-delivered counter=%d, want 0", hub.AlreadyDeliveredRelayEventCount())
	}
}

// TA8/AC10: a hub with no --handoffkeep-env calls handoffkeep zero times and
// injects exactly as it did before R20.
func TestR20T7HubWithoutHandoffkeepStillInjects(t *testing.T) {
	hub := r20Hub(t, r20t7Lanes, nil, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}

	hub.relayJobCompletion(hubJobEventPayload{JobID: "r20t7-nokeep", Epoch: 1, OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done", Replay: true})

	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("a hub without handoffkeep injected %d times, want 1", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 0 {
		t.Fatalf("a hub with no durable record acknowledged %d times, want 0", len(acknowledgements))
	}
	if hub.AlreadyDeliveredRelayEventCount() != 0 {
		t.Fatalf("already-delivered counter=%d on a hub with no handoffkeep", hub.AlreadyDeliveredRelayEventCount())
	}
}

// AC12: the snapshot carries a row for a job called probe-absent whose report
// lives under a scratch directory. That is a verification run that redirected
// --inbox-root but still reached the operator's live daemon over the default
// socket, so its relay outbox row landed in the production journal. A daemon
// must refuse a record from a namespace it does not relay.
func TestR20T7EmitCannotReachADaemonWatchingAnotherInbox(t *testing.T) {
	rows := r20t7LoadSnapshot(t)
	polluting := 0
	for _, row := range rows {
		if strings.HasPrefix(row.ReportPath, "/tmp/") {
			polluting++
		}
	}
	if polluting == 0 {
		t.Fatal("the fixture no longer carries the scratch-directory row this test exists for")
	}

	daemonInbox := t.TempDir()
	foreignInbox := t.TempDir()
	node := &HubClient{jobsInboxRoot: daemonInbox, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	node.SetRelayOutbox(store)
	daemon := NewDaemon(Config{InboxRoot: daemonInbox, Hub: HubDaemonConfig{Enabled: true, Client: node}})

	var mu sync.Mutex
	var lastErr error
	socket := r20t4Socket(t, func(request localRequest) {
		mu.Lock()
		defer mu.Unlock()
		lastErr = daemon.emitRelayEvent(request)
	})

	args := func(root, job string) []string {
		return []string{"--kind", "job.completed", "--job", job, "--report", filepath.Join(root, "r20-report.md"),
			"--owner-lane", "lane-w", "--label", "lane-w", "--host", "host-a", "--report-last-line", "VERDICT: DONE", "--inbox-root", root}
	}
	if code := runEmitCLI(args(foreignInbox, "r20t7-foreign"), &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("emit code=%d", code)
	}
	mu.Lock()
	refused := lastErr
	mu.Unlock()
	if refused == nil {
		t.Fatal("the daemon accepted a relay event recorded in another inbox root; that is how a test run stamps the production journal")
	}
	select {
	case event := <-node.events:
		t.Fatalf("the live node queued a foreign-namespace record: %+v", event)
	default:
	}
	if state, err := store.RelayOutboxState(context.Background(), relayOutboxKey{Kind: "job.completed", JobID: "r20t7-foreign", Epoch: 1, ReportPath: filepath.Join(foreignInbox, "r20-report.md")}); err != nil || state.Found {
		t.Fatalf("a foreign-namespace record reached the production outbox: %+v err=%v", state, err)
	}
	// The event file is still durable in the namespace the caller chose.
	if _, err := os.Stat(filepath.Join(foreignInbox, "jobs", "r20t7-foreign", "events")); err != nil {
		t.Fatalf("the foreign namespace lost its own event file: %v", err)
	}

	// The daemon's own namespace is unaffected: this is the operator path.
	if code := runEmitCLI(args(daemonInbox, "r20t7-local"), &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("local emit code=%d", code)
	}
	select {
	case event := <-node.events:
		if event.relayKey.JobID != "r20t7-local" {
			t.Fatalf("the daemon queued %+v", event.relayKey)
		}
	case <-time.After(2 * time.Second):
		mu.Lock()
		err := lastErr
		mu.Unlock()
		t.Fatalf("the daemon refused a record from its own inbox root: %v", err)
	}
}
