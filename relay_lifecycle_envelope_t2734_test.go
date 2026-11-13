package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// `wrk done` writes two job.completed files per round: the arbiter envelope
// from internal/fleet/lifecycle.go, then the flat record `panewire emit` wrote
// from it. These tests pin the node-side rule that keeps the envelope from
// being resent forever to a lane that no longer has a route.

const (
	t2734Job        = "job-a"
	t2734Parent     = "parent-lane"
	t2734Report     = "/inbox/jobs/job-a/report.md"
	t2734Envelope   = "00005-job.completed.json"
	t2734FlatRecord = "00006-job.completed.json"
	t2734Lanes      = `{"lanes":{"parent-lane":{"machine":"host-a","pane":"w1:p9"}}}`
)

// t2734SettledAt is old enough that an envelope has passed the settle window,
// so a fixture states "emit had its chance" without waiting for the clock.
func t2734SettledAt() string {
	return time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
}

// t2734EnvelopeJSON copies the real arbiter body: lifecycle.go's payload map
// inside the store's created_at/job_id/kind/payload/seq record. role is empty
// for every envelope written today; a non-empty one tests the role branch.
func t2734EnvelopeJSON(createdAt, owner, parent, role string) string {
	roleField := ""
	if role != "" {
		roleField = fmt.Sprintf(`"role":%q,`, role)
	}
	return fmt.Sprintf(`{"created_at":%q,"job_id":%q,"kind":"job.completed","payload":{"document":"reports/job-a/report.md","label":"job-a","owner_lane":%q,"pane_id":"w1:p1","parent_lane":%q,"question":"",%s"report":%q,"report_sha256":"abc123"},"seq":5}`,
		createdAt, t2734Job, owner, parent, roleField, t2734Report)
}

// t2734FlatJSON copies the record writeEmitRecord produces for lifecycle.go's
// argument list: owner is the parent lane for a builder or captain.
func t2734FlatJSON(createdAt string, epoch int) string {
	return fmt.Sprintf(`{"type":"job.completed","kind":"job.completed","job_id":%q,"epoch":%d,"created_at":%q,"agent_label":"job-a","owner_lane":%q,"label":"job-a","host":"host-a","report_path":%q,"report_last_line":"VERDICT: DONE","pane_id":"w1:p1"}`,
		t2734Job, epoch, createdAt, t2734Parent, t2734Report)
}

func t2734Completions(t *testing.T, inbox string) []hubScannedRelayEvent {
	t.Helper()
	var completed []hubScannedRelayEvent
	for _, event := range scanHubRelayEventsWithin(inbox, 0) {
		if event.Kind == "job.completed" {
			completed = append(completed, event)
		}
	}
	return completed
}

// The observed fleet shape: exactly one completion, on the parent lane, from
// the flat record.
func TestT2734EnvelopeBesideEmitRecordYieldsOneParentCompletion(t *testing.T) {
	inbox := t.TempDir()
	at := t2734SettledAt()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(at, t2734Job, t2734Parent, ""))
	r20t7WriteEvent(t, inbox, t2734Job, t2734FlatRecord, t2734FlatJSON(at, 1))

	completed := t2734Completions(t, inbox)
	if len(completed) != 1 {
		t.Fatalf("job.completed events=%d (%+v), want exactly the flat record", len(completed), completed)
	}
	if completed[0].OwnerLane != t2734Parent || completed[0].EventID != t2734FlatRecord {
		t.Fatalf("owner lane=%q event=%q, want %q from %s", completed[0].OwnerLane, completed[0].EventID, t2734Parent, t2734FlatRecord)
	}
	if completed[0].ReportPath != t2734Report {
		t.Fatalf("report path=%q, want %q", completed[0].ReportPath, t2734Report)
	}
}

// A flat record of another epoch announces another assignment, so it does not
// stand in for this envelope.
func TestT2734EmitRecordOfAnotherEpochDoesNotSkipEnvelope(t *testing.T) {
	inbox := t.TempDir()
	at := t2734SettledAt()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(at, t2734Job, t2734Parent, ""))
	r20t7WriteEvent(t, inbox, t2734Job, t2734FlatRecord, t2734FlatJSON(at, 2))

	byEvent := map[string]hubScannedRelayEvent{}
	for _, event := range t2734Completions(t, inbox) {
		byEvent[event.EventID] = event
	}
	if len(byEvent) != 2 || byEvent[t2734Envelope].Epoch != 1 || byEvent[t2734FlatRecord].Epoch != 2 {
		t.Fatalf("completions=%+v, want the epoch 1 envelope and the epoch 2 record", byEvent)
	}
}

// The scan's mtime cut can drop the flat record while its envelope survives.
// The record still exists, so the envelope must not come back as a fallback.
func TestT2734AgeCutEmitRecordStillSkipsEnvelope(t *testing.T) {
	inbox := t.TempDir()
	at := t2734SettledAt()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(at, t2734Job, t2734Parent, ""))
	r20WriteEvent(t, inbox, t2734Job, t2734FlatRecord, t2734FlatJSON(at, 1), time.Now().Add(-2*time.Hour))

	if events := scanHubRelayEventsWithin(inbox, time.Hour); len(events) != 0 {
		t.Fatalf("offered=%+v, want nothing: the record was cut by age and the envelope is its duplicate", events)
	}
}

// Another job's flat record says nothing about this job's envelope.
func TestT2734EmitRecordInAnotherJobDoesNotSkipEnvelope(t *testing.T) {
	inbox := t.TempDir()
	at := t2734SettledAt()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(at, t2734Job, t2734Parent, ""))
	r20t7WriteEvent(t, inbox, "job-b", t2734FlatRecord, t2734FlatJSON(at, 1))

	var envelopes int
	for _, event := range t2734Completions(t, inbox) {
		if event.JobID == t2734Job && event.EventID == t2734Envelope {
			envelopes++
		}
	}
	if envelopes != 1 {
		t.Fatalf("envelopes offered for %s=%d, want 1", t2734Job, envelopes)
	}
}

// relay=file-only: the envelope is the only record of the completion, so it
// routes the way lifecycle.go chose for the emit it could not finish.
func TestT2734EnvelopeOnlyRoutesLikeLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, owner, parent, role, want string
	}{
		{"builder goes to parent", "owner-lane", t2734Parent, "builder", t2734Parent},
		{"implementer stays on owner", "owner-lane", t2734Parent, "implementer", "owner-lane"},
		{"implementer stays on owner even when owner is the job", t2734Job, t2734Parent, "implementer", t2734Job},
		{"builder without parent stays on owner", "owner-lane", "", "builder", "owner-lane"},
		{"no role and owner is the job goes to parent", t2734Job, t2734Parent, "", t2734Parent},
		{"no role and owner is a lane stays on owner", "owner-lane", t2734Parent, "", "owner-lane"},
		{"no role and no parent stays on owner", t2734Job, "", "", t2734Job},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbox := t.TempDir()
			r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(t2734SettledAt(), tc.owner, tc.parent, tc.role))
			completed := t2734Completions(t, inbox)
			if len(completed) != 1 {
				t.Fatalf("job.completed events=%d, want 1", len(completed))
			}
			if completed[0].OwnerLane != tc.want {
				t.Fatalf("owner lane=%q, want %q", completed[0].OwnerLane, tc.want)
			}
			// The envelope spells it payload.report; without this the note
			// would announce a completion with no report to read.
			if completed[0].ReportPath != t2734Report {
				t.Fatalf("report path=%q, want %q", completed[0].ReportPath, t2734Report)
			}
		})
	}
}

// lifecycle.go writes the envelope and only then runs emit. A scan between the
// two must not deliver the envelope, or the parent gets the note twice.
func TestT2734EnvelopeWaitsForEmitBeforeFallback(t *testing.T) {
	inbox := t.TempDir()
	now := time.Now().UTC().Format(time.RFC3339)
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(now, t2734Job, t2734Parent, ""))
	if completed := t2734Completions(t, inbox); len(completed) != 0 {
		t.Fatalf("a fresh envelope was offered before emit could write: %+v", completed)
	}

	r20t7WriteEvent(t, inbox, t2734Job, t2734FlatRecord, t2734FlatJSON(now, 1))
	completed := t2734Completions(t, inbox)
	if len(completed) != 1 || completed[0].EventID != t2734FlatRecord {
		t.Fatalf("completions=%+v, want only the flat record once emit landed", completed)
	}
}

// The other envelope shape carries report_path itself and has no emit twin.
// The rule is for lifecycle.go's record, so this one is offered as before.
func TestT2734OtherEnvelopeShapeIsUnchanged(t *testing.T) {
	inbox := t.TempDir()
	now := time.Now().UTC().Format(time.RFC3339)
	r20t7WriteEvent(t, inbox, t2734Job, "00002-job.completed.json",
		`{"created_at":"`+now+`","job_id":"job-a","kind":"job.completed","payload":{"agent_label":"agent-a","owner_lane":"job-a","parent_lane":"parent-lane","label":"agent-a","report_path":"report.md","host":"host-a"},"seq":2}`)
	completed := t2734Completions(t, inbox)
	if len(completed) != 1 || completed[0].OwnerLane != t2734Job || completed[0].ReportPath != "report.md" {
		t.Fatalf("completions=%+v, want the record offered unchanged", completed)
	}
}

// End to end for the loop itself. A node upgrading from the looping binary
// holds the unacknowledged envelope row; it must stop resending once the flat
// record is acknowledged, however far the clock moves.
func TestT2734UpgradedNodeStopsResendingEnvelope(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t2734Lanes, client, 8)
	events := r20t5Subscribe(t, hub)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	at := t2734SettledAt()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(at, t2734Job, t2734Parent, ""))
	r20t7WriteEvent(t, inbox, t2734Job, t2734FlatRecord, t2734FlatJSON(at, 1))

	clock := time.Now().UTC()
	node := r20Node(inbox, store)
	node.now = func() time.Time { return clock }
	stale := relayOutboxKey{Kind: "job.completed", JobID: t2734Job, Epoch: 1, EventID: t2734Envelope}
	if err := store.RecordRelaySent(context.Background(), stale, clock.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	sent := r20Sent(node)
	if len(sent) != 1 || sent[0].relayKey.EventID != t2734FlatRecord {
		t.Fatalf("offered=%+v, want only the flat record", sent)
	}
	directives, acknowledgements := t507Deliver(t, hub, agent, node, sent[0])
	if len(directives) != 1 || directives[0].Pane != "w1:p9" || len(acknowledgements) != 1 {
		t.Fatalf("injected=%+v acknowledgements=%+v, want one note in the parent pane", directives, acknowledgements)
	}

	for step := 1; step <= 5; step++ {
		clock = clock.Add(relayOutboxBackoff)
		if resent := r20Sent(node); len(resent) != 0 {
			t.Fatalf("step %d re-presented %+v", step, resent)
		}
	}
	if unrouted := events("relay.unrouted"); len(unrouted) != 0 {
		t.Fatalf("relay.unrouted=%d, want 0", len(unrouted))
	}
	if got := fake.attemptsFor("job.completed", t2734Job, 1, t2734Report, ""); got != 1 {
		t.Fatalf("handoffkeep attempts=%d, want 1", got)
	}
}

// file-only fallback end to end, starting from the row the looping binary left
// (empty report_path, persisted_at NULL). The event-keyed row must still be the
// one the acknowledgement retires, or the resend would simply move lanes.
func TestT2734FileOnlyEnvelopeReachesParentAndRetiresItsRow(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t2734Lanes, client, 8)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, t2734Job, "00001-job.claim.json", `{"kind":"job.claim","job_id":"job-a","payload":{"agent_label":"agent-a","owner_lane":"job-a"},"seq":1}`)
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(t2734SettledAt(), t2734Job, t2734Parent, ""))

	clock := time.Now().UTC()
	node := r20Node(inbox, store)
	node.now = func() time.Time { return clock }
	stale := relayOutboxKey{Kind: "job.completed", JobID: t2734Job, Epoch: 1, EventID: t2734Envelope}
	if err := store.RecordRelaySent(context.Background(), stale, clock.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	sent := r20Sent(node)
	if len(sent) != 1 {
		t.Fatalf("offered=%d, want the envelope", len(sent))
	}
	var payload hubJobEventPayload
	if err := json.Unmarshal(sent[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.OwnerLane != t2734Parent || payload.ReportPath != t2734Report || payload.AgentLabel != "agent-a" {
		t.Fatalf("wire payload=%s, want the parent lane, the report and the claim's label", sent[0].Payload)
	}
	directives, acknowledgements := t507Deliver(t, hub, agent, node, sent[0])
	if len(directives) != 1 || directives[0].Pane != "w1:p9" || len(acknowledgements) != 1 {
		t.Fatalf("injected=%+v acknowledgements=%+v, want one note in the parent pane", directives, acknowledgements)
	}
	clock = clock.Add(relayOutboxBackoff)
	if resent := r20Sent(node); len(resent) != 0 {
		t.Fatalf("an acknowledged envelope was re-presented: %+v", resent)
	}
}

// When even the resolved lane has no route, the hub answers nothing and the row
// stays open. The existing outbox gate is what bounds that: one offer per
// backoff window, and the replay flag on each later one.
func TestT2734UnroutableEnvelopeIsOfferedOncePerBackoffWindow(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t2734Lanes, client, 8)
	events := r20t5Subscribe(t, hub)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, t2734Job, t2734Envelope, t2734EnvelopeJSON(t2734SettledAt(), t2734Job, "", ""))

	start := time.Now().UTC()
	clock := start
	node := r20Node(inbox, store)
	node.now = func() time.Time { return clock }

	first := r20Sent(node)
	if len(first) != 1 {
		t.Fatalf("first scan offered=%d, want 1", len(first))
	}
	directives, acknowledgements := t507Deliver(t, hub, agent, node, first[0])
	if len(directives) != 0 || len(acknowledgements) != 0 {
		t.Fatalf("an unroutable envelope injected=%+v acknowledged=%+v", directives, acknowledgements)
	}
	if unrouted := events("relay.unrouted"); len(unrouted) != 1 {
		t.Fatalf("relay.unrouted=%d, want 1", len(unrouted))
	}

	for _, elapsed := range []time.Duration{time.Second, 10 * time.Second, relayOutboxBackoff - time.Millisecond} {
		clock = start.Add(elapsed)
		if offered := r20Sent(node); len(offered) != 0 {
			t.Fatalf("re-presented %v after the attempt, inside the %v backoff", elapsed, relayOutboxBackoff)
		}
	}

	clock = start.Add(relayOutboxBackoff)
	second := r20Sent(node)
	if len(second) != 1 {
		t.Fatalf("offered=%d at the backoff boundary, want 1", len(second))
	}
	clock = clock.Add(relayOutboxBackoff - time.Millisecond)
	if offered := r20Sent(node); len(offered) != 0 {
		t.Fatalf("the second window offered again: %+v", offered)
	}
}
