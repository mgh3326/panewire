package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func r20WriteEvent(t *testing.T, inbox, jobID, name, contents string, mtime time.Time) string {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func r20Node(inbox string, store *Store) *HubClient {
	client := &HubClient{jobsInboxRoot: inbox, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 8)}
	client.SetRelayOutbox(store)
	return client
}

// r20Sent is one successful write of everything the node offered. The send
// stamp lands only after the write leaves the node, so a test that skips the
// commit is modelling a node whose events never made it onto the wire.
func r20Sent(node *HubClient) []hubClientEvent {
	events := node.jobCompletionEvents()
	for _, event := range events {
		node.commitRelaySent(event)
	}
	return events
}

// TE6: persisted rows never come back after a restart; rows only marked sent do,
// and they carry the replay flag so the hub can tell them apart.
func TestR20OutboxSurvivesNodeRestart(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "r20-persisted", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"a.md","report_last_line":"done"}`, time.Time{})
	r20WriteEvent(t, inbox, "r20-pending", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"b.md","report_last_line":"done"}`, time.Time{})

	first := r20Node(inbox, store)
	if events := first.jobCompletionEvents(); len(events) != 2 {
		t.Fatalf("first process sent %d events, want 2", len(events))
	}
	// The hub confirms only one of the two.
	first.recordRelayPersisted(hubOutboundMessage{Type: "relay.persisted", JobID: "r20-persisted", Kind: "job.completed", Epoch: 1, ReportPath: "a.md", EventID: 5})

	// A restart is a fresh client over the same SQLite file. Age both rows out
	// of the retry backoff so persisted_at is the only thing separating them.
	aged := time.Now().Add(-2 * relayOutboxBackoff)
	for _, key := range []relayOutboxKey{
		{Kind: "job.completed", JobID: "r20-persisted", Epoch: 1, ReportPath: "a.md"},
		{Kind: "job.completed", JobID: "r20-pending", Epoch: 1, ReportPath: "b.md"},
	} {
		if err := store.RecordRelaySent(context.Background(), key, aged); err != nil {
			t.Fatal(err)
		}
	}
	restarted := r20Node(inbox, store)
	events := restarted.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("after restart the node sent %d events, want only the unpersisted one", len(events))
	}
	var payload struct {
		JobID  string `json:"job_id"`
		Replay bool   `json:"replay"`
	}
	if json.Unmarshal(events[0].Payload, &payload) != nil {
		t.Fatalf("payload=%s", events[0].Payload)
	}
	if payload.JobID != "r20-pending" {
		t.Fatalf("resent job=%s, want r20-pending", payload.JobID)
	}
	if !payload.Replay {
		t.Fatalf(`a record resent after restart is missing "replay":true: %s`, events[0].Payload)
	}
	// The hub must still accept the flagged payload.
	if completion, ok := decodeHubJobCompletionPayload(events[0].Payload); !ok || !completion.Replay {
		t.Fatalf("hub rejected the replay payload: %s", events[0].Payload)
	}
}

func TestR20OutboxBacksOffWithinSixtySeconds(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "r20-backoff", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"a.md","report_last_line":"done"}`, time.Time{})
	if events := r20Sent(r20Node(inbox, store)); len(events) != 1 {
		t.Fatalf("first send=%d", len(events))
	}
	if events := r20Sent(r20Node(inbox, store)); len(events) != 0 {
		t.Fatalf("a fresh attempt inside the backoff window resent %d events", len(events))
	}
	if err := store.RecordRelaySent(context.Background(), relayOutboxKey{Kind: "job.completed", JobID: "r20-backoff", Epoch: 1, ReportPath: "a.md"}, time.Now().Add(-2*relayOutboxBackoff)); err != nil {
		t.Fatal(err)
	}
	if events := r20Sent(r20Node(inbox, store)); len(events) != 1 {
		t.Fatalf("an aged-out attempt was not retried: %d", len(events))
	}
}

// TE7: the outbox scan keeps a 24h window on event-file mtime.
func TestR20OutboxScanKeepsTwentyFourHourWindow(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	// created_at is absent, so the scan falls back to the file's mtime.
	const body = `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"REPORT","report_last_line":"done"}`
	r20WriteEvent(t, inbox, "r20-fresh", "00001-job.completed.json", strings.Replace(body, "REPORT", "fresh.md", 1), time.Now().Add(-23*time.Hour))
	r20WriteEvent(t, inbox, "r20-stale", "00001-job.completed.json", strings.Replace(body, "REPORT", "stale.md", 1), time.Now().Add(-25*time.Hour))

	events := r20Node(inbox, store).jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("the 24h window produced %d events, want 1", len(events))
	}
	var payload struct {
		JobID string `json:"job_id"`
	}
	if json.Unmarshal(events[0].Payload, &payload) != nil || payload.JobID != "r20-fresh" {
		t.Fatalf("scan returned %s, want only r20-fresh", events[0].Payload)
	}
}

// AC12: the outbox window is its own axis; the 72h active-job default stands.
func TestR20ActiveJobMaxAgeDefaultIsUnchanged(t *testing.T) {
	if defaultHubJobActiveMaxAge != 72*time.Hour {
		t.Fatalf("PANEWIRE_JOB_ACTIVE_MAX_AGE default=%s, want 72h", defaultHubJobActiveMaxAge)
	}
	if defaultRelayOutboxMaxAge != 24*time.Hour {
		t.Fatalf("relay outbox default=%s, want 24h", defaultRelayOutboxMaxAge)
	}
	t.Setenv("PANEWIRE_RELAY_OUTBOX_MAX_AGE", "1h")
	if relayOutboxMaxAge() != time.Hour {
		t.Fatalf("override=%s", relayOutboxMaxAge())
	}
	if hubJobActiveMaxAge() != 72*time.Hour {
		t.Fatalf("the outbox override leaked into the active-job window: %s", hubJobActiveMaxAge())
	}
}

func TestR20RelayPersistedMessageIsAClosedShape(t *testing.T) {
	valid, _ := json.Marshal(hubRelayPersistedEvent{Type: "relay.persisted", JobID: "r20-shape", Kind: "job.completed", Epoch: 1, ReportPath: "a.md", Reason: "", EventID: 3})
	message, ok := parseHubOutbound(valid)
	if !ok || message.Kind != "job.completed" || message.EventID != 3 || message.ReportPath != "a.md" {
		t.Fatalf("message=%+v ok=%v", message, ok)
	}
	for _, bad := range []string{
		`{"type":"relay.persisted","job_id":"r20-shape","kind":"job.other","epoch":1,"report_path":"a.md","reason":"","event_id":3}`,
		`{"type":"relay.persisted","job_id":"r20-shape","kind":"job.completed","epoch":1,"report_path":"a.md","reason":"","event_id":0}`,
		`{"type":"relay.persisted","job_id":"r20-shape","kind":"job.completed","epoch":1,"report_path":"a.md","event_id":3}`,
	} {
		if _, ok := parseHubOutbound([]byte(bad)); ok {
			t.Fatalf("accepted %s", bad)
		}
	}
}
