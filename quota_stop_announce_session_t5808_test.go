package panewire

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeQuotaBindingJob writes a job whose only event binds pane in session at
// createdAt, so a test can order a reuse explicitly (hk 5808).
func writeQuotaBindingJob(t *testing.T, inbox, jobID, pane, session, createdAt string) {
	t.Helper()
	eventsDir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ev := map[string]any{
		"kind":       "job.spawned",
		"created_at": createdAt,
		"payload":    map[string]any{"pane_id": pane, "herdr_session": session, "label": jobID, "role": "worker"},
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "0001-job.spawned.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeQuotaJobEnded ends jobID with a job.completed event (hk 5808).
func writeQuotaJobEnded(t *testing.T, inbox, jobID, createdAt string) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"kind": "job.completed", "created_at": createdAt})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inbox, "jobs", jobID, "events", "0002-job.completed.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Acceptance 1 (hk 5808): job A is queued on pane w1:pX in sess-a; job B later
// reuses the same pane id in sess-b. The announce for A resolves A's session and
// never reads B's pane. Base (pane-only resolution) returns sess-b here.
func TestQuotaAnnounceSessionUsesJobBindingNotReusedPane_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-a", "w1:pX", "sess-a", "2026-11-12T01:00:00Z")
	writeQuotaBindingJob(t, inbox, "job-b", "w1:pX", "sess-b", "2026-11-12T02:00:00Z")

	if got := nodeJobHerdrSessionByPaneFromRoot(inbox, "w1:pX"); got != "sess-b" {
		t.Fatalf("pane-only resolution = %q, want sess-b (the reuse this test guards against)", got)
	}
	got, err := quotaAnnounceSession(inbox, "job-a", "w1:pX", nil)
	if err != nil {
		t.Fatalf("announce for job-a refused: %v", err)
	}
	if got != "sess-a" {
		t.Fatalf("announce session = %q, want job-a's sess-a", got)
	}
}

// Review (hk 5808): a job with no binding of the pane takes the node's own
// session, the way the daemon wires it, never another job's session by pane id.
// spawn.json names fleet and HERDR_SESSION names default, so the two sources
// must not be confused.
func TestQuotaAnnounceSessionNoBindingUsesNodeSpawnSession_t5808(t *testing.T) {
	t.Setenv("HERDR_SESSION", "default")
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-b", "w1:pY", "sess-b", "2026-11-12T02:00:00Z")
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"cwd_map":{},"workspace":"w","herdr_session":"fleet","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config, jobsInboxRoot: inbox}

	got, err := quotaAnnounceSession(inbox, "job-unbound", "w1:pY", quotaNodeAnnounceSession(client))
	if err != nil {
		t.Fatalf("unbound job refused: %v", err)
	}
	if got != "fleet" {
		t.Fatalf("unbound job session = %q, want the node spawn session fleet (not HERDR_SESSION, not sess-b)", got)
	}
}

// A pane that another job now owns in the job's own session is not announced
// on (hk 5808): another job's binding of the same pane id is newer, in the same
// session, and that job is still live.
func TestQuotaAnnounceSessionRefusesPaneReboundInSession_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-a", "w1:pZ", "sess-a", "2026-11-12T01:00:00Z")
	writeQuotaBindingJob(t, inbox, "job-c", "w1:pZ", "sess-a", "2026-11-12T03:00:00Z")

	got, err := quotaAnnounceSession(inbox, "job-a", "w1:pZ", nil)
	if !errors.Is(err, errQuotaStopPaneReused) {
		t.Fatalf("announce for job-a = (%q, %v), want errQuotaStopPaneReused", got, err)
	}
	if got != "" {
		t.Fatalf("refused announce returned session %q", got)
	}
}

// Review (hk 5808): a newer binding of the pane in the same session is a reuse
// even after its job ended, while the pane stays open. Previously a live job's
// binding ranked above it, so the announce read the reused pane as job A's.
func TestQuotaAnnounceSessionRefusesPaneReboundByEndedJob_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-a", "w1:pE", "sess-a", "2026-11-12T01:00:00Z")
	writeQuotaBindingJob(t, inbox, "job-b", "w1:pE", "sess-a", "2026-11-12T03:00:00Z")
	writeQuotaJobEnded(t, inbox, "job-b", "2026-11-12T03:30:00Z")

	// The fixture precondition: job B is ended and job A is live.
	ended := map[string]bool{}
	if err := hubIndexFor(inbox).visitJobs(nil, true, func(job *hubIndexedJob) bool {
		ended[job.id] = job.ended
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !ended["job-b"] || ended["job-a"] {
		t.Fatalf("fixture: ended = %v, want job-b ended and job-a live", ended)
	}

	got, err := quotaAnnounceSession(inbox, "job-a", "w1:pE", nil)
	if !errors.Is(err, errQuotaStopPaneReused) {
		t.Fatalf("announce for job-a = (%q, %v), want errQuotaStopPaneReused", got, err)
	}
}

// Review (hk 5808): a refused reuse is a terminal disposition. The decision's
// row is recorded done with result pane_reused, not backed off for a retry, so
// the tick never selects it again, even after the replacement's ledger is pruned.
func TestQuotaHandOverRetiresReusedPaneTerminally_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-a", "w1:pR", "sess-a", "2026-11-12T01:00:00Z")
	writeQuotaBindingJob(t, inbox, "job-b", "w1:pR", "sess-a", "2026-11-12T03:00:00Z")

	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	q, err := newQuotaStopManager(store, inbox, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO quota_stop_decisions (event_id, job, pane, label, decision, reason) VALUES ('ev-reuse', 'job-a', 'w1:pR', 'job-a', 'handover', 'quota')`); err != nil {
		t.Fatal(err)
	}

	q.handOver(t.Context(), "ev-reuse", "job-a", "w1:pR", "claude", "", "attempt-1", time.Now())

	var recorded int
	var retry int64
	var result string
	if err := store.db.QueryRow(`SELECT action_recorded, action_retry, result FROM quota_stop_decisions WHERE event_id='ev-reuse'`).Scan(&recorded, &retry, &result); err != nil {
		t.Fatal(err)
	}
	if recorded != 1 || retry != 0 || result != "pane_reused" {
		t.Fatalf("row after refused hand-over: action_recorded=%d action_retry=%d result=%q, want 1, 0, pane_reused", recorded, retry, result)
	}
}
