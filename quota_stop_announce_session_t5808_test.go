package panewire

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
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
	got, err := quotaAnnounceSession(inbox, "job-a", "w1:pX")
	if err != nil {
		t.Fatalf("announce for job-a refused: %v", err)
	}
	if got != "sess-a" {
		t.Fatalf("announce session = %q, want job-a's sess-a", got)
	}
}

// Acceptance 2 (hk 5808): a job with no binding of the pane uses the node
// default, which quotaAnnounceSession reports as the empty session, even when
// another job has bound that pane id.
func TestQuotaAnnounceSessionNoBindingUsesNodeDefault_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-b", "w1:pY", "sess-b", "2026-11-12T02:00:00Z")

	got, err := quotaAnnounceSession(inbox, "job-unbound", "w1:pY")
	if err != nil {
		t.Fatalf("unbound job refused: %v", err)
	}
	if got != "" {
		t.Fatalf("unbound job session = %q, want node default (empty)", got)
	}
}

// A pane that another job now owns in the job's own session is not announced
// on (hk 5808): the job's binding is older than a live binding of another job
// on the same pane id in the same session.
func TestQuotaAnnounceSessionRefusesPaneReboundInSession_t5808(t *testing.T) {
	inbox := t.TempDir()
	writeQuotaBindingJob(t, inbox, "job-a", "w1:pZ", "sess-a", "2026-11-12T01:00:00Z")
	writeQuotaBindingJob(t, inbox, "job-c", "w1:pZ", "sess-a", "2026-11-12T03:00:00Z")

	got, err := quotaAnnounceSession(inbox, "job-a", "w1:pZ")
	if !errors.Is(err, errQuotaStopPaneReused) {
		t.Fatalf("announce for job-a = (%q, %v), want errQuotaStopPaneReused", got, err)
	}
	if got != "" {
		t.Fatalf("refused announce returned session %q", got)
	}
}
