package fleet

import (
	"path/filepath"
	"testing"
)

func TestJobDirReportsRequiresHKConfirmationForEvent(t *testing.T) {
	r := newTTLRig(t)
	body := []byte("local answer\n")
	path := filepath.Join(r.dir("j1"), "report.md")
	r.write("j1", "report.md", body)
	r.doc("reports/j1/first.md", []byte("first answer\n"), "")
	events := []Event{{Kind: "job.completed", Payload: map[string]any{
		"report": path, "report_sha256": sha(body), "document": "",
	}}}
	if got := jobDirReports(t.Context(), []string{path}, "j1", map[string]any{"report_doc": "reports/j1/first.md", "report_sha256": sha([]byte("first answer\n"))}, events); got != "report-unconfirmed" {
		t.Fatalf("reason=%q, want report-unconfirmed for an event without a document", got)
	}
}

func TestJobDirReportsChecksEventDocumentAgainstHK(t *testing.T) {
	r := newTTLRig(t)
	body := []byte("local answer\n")
	path := filepath.Join(r.dir("j1"), "report.md")
	r.write("j1", "report.md", body)
	r.doc("reports/j1/report.md", []byte("different HK bytes\n"), "")
	r.doc("reports/j1/first.md", []byte("first answer\n"), "")
	events := []Event{{Kind: "job.completed", Payload: map[string]any{
		"report": path, "report_sha256": sha(body), "document": "reports/j1/report.md",
	}}}
	if got := jobDirReports(t.Context(), []string{path}, "j1", map[string]any{"report_doc": "reports/j1/first.md", "report_sha256": sha([]byte("first answer\n"))}, events); got != "report-sha-mismatch" {
		t.Fatalf("reason=%q, want report-sha-mismatch when HK holds different bytes", got)
	}
}

func TestJobDirReportsUsesPinnedDocumentWithoutEvents(t *testing.T) {
	r := newTTLRig(t)
	body := []byte("pinned answer\n")
	path := filepath.Join(r.dir("j1"), "report.md")
	r.write("j1", "report.md", body)
	key := "reports/j1/answer-" + sha(body) + ".md"
	r.doc(key, body, "")
	record := map[string]any{"report_doc": key, "report_sha256": sha(body)}
	if got := jobDirReports(t.Context(), []string{path}, "j1", record, nil); got != "" {
		t.Fatalf("reason=%q, want pinned report confirmed without local events", got)
	}
}
