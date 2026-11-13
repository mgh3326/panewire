package fleet

import (
	"net/http"
	"path/filepath"
	"testing"
)

// The reaper confirms a local report by its own bytes: the content-addressed
// key `done` publishes to, GET and compared, then the legacy key. It reads no
// event and needs no pin on the job record.
func TestJobDirReportsConfirmedByBytes(t *testing.T) {
	body := []byte("the report\n")
	other := []byte("other bytes\n")
	for _, tc := range []struct {
		name  string
		setup func(r *ttlRig)
		want  string
	}{
		{"published, record unpinned", func(r *ttlRig) {
			r.doc(reportDocumentKey("j1", sha(body)), body, "")
		}, ""},
		{"published, record pins another report", func(r *ttlRig) {
			r.doc(reportDocumentKey("j1", sha(body)), body, "")
			r.store.records["j1"] = map[string]any{"report_doc": "reports/j1/other.md", "report_sha256": sha(other)}
		}, ""},
		{"legacy key, same bytes", func(r *ttlRig) {
			r.doc("reports/j1/report.md", body, "")
		}, ""},
		{"legacy key, other bytes", func(r *ttlRig) {
			r.doc("reports/j1/report.md", other, "")
		}, "report-sha-mismatch"},
		{"content-addressed key wins over a stale legacy key", func(r *ttlRig) {
			r.doc(reportDocumentKey("j1", sha(body)), body, "")
			r.doc("reports/j1/report.md", other, "")
		}, ""},
		{"not in hk", func(r *ttlRig) {}, "report-unconfirmed"},
		{"hk unreadable", func(r *ttlRig) {
			r.hk.Handle(hkFails("GET /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
		}, "hk-unreadable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTTLRig(t)
			path := filepath.Join(r.dir("j1"), "report.md")
			r.write("j1", "report.md", body)
			tc.setup(r)
			if got := jobDirReports(t.Context(), []string{path}, "j1"); got != tc.want {
				t.Fatalf("reason=%q, want %q", got, tc.want)
			}
		})
	}
}

// Each report round is confirmed by its own bytes: a -rN file whose bytes were
// never published keeps the directory.
func TestJobDirReportsEveryRoundNeedsItsOwnDocument(t *testing.T) {
	r := newTTLRig(t)
	first, second := []byte("first\n"), []byte("second\n")
	r.write("j1", "report.md", first)
	r.write("j1", "report-r2.md", second)
	paths := []string{filepath.Join(r.dir("j1"), "report.md"), filepath.Join(r.dir("j1"), "report-r2.md")}
	r.doc(reportDocumentKey("j1", sha(first)), first, "")
	if got := jobDirReports(t.Context(), paths, "j1"); got != "report-unconfirmed" {
		t.Fatalf("reason=%q, want report-unconfirmed while the second round is unpublished", got)
	}
	r.doc(reportDocumentKey("j1", sha(second)), second, "")
	if got := jobDirReports(t.Context(), paths, "j1"); got != "" {
		t.Fatalf("reason=%q, want both rounds confirmed", got)
	}
}
