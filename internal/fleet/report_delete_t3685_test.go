package fleet

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3685 item 2: `done` deletes the local report once hk holds its bytes and
// the record pins them, and leaves the result line job.done answers from. The
// [report] relay names the hk key in place of the deleted path.

func TestDoneDeletesThePinnedReport(t *testing.T) {
	r := newDoneRig(t)
	body := "work done\nREADY: done\n"
	r.write(body)
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Lstat(r.path); !os.IsNotExist(e) {
		t.Fatalf("report still exists after the pin (err=%v)", e)
	}
	if doc, ok := storedDoc(r.hk, r.key(body)); !ok || doc["job"] != "j" {
		t.Fatalf("hk document %v present=%v, want the report under %s", doc, ok, r.key(body))
	}
	marker, e := os.ReadFile(filepath.Join(r.a.Jobs, "j", JobDoneMarker))
	if e != nil {
		t.Fatal(e)
	}
	lines := strings.Split(strings.TrimRight(string(marker), "\n"), "\n")
	want := "OK job=j report=" + r.path + " kind=job.completed"
	if len(lines) != 2 || lines[0] != reportDigest([]byte(body)) || lines[1] != want {
		t.Fatalf("marker=%q, want the digest and %q", marker, want)
	}
	// The ledger keeps the report's receipts: the file is not the record.
	if got := len(r.events(eventReportPublished)); got != 1 {
		t.Fatalf("%s receipts=%d, want one", eventReportPublished, got)
	}
}

// hk 3685 item 3: a failed upload keeps the local copy and blocks nothing.
func TestDoneKeepsTheReportWhenTheUploadFails(t *testing.T) {
	r := newDoneRig(t)
	r.hk.Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
	r.write("kept\n")
	wantNotInHK(t, r.done())
	if _, e := os.Lstat(r.path); e != nil {
		t.Fatalf("report gone after a failed upload: %v", e)
	}
	if done := r.events("job.completed"); len(done) != 1 || str(done[0], "document") != "" {
		t.Fatalf("completion events=%v, want one without a document", done)
	}
	if _, e := os.Lstat(filepath.Join(r.a.Jobs, "j", JobDoneMarker)); !os.IsNotExist(e) {
		t.Fatalf("a failed upload wrote the done marker")
	}
}

// A pin hk did not answer keeps the file: the record does not name its bytes.
func TestDoneKeepsTheReportWhileThePinIsPending(t *testing.T) {
	r := newDoneRig(t)
	uploadFails := true
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if uploadFails && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			return http.StatusInternalServerError, `{"error":"internal_error"}`, true
		}
		return 0, "", false
	})
	r.a.JobStore = &pinFailStore{fakeJobStore: r.store, n: 1}
	r.write("pending\n")
	wantNotInHK(t, r.done())
	uploadFails = false
	e := r.done()
	if code(e) != exitFailure || e == nil || !strings.Contains(e.Error(), "report not pinned in hk") {
		t.Fatalf("done error=%v, want the pending pin named", e)
	}
	if _, e := os.Lstat(r.path); e != nil {
		t.Fatalf("report gone with a pending pin: %v", e)
	}
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Lstat(r.path); !os.IsNotExist(e) {
		t.Fatalf("the rerun's pin kept the report")
	}
}

// The rerun after the delete is the duplicate the ledger says it is: no hk
// call, no report file, the same line the first run printed.
func TestDoneRerunAfterDeleteAnswersDuplicate(t *testing.T) {
	r := newDoneRig(t)
	r.write("once\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	calls := r.hkCalls()
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.hkCalls(); got != calls {
		t.Fatalf("the duplicate made %d hk calls", got-calls)
	}
	if out := r.stdout(); !strings.Contains(out, "duplicate=yes") {
		t.Fatalf("out=%q, want a duplicate answer", out)
	}
}

// The [report] relay ends at the hk key and does not repeat it as a doc:
// suffix: the local path it used to name is deleted.
func TestDoneRelayNamesTheHKKey(t *testing.T) {
	r := newDoneRig(t)
	body := "last line\n"
	r.write(body)
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.relayReports(); len(got) != 1 || got[0] != r.key(body) {
		t.Fatalf("relay reports=%q, want the hk key %s", got, r.key(body))
	}
	if got := r.relays(); len(got) != 1 || got[0] != "last line" {
		t.Fatalf("relay last lines=%q, want the plain last line", got)
	}
}

// hk 3685 item 4: the node sweep leaves a report the record pins alone, even
// though `done` deleted the file after the pin.
func TestReportRepublishSkipsAPinnedDeletedReport(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	body := "pinned and gone\n"
	path := reportFile(t, a, "j", body)
	sha := reportDigest([]byte(body))
	key := reportDocumentKey("j", sha)
	putEvent(t, a, "j", "job.completed", map[string]any{"report": path, "report_sha256": sha, "document": key})
	putEvent(t, a, "j", "job.report_published", map[string]any{"report_sha256": sha, "document": key})
	putEvent(t, a, "j", "job.report_pinned", map[string]any{"report_sha256": sha, "document": key, "outcome": "pinned"})
	putEvent(t, a, "j", "job.record_settled", map[string]any{"report_sha256": sha, "pinned": true})
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	before := len(hk.Requests())
	s, lines := loggingSweeper()
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if got := len(hk.Requests()) - before; got != 0 {
		t.Fatalf("the sweep made %d hk calls for a pinned report", got)
	}
	if len(*lines) != 0 {
		t.Fatalf("lines=%q, want none", *lines)
	}
}

// The [reap] job-dir gate does not hold a dir whose report the fleet deleted:
// the file is hk's now, so it is not local evidence left behind.
func TestJobDirTTLDeletesAfterTheReportIsDeleted(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	if e := os.Remove(filepath.Join(r.dir("j1"), "report.md")); e != nil {
		t.Fatal(e)
	}
	r.age("j1")
	out := r.pass(true)
	wantDelete(t, r, out, "j1", "confirmed")
}
