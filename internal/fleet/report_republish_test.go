package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// The node sweep republishes a report whose `done` upload failed (hk 4372).
// These tests cover the closed contract of report_republish.go: the failed
// upload is republished on the next pass with the done path's receipt and pin,
// an already pinned report is left alone, a still-changing report waits, an hk
// error is retried next pass without spinning, and one pass is bounded.

// republishFixture is a done-ready job "j" whose completion the tests write.
func republishFixture(t *testing.T) (*App, *spawnHK) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	return a, hk
}

// seedDoneJobNamed is seedDoneJob for any job id: a spawned implementer the
// done path may complete.
func seedDoneJobNamed(t *testing.T, a *App, id string) {
	t.Helper()
	seedJob(t, a, id, "implementer")
	if e := a.JobStore.(*fakeJobStore).claimSpawned(context.Background(), id, "42", a.machine()); e != nil {
		t.Fatal(e)
	}
}

// reportFile writes the job's report and returns its path.
func reportFile(t *testing.T, a *App, id, body string) string {
	t.Helper()
	path := filepath.Join(a.Jobs, id, "report.md")
	if e := os.WriteFile(path, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
	return path
}

// failReportUploads makes the fake hk refuse report document PUTs while the
// returned gate is open, as an hk outage does, and answer them normally once
// it closes.
func failReportUploads(hk *spawnHK) *atomic.Bool {
	var fail atomic.Bool
	fail.Store(true)
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if fail.Load() && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			return http.StatusInternalServerError, `{"error":"internal_error"}`, true
		}
		return 0, "", false
	})
	return &fail
}

// documentPuts counts the document PUTs the fake hk received.
func documentPuts(hk *spawnHK) int {
	n := 0
	for _, r := range hk.Requests() {
		if strings.HasPrefix(r.Route(), "PUT /v1/documents/") {
			n++
		}
	}
	return n
}

// storedDoc is the document the fake hk holds at key.
func storedDoc(hk *spawnHK, key string) (map[string]any, bool) {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	raw, ok := hk.docs[key]
	if !ok {
		return nil, false
	}
	var doc map[string]any
	if json.Unmarshal([]byte(raw), &doc) != nil {
		return nil, false
	}
	return doc, true
}

// loggingSweeper is a sweeper whose Info lines the test can read, one entry per
// logged line with its attributes rendered as key=value pairs.
func loggingSweeper() (*TaskPaneSweeper, *[]string) {
	lines := []string{}
	return &TaskPaneSweeper{Info: func(message string, attrs ...any) {
		var b strings.Builder
		b.WriteString(message)
		for i := 0; i+1 < len(attrs); i += 2 {
			fmt.Fprintf(&b, " %v=%v", attrs[i], attrs[i+1])
		}
		lines = append(lines, b.String())
	}}, &lines
}

func countLines(lines []string, prefix string) int {
	n := 0
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// A report whose `done` upload failed is published by the next sweep, with the
// receipt and the record pin the done path would have written.
func TestReportRepublishPublishesAFailedUpload(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	fail := failReportUploads(hk)
	body := "the report\n"
	path := reportFile(t, a, "j", body)
	wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
	sha := reportDigest([]byte(body))
	key := reportDocumentKey("j", sha)
	if _, ok := storedDoc(hk, key); ok {
		t.Fatalf("report was in hk before the sweep")
	}
	fail.Store(false)
	s, lines := loggingSweeper()
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if doc, ok := storedDoc(hk, key); !ok || doc["job"] != "j" {
		t.Fatalf("hk document %v present=%v, want the report under %s", doc, ok, key)
	}
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	if got := publishedDocument(evs, sha); got != key {
		t.Fatalf("publish receipt document=%q, want %s", got, key)
	}
	rec := a.JobStore.(*fakeJobStore).record(t, "j")
	if str(rec, "report_doc") != key || str(rec, "report_sha256") != sha {
		t.Fatalf("record=%v, want the report pinned to %s", rec, key)
	}
	if countLines(*lines, "report republished ") != 1 {
		t.Fatalf("lines=%q, want one republished line", *lines)
	}
	// Every receipt now exists, so the next pass makes no upload at all.
	before := documentPuts(hk)
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if got := documentPuts(hk) - before; got != 0 {
		t.Fatalf("second pass made %d document PUTs, want none", got)
	}
}

// A report the record already pins is nothing: no upload, no event, no line,
// even when the local publish receipt is missing.
func TestReportRepublishSkipsAPinnedReport(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	body := "pinned already\n"
	path := reportFile(t, a, "j", body)
	sha := reportDigest([]byte(body))
	putEvent(t, a, "j", "job.completed", map[string]any{"report": path, "report_sha256": sha, "document": ""})
	putEvent(t, a, "j", "job.record_settled", map[string]any{"report_sha256": sha, "pinned": true})
	before := len(hk.Requests())
	s, lines := loggingSweeper()
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if got := len(hk.Requests()) - before; got != 0 {
		t.Fatalf("sweep made %d hk calls, want none", got)
	}
	if len(*lines) != 0 {
		t.Fatalf("lines=%q, want none", *lines)
	}
}

// A report whose file no longer holds the announced bytes is still changing,
// or a later round: the sweep waits and logs the refusal once.
func TestReportRepublishWaitsForAChangingReport(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	fail := failReportUploads(hk)
	path := reportFile(t, a, "j", "first\n")
	wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
	fail.Store(false)
	if e := os.WriteFile(path, []byte("first\nstill writing\n"), 0600); e != nil {
		t.Fatal(e)
	}
	sha := reportDigest([]byte("first\n"))
	s, lines := loggingSweeper()
	before := documentPuts(hk)
	for pass := range 2 {
		if e := s.RepublishReports(ctx, a); e != nil {
			t.Fatal(e)
		}
		if got := documentPuts(hk) - before; got != 0 {
			t.Fatalf("pass %d published %d documents, want none", pass, got)
		}
	}
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	if got := publishedDocument(evs, sha); got != "" {
		t.Fatalf("publish receipt document=%q, want none", got)
	}
	if got := countLines(*lines, "report republish kept "); got != 1 {
		t.Fatalf("lines=%q, want the refusal logged once", *lines)
	}
	if got := strings.Join(*lines, "\n"); !strings.Contains(got, "reason=report_changed") {
		t.Fatalf("lines=%q, want reason=report_changed", *lines)
	}
}

// An hk error is retried by the next pass, one attempt per pass, and the
// attempt is not spun: nothing is receipted and the log holds one line.
func TestReportRepublishRetriesAFailedUploadNextPass(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	fail := failReportUploads(hk)
	body := "retry me\n"
	path := reportFile(t, a, "j", body)
	wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
	sha := reportDigest([]byte(body))
	key := reportDocumentKey("j", sha)
	s, lines := loggingSweeper()
	before := documentPuts(hk)
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if got := documentPuts(hk) - before; got != 1 {
		t.Fatalf("pass made %d upload attempts, want one", got)
	}
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	if got := publishedDocument(evs, sha); got != "" {
		t.Fatalf("publish receipt document=%q after a failed upload, want none", got)
	}
	if got := countLines(*lines, "report republish kept "); got != 1 || !strings.Contains(strings.Join(*lines, "\n"), "reason=publish_failed") {
		t.Fatalf("lines=%q, want one publish_failed line", *lines)
	}
	fail.Store(false)
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if _, ok := storedDoc(hk, key); !ok {
		t.Fatalf("hk does not hold %s after the retry pass", key)
	}
	evs, e = a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	if got := publishedDocument(evs, sha); got != key {
		t.Fatalf("publish receipt document=%q after the retry pass, want %s", got, key)
	}
	if got := countLines(*lines, "report republished "); got != 1 {
		t.Fatalf("lines=%q, want one republished line", *lines)
	}
}

// One pass uploads at most reportRepublishPerPass reports; the rest wait for
// the next pass, so a backlog never turns into one unbounded pass.
func TestReportRepublishIsBoundedPerPass(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	fail := failReportUploads(hk)
	ids := []string{"j1", "j2", "j3", "j4", "j5", "j6", "j7"}
	for _, id := range ids {
		seedDoneJobNamed(t, a, id)
		path := reportFile(t, a, id, "report of "+id+"\n")
		wantNotInHK(t, a.lifecycle(ctx, "done", []string{id, "--report", path}))
	}
	fail.Store(false)
	s, _ := loggingSweeper()
	before := documentPuts(hk)
	passes := 0
	for documentPuts(hk)-before < len(ids) {
		passes++
		if passes > len(ids) {
			t.Fatalf("after %d passes hk holds %d of %d reports", passes, documentPuts(hk)-before, len(ids))
		}
		prev := documentPuts(hk)
		if e := s.RepublishReports(ctx, a); e != nil {
			t.Fatal(e)
		}
		if got := documentPuts(hk) - prev; got > reportRepublishPerPass {
			t.Fatalf("pass %d uploaded %d reports, over the bound of %d", passes, got, reportRepublishPerPass)
		}
	}
	if passes < 2 {
		t.Fatalf("one pass published all %d reports, want the bound to spread them", len(ids))
	}
	for _, id := range ids {
		body := "report of " + id + "\n"
		key := reportDocumentKey(id, reportDigest([]byte(body)))
		if _, ok := storedDoc(hk, key); !ok {
			t.Fatalf("%s: hk does not hold %s", id, key)
		}
	}
}

// A pin write hk did not answer is repeated by a later pass: the publish
// receipt alone must not end the retry.
func TestReportRepublishRetriesAFailedPin(t *testing.T) {
	ctx := context.Background()
	a, hk := republishFixture(t)
	fail := failReportUploads(hk)
	body := "pin later\n"
	path := reportFile(t, a, "j", body)
	wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
	fail.Store(false)
	store := a.JobStore.(*fakeJobStore)
	store.putErr = errors.New("hk unreachable")
	s, lines := loggingSweeper()
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if doc := store.record(t, "j")["report_doc"]; doc != nil {
		t.Fatalf("report_doc=%v after a failed pin, want none", doc)
	}
	if got := strings.Join(*lines, "\n"); !strings.Contains(got, "reason=pin_failed") {
		t.Fatalf("lines=%q, want the pin refusal", *lines)
	}
	store.putErr = nil
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	sha := reportDigest([]byte(body))
	rec := store.record(t, "j")
	if str(rec, "report_doc") != reportDocumentKey("j", sha) || str(rec, "report_sha256") != sha {
		t.Fatalf("record=%v, want the report pinned by the retry pass", rec)
	}
	// The third pass has nothing left to write.
	before := len(hk.Requests())
	if e := s.RepublishReports(ctx, a); e != nil {
		t.Fatal(e)
	}
	if got := len(hk.Requests()) - before; got != 0 {
		t.Fatalf("pass after the pin made %d hk calls, want none", got)
	}
}
