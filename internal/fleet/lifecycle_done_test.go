package fleet

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func (r *doneRig) storePuts() int {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	n := 0
	for _, c := range r.store.calls {
		if strings.HasPrefix(c, "put ") {
			n++
		}
	}
	return n
}

// "Duplicate" is not a branch of `done`: it is the case where every step has
// its receipt, and it calls hk zero times (no document, no record, no task
// comment), writes no event and relays nothing.
func TestDoneFullDuplicateMakesNoHKCall(t *testing.T) {
	r := newDoneRig(t)
	r.write("same\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	events, _ := r.a.events("j")
	calls, relays := r.hkCalls(), len(r.relays())
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.hkCalls(); got != calls {
		t.Fatalf("the duplicate made %d hk calls", got-calls)
	}
	if after, _ := r.a.events("j"); len(after) != len(events) {
		t.Fatalf("the duplicate wrote events: %d -> %d", len(events), len(after))
	}
	if got := len(r.relays()); got != relays {
		t.Fatalf("relays %d -> %d", relays, got)
	}
	if out := r.stdout(); !strings.Contains(out, "duplicate=yes") {
		t.Fatalf("out=%q", out)
	}
}

// A later report (other bytes) is a report of its own: its own document, its
// own job.completed and relay. It never writes the record, which keeps the
// completing report's pin.
func TestDoneLaterReportGetsOwnDocumentAndNeverWritesRecord(t *testing.T) {
	r := newDoneRig(t)
	r.write("first\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	first := r.key("first\n")
	puts := r.storePuts()
	r.write("second\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	second := r.key("second\n")
	if second == first {
		t.Fatal("fixture: both reports have one key")
	}
	if got := r.storePuts(); got != puts {
		t.Fatalf("the later report wrote the record: %d -> %d puts", puts, got)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != first || str(rec, "report_sha256") != reportDigest([]byte("first\n")) {
		t.Fatalf("record=%v, want the first report's pin", rec)
	}
	var docs []string
	for _, p := range r.events("job.completed") {
		docs = append(docs, str(p, "document"))
	}
	if !slices.Equal(docs, []string{first, second}) {
		t.Fatalf("job.completed documents=%q, want %q", docs, []string{first, second})
	}
	if got := len(r.relays()); got != 2 {
		t.Fatalf("relays=%d, want one per report", got)
	}
	if got := len(r.events(eventRecordSettled)); got != 1 {
		t.Fatalf("%s receipts=%d, want one", eventRecordSettled, got)
	}
	if got := len(r.events(eventReportPublished)); got != 2 {
		t.Fatalf("%s receipts=%d, want one per report", eventReportPublished, got)
	}
}

// hk takes a result only in the spawned→completed write. A `--result` on a
// record that is already completed without one is terminal_without_result on
// both paths: the interactive done fails closed, a headless done warns and goes
// on, and neither pretends to attach it.
func TestDoneResultOnTerminalWithoutResultOnBothPaths(t *testing.T) {
	for _, headless := range []bool{false, true} {
		name := "interactive"
		if headless {
			name = "headless"
		}
		t.Run(name, func(t *testing.T) {
			r := newDoneRig(t)
			if headless {
				putEvent(t, r.a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate", "pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a"})
			}
			r.write("answer\n")
			if e := r.done(); e != nil {
				t.Fatal(e)
			}
			findings := filepath.Join(r.a.Jobs, "j", "findings.json")
			if e := os.WriteFile(findings, []byte(`{"findings":[]}`), 0600); e != nil {
				t.Fatal(e)
			}
			e := r.done("--result", findings)
			if headless {
				if e != nil || !strings.Contains(r.stderr(), "reason=terminal_without_result") {
					t.Fatalf("error=%v warnings=%q, want a warning with reason=terminal_without_result", e, r.stderr())
				}
			} else if code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=terminal_without_result") {
				t.Fatalf("error=%v, want terminal_without_result", e)
			}
			if rec := r.store.record(t, "j"); rec["result"] != nil {
				t.Fatalf("record=%v, want no result", rec)
			}
		})
	}
}

// The upload fails on the first run: the record settles without a pin, the
// parent is told once, and the command exits non-zero so the caller reruns it.
// The same command run again publishes the report, pins the record with the
// exact key (the late write hk 4354 allows) and does not notify a second time.
func TestDoneUploadFailureThenRerunPublishesPinsAndNotifiesOnce(t *testing.T) {
	r := newDoneRig(t)
	failing := true
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if failing && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			return http.StatusInternalServerError, `{"error":"internal_error"}`, true
		}
		return 0, "", false
	})
	body := "answer\n"
	r.write(body)
	wantNotInHK(t, r.done())
	if rec := r.store.record(t, "j"); str(rec, "state") != "completed" || rec["report_doc"] != nil {
		t.Fatalf("record=%v, want completed with no pin", rec)
	}
	if got := len(r.events("job.completed")); got != 1 {
		t.Fatalf("job.completed events=%d after the failed run, want one: publication never blocks completion", got)
	}

	failing = false
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	key, digest := r.key(body), reportDigest([]byte(body))
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != key || str(rec, "report_sha256") != digest {
		t.Fatalf("record=%v, want the pin %s %s", rec, key, digest)
	}
	if receipts := r.events(eventReportPublished); len(receipts) != 1 || str(receipts[0], "document") != key {
		t.Fatalf("publish receipts=%v, want one naming %s", receipts, key)
	}
	if pins := r.events(eventReportPinned); len(pins) != 1 || str(pins[0], "outcome") != "pinned" || str(pins[0], "document") != key {
		t.Fatalf("pin receipts=%v", pins)
	}
	if got := len(r.events("job.completed")); got != 1 {
		t.Fatalf("job.completed events=%d after the rerun, want still one", got)
	}
	if got := len(r.relays()); got != 1 {
		t.Fatalf("relays=%d, want one", got)
	}
	calls := r.hkCalls()
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.hkCalls(); got != calls {
		t.Fatalf("the converged duplicate made %d hk calls", got-calls)
	}
}

// A settling write that carried the pin needs no pin step.
func TestDoneSettlingWriteCarriesThePin(t *testing.T) {
	r := newDoneRig(t)
	r.write("answer\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if pins := r.events(eventReportPinned); len(pins) != 0 {
		t.Fatalf("pin receipts=%v, want none", pins)
	}
	settled := r.events(eventRecordSettled)
	if len(settled) != 1 || !boolean(settled[0], "pinned", false) {
		t.Fatalf("settle receipts=%v, want one with pinned", settled)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != r.key("answer\n") {
		t.Fatalf("record=%v", rec)
	}
}

// hk answers 409 to the late pin when the record already names another report.
// That is an outcome, logged and receipted: done succeeds, leaves the record's
// pin alone and does not repeat the write.
func TestDonePinConflictIsLoggedNotAnError(t *testing.T) {
	r := newDoneRig(t)
	other := strings.Repeat("a", 64)
	r.store.mu.Lock()
	r.store.records["j"]["state"] = "completed"
	r.store.records["j"]["report_doc"] = "reports/j/answer-" + other + ".md"
	r.store.records["j"]["report_sha256"] = other
	r.store.mu.Unlock()
	r.write("answer\n")
	if e := r.done(); e != nil {
		t.Fatalf("done failed on a pin conflict: %v", e)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_sha256") != other {
		t.Fatalf("record=%v, want the held pin", rec)
	}
	pins := r.events(eventReportPinned)
	if len(pins) != 1 || str(pins[0], "outcome") != "held" {
		t.Fatalf("pin receipts=%v, want one held", pins)
	}
	if !strings.Contains(r.stderr(), "report pin kept") {
		t.Fatalf("warnings=%q, want the pin outcome logged", r.stderr())
	}
	calls := r.hkCalls()
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.hkCalls(); got != calls {
		t.Fatalf("the rerun made %d hk calls, want none", got-calls)
	}
}

// pinFailStore refuses the first n late pin writes as hk being unavailable.
type pinFailStore struct {
	*fakeJobStore
	n int
}

func (s *pinFailStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if hkLatePin(fields) && s.n > 0 {
		s.n--
		return &BackendError{Status: http.StatusServiceUnavailable, Code: "unavailable"}
	}
	return s.fakeJobStore.Put(ctx, id, fields)
}

// A pin write hk did not answer leaves no receipt: the run exits non-zero
// naming the pin, and the rerun writes it.
func TestDonePinFailureIsRetriedByTheRerun(t *testing.T) {
	r := newDoneRig(t)
	uploadFails := true
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if uploadFails && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			return http.StatusInternalServerError, `{"error":"internal_error"}`, true
		}
		return 0, "", false
	})
	r.a.JobStore = &pinFailStore{fakeJobStore: r.store, n: 1}
	r.write("answer\n")
	wantNotInHK(t, r.done())
	uploadFails = false
	e := r.done()
	if code(e) != exitFailure || e == nil || !strings.Contains(e.Error(), "report not pinned in hk") {
		t.Fatalf("done error=%v, want the failed pin named", e)
	}
	if pins := r.events(eventReportPinned); len(pins) != 0 {
		t.Fatalf("pin receipts=%v, want none after a failed write", pins)
	}
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != r.key("answer\n") {
		t.Fatalf("record=%v, want the pin written by the rerun", rec)
	}
}

// The watcher and a headless delegate have no one to read an exit status: a
// failed upload warns, the job completes and the command exits 0.
func TestDoneUploadFailureStillCompletesForWatcherAndHeadless(t *testing.T) {
	for _, name := range []string{"watcher", "headless"} {
		t.Run(name, func(t *testing.T) {
			r := newDoneRig(t)
			r.hk.Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
			r.write("answer\n")
			var e error
			if name == "watcher" {
				e = r.a.lifecycleWithOptions(t.Context(), "done", []string{"j", "--report", r.path}, true, false)
			} else {
				putEvent(t, r.a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate", "pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a"})
				e = r.done()
			}
			if e != nil {
				t.Fatalf("done error=%v, want a warning only", e)
			}
			if !strings.Contains(r.stderr(), "report upload failed; local report retained") {
				t.Fatalf("warnings=%q", r.stderr())
			}
			if rec := r.store.record(t, "j"); str(rec, "state") != "completed" || rec["report_doc"] != nil {
				t.Fatalf("record=%v, want completed with no pin", rec)
			}
			if got := len(r.events("job.completed")); got != 1 {
				t.Fatalf("job.completed events=%d", got)
			}
		})
	}
}

// escalate still uploads its report, under the report file's name, and pins it
// in the escalated record.
func TestEscalateStillPublishesReportUnderItsFileName(t *testing.T) {
	r := newDoneRig(t)
	r.write("blocked\n")
	if e := r.a.lifecycle(t.Context(), "escalate", []string{"j", "--report", r.path, "--question", "which way?"}); e != nil {
		t.Fatal(e)
	}
	rec := r.store.record(t, "j")
	if str(rec, "state") != "escalated" || str(rec, "report_doc") != "reports/j/report.md" || str(rec, "report_sha256") != reportDigest([]byte("blocked\n")) {
		t.Fatalf("record=%v", rec)
	}
	if got := len(r.events(eventReportPublished)); got != 0 {
		t.Fatalf("escalate wrote %d publish receipts", got)
	}
}

// A job that completed before receipts existed has a job.completed event, a
// terminal record pinned to the legacy key, and no receipt. The same done
// finishes it: it publishes the content-addressed document, and the pin hk
// already holds stays (a logged 409), not an error.
func TestDoneFinishesAJobCompletedBeforeReceipts(t *testing.T) {
	r := newDoneRig(t)
	body := "legacy answer\n"
	digest := reportDigest([]byte(body))
	r.write(body)
	putEvent(t, r.a, "j", "job.completed", map[string]any{"report": r.path, "report_sha256": digest, "question": "", "document": "reports/j/report.md"})
	r.store.mu.Lock()
	r.store.records["j"]["state"] = "completed"
	r.store.records["j"]["report_doc"] = "reports/j/report.md"
	r.store.records["j"]["report_sha256"] = digest
	r.store.mu.Unlock()
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != "reports/j/report.md" {
		t.Fatalf("record=%v, want the original pin kept", rec)
	}
	if got := len(r.events(eventReportPublished)); got != 1 {
		t.Fatalf("publish receipts=%d, want one", got)
	}
	if got := len(r.events("job.completed")); got != 1 {
		t.Fatalf("job.completed events=%d, want the legacy one only", got)
	}
	if got := len(r.relays()); got != 0 {
		t.Fatalf("relays=%d, want none: the parent was told when it completed", got)
	}
}

// hk refuses a report whose bytes look like a secret. Nothing is remembered:
// every run tries again, fails the same way and leaves no refusal file.
func TestDoneSecretRefusalRepeatsByItself(t *testing.T) {
	r := newDoneRig(t)
	refusals := 0
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			refusals++
			return http.StatusBadRequest, `{"error":"secret_like_content","pattern":"api_key"}`, true
		}
		return 0, "", false
	})
	r.write("token ghp_example\n")
	for range 2 {
		wantNotInHK(t, r.done())
	}
	if refusals != 2 {
		t.Fatalf("document PUTs=%d, want one per run", refusals)
	}
	entries, _ := os.ReadDir(filepath.Join(r.a.Jobs, "j"))
	for _, e := range entries {
		if strings.Contains(e.Name(), "refus") || strings.Contains(e.Name(), "intent") {
			t.Fatalf("job dir keeps %q: nothing about a refusal is remembered", e.Name())
		}
	}
	if got := len(r.events("job.completed")); got != 1 {
		t.Fatalf("job.completed events=%d, want one", got)
	}
}

// An unattended `done --result` (headless delegate, watcher) that meets a
// different stored result raises the same fleet signal the interactive one
// does: a job_result conflict event and the Sentry warning.
func TestDoneResultConflictOnUnattendedPathRaisesTheSignal(t *testing.T) {
	r := newDoneRig(t)
	putEvent(t, r.a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate", "pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a"})
	r.store.mu.Lock()
	r.store.records["j"]["state"] = "completed"
	r.store.records["j"]["result"] = map[string]any{"source": "stored"}
	r.store.mu.Unlock()
	events := jobResultEvents(r.a)
	var warnings []string
	r.a.SentryWarning = func(fingerprint []string, _ string, _ map[string]string) {
		warnings = append(warnings, strings.Join(fingerprint, "|"))
	}
	findings := filepath.Join(r.a.Jobs, "j", "findings.json")
	if e := os.WriteFile(findings, []byte(`{"source":"later"}`), 0600); e != nil {
		t.Fatal(e)
	}
	r.write("answer\n")
	if e := r.done("--result", findings); e != nil {
		t.Fatalf("headless done failed: %v", e)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "conflict" {
		t.Fatalf("job_result events=%+v", *events)
	}
	if len(warnings) != 1 || warnings[0] != "job-result-conflict" {
		t.Fatalf("Sentry fingerprints=%v", warnings)
	}
}

// A file named like a content-addressed key never lands on one: the two key
// schemes stay disjoint.
func TestBasenameReportKeyNeverNamesAContentAddressedKey(t *testing.T) {
	digest := reportDigest([]byte("x"))
	got := BasenameReportKey("j", "/jobs/j/answer-"+digest+".md")
	if got == reportDocumentKey("j", digest) || !strings.HasPrefix(got, "reports/j/") {
		t.Fatalf("key=%q collides with %q", got, reportDocumentKey("j", digest))
	}
	if got := BasenameReportKey("j", "/jobs/j/report.md"); got != "reports/j/report.md" {
		t.Fatalf("key=%q", got)
	}
}
