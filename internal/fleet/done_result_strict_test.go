package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func strictDoneFixture(t *testing.T, state string) (*App, *fakeRunner, *fakeHK, *spawnHK, string, string) {
	t.Helper()
	a := testApp(t)
	runner := ledgerRunner(t, a)
	docs := useFakeHK(t)
	docs.Handle(hkCommentAnswer)
	noHKCLI(t, runner)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = state
	a.JobStore = h.client()
	seedJob(t, a, "j", "reviewer")
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "task_id": "42"})
	job, err := a.job("j")
	if err != nil || job.Task == "" {
		t.Fatalf("fixture task unavailable: task=%q err=%v", job.Task, err)
	}
	seedLedger(t, a, "j")
	fixtureNow := a.Now
	a.Now = func() time.Time { return fixtureNow().Add(3 * time.Second) }
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "role": "reviewer"})
	report := filepath.Join(a.Jobs, "j", "report.md")
	result := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, report, "final report\n")
	writeReport(t, result, `{"findings":[{"severity":"high"}]}`)
	return a, runner, h, docs, report, result
}

func strictClaimedWithoutReceiptFixture(t *testing.T) (*App, *fakeHK, string, string) {
	t.Helper()
	a := testApp(t)
	runner := ledgerRunner(t, a)
	docs := useFakeHK(t)
	docs.Handle(hkCommentAnswer)
	noHKCLI(t, runner)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	a.JobStore = h.client()
	seedLedger(t, a, "j")
	fixtureNow := a.Now
	a.Now = func() time.Time { return fixtureNow().Add(3 * time.Second) }
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "role": "reviewer"})
	report := filepath.Join(a.Jobs, "j", "report.md")
	result := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, report, "final report\n")
	writeReport(t, result, `{"findings":[{"severity":"high"}]}`)
	return a, h, report, result
}

func seedDuplicateCompletion(t *testing.T, a *App, report string) {
	t.Helper()
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	putEvent(t, a, "j", "job.completed", map[string]any{"report": report, "report_sha256": hex.EncodeToString(sum[:]), "question": ""})
}

func strictDone(t *testing.T, a *App, report, result string) error {
	t.Helper()
	return a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", result})
}

func assertStrictFailure(t *testing.T, a *App, err error, exit int, reason string) {
	t.Helper()
	if code(err) != exit || !strings.Contains(err.Error(), "reason="+reason) {
		t.Fatalf("error=%v, want exit %d reason=%s", err, exit, reason)
	}
	if got := a.Out.(*bytes.Buffer).String(); strings.Contains(got, "OK") {
		t.Fatalf("stdout=%q", got)
	}
	var failures []Event
	events, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	for _, event := range events {
		if event.Kind == "job.record_failed" {
			failures = append(failures, event)
		}
		if slices.Contains([]string{"job.completed", "job.result_attached"}, event.Kind) {
			t.Fatalf("success event %s after failed strict done", event.Kind)
		}
	}
	if len(failures) != 1 || len(failures[0].Payload) != 2 || failures[0].Payload["op"] != "completed" || failures[0].Payload["reason"] != reason {
		t.Fatalf("job.record_failed=%+v", failures)
	}
}

func TestDoneResultStrictRefusalStopsSuccessPath(t *testing.T) {
	a, runner, h, docs, report, result := strictDoneFixture(t, "spawned")
	h.putStatuses = []int{http.StatusBadRequest}
	h.putPattern = "private_key"
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitFailure, "secret_like_content:private_key")
	if len(h.putBodies) != 1 {
		t.Fatalf("completed PUTs=%d", len(h.putBodies))
	}
	if _, pools := ledgerState(t, a, "j"); len(pools) == 0 {
		t.Fatal("quota was released after refusal")
	}
	for _, call := range runner.Calls {
		if call.Name == "panewire" {
			t.Fatalf("relay emitted after refusal: %+v", call)
		}
	}
	for _, request := range docs.Requests() {
		if strings.HasSuffix(request.Path, "/comments") {
			t.Fatalf("task comment issued after refusal: %s", request.Route())
		}
	}
}

func TestDoneResultStrictRetriesIdenticalPut(t *testing.T) {
	a, runner, h, docs, report, result := strictDoneFixture(t, "spawned")
	h.putStatuses = []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK}
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if len(h.putBodies) != 3 {
		t.Fatalf("PUT bodies=%d", len(h.putBodies))
	}
	uploads := 0
	for _, request := range docs.Requests() {
		if request.Method == http.MethodPut && strings.Contains(request.Path, "/documents/") {
			uploads++
		}
	}
	if uploads != 1 {
		t.Fatalf("report uploads=%d", uploads)
	}
	want, _ := json.Marshal(h.putBodies[0])
	for _, body := range h.putBodies[1:] {
		got, _ := json.Marshal(body)
		if !bytes.Equal(got, want) {
			t.Fatalf("non-identical PUTs: %s != %s", got, want)
		}
	}
	if countEvents(t, a, "job.completed") != 1 {
		t.Fatal("completion event count is not one")
	}
	emits := 0
	for _, call := range runner.Calls {
		if call.Name == "panewire" {
			emits++
		}
	}
	comments := 0
	for _, request := range docs.Requests() {
		if strings.HasSuffix(request.Path, "/comments") {
			comments++
		}
	}
	if emits != 1 || comments != 1 {
		job, _ := a.job("j")
		t.Fatalf("emits=%d comments=%d task=%q requests=%+v stderr=%q", emits, comments, job.Task, docs.Requests(), a.Err)
	}
	if _, pools := ledgerState(t, a, "j"); len(pools) != 0 {
		t.Fatal("quota was not released")
	}
}

func TestDoneResultStrictExhaustsRetryBudgetSafely(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	h.putStatuses = []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable}
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitFailure, "http_503")
	if len(h.putBodies) != 3 {
		t.Fatalf("PUT attempts=%d", len(h.putBodies))
	}
}

func TestDoneResultStrictReadsBackDroppedCommit(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	h.dropAfterCommit = true
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if len(h.putBodies) != 1 || countEvents(t, a, "job.completed") != 1 {
		t.Fatalf("PUTs=%d completion events=%d", len(h.putBodies), countEvents(t, a, "job.completed"))
	}
	gets := 0
	for _, call := range h.calls {
		if strings.HasPrefix(call, "GET /v1/jobs/") {
			gets++
		}
	}
	if gets != 1 {
		t.Fatalf("read-back GETs=%d", gets)
	}
}

func TestDoneResultStrictConflictEmitsJobResultAndSentry(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "completed")
	h.records["j"]["result"] = map[string]any{"different": true}
	resultEvents := jobResultEvents(a)
	warnings := 0
	a.SentryWarning = func(fingerprint []string, _ string, _ map[string]string) {
		if slices.Equal(fingerprint, []string{"job-result-conflict"}) {
			warnings++
		}
	}
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitStateConflict, "result_conflict")
	if len(*resultEvents) != 1 || (*resultEvents)[0].Outcome != "conflict" || warnings != 1 {
		t.Fatalf("job_result=%+v Sentry warnings=%d", *resultEvents, warnings)
	}
}

func TestDoneResultStrictDuplicateEqualRepairsMissingAttachEvent(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	seedDuplicateCompletion(t, a, report)
	h.records["j"]["result"] = map[string]any{"findings": []any{map[string]any{"severity": "high"}}}
	if countEvents(t, a, "job.result_attached") != 0 {
		t.Fatal("fixture unexpectedly has a result attach event")
	}
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if countEvents(t, a, "job.result_attached") != 1 {
		t.Fatal("equal stored result did not repair the missing attach event")
	}
}

func TestDoneResultStrictDuplicateDroppedCommitWritesAttachEvent(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	events := jobResultEvents(a)
	seedDuplicateCompletion(t, a, report)
	h.dropAfterCommit = true

	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if len(h.putBodies) != 1 {
		t.Fatalf("completed PUTs=%d, want one ambiguous commit", len(h.putBodies))
	}
	if got := countEvents(t, a, "job.result_attached"); got != 1 {
		t.Fatalf("job.result_attached events=%d, want one", got)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "attached" || (*events)[0].Reason != "duplicate_done" {
		t.Fatalf("job_result events=%+v, want one duplicate attachment", *events)
	}
}

func TestDoneResultStrictDuplicateAddsPublishedReportPin(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	completionID := "already-published"
	document := reportDocumentKey("j", completionID, digest, false)
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": digest, "question": "",
		"completion_id": completionID, "document": document,
	})
	h.dropAfterCommit = true

	err = a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", result, "--completion-id", completionID})
	if err != nil {
		t.Fatal(err)
	}
	record := h.records["j"]
	if str(record, "report_doc") != document || str(record, "report_sha256") != digest {
		t.Fatalf("record report pin=(%q,%q), want event pin=(%q,%q)", str(record, "report_doc"), str(record, "report_sha256"), document, digest)
	}
	if _, ok := record["result"]; !ok {
		t.Fatalf("record=%v, want result stored with the report pin", record)
	}
	if len(h.putBodies) != 1 || str(h.putBodies[0], "report_doc") != document || str(h.putBodies[0], "report_sha256") != digest {
		t.Fatalf("completed PUTs=%v, want one atomic result and event-derived report pin", h.putBodies)
	}
}

func TestDoneResultRetryUnpinnedTerminalKeepsEqualStoredResult(t *testing.T) {
	a, _, h, docs, report, resultPath := strictDoneFixture(t, "completed")
	resultBody, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var storedResult any
	if err := json.Unmarshal(resultBody, &storedResult); err != nil {
		t.Fatal(err)
	}
	h.records["j"]["result"] = storedResult
	reportBody, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(reportBody)
	reportSHA := hex.EncodeToString(digest[:])
	completionID, err := completionIdentity("j", "", reportBody)
	if err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": reportSHA, "completion_id": completionID, "document": "",
	})

	err = a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", resultPath})
	if err != nil {
		t.Fatalf("retry with an equal stored result and no existing pin: %v", err)
	}
	if len(completionDocumentRequests(docs)) == 0 {
		t.Fatal("retry did not publish the report")
	}
	if !sameJSONValue(h.records["j"]["result"], storedResult) {
		t.Fatalf("stored result=%v, want %v", h.records["j"]["result"], storedResult)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	var published bool
	for _, event := range job.Events {
		if event.Kind == "job.completed" && str(event.Payload, "document") != "" {
			published = true
		}
	}
	if !published {
		t.Fatalf("completion events=%+v, want a document-bearing retry event", job.Events)
	}
}

func TestDoneDuplicateCompletesSpawnedRecordFromPublishedEvent(t *testing.T) {
	a, _, h, _, report, _ := strictDoneFixture(t, "spawned")
	reportBody, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(reportBody)
	reportSHA := hex.EncodeToString(digest[:])
	completionID := "published-before-record"
	document := reportDocumentKey("j", completionID, reportSHA, false)
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": reportSHA, "completion_id": completionID, "document": document,
	})

	err = a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--completion-id", completionID})
	if err != nil {
		t.Fatal(err)
	}
	if str(h.records["j"], "state") != "completed" || str(h.records["j"], "report_doc") != document || str(h.records["j"], "report_sha256") != reportSHA {
		t.Fatalf("record=%v, want completed with the matching event pin", h.records["j"])
	}
}

func TestDoneResultStrictPinReadbackUsesTheStoredResult(t *testing.T) {
	a, _, h, _, report, resultPath := strictDoneFixture(t, "completed")
	resultBody, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var result any
	if err := json.Unmarshal(resultBody, &result); err != nil {
		t.Fatal(err)
	}
	h.records["j"]["result"] = result
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	reportSHA := hex.EncodeToString(digest[:])
	reportDoc := reportDocumentKey("j", "new-answer", reportSHA, false)
	fields := map[string]any{"state": "completed", "report_doc": reportDoc, "report_sha256": reportSHA}
	_, err = a.strictCompletedPutUntil(context.Background(), "j", fields, result, reportDoc, reportSHA, nil, time.Now().Add(time.Second))
	if !isDoneResultFailure(err, "report_conflict") || isDoneResultFailure(err, "result_conflict") {
		t.Fatalf("pin-only readback error=%v, want report_conflict for the immutable terminal record", err)
	}
}

func TestDoneResultRetryPublishesMatchingUnpublishedCompletion(t *testing.T) {
	a, _, h, docs, report, _ := strictDoneFixture(t, "spawned")
	failedUploads := 0
	docs.Handle(func(req hkRequest) (int, string, bool) {
		if strings.HasPrefix(req.Route(), "PUT /v1/documents/") && failedUploads < 3 {
			failedUploads++
			return http.StatusServiceUnavailable, `{"error":"unavailable"}`, true
		}
		return 0, "", false
	})
	args := []string{"j", "--report", report}
	if err := a.lifecycleWithOptions(context.Background(), "done", args, true, false); err != nil {
		t.Fatalf("watcher completion should keep warn-and-continue: %v", err)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Events) == 0 || str(job.Events[len(job.Events)-1].Payload, "document") != "" {
		t.Fatalf("initial completion=%+v, want an event without a published document", job.Events)
	}
	if str(h.records["j"], "state") != "completed" {
		t.Fatalf("remote state=%q, want watcher completion retained", str(h.records["j"], "state"))
	}
	putsBeforeRetry := len(completionDocumentRequests(docs))
	if err := a.lifecycle(context.Background(), "done", args); err != nil {
		t.Fatalf("same-report CLI retry: %v", err)
	}
	putsAfterRetry := len(completionDocumentRequests(docs))
	if putsAfterRetry <= putsBeforeRetry {
		t.Fatalf("document PUTs before=%d after=%d, want retry to publish the matching report", putsBeforeRetry, putsAfterRetry)
	}
	job, err = a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	var published bool
	for _, event := range job.Events {
		if event.Kind == "job.completed" && str(event.Payload, "report") == report && str(event.Payload, "document") != "" {
			published = true
		}
	}
	if !published {
		t.Fatalf("completion events=%+v, want a published retry event", job.Events)
	}
	if strings.Contains(a.Out.(*bytes.Buffer).String(), "duplicate=yes") {
		t.Fatalf("stdout=%q, want the unpublished completion not to be reported as a successful duplicate", a.Out.(*bytes.Buffer).String())
	}
}

func TestDoneResultEventWriteFailureIsNotReportedAsHKFailure(t *testing.T) {
	a := testApp(t)
	putEvent(t, a, "j", "job.claim", map[string]any{"task_id": "42"})
	err := a.strictDoneFailure(context.Background(), "j", &doneResultEventWriteFailure{cause: errors.New("local ledger /private/path unavailable")})
	if code(err) != exitFailure || !strings.Contains(err.Error(), "reason=local_event_failed") || strings.Contains(err.Error(), "hk_unconfigured") {
		t.Fatalf("error=%v", err)
	}
	if strings.Contains(err.Error(), "/private/path") || strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("local event error details escaped: %v", err)
	}
	if kinds := eventKindsOf(t, a, "j"); slices.Contains(kinds, "job.record_failed") {
		t.Fatalf("local event failure was misrecorded as an hk write failure: %v", kinds)
	}
}

func TestDoneResultStrictRepairsClaimedFromReceipt(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "claimed")
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if len(h.putBodies) != 3 || h.putBodies[1]["state"] != "spawned" || h.putBodies[1]["pane_id"] != "w:p1" {
		t.Fatalf("PUT bodies=%v", h.putBodies)
	}
	if _, ok := h.putBodies[1]["host"]; ok {
		t.Fatalf("repair included host: %v", h.putBodies[1])
	}
}

func TestDoneResultStrictReadsBackAmbiguousSpawnRepair(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "claimed")
	h.dropSpawnedAfterCommit = true
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	spawnedPuts, gets := 0, 0
	for _, body := range h.putBodies {
		if body["state"] == "spawned" {
			spawnedPuts++
		}
	}
	for _, call := range h.calls {
		if strings.HasPrefix(call, "GET /v1/jobs/") {
			gets++
		}
	}
	if spawnedPuts != 1 || gets != 2 || str(h.record(t, "j"), "state") != "completed" {
		t.Fatalf("spawned PUTs=%d GETs=%d record=%v", spawnedPuts, gets, h.record(t, "j"))
	}
}

func TestDoneResultStrictClaimedWithoutReceiptDoesNotRepair(t *testing.T) {
	a, h, report, result := strictClaimedWithoutReceiptFixture(t)
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitStateConflict, "no_spawn_evidence")
	for _, body := range h.putBodies {
		if body["state"] == "spawned" {
			t.Fatalf("unexpected spawned repair PUT: %v", body)
		}
	}
}

func mustEvents(t *testing.T, a *App, id string) []Event {
	t.Helper()
	events, err := a.events(id)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestDoneResultStrictDuplicateFailureKeepsQuotaUntilRecovery(t *testing.T) {
	for _, failure := range []string{"unreachable", "put"} {
		t.Run(failure, func(t *testing.T) {
			a, _, h, _, report, result := strictDoneFixture(t, "spawned")
			seedDuplicateCompletion(t, a, report)
			if failure == "unreachable" {
				h.down = true
			} else {
				h.putStatuses = []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable}
			}
			err := strictDone(t, a, report, result)
			if err == nil || code(err) == 0 || strings.Contains(err.Error(), "result=unattached") {
				t.Fatalf("duplicate failure=%v", err)
			}
			if _, pools := ledgerState(t, a, "j"); len(pools) == 0 {
				t.Fatal("quota released after duplicate result failure")
			}
			h.down = false
			h.putStatuses = nil
			if err := strictDone(t, a, report, result); err != nil {
				t.Fatal(err)
			}
			if _, pools := ledgerState(t, a, "j"); len(pools) != 0 {
				t.Fatal("quota not released after recovery")
			}
		})
	}
}

func TestDoneResultStrictRejectsTerminalReadbackStates(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason string
		result              any
		reportDoc           string
	}{
		{name: "terminal without result", state: "completed", reason: "terminal_without_result"},
		{name: "different result", state: "completed", result: map[string]any{"different": true}, reason: "result_conflict"},
		{name: "different report", state: "completed", result: map[string]any{"findings": []any{map[string]any{"severity": "high"}}}, reportDoc: "reports/j/other.md", reason: "report_conflict"},
		{name: "escalated", state: "escalated", reason: "job_escalated"},
		{name: "lost", state: "lost", reason: "job_lost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, h, _, report, result := strictDoneFixture(t, tc.state)
			if tc.result != nil {
				h.records["j"]["result"] = tc.result
			}
			h.records["j"]["report_doc"] = tc.reportDoc
			err := strictDone(t, a, report, result)
			assertStrictFailure(t, a, err, exitStateConflict, tc.reason)
		})
	}
}

func TestDoneResultStrictDiagnosticDoesNotLeakBody(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	h.putStatuses = []int{http.StatusBadRequest}
	h.putErrorBody = `{"error":"secret_like_content","reason":"private payload ../x\n","pattern":"../x\n","unknown":"must not escape"}`
	_ = strictDone(t, a, report, result)
	if strings.Contains(a.Err.(*bytes.Buffer).String(), "../x") || strings.Contains(a.Err.(*bytes.Buffer).String(), "private payload") || strings.Contains(a.Err.(*bytes.Buffer).String(), "must not escape") {
		t.Fatalf("diagnostic leaked response data: %q", a.Err)
	}
	for _, event := range mustEvents(t, a, "j") {
		b, _ := json.Marshal(event.Payload)
		if strings.Contains(string(b), "private payload") || strings.Contains(string(b), "must not escape") || strings.Contains(string(b), "../x") {
			t.Fatalf("event leaked response data: %s", b)
		}
	}
}

func TestDoneResultHeadlessDelegateKeepsWarnAndContinue(t *testing.T) {
	a, _, h, _, report, result := strictDoneFixture(t, "spawned")
	putEvent(t, a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate"})
	h.putStatuses = []int{http.StatusBadRequest}
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(a.Err.(*bytes.Buffer).String(), "reason=http_400") {
		t.Fatalf("safe refusal token absent: %q", a.Err)
	}
	if _, pools := ledgerState(t, a, "j"); len(pools) != 0 {
		t.Fatal("delegate quota was not released")
	}
}
