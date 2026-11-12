package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/handoffkeep/client"
)

func completionReport(t *testing.T, a *App, content string) string {
	t.Helper()
	path := filepath.Join(a.Jobs, "j", "report.md")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func completionDocumentRequests(hk *spawnHK) []hkRequest {
	var out []hkRequest
	for _, req := range hk.Requests() {
		if strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			out = append(out, req)
		}
	}
	return out
}

func TestDoneArtifactDuplicateSkipsHKUpload(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	path := completionReport(t, a, "same answer\n")
	args := []string{"j", "--report", path, "--completion-id", "same-answer"}
	if err := a.lifecycle(ctx, "done", args); err != nil {
		t.Fatal(err)
	}
	before := len(hk.Requests())
	if err := a.lifecycle(ctx, "done", args); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	if got := len(hk.Requests()); got != before {
		t.Fatalf("HK requests after duplicate = %d, before = %d; duplicate must not depend on HK availability", got, before)
	}
}

func TestDoneArtifactDuplicateWithoutResultSurvivesRecordStoreFailure(t *testing.T) {
	a, _, store, _, report, _ := strictDoneFixture(t, "spawned")
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": hex.EncodeToString(digest[:]), "question": "",
		"document": "reports/j/already-published/answer.md",
	})
	store.down = true

	if err := a.lifecycle(context.Background(), "done", []string{"j", "--report", report}); err != nil {
		t.Fatalf("duplicate without --result should remain successful while spawned-record repair is best effort: %v", err)
	}
	if !strings.Contains(strings.Join(store.calls, "\n"), "GET /v1/jobs/j") {
		t.Fatalf("job record reads=%v, want best-effort spawned-record repair attempt", store.calls)
	}
	if !strings.Contains(a.Out.(*bytes.Buffer).String(), "duplicate=yes") {
		t.Fatalf("stdout=%q, want duplicate success", a.Out.(*bytes.Buffer).String())
	}
}

func TestDoneArtifactDuplicateAttachesResultToCompletedRecordWithoutResult(t *testing.T) {
	a, _, _, _, report, result := strictDoneFixture(t, "completed")
	store := newFakeJobStore()
	store.records["j"] = map[string]any{"state": "completed"}
	a.JobStore = &ambiguousCompletedResultStore{fakeJobStore: store}
	putEvent(t, a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate"})
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	const eventDoc = "reports/j/original-answer/answer-original.md"
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": hex.EncodeToString(digest[:]), "question": "", "document": eventDoc,
	})

	if err := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", result}); err != nil {
		t.Fatalf("headless duplicate should attach result to completed record: %v", err)
	}
	if !sameJSONValue(store.records["j"]["result"], map[string]any{"findings": []any{map[string]any{"severity": "high"}}}) {
		t.Fatalf("completed record result=%v, want incoming findings", store.records["j"]["result"])
	}
	if strings.Join(store.calls, ",") != "get j,put j,get j" {
		t.Fatalf("job store calls=%v, want completed+result PUT followed by readback", store.calls)
	}
	if countEvents(t, a, "job.result_attached") != 1 {
		t.Fatal("result attachment event missing")
	}
}

type ambiguousCompletedResultStore struct{ *fakeJobStore }

func (s *ambiguousCompletedResultStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if err := s.fakeJobStore.Put(ctx, id, fields); err != nil {
		return err
	}
	return errHKTransport
}

func TestDoneArtifactStrictDuplicateRejectsOperationDocumentDifferentFromStoredPin(t *testing.T) {
	a, _, store, _, report, resultPath := strictDoneFixture(t, "completed")
	reportBody, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	reportSum := sha256.Sum256(reportBody)
	operationSHA := hex.EncodeToString(reportSum[:])
	operationDoc := reportDocumentKey("j", "new-answer", operationSHA, false)
	store.records["j"]["report_doc"] = "reports/j/old-answer/answer-old.md"
	store.records["j"]["report_sha256"] = sha256String("old answer\n")
	resultBody, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var result any
	if err := json.Unmarshal(resultBody, &result); err != nil {
		t.Fatal(err)
	}
	store.records["j"]["result"] = result
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": operationSHA, "question": "",
		"completion_id": "new-answer", "document": operationDoc,
	})
	completedBefore := countEvents(t, a, "job.completed")
	attachedBefore := countEvents(t, a, "job.result_attached")

	err = a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", resultPath, "--completion-id", "new-answer"})
	if code(err) != exitStateConflict || !strings.Contains(err.Error(), "reason=report_conflict") {
		t.Fatalf("error=%v, want exit %d reason=report_conflict", err, exitStateConflict)
	}
	if countEvents(t, a, "job.completed") != completedBefore || countEvents(t, a, "job.result_attached") != attachedBefore {
		t.Fatal("strict report conflict wrote a success event")
	}
	if strings.Contains(a.Out.(*bytes.Buffer).String(), "OK") {
		t.Fatalf("stdout=%q", a.Out.(*bytes.Buffer).String())
	}
	if store.records["j"]["report_doc"] != "reports/j/old-answer/answer-old.md" {
		t.Fatalf("stored report pin changed: %v", store.records["j"])
	}
}

func TestDoneArtifactSameCompletionIDRejectsChangedBytes(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	path := completionReport(t, a, "first bytes\n")
	args := []string{"j", "--report", path, "--completion-id", "fixed-id"}
	if err := a.lifecycle(ctx, "done", args); err != nil {
		t.Fatal(err)
	}
	before := len(completionDocumentRequests(hk))
	completionReport(t, a, "changed bytes\n")
	err := a.lifecycle(ctx, "done", args)
	if err == nil || !strings.Contains(err.Error(), "reason=report_conflict") || strings.Contains(err.Error(), "retry") {
		t.Fatalf("error=%v, want permanent report_conflict with no retry instruction", err)
	}
	if got := len(completionDocumentRequests(hk)); got != before {
		t.Fatalf("document PUT count=%d, want no upload after completion-id conflict", got)
	}
}

func TestDoneArtifactRetryPublishesMatchingUnpinnedTerminalCompletion(t *testing.T) {
	a, _, store, hk, report, _ := strictDoneFixture(t, "completed")
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	completionID, err := completionIdentity("j", "", body)
	if err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.completed", map[string]any{
		"report": report, "report_sha256": sha256String(string(body)),
		"completion_id": completionID, "document": "",
	})

	if err := a.lifecycle(context.Background(), "done", []string{"j", "--report", report}); err != nil {
		t.Fatal(err)
	}
	if len(completionDocumentRequests(hk)) == 0 {
		t.Fatal("same-ID retry did not publish the report from the matching unpublished completion")
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	var eventDocument string
	for _, event := range job.Events {
		if event.Kind == "job.completed" && str(event.Payload, "report") == report && str(event.Payload, "document") != "" {
			eventDocument = str(event.Payload, "document")
		}
	}
	if eventDocument == "" || strings.Contains(a.Out.(*bytes.Buffer).String(), "duplicate=yes") {
		t.Fatalf("stdout=%q events=%+v, want a published completion retry, not a successful duplicate", a.Out.(*bytes.Buffer).String(), job.Events)
	}
	if str(store.records["j"], "report_doc") != "" {
		t.Fatalf("terminal record unexpectedly accepted a late report pin: %v", store.records["j"])
	}
}

func TestDoneArtifactWatcherUploadFailureStillCompletes(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkFails("PUT /v1/documents/", http.StatusServiceUnavailable, `{"error":"unavailable"}`))
	seedDoneJob(t, a)
	path := completionReport(t, a, "watcher answer\n")
	if err := a.lifecycleWithOptions(ctx, "done", []string{"j", "--report", path}, true, false); err != nil {
		t.Fatalf("watcher completion: %v", err)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, event := range job.Events {
		if event.Kind == "job.completed" {
			completed++
			if str(event.Payload, "document") != "" {
				t.Fatalf("completion document=%q, want empty after upload failure", str(event.Payload, "document"))
			}
		}
	}
	if completed != 1 {
		t.Fatalf("job.completed events=%d, want one", completed)
	}
	if got := str(a.JobStore.(*fakeJobStore).record(t, "j"), "state"); got != "completed" {
		t.Fatalf("job state=%q, want completed", got)
	}
	if _, relays := completions(t, a, f); relays != 1 {
		t.Fatalf("completion relays=%d, want one", relays)
	}
}

func TestDoneArtifactHeadlessUploadFailureStillCompletes(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkFails("PUT /v1/documents/", http.StatusServiceUnavailable, `{"error":"unavailable"}`))
	seedDoneJob(t, a)
	putEvent(t, a, "j", "job.spawned", map[string]any{"headless": true})
	path := completionReport(t, a, "headless answer\n")
	resultPath := filepath.Join(a.Jobs, "j", "result.json")
	if err := os.WriteFile(resultPath, []byte(`{"answer":"headless answer"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", path, "--result", resultPath}); err != nil {
		t.Fatalf("headless completion: %v", err)
	}
	if _, relays := completions(t, a, f); relays != 1 {
		t.Fatalf("completion relays=%d, want one", relays)
	}
	if got := str(a.JobStore.(*fakeJobStore).record(t, "j"), "state"); got != "completed" {
		t.Fatalf("job state=%q, want completed", got)
	}
}

func TestDoneArtifactEscalateStillPublishesReport(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	path := completionReport(t, a, "escalation evidence\n")
	if err := a.lifecycle(ctx, "escalate", []string{"j", "--question", "Which option?", "--report", path}); err != nil {
		t.Fatal(err)
	}
	const wantKey = "reports/j/report.md"
	if got := str(a.JobStore.(*fakeJobStore).record(t, "j"), "report_doc"); got != wantKey {
		t.Fatalf("report_doc=%q, want legacy escalation key %q", got, wantKey)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	escalated := false
	for _, event := range job.Events {
		if event.Kind == "job.escalate" {
			escalated = true
			if str(event.Payload, "document") != wantKey {
				t.Fatalf("escalate document=%q, want %q", str(event.Payload, "document"), wantKey)
			}
		}
	}
	if !escalated {
		t.Fatal("job.escalate event missing")
	}
	if routes := hk.Routes(); !strings.Contains(strings.Join(routes, "\n"), docRoute(http.MethodPut, wantKey)) {
		t.Fatalf("HK routes=%q, want escalation report PUT", routes)
	}
	if len(f.Calls) == 0 {
		t.Fatal("escalation relay was not run")
	}
}

func TestDoneArtifactLaterStrictResultAttachesWithoutReplacingPin(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	firstPath := completionReport(t, a, "first answer\n")
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", firstPath, "--completion-id", "answer-1"}); err != nil {
		t.Fatal(err)
	}
	firstPin := str(a.JobStore.(*fakeJobStore).record(t, "j"), "report_doc")
	secondPath := completionReport(t, a, "second answer\n")
	resultPath := filepath.Join(a.Jobs, "j", "result.json")
	if err := os.WriteFile(resultPath, []byte(`{"findings":["second"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", secondPath, "--completion-id", "answer-2", "--result", resultPath}); err != nil {
		t.Fatalf("later strict result: %v", err)
	}
	record := a.JobStore.(*fakeJobStore).record(t, "j")
	if got := str(record, "report_doc"); got != firstPin {
		t.Fatalf("terminal report_doc=%q, want original %q", got, firstPin)
	}
	if !sameJSONValue(record["result"], map[string]any{"findings": []any{"second"}}) {
		t.Fatalf("record result=%v, want the second answer result", record["result"])
	}
	if !containsString(eventKindsOf(t, a, "j"), "job.result_attached") {
		t.Fatal("later result did not emit job.result_attached")
	}
}

func TestDoneArtifactLegacyCompletionKeepsOriginalPin(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	ledgerRunner(t, a)
	hk := useFakeHK(t)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	path := completionReport(t, a, "legacy answer\n")
	const oldKey = "reports/j/legacy.md"
	if err := a.JobStore.Put(ctx, "j", map[string]any{"state": "completed", "report_doc": oldKey, "report_sha256": sha256String("legacy answer\n")}); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.completed", map[string]any{"report": path, "report_sha256": sha256String("legacy answer\n"), "document": oldKey})
	completionReport(t, a, "later answer\n")
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", path, "--completion-id", "later-answer"}); err != nil {
		t.Fatal(err)
	}
	if got := str(a.JobStore.(*fakeJobStore).record(t, "j"), "report_doc"); got != oldKey {
		t.Fatalf("terminal report_doc=%q, want original legacy pin %q", got, oldKey)
	}
}

func TestDoneArtifactStrictUploadFailureReportsHKReason(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	ledgerRunner(t, a)
	hk := useFakeHK(t)
	seedDoneJob(t, a)
	hk.Handle(hkFails("PUT /v1/documents/", http.StatusBadRequest, `{"error":"secret_like_content","pattern":"private_key"}`))
	reportPath := completionReport(t, a, "secret answer\n")
	resultPath := filepath.Join(a.Jobs, "j", "result.json")
	if err := os.WriteFile(resultPath, []byte(`{"findings":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	err := a.lifecycle(ctx, "done", []string{"j", "--report", reportPath, "--result", resultPath})
	if err == nil || !strings.Contains(err.Error(), "reason=secret_like_content:private_key") {
		t.Fatalf("error=%v, want exact safe HK refusal reason", err)
	}
	if got := exitCode(err); got != exitStateConflict {
		t.Fatalf("exit code=%d, want refusal conflict %d", got, exitStateConflict)
	}
}

func TestCompletionRefusalPathDoesNotCollideWithIntentID(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	useFakeHK(t)
	body := []byte("stable report\n")
	completionID := "refused-" + sha256String(string(body))
	if _, _, err := a.publishCompletionReport(ctx, "j", "42", completionID, body); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.publishCompletionReport(ctx, "j", "42", completionID, body); err != nil {
		t.Fatalf("reusing a valid completion id that resembles a refusal key: %v", err)
	}
}

func TestCompletionUploadErrorReasonsDistinguishHKAndLocalFailures(t *testing.T) {
	ctx := context.Background()
	hk := useFakeHK(t)
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			return http.StatusOK, `{}`, true
		}
		return 0, "", false
	})
	err := putCompletionDocument(ctx, client.Document{Key: "reports/j/answer.md", Kind: "report", Job: "j", Body: "answer"})
	if err == nil {
		t.Fatal("unconfirmed document write succeeded")
	}
	if got := completionUploadReason(err); got != "hk_unreachable" || !completionUploadCanRetry(err) {
		t.Fatalf("unconfirmed write reason=%q retry=%v, want hk_unreachable and retryable", got, completionUploadCanRetry(err))
	}
	localErr := &os.PathError{Op: "write", Path: filepath.Join(t.TempDir(), "intent.json"), Err: os.ErrPermission}
	if got := completionUploadReason(localErr); got != "local_io_error" || completionUploadCanRetry(localErr) {
		t.Fatalf("filesystem error reason=%q retry=%v, want local_io_error and non-retryable", got, completionUploadCanRetry(localErr))
	}
	stateErr := fail(exitUsage, "intent path is a symlink")
	if got := completionUploadReason(stateErr); got != "local_state_error" || completionUploadCanRetry(stateErr) {
		t.Fatalf("local state error reason=%q retry=%v, want local_state_error and non-retryable", got, completionUploadCanRetry(stateErr))
	}
}

func sha256String(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func TestDoneArtifactRetryReuploadsSameCompletion(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	seedDoneJob(t, a)
	hk.Handle(hkCommentAnswer)
	failedCount := 0
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if failedCount < 3 && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			failedCount++
			return http.StatusServiceUnavailable, `{"error":"unavailable"}`, true
		}
		return 0, "", false
	})
	path := completionReport(t, a, "stable answer\n")
	args := []string{"j", "--report", path, "--completion-id", "consult-1"}
	if err := a.lifecycle(ctx, "done", args); err == nil {
		t.Fatal("first completion succeeded despite the failed report upload")
	}
	if events, err := a.events("j"); err != nil {
		t.Fatal(err)
	} else {
		for _, event := range events {
			if event.Kind == "job.completed" {
				t.Fatalf("events after failed upload = %+v, want no completion", events)
			}
		}
	}
	if err := a.lifecycle(ctx, "done", args); err != nil {
		t.Fatalf("retry same completion: %v", err)
	}
	reqs := completionDocumentRequests(hk)
	if len(reqs) != 4 {
		t.Fatalf("document PUTs = %d, want three failed attempts and retry", len(reqs))
	}
	decoded, _ := url.PathUnescape(reqs[0].Path)
	if !strings.Contains(decoded, "/consult-1/") {
		t.Fatalf("document route = %q, want completion id in the key", reqs[0].Route())
	}
	for _, req := range reqs[1:] {
		if req.Route() != reqs[0].Route() {
			t.Fatalf("document routes = %q and %q; want same content-addressed completion key", reqs[0].Route(), req.Route())
		}
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	var completed []Event
	for _, event := range job.Events {
		if event.Kind == "job.completed" {
			completed = append(completed, event)
		}
	}
	if len(completed) != 1 || str(completed[0].Payload, "completion_id") != "consult-1" || str(completed[0].Payload, "document") == "" {
		t.Fatalf("completion events = %+v, want one event with completion id and document key", completed)
	}
}

func TestDoneArtifactSecretRefusalIsPermanentForBytes(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	seedDoneJob(t, a)
	refused := 0
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			refused++
			return http.StatusBadRequest, `{"error":"secret_like_content","pattern":"private_key"}`, true
		}
		return 0, "", false
	})
	path := completionReport(t, a, "unchanged refused bytes\n")
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", path, "--completion-id", "secret-1"}); err == nil {
		t.Fatal("secret-like report was accepted")
	}
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", path, "--completion-id", "secret-2"}); err == nil {
		t.Fatal("same refused bytes were uploaded under another completion id")
	}
	if refused != 1 {
		t.Fatalf("secret refusal requests = %d, want one permanent refusal", refused)
	}
	sum := sha256.Sum256([]byte("unchanged refused bytes\n"))
	if _, err := os.Stat(completionRefusalPath(a.Jobs, "j", hex.EncodeToString(sum[:]))); err != nil {
		t.Fatalf("per-byte refusal record missing: %v", err)
	}
}

func TestDoneArtifactPublishesLaterAnswerWithoutChangingTerminalPin(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	firstPath := completionReport(t, a, "first answer\n")
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", firstPath, "--completion-id", "answer-1"}); err != nil {
		t.Fatal(err)
	}
	firstPin := str(a.JobStore.(*fakeJobStore).record(t, "j"), "report_doc")
	secondPath := completionReport(t, a, "second answer\n")
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", secondPath, "--completion-id", "answer-2"}); err != nil {
		t.Fatalf("publish follow-up answer: %v", err)
	}
	if got := str(a.JobStore.(*fakeJobStore).record(t, "j"), "report_doc"); got != firstPin {
		t.Fatalf("terminal report_doc changed from %q to %q", firstPin, got)
	}
	var docs []string
	for _, req := range completionDocumentRequests(hk) {
		decoded, _ := url.PathUnescape(req.Path)
		docs = append(docs, decoded)
	}
	if len(docs) != 2 || docs[0] == docs[1] || !strings.Contains(docs[0], "/answer-1/") || !strings.Contains(docs[1], "/answer-2/") {
		t.Fatalf("answer document routes = %q, want separate completion ids", docs)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	var linked []string
	for _, ev := range job.Events {
		if ev.Kind == "job.completed" {
			linked = append(linked, str(ev.Payload, "document"))
		}
	}
	if len(linked) != 2 || linked[0] == "" || linked[1] == "" || linked[0] == linked[1] {
		t.Fatalf("completion document refs = %q, want both answer documents", linked)
	}
}

func TestDoneArtifactUsesAttachmentManifestAbove512KiB(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	seedDoneJob(t, a)
	hk.Handle(hkCommentAnswer)
	content := strings.Repeat("a", (512<<10)+1)
	attachmentSHA := sha256.Sum256([]byte(content))
	attachmentSeen := false
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if req.Route() != "PUT /v1/attachments" {
			return 0, "", false
		}
		attachmentSeen = true
		if string(req.Body) != content {
			t.Errorf("attachment body length = %d, want %d", len(req.Body), len(content))
		}
		return http.StatusCreated, fmt.Sprintf(`{"attachment":{"sha256":%q,"size_bytes":%d,"mime":"text/markdown"},"created":true}`, hex.EncodeToString(attachmentSHA[:]), len(content)), true
	})
	path := completionReport(t, a, content)
	if err := a.lifecycle(ctx, "done", []string{"j", "--report", path, "--completion-id", "large-answer"}); err != nil {
		t.Fatal(err)
	}
	if !attachmentSeen {
		t.Fatal("large report was not uploaded as an attachment")
	}
	wantAttachmentName := "answer-" + hex.EncodeToString(attachmentSHA[:]) + ".md"
	for _, req := range hk.Requests() {
		if req.Route() == "PUT /v1/attachments" && req.HKName != wantAttachmentName {
			t.Fatalf("attachment original_name=%q, want manifest name %q", req.HKName, wantAttachmentName)
		}
	}
	reqs := completionDocumentRequests(hk)
	if len(reqs) != 1 {
		t.Fatalf("manifest document PUTs = %d, want one", len(reqs))
	}
	var document map[string]any
	if err := json.Unmarshal(reqs[0].Body, &document); err != nil {
		t.Fatalf("manifest body is not JSON: %v", err)
	}
	var manifest map[string]any
	if err := json.Unmarshal([]byte(document["body"].(string)), &manifest); err != nil {
		t.Fatalf("manifest document body is not JSON: %v", err)
	}
	if manifest["attachment_sha256"] != hex.EncodeToString(attachmentSHA[:]) || manifest["sha256"] != hex.EncodeToString(attachmentSHA[:]) {
		t.Fatalf("manifest = %v, want report and attachment sha256", manifest)
	}
}
