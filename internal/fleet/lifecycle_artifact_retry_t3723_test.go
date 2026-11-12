package fleet

import (
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
