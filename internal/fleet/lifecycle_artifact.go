package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/handoffkeep/client"
	"github.com/mgh3326/panewire/internal/durable"
)

const reportDocumentMaxBytes = 512 << 10

type completionIntent struct {
	CompletionID string `json:"completion_id"`
	ReportSHA256 string `json:"report_sha256"`
	DocumentKey  string `json:"document_key"`
	Bytes        int    `json:"bytes"`
}

func completionIdentity(jobID, supplied string, report []byte) (string, error) {
	if supplied != "" {
		if !validID(supplied) {
			return "", fail(exitUsage, "--completion-id must be a valid id")
		}
		return supplied, nil
	}
	input := append([]byte(jobID+"\x00"), report...)
	sum := sha256.Sum256(input)
	return "auto-" + hex.EncodeToString(sum[:]), nil
}

func completionIntentPath(jobs, jobID, completionID string) string {
	return filepath.Join(jobs, jobID, ".completion-intents", completionID+".json")
}

func completionRefusalPath(jobs, jobID, digest string) string {
	return filepath.Join(jobs, jobID, ".completion-intents", "refused-"+digest+".json")
}

func readCompletionIntent(path string) (completionIntent, bool, error) {
	var intent completionIntent
	if err := noSymlinks(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return intent, false, nil
		}
		return intent, false, err
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return intent, false, nil
	}
	if err != nil {
		return intent, false, err
	}
	if err := json.Unmarshal(body, &intent); err != nil || intent.CompletionID == "" || intent.ReportSHA256 == "" || intent.DocumentKey == "" {
		return intent, false, fail(exitStateConflict, "completion intent is unreadable")
	}
	return intent, true, nil
}

func writeCompletionIntent(path string, intent completionIntent) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := noSymlinks(dir); err != nil {
		return err
	}
	body, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(path, ".completion-intent-*", append(body, '\n'), 0600)
}

func reportDocumentKey(jobID, completionID, digest string, large bool) string {
	name := "answer-" + digest + ".md"
	if large {
		name = "answer-" + digest + ".manifest.json"
	}
	return "reports/" + jobID + "/" + completionID + "/" + name
}

func (a *App) publishCompletionReport(ctx context.Context, jobID, taskID, completionID string, body []byte) (string, string, error) {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	refusalPath := completionRefusalPath(a.Jobs, jobID, digest)
	if err := noSymlinks(refusalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if refusal, err := os.ReadFile(refusalPath); err == nil {
		var rejected struct {
			Reason string `json:"reason"`
		}
		if json.Unmarshal(refusal, &rejected) != nil || rejected.Reason == "" {
			return "", "", fail(exitStateConflict, "report refusal record is unreadable")
		}
		return "", "", fail(exitStateConflict, "report bytes were permanently refused reason=%s", rejected.Reason)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	large := len(body) > reportDocumentMaxBytes
	key := reportDocumentKey(jobID, completionID, digest, large)
	path := completionIntentPath(a.Jobs, jobID, completionID)
	intent, exists, err := readCompletionIntent(path)
	if err != nil {
		return "", "", err
	}
	if exists && (intent.ReportSHA256 != digest || intent.DocumentKey != key || intent.Bytes != len(body)) {
		return "", "", fail(exitStateConflict, "completion id already names different report bytes")
	}
	if !exists {
		intent = completionIntent{CompletionID: completionID, ReportSHA256: digest, DocumentKey: key, Bytes: len(body)}
		if err := writeCompletionIntent(path, intent); err != nil {
			return "", "", err
		}
	}

	documentBody := body
	if large {
		if err := putCompletionAttachment(ctx, key, body); err != nil {
			if reason := completionUploadReason(err); strings.HasPrefix(reason, "secret_like_content") {
				if writeErr := recordCompletionRefusal(refusalPath, reason); writeErr != nil {
					return "", "", writeErr
				}
			}
			return "", "", err
		}
		manifest, err := json.Marshal(map[string]any{
			"schema": "panewire.report-manifest.v1", "job_id": jobID,
			"completion_id": completionID, "bytes": len(body),
			"sha256": digest, "attachment_sha256": digest,
			"attachment_name": "answer-" + digest + ".md", "mime": "text/markdown",
		})
		if err != nil {
			return "", "", err
		}
		documentBody = manifest
	}
	task, taskErr := hkTaskID(taskID)
	var taskPointer *int64
	if taskErr == nil {
		taskPointer = &task
	}
	doc := client.Document{Key: key, Kind: "report", Job: jobID, Body: string(documentBody), TaskID: taskPointer, OriginClass: "job"}
	if err := putCompletionDocument(ctx, doc); err != nil {
		if reason := completionUploadReason(err); strings.HasPrefix(reason, "secret_like_content") {
			if writeErr := recordCompletionRefusal(refusalPath, reason); writeErr != nil {
				return "", "", writeErr
			}
		}
		return "", "", err
	}
	return key, digest, nil
}

func putCompletionAttachment(ctx context.Context, key string, body []byte) error {
	attachmentKey := strings.TrimPrefix(key, "reports/")
	sum := sha256.Sum256(body)
	want := hex.EncodeToString(sum[:])
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		client, err := newHKClientWithin(hkUploadTimeout)
		if err != nil {
			return err
		}
		attachment, _, putErr := client.PutAttachment(ctx, filepath.Base(attachmentKey), "text/markdown", "none", body)
		if putErr == nil {
			if attachment.SHA256 != want || attachment.SizeBytes != int64(len(body)) {
				return fail(exitStateConflict, "report attachment did not match submitted bytes")
			}
			return nil
		}
		lastErr = putErr
		if !retryableCompletionUpload(putErr) {
			return putErr
		}
		client, readErr := newHKClientWithin(hkUploadTimeout)
		if readErr == nil {
			attachment, stream, getErr := client.GetAttachment(ctx, want)
			if getErr == nil {
				readback, copyErr := io.ReadAll(io.LimitReader(stream, int64(len(body))+1))
				_ = stream.Close()
				if copyErr == nil && int64(len(readback)) == int64(len(body)) && attachment.SHA256 == want && sha256.Sum256(readback) == sum {
					return nil
				}
			}
		}
	}
	return lastErr
}

func putCompletionDocument(ctx context.Context, doc client.Document) error {
	want := sha256.Sum256([]byte(doc.Body))
	for attempt := 0; attempt < 3; attempt++ {
		client, err := newHKClientWithin(hkUploadTimeout)
		if err != nil {
			return err
		}
		stored, _, putErr := client.PutDocument(ctx, doc)
		if putErr == nil && stored.Key == doc.Key && stored.SHA256 == hex.EncodeToString(want[:]) {
			return nil
		}
		if putErr == nil && stored.Key != "" && (stored.Key != doc.Key || stored.SHA256 != hex.EncodeToString(want[:])) {
			return fail(exitStateConflict, "report document key contains different bytes")
		}
		if putErr != nil && !retryableCompletionUpload(putErr) {
			return putErr
		}
		client, getErr := newHKClientWithin(hkUploadTimeout)
		if getErr == nil {
			stored, found, getErr := client.GetDocument(ctx, doc.Key)
			if getErr == nil && found {
				if stored.Key == doc.Key && stored.SHA256 == hex.EncodeToString(want[:]) {
					return nil
				}
				return fail(exitStateConflict, "report document key contains different bytes")
			}
		}
		if attempt == 2 {
			if putErr != nil {
				return putErr
			}
			if getErr != nil {
				return getErr
			}
			return fmt.Errorf("report document write was not confirmed")
		}
	}
	return fmt.Errorf("report document write was not confirmed")
}

func retryableCompletionUpload(err error) bool {
	var httpErr *client.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Status >= 500 || httpErr.Status == 429
	}
	return true
}

func completionUploadReason(err error) string {
	if pattern, ok := client.SecretPattern(err); ok && safeHKPattern.MatchString(pattern) {
		return "secret_like_content:" + pattern
	}
	if code := client.ErrorCode(err); doneResultCodeRE.MatchString(code) {
		return code
	}
	var httpErr *client.HTTPError
	if errors.As(err, &httpErr) && httpErr.Status > 0 {
		return fmt.Sprintf("http_%d", httpErr.Status)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, errHKTransport) {
		return "hk_unreachable"
	}
	return "hk_unconfigured"
}

func recordCompletionRefusal(path, reason string) error {
	body, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(path, ".completion-refusal-*", append(body, '\n'), 0600)
}

func hkReportDocumentMatches(ctx context.Context, key, digest string, size int64) bool {
	doc, err := hkGetDocument(ctx, key)
	if err != nil || doc.Key != key || doc.SHA256 == "" {
		return false
	}
	if doc.SHA256 == digest {
		return true
	}
	var manifest struct {
		Schema           string `json:"schema"`
		Bytes            int64  `json:"bytes"`
		SHA256           string `json:"sha256"`
		AttachmentSHA256 string `json:"attachment_sha256"`
	}
	if json.Unmarshal([]byte(doc.Body), &manifest) != nil || manifest.Schema != "panewire.report-manifest.v1" || manifest.Bytes != size || manifest.SHA256 != digest || manifest.AttachmentSHA256 != digest {
		return false
	}
	client, err := newHKClientWithin(hkUploadTimeout)
	if err != nil {
		return false
	}
	attachment, stream, err := client.GetAttachment(ctx, digest)
	if err != nil {
		return false
	}
	defer stream.Close()
	h := sha256.New()
	n, err := io.Copy(h, stream)
	return err == nil && n == size && attachment.SHA256 == digest && hex.EncodeToString(h.Sum(nil)) == digest
}
