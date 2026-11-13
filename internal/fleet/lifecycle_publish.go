package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/mgh3326/handoffkeep/client"
)

// Report publication for `panewire done`.
//
// A report's identity is (job, sha256 of its bytes). Its hk document key is
// content-addressed, so a key names exactly one byte string and publishing is
// idempotent: the same bytes always PUT the same key.
//
// Each fact `done` establishes has one authority and one local receipt, and a
// step whose receipt exists is skipped (lifecycle_done.go):
//
//	published  hk holds S, inline or as a manifest plus attachment             job.report_published {report_sha256, document}
//	settled    the hk record is terminal       job.record_settled {report_sha256, pinned, result_sha256}
//	pinned     the record names S's document   job.report_pinned {report_sha256, document, outcome}
//	notified   the parent was told about S     job.completed {report_sha256, document}

const (
	eventReportPublished   = "job.report_published"
	eventRecordSettled     = "job.record_settled"
	eventReportPinned      = "job.report_pinned"
	reportDocumentMaxBytes = 512 << 10
	// hk caps the JSON request body at DocumentMaxBytes plus 4 KiB for its envelope.
	reportDocumentRequestMaxBytes = reportDocumentMaxBytes + 4096
	reportAttachmentMIME          = "text/markdown"
)

type reportManifest struct {
	Schema           string `json:"schema"`
	JobID            string `json:"job_id"`
	ReportSHA256     string `json:"report_sha256"`
	SizeBytes        int64  `json:"size_bytes"`
	AttachmentSHA256 string `json:"attachment_sha256"`
	MIME             string `json:"mime"`
	OriginalName     string `json:"original_name"`
}

// reportDocumentKey is the hk document key of the report whose bytes have
// sha256 digest. It lies under reports/<job>/sha256/, which a basename key
// (reports/<job>/<file name>) can never reach: a file name holds no slash, so
// the two schemes are disjoint by construction.
func reportDocumentKey(job, digest string) string {
	return "reports/" + job + "/sha256/" + digest + ".md"
}

// legacyReportDocumentKey is the key `done` used before reports were
// content-addressed: the report file's name, so a later report at the same
// path overwrote an earlier one.
func legacyReportDocumentKey(job, reportPath string) string {
	return "reports/" + job + "/" + filepath.Base(reportPath)
}

// publishedDocument is the document the job.report_published receipt names
// for the report with sha256 digest, "" when no receipt exists.
func publishedDocument(events []Event, digest string) string {
	for _, ev := range events {
		if ev.Kind == eventReportPublished && str(ev.Payload, "report_sha256") == digest {
			return str(ev.Payload, "document")
		}
	}
	return ""
}

// errReportKeyHoldsOtherBytes is hk answering a content-addressed key with a
// different sha256: it cannot happen unless hk stored the key wrongly.
var errReportKeyHoldsOtherBytes = errors.New("report document key holds other bytes")
var errReportAttachmentMismatch = errors.New("report attachment metadata mismatch")

// publishReport PUTs a report to its content-addressed key. Small reports are
// the document body; a large report is an attachment referenced by a JSON
// manifest at that key. It confirms the attachment metadata and document body
// by the sha256 in hk's answer or, when the answer carries none, by reading
// the document back. It returns the key. Publication never blocks completion.
func (a *App) publishReport(ctx context.Context, j Job, report string, body []byte, digest string) (string, error) {
	key := reportDocumentKey(j.ID, digest)
	upload := client.Document{Key: key, Kind: "report", Job: j.ID, Body: string(body), OriginClass: "job"}
	if task, err := hkTaskID(j.Task); err == nil {
		upload.TaskID = &task
	}
	useAttachment := len(body) > reportDocumentMaxBytes
	if !useAttachment {
		wire, err := json.Marshal(upload)
		if err != nil {
			return "", err
		}
		useAttachment = len(wire) > reportDocumentRequestMaxBytes
	}
	if useAttachment {
		name := filepath.Base(report)
		if name == "." || name == "" {
			name = "report.md"
		}
		attachment, err := hkPutAttachment(ctx, name, reportAttachmentMIME, "document:"+key, body)
		if err != nil {
			return "", err
		}
		if attachment.SHA256 != digest || attachment.SizeBytes != int64(len(body)) || attachment.MIME != reportAttachmentMIME || attachment.OriginalName != name {
			return "", errReportAttachmentMismatch
		}
		manifest, err := json.Marshal(reportManifest{
			Schema:           "panewire.report-manifest.v1",
			JobID:            j.ID,
			ReportSHA256:     digest,
			SizeBytes:        int64(len(body)),
			AttachmentSHA256: attachment.SHA256,
			MIME:             attachment.MIME,
			OriginalName:     attachment.OriginalName,
		})
		if err != nil {
			return "", err
		}
		upload.Body = string(manifest)
	}
	stored, err := hkPutDocumentStored(ctx, upload)
	if err != nil {
		return "", err
	}
	documentDigest := reportDigest([]byte(upload.Body))
	if stored.Key == key && stored.SHA256 == documentDigest {
		return key, nil
	}
	read, err := hkGetDocumentWithin(ctx, key, hkUploadTimeout)
	if err != nil {
		return "", err
	}
	if read.Key != key || read.SHA256 != documentDigest {
		return "", errReportKeyHoldsOtherBytes
	}
	return key, nil
}

// reportDigest is the lower-case hex sha256 of body.
func reportDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
