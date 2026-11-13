package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
//	published  hk holds the bytes of S         job.report_published {report_sha256, document}
//	settled    the hk record is terminal       job.record_settled {report_sha256, pinned, result_sha256}
//	pinned     the record names S's document   job.report_pinned {report_sha256, document, outcome}
//	notified   the parent was told about S     job.completed {report_sha256, document}

const (
	eventReportPublished = "job.report_published"
	eventRecordSettled   = "job.record_settled"
	eventReportPinned    = "job.report_pinned"
)

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

// publishReport PUTs the report to its content-addressed key and confirms hk
// holds exactly these bytes, by the sha256 in hk's answer or, when the
// answer carries none, by reading the document back. It returns the key. A
// failure is the caller's warning: publication never blocks completion.
func (a *App) publishReport(ctx context.Context, j Job, body []byte, digest string) (string, error) {
	key := reportDocumentKey(j.ID, digest)
	upload := client.Document{Key: key, Kind: "report", Job: j.ID, Body: string(body), OriginClass: "job"}
	if task, err := hkTaskID(j.Task); err == nil {
		upload.TaskID = &task
	}
	stored, err := hkPutDocumentStored(ctx, upload)
	if err != nil {
		return "", err
	}
	if stored.Key == key && stored.SHA256 == digest {
		return key, nil
	}
	read, err := hkGetDocumentWithin(ctx, key, hkUploadTimeout)
	if err != nil {
		return "", err
	}
	if read.Key != key || read.SHA256 != digest {
		return "", errReportKeyHoldsOtherBytes
	}
	return key, nil
}

// reportDigest is the lower-case hex sha256 of body.
func reportDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
