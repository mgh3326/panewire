package fleet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestDoneKeepsReportAtDocumentLimitInline(t *testing.T) {
	r := newDoneRig(t)
	body := strings.Repeat("x", reportDocumentMaxBytes)
	r.write(body)
	if err := r.done(); err != nil {
		t.Fatalf("done report at document limit: %v", err)
	}
	key := r.key(body)
	for _, req := range r.hk.Requests() {
		if req.Method == http.MethodPut && req.Path == "/v1/attachments" {
			t.Fatal("report at the 512 KiB limit was uploaded as an attachment")
		}
		if req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key) {
			var doc struct {
				Body string `json:"body"`
			}
			if err := json.Unmarshal(req.Body, &doc); err != nil {
				t.Fatalf("decode report document: %v", err)
			}
			if len(doc.Body) != reportDocumentMaxBytes {
				t.Fatalf("document body has %d bytes, want %d", len(doc.Body), reportDocumentMaxBytes)
			}
			return
		}
	}
	t.Fatal("report document was not uploaded")
}

func TestDonePublishesLargeReportAsAttachmentAndManifest(t *testing.T) {
	const documentMaxBytes = 512 << 10
	r := newDoneRig(t)
	body := strings.Repeat("x", documentMaxBytes+1)
	digest := reportDigest([]byte(body))
	key := reportDocumentKey("j", digest)
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		switch {
		case req.Method == http.MethodPut && req.Path == "/v1/attachments":
			sha := reportDigest(req.Body)
			return http.StatusCreated, fmt.Sprintf(`{"attachment":{"sha256":%q,"size_bytes":%d,"mime":"text/markdown","original_name":%q},"created":true}`, sha, len(req.Body), req.Header.Get("X-HK-Name")), true
		case req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key):
			var doc struct {
				Body string `json:"body"`
			}
			if err := json.Unmarshal(req.Body, &doc); err != nil {
				t.Fatalf("decode document request: %v", err)
			}
			if len(doc.Body) > documentMaxBytes {
				return http.StatusRequestEntityTooLarge, `{"error":"invalid_document"}`, true
			}
		}
		return 0, "", false
	})
	r.write(body)
	if err := r.done(); err != nil {
		t.Fatalf("done large report: %v", err)
	}

	var attachment hkRequest
	var manifest struct {
		Body string `json:"body"`
	}
	for _, req := range r.hk.Requests() {
		switch {
		case req.Method == http.MethodPut && req.Path == "/v1/attachments":
			attachment = req
		case req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key):
			if err := json.Unmarshal(req.Body, &manifest); err != nil {
				t.Fatalf("decode manifest document request: %v", err)
			}
		}
	}
	if attachment.Path == "" {
		t.Fatal("large report was not uploaded as an attachment")
	}
	if string(attachment.Body) != body || attachment.Header.Get("Content-Type") != "text/markdown" || attachment.Header.Get("X-HK-Name") != "report.md" {
		t.Fatalf("attachment name=%q MIME=%q bytes=%d; want report.md, text/markdown, and all %d report bytes", attachment.Header.Get("X-HK-Name"), attachment.Header.Get("Content-Type"), len(attachment.Body), len(body))
	}
	var fields struct {
		Schema           string `json:"schema"`
		Job              string `json:"job_id"`
		ReportSHA256     string `json:"report_sha256"`
		SizeBytes        int64  `json:"size_bytes"`
		AttachmentSHA256 string `json:"attachment_sha256"`
		MIME             string `json:"mime"`
		Name             string `json:"original_name"`
	}
	if err := json.Unmarshal([]byte(manifest.Body), &fields); err != nil {
		t.Fatalf("decode report manifest: %v", err)
	}
	if fields.Schema != "panewire.report-manifest.v1" || fields.Job != "j" || fields.ReportSHA256 != digest || fields.SizeBytes != int64(len(body)) || fields.AttachmentSHA256 != digest || fields.MIME != "text/markdown" || fields.Name != "report.md" {
		t.Fatalf("manifest=%+v; want metadata for report sha=%s size=%d", fields, digest, len(body))
	}
	if len(manifest.Body) > documentMaxBytes {
		t.Fatalf("manifest is %d bytes, want at most %d", len(manifest.Body), documentMaxBytes)
	}
	if req := attachment.Header.Get("X-HK-Ref"); req != "document:"+key {
		t.Fatalf("attachment ref=%q, want document:%s", req, key)
	}
	if records := r.store.record(t, "j"); str(records, "report_doc") != key || str(records, "report_sha256") != digest {
		t.Fatalf("job record=%v, want report pin %s (%s)", records, key, digest)
	}
	if err := r.done(); err != nil {
		t.Fatalf("repeat done for published large report: %v", err)
	}
	attachments, documents := 0, 0
	for _, req := range r.hk.Requests() {
		if req.Method == http.MethodPut && req.Path == "/v1/attachments" {
			attachments++
		}
		if req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key) {
			documents++
		}
	}
	if attachments != 1 || documents != 1 {
		t.Fatalf("repeat done wrote %d attachments and %d report manifests, want one each", attachments, documents)
	}
}

func TestDoneUsesAttachmentWhenJSONEnvelopeExceedsHKLimit(t *testing.T) {
	const requestLimit = (512 << 10) + 4096
	r := newDoneRig(t)
	body := strings.Repeat("<>&\n", 30_000)
	if len(body) >= reportDocumentMaxBytes {
		t.Fatalf("fixture has %d raw bytes, want less than %d", len(body), reportDocumentMaxBytes)
	}
	digest := reportDigest([]byte(body))
	key := reportDocumentKey("j", digest)
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		switch {
		case req.Method == http.MethodPut && req.Path == "/v1/attachments":
			sha := reportDigest(req.Body)
			return http.StatusCreated, fmt.Sprintf(`{"attachment":{"sha256":%q,"size_bytes":%d,"mime":"text/markdown","original_name":%q},"created":true}`, sha, len(req.Body), req.Header.Get("X-HK-Name")), true
		case req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key) && len(req.Body) > requestLimit:
			return http.StatusRequestEntityTooLarge, `{"error":"invalid_document"}`, true
		}
		return 0, "", false
	})
	r.write(body)
	if err := r.done(); err != nil {
		t.Fatalf("done report whose JSON envelope exceeds HK's limit: %v", err)
	}

	var attachment hkRequest
	var manifest struct {
		Body string `json:"body"`
	}
	var documentRequestBytes int
	for _, req := range r.hk.Requests() {
		switch {
		case req.Method == http.MethodPut && req.Path == "/v1/attachments":
			attachment = req
		case req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key):
			documentRequestBytes = len(req.Body)
			if err := json.Unmarshal(req.Body, &manifest); err != nil {
				t.Fatalf("decode report manifest: %v", err)
			}
		}
	}
	if string(attachment.Body) != body {
		t.Fatalf("attachment has %d bytes, want the original %d-byte report", len(attachment.Body), len(body))
	}
	if documentRequestBytes == 0 || documentRequestBytes > requestLimit {
		t.Fatalf("manifest request has %d bytes, want 1..%d", documentRequestBytes, requestLimit)
	}
	var fields reportManifest
	if err := json.Unmarshal([]byte(manifest.Body), &fields); err != nil || fields.ReportSHA256 != digest || fields.SizeBytes != int64(len(body)) {
		t.Fatalf("manifest=%+v err=%v, want report sha %s and size %d", fields, err, digest, len(body))
	}
}

func TestDonePublishesDeduplicatedAttachmentWithStoredMetadata(t *testing.T) {
	r := newDoneRig(t)
	body := strings.Repeat("x", reportDocumentMaxBytes+1)
	digest := reportDigest([]byte(body))
	key := reportDocumentKey("j", digest)
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if req.Method != http.MethodPut || req.Path != "/v1/attachments" {
			return 0, "", false
		}
		sha := reportDigest(req.Body)
		return http.StatusOK, fmt.Sprintf(`{"attachment":{"sha256":%q,"size_bytes":%d,"mime":"text/plain","original_name":"first-upload.txt"},"created":false}`, sha, len(req.Body)), true
	})
	r.write(body)
	if err := r.done(); err != nil {
		t.Fatalf("done report whose attachment already exists with different metadata: %v", err)
	}

	var manifestBody string
	for _, req := range r.hk.Requests() {
		if req.Method == http.MethodPut && req.Path == "/v1/documents/"+url.PathEscape(key) {
			var doc struct {
				Body string `json:"body"`
			}
			if err := json.Unmarshal(req.Body, &doc); err != nil {
				t.Fatalf("decode report manifest: %v", err)
			}
			manifestBody = doc.Body
		}
	}
	var manifest reportManifest
	if err := json.Unmarshal([]byte(manifestBody), &manifest); err != nil {
		t.Fatalf("decode report manifest body %q: %v", manifestBody, err)
	}
	if manifest.ReportSHA256 != digest || manifest.SizeBytes != int64(len(body)) || manifest.MIME != reportAttachmentMIME || manifest.OriginalName != "report.md" {
		t.Fatalf("manifest=%+v, want current report identity and metadata", manifest)
	}
}
