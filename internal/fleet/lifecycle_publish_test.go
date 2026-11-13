package fleet

import (
	"net/http"
	"strings"
	"testing"
)

// Step 1 of `done`: the report is published to its content-addressed key, the
// job.report_published receipt names that exact key, and the completion event
// and the relay text carry it.
func TestDonePublishesToContentAddressedKeyAndWritesReceipt(t *testing.T) {
	r := newDoneRig(t)
	body := "first line\nREADY: done\n"
	r.write(body)
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	key, digest := r.key(body), reportDigest([]byte(body))
	var puts []string
	for _, req := range r.hk.Requests() {
		if req.Method == http.MethodPut && strings.HasPrefix(req.Path, "/v1/documents/") {
			puts = append(puts, req.Route())
		}
	}
	if want := docRoute(http.MethodPut, key); len(puts) != 1 || puts[0] != want {
		t.Fatalf("document PUTs=%q, want exactly %q", puts, want)
	}
	receipts := r.events(eventReportPublished)
	if len(receipts) != 1 || str(receipts[0], "document") != key || str(receipts[0], "report_sha256") != digest {
		t.Fatalf("receipts=%v, want one naming %s and %s", receipts, key, digest)
	}
	done := r.events("job.completed")
	if len(done) != 1 || str(done[0], "document") != key {
		t.Fatalf("completion events=%v, want one carrying the exact key %s", done, key)
	}
	if lines := r.relays(); len(lines) != 1 || !strings.HasSuffix(lines[0], " doc:"+key) {
		t.Fatalf("relay last lines=%q, want one ending in doc:%s", lines, key)
	}
}

// A failed upload leaves no receipt, and the completion event carries no key:
// the job still completes.
func TestDoneUploadFailureWritesNoReceipt(t *testing.T) {
	r := newDoneRig(t)
	r.hk.Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
	r.write("done\n")
	wantNotInHK(t, r.done())
	if got := r.events(eventReportPublished); len(got) != 0 {
		t.Fatalf("receipts=%v, want none", got)
	}
	if done := r.events("job.completed"); len(done) != 1 || str(done[0], "document") != "" {
		t.Fatalf("completion events=%v, want one without a document", done)
	}
}

// hk's answer to the PUT may carry no sha256: the step then reads the document
// back, and takes a document whose sha256 is not the report's as a failure.
func TestPublishReportConfirmsByReadBack(t *testing.T) {
	body := []byte("answer\n")
	digest := reportDigest(body)
	key := reportDocumentKey("j", digest)
	for name, tc := range map[string]struct {
		stored string
		want   bool
	}{
		"matching": {digest, true},
		"other":    {strings.Repeat("0", 64), false},
	} {
		t.Run(name, func(t *testing.T) {
			r := newDoneRig(t)
			r.hk.Handle(func(req hkRequest) (int, string, bool) {
				switch req.Route() {
				case docRoute(http.MethodPut, key):
					return http.StatusOK, `{"document":{"key":"` + key + `"},"changed":true}`, true
				case docRoute(http.MethodGet, key):
					return http.StatusOK, `{"key":"` + key + `","kind":"report","sha256":"` + tc.stored + `"}`, true
				}
				return 0, "", false
			})
			got, err := r.a.publishReport(t.Context(), Job{ID: "j", Task: "42"}, body, digest)
			if tc.want && (err != nil || got != key) {
				t.Fatalf("key=%q err=%v, want %q", got, err, key)
			}
			if !tc.want && err == nil {
				t.Fatalf("key=%q, want an error for a document holding other bytes", got)
			}
		})
	}
}
