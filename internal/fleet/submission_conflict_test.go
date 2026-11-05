package fleet

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A 409 used to print "job id already claimed" and drop the hub's reason. The
// submitter needs the holder and the fix to act on a held role reservation.
func TestSubmitConflictPrintsHubReasonAndFix(t *testing.T) {
	for name, test := range map[string]struct {
		body string
		want []string
	}{
		"holder named": {
			`{"error":"job_already_claimed","request_id":"4a2514ef","job":"pw-author-trailer","host":"m1b","pane":"w2:p3","fix":"close pane w2:p3 on m1b, then resubmit"}`,
			[]string{"job_already_claimed", "request=4a2514ef", "job=pw-author-trailer", "host=m1b", "pane=w2:p3", "fix: close pane w2:p3 on m1b, then resubmit"},
		},
		"no holder": {
			`{"error":"idempotency_payload_conflict","fix":"use a new job id"}`,
			[]string{"idempotency_payload_conflict", "fix: use a new job id"},
		},
		"unreadable body": {`not json`, []string{"job id already claimed"}},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
			_, err := c.Submit(t.Context(), Submission{RequestID: "00000000-0000-4000-8000-000000000002"})
			if err == nil || ErrorCode(err) != exitDuplicate {
				t.Fatalf("err=%v code=%d", err, ErrorCode(err))
			}
			for _, want := range test.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("missing %q in %q", want, err)
				}
			}
		})
	}
}
