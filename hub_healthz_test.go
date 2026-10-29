package panewire

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// /healthz is a wire contract: the body has always been the literal bytes
// {"status":"ok"} followed by a backslash and n, with no real newline, and
// the handler never calls WriteHeader. Pin those exact bytes so a refactor of
// the JSON helpers cannot change what a byte-exact probe receives.
func TestHealthzWireBytesUnchanged(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&HubServer{}).handleHealthz(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", recorder.Code)
	}
	if got, want := recorder.Header().Get("Content-Type"), "application/json"; got != want {
		t.Fatalf("content-type=%q, want %q", got, want)
	}
	if got, want := recorder.Body.String(), `{"status":"ok"}\n`; got != want {
		t.Fatalf("body=%q, want %q", got, want)
	}
}
