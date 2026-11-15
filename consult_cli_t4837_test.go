package panewire

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A worker's node credential is refused by the hub; the refusal names the
// ESC route to the design lane instead of leaving the worker to retry the
// hub or a model API (hk 4837, operator decision 313).
func TestConsultCLIRefusalNamesESCRouteT4837(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
	}))
	t.Cleanup(server.Close)

	envPath := filepath.Join(t.TempDir(), "consult.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=worker-host\nHUB_TOKEN=node-worker-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runConsultCLI([]string{
		"request", "--hub-url", server.URL, "--hub-token-env", envPath,
		"--task", "42", "--question", "hk:doc/design/consult-q",
	}, &stdout, &stderr, hubCLIDeps{AllowInsecureForTests: true})
	if code != ExitConditionInvalid {
		t.Fatalf("rc = %d, want %d; stderr=%s", code, ExitConditionInvalid, stderr.String())
	}
	if got := stderr.String(); !strings.Contains(got, "ask your design lane by ESC with an hk packet") {
		t.Fatalf("stderr = %q, want the ESC route", got)
	}
}
