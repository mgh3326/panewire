package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsultCLISendsNodeCredentialAndPrintsRow(t *testing.T) {
	var seenMachine, seenAuth string
	var seen map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/consults" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		seenMachine = r.Header.Get("X-Panewire-Machine-ID")
		seenAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&seen)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"0f1e2d3c-4b5a-4697-8877-665544332211","state":"queued","seat":{"profile":"codex-astra","family":"openai","grade":"SS"},"job":"consult-42-abcd","created_at":"2026-11-15T00:00:00Z"}`)
	}))
	t.Cleanup(server.Close)

	envPath := filepath.Join(t.TempDir(), "consult.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=oci\nHUB_TOKEN=node-consult-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runConsultCLI([]string{
		"request", "--hub-url", server.URL, "--hub-token-env", envPath,
		"--task", "42", "--question", "hk:doc/design/consult-q",
		"--author-job", "author-job", "--pr", "example/project#7", "--head", strings.Repeat("a", 40),
	}, &stdout, &stderr, hubCLIDeps{AllowInsecureForTests: true})
	if code != ExitOK {
		t.Fatalf("rc = %d stderr=%s", code, stderr.String())
	}
	if seenMachine != "oci" || seenAuth != "Bearer node-consult-token" {
		t.Fatalf("credential = %q/%q, want the node's own", seenMachine, seenAuth)
	}
	if seen["task_id"] != "42" || seen["question_doc"] != "hk:doc/design/consult-q" || seen["author_job"] != "author-job" || seen["pr"] != "example/project#7" {
		t.Fatalf("body = %v", seen)
	}
	if seen["requester"] != "oci" {
		t.Fatalf("requester = %q, want the credential's machine id", seen["requester"])
	}
	if line := stdout.String(); !strings.Contains(line, "CONSULT id=0f1e2d3c-4b5a-4697-8877-665544332211 state=queued seat=codex-astra@SS job=consult-42-abcd") {
		t.Fatalf("stdout = %q", line)
	}
}
