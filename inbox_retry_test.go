package panewire

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// retryInboxDeps are the CLI's dependencies for a fake hub. Backoff is a
// millisecond, so the budget alone decides how many sends a test sees.
func retryInboxDeps(server *httptest.Server, budget time.Duration) hubCLIDeps {
	return hubCLIDeps{
		HTTPClient:            server.Client(),
		AllowInsecureForTests: true,
		HubRetry:              hubretry.Policy{Budget: budget, Initial: time.Millisecond, Max: time.Millisecond},
	}
}

func retryInboxTokenEnv(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(path, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN=node\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// An inbox command that meets 503s retries them, prints its answer as before,
// and says on stderr, in one line, how many times it sent again and how long
// it waited. Acceptance: a fake hub that answers 503 for N seconds and then
// recovers, with each listed call succeeding.
func TestInboxCountRetriesThroughUnavailableAnswers(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"lane-a":1}`))
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	args := []string{"count", "--lane", "lane-a", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}
	code := runInboxCLI(args, &out, &errOut, retryInboxDeps(server, 5*time.Second))
	if code != ExitOK || out.String() != "inbox: 1 unread\n" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("hub saw %d sends, want 3", got)
	}
	lines := strings.Split(strings.TrimSpace(errOut.String()), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "hub: retried 2 times, waited ") {
		t.Fatalf("stderr=%q", errOut.String())
	}
}

// A connection dropped mid-call is a transport error. The ack is repeatable,
// so it is sent again with the same body, and the answer is the ack's.
func TestInboxAckResendsItsBodyAfterADroppedConnection(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(raw))
		mu.Unlock()
		if calls.Add(1) == 1 {
			panic(http.ErrAbortHandler)
		}
		_, _ = w.Write([]byte(`{"acked":1}`))
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	args := []string{"ack", "7", "--lane", "lane-a", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}
	if code := runInboxCLI(args, &out, &errOut, retryInboxDeps(server, 5*time.Second)); code != ExitOK || out.String() != "acked: 1\n" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || bodies[0] == "" || bodies[0] != bodies[1] {
		t.Fatalf("bodies=%q: the retry must resend the same ack body", bodies)
	}
}

// When the budget ends with the hub still answering 503, the command reports
// that answer and exits as it did before retries existed.
func TestInboxReadGivesUpWithTheLastAnswerAndItsExitCode(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	args := []string{"count", "--lane", "lane-a", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}
	code := runInboxCLI(args, &out, &errOut, retryInboxDeps(server, 30*time.Millisecond))
	if code != ExitDeliveryFailure || !strings.Contains(errOut.String(), "inbox: hub returned HTTP 503") {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if calls.Load() < 2 || !strings.Contains(errOut.String(), "hub: retried ") {
		t.Fatalf("calls=%d err=%q: a budget that ends must still name the retries", calls.Load(), errOut.String())
	}
}

// A long poll is a repeatable GET, but its retries stop at the CLI deadline and
// do not run past --timeout. A 503 still standing when the deadline ends the
// retries makes the wait a timeout: exit 3, nothing on stdout, and the retry
// line on stderr.
func TestInboxWaitRetriesStayInsideItsDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	args := []string{"wait", "--lane", "lane-a", "--timeout", "40ms", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}
	started := time.Now()
	code := runInboxCLI(args, &out, &errOut, retryInboxDeps(server, time.Minute))
	if code != ExitTimeout || out.Len() != 0 || !strings.Contains(errOut.String(), "hub: retried ") {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("wait ran %v past its 40ms deadline", elapsed)
	}
}

// A long poll that the hub never answers ends at the CLI deadline, not after
// the per-request grace: no attempt, first or retried, outlives --timeout.
func TestInboxWaitStalledAttemptEndsAtTheDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulated stall: hold the poll until the client gives up.
		<-r.Context().Done()
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	args := []string{"wait", "--lane", "lane-a", "--timeout", "100ms", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}
	started := time.Now()
	code := runInboxCLI(args, &out, &errOut, retryInboxDeps(server, time.Minute))
	if code != ExitTimeout || out.Len() != 0 {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("stalled wait ran %v past its 100ms deadline", elapsed)
	}
}
