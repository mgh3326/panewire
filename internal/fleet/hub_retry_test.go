package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// fastRetry keeps backoff at a millisecond so the budget decides how many
// sends a test sees.
func fastRetry() hubretry.Policy {
	return hubretry.Policy{Budget: 5 * time.Second, Initial: time.Millisecond, Max: time.Millisecond}
}

// A spawn submit whose answer was lost to a 503 is sent again under the same
// request id, and the hub holds one operation for it. Acceptance: a submit is
// not duplicated (same key, one operation).
func TestSubmitResendsTheSameRequestWithoutDuplicatingIt(t *testing.T) {
	const requestID = "00000000-0000-4000-8000-000000000042"
	var mu sync.Mutex
	operations := map[string]string{} // request id -> job id
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request Submission
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		job, known := operations[request.RequestID]
		if !known {
			job = "pw-job-retry"
			operations[request.RequestID] = job
		}
		mu.Unlock()
		if sends.Add(1) == 1 {
			// The operation was recorded, but the answer never arrived.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"job_id": job, "request_id": request.RequestID})
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), retry: fastRetry()}
	accepted, err := c.Submit(context.Background(), Submission{RequestID: requestID})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if accepted.Job != "pw-job-retry" || accepted.Request != requestID || sends.Load() != 2 || len(operations) != 1 {
		t.Fatalf("accepted=%+v sends=%d operations=%v", accepted, sends.Load(), operations)
	}
}

// Spawn is not keyed, so a 503 is its answer: it is sent once, and a second
// send could start a second pane.
func TestSpawnIsSentOnceThroughAnUnavailableAnswer(t *testing.T) {
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), retry: fastRetry()}
	if _, err := c.Spawn(context.Background(), HubSpawnRequest{}); err == nil {
		t.Fatal("a 503 answer must still fail the spawn")
	}
	if sends.Load() != 1 {
		t.Fatalf("sends=%d", sends.Load())
	}
}

// A spawn --wait keeps following its one operation through an outage: reads
// that meet 503s are repeated, and the wait's own failure limit is not used up
// by them. Acceptance: spawn --wait keeps following the same request across a
// hub restart.
func TestWaitSpawnFollowsTheOperationThroughAnOutage(t *testing.T) {
	const outageReads = 20 // more than spawnStatusFailureLimit, so a read-count limit would give up
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sends.Add(1) <= outageReads {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "succeeded", "result": map[string]any{"rc": 0, "host": "node-b"}})
	}))
	defer server.Close()
	var retried hubretry.Stats
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1, retry: fastRetry(), onRetry: retried.Add}
	attempt, err := c.WaitSpawn(context.Background(), statusTestOperation)
	if err != nil || attempt.Host != "node-b" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
	if retried.Retries != outageReads || sends.Load() != outageReads+1 {
		t.Fatalf("retried=%+v sends=%d", retried, sends.Load())
	}
}

// The command prints one stderr line for all the retries it made, then clears
// its tally so a second report says nothing.
func TestReportHubRetriesPrintsOneLinePerCommand(t *testing.T) {
	var errOut bytes.Buffer
	a := &App{Err: &errOut}
	a.noteHubRetry(hubretry.Stats{Retries: 1, Waited: 100 * time.Millisecond})
	a.noteHubRetry(hubretry.Stats{Retries: 2, Waited: 400 * time.Millisecond})
	a.reportHubRetries()
	if got := errOut.String(); got != "hub: retried 3 times, waited 500ms\n" {
		t.Fatalf("stderr=%q", got)
	}
	a.reportHubRetries()
	if errOut.String() != "hub: retried 3 times, waited 500ms\n" {
		t.Fatalf("second report printed: %q", errOut.String())
	}
}
