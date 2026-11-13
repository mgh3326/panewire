package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// hkJobUpdateContract answers PUT /v1/jobs/{job} by the rule handoffkeep's
// store enforces (checkJobUpdate): a result is accepted only together with
// state=completed, and any other rejection is 400 invalid_context.
func hkJobUpdateContract(t *testing.T) (*hkJobs, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/v1/jobs/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		bodies = append(bodies, string(raw))
		mu.Unlock()
		var body struct {
			State  *string         `json:"state"`
			Result json.RawMessage `json:"result"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		if decoder.Decode(&body) != nil || len(body.Result) > 0 && (body.State == nil || *body.State != "completed") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_context"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)
	return &hkJobs{q: &hkQuota{base: server.URL, token: "fixture", client: server.Client()}, now: time.Now}, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
}

// The 400 in the rc76 output: PUT /v1/jobs/{job} with a result but no
// state=completed. The rule predates hk v0.0.6 (it is in v0.0.4 too).
func TestJobResultWithoutCompletedIsHKBadRequest(t *testing.T) {
	jobs, _ := hkJobUpdateContract(t)
	err := jobs.Put(t.Context(), "job", map[string]any{"result": map[string]any{"spawn_retryable": true}})
	var backend *BackendError
	if !errors.As(err, &backend) || backend.Status != http.StatusBadRequest || backend.Code != "invalid_context" {
		t.Fatalf("err=%v", err)
	}
	if got := err.Error(); got != "handoffkeep HTTP 400 (response withheld)" {
		t.Fatalf("message=%q", got)
	}
	if err = jobs.Put(t.Context(), "job", map[string]any{"state": "completed", "result": map[string]any{"findings": 0}}); err != nil {
		t.Fatalf("completed result refused: %v", err)
	}
}

func TestSpawnCleanupMarkersWriteNothingHKRefuses(t *testing.T) {
	for name, mark := range map[string]struct {
		run   func(*App, context.Context, string)
		event string
	}{
		"retryable": {(*App).markSpawnRetryable, "job.spawn_retryable"},
		"uncertain": {(*App).markSpawnUncertain, "job.cleanup_pending"},
	} {
		t.Run(name, func(t *testing.T) {
			a := testApp(t)
			var stderr bytes.Buffer
			a.Err = &stderr
			jobs, bodies := hkJobUpdateContract(t)
			a.JobStore = jobs
			mark.run(a, t.Context(), "job")
			if strings.Contains(stderr.String(), "HTTP 400") || strings.Contains(stderr.String(), "write failed") {
				t.Fatalf("cleanup marker hit a refused write: %s", stderr.String())
			}
			for _, body := range bodies() {
				if strings.Contains(body, `"result"`) {
					t.Fatalf("result without state=completed sent: %s", body)
				}
			}
			if !hasEvent(t, a, "job", mark.event) {
				t.Fatalf("durable %s event missing", mark.event)
			}
			if hasEvent(t, a, "job", "job.record_failed") {
				t.Fatal("job.record_failed recorded for a marker hk can never accept")
			}
		})
	}
}

func hasEvent(t *testing.T, a *App, job, kind string) bool {
	t.Helper()
	events, err := a.events(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}
