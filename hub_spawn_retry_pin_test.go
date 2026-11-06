package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// retryableDesktopTask is a stored operation whose first attempt failed on
// desktop and was resumed as a second attempt, the state the hub's
// verified-cleanup retry rewrites.
func retryableDesktopTask(t *testing.T, h *HubServer, request fleet.Submission) hubSpawnTask {
	t.Helper()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	failed := hubSpawnOutcome{RC: 124, Stage: "admission", Reason: "timeout", Host: "desktop", Profile: "codex-luna", Retryable: true}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, failed, time.Now()); err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := h.inbox.store.resumeCleanedSpawn(t.Context(), saved, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return retried
}

func TestHubSpawnRetryReplayIsNotAPlacementPin(t *testing.T) {
	h := asyncSpawnHub(t)
	root := t.TempDir()
	clock := time.Unix(1_800_000_000, 0)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{
			Config:   fleet.Config{Hosts: map[string]map[string]any{"desktop": {}}, HostOrder: []string{"desktop"}},
			JobStore: spawnJobsFixture{}, Home: root, Jobs: filepath.Join(root, "jobs"), Out: out, Err: errout,
			Now:   func() time.Time { return clock },
			Sleep: func(_ context.Context, d time.Duration) error { clock = clock.Add(d); return nil },
		}, nil
	}
	request := durableSpawnRequest()
	task := retryableDesktopTask(t, h, request)
	if task.Outcome.Host != "desktop" || !task.Outcome.Retryable {
		t.Fatalf("fixture lost the stored outcome: %+v", task.Outcome)
	}
	h.executeSpawnTask(t.Context(), task)
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture app has no quota catalog, so the replay ends at selection;
	// before the marker it ended at containment, before placement.
	if strings.Contains(saved.Outcome.Reason, "containment_required") || saved.Outcome.Reason == "" {
		t.Fatalf("the hub's own retry was refused as an operator pin: %+v", saved.Outcome)
	}
	// Same request, same idempotency key: the retry rewrites an in-memory copy.
	raw, err := json.Marshal(saved.Request)
	if err != nil || !strings.Contains(string(raw), `"--host","auto"`) || strings.Contains(string(raw), "desktop") {
		t.Fatalf("the stored request changed: %s %v", raw, err)
	}
}

func TestHubSpawnSubmitterPinStaysRefused(t *testing.T) {
	h := asyncSpawnHub(t)
	submit := func(request fleet.Submission, body func(map[string]any)) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(request)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		if body != nil {
			body(fields)
		}
		raw, _ = json.Marshal(fields)
		r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	pinned := durableSpawnRequest()
	pinned.Args = append([]string(nil), pinned.Args...)
	for i := range pinned.Args {
		if pinned.Args[i] == "--host" {
			pinned.Args[i+1] = "desktop"
		}
	}
	if _, err := fleet.ValidateSubmission(pinned); err == nil || !strings.Contains(err.Error(), "containment_required") {
		t.Fatalf("submitter pin: %v", err)
	}
	for name, body := range map[string]func(map[string]any){
		"args only":       nil,
		"RetryHost field": func(f map[string]any) { f["RetryHost"] = "desktop" },
		"retry_host key":  func(f map[string]any) { f["retry_host"] = "desktop" },
	} {
		if w := submit(pinned, body); w.Code != 400 {
			t.Fatalf("%s: pin accepted through the hub API: %d %s", name, w.Code, w.Body)
		}
		if _, err := h.inbox.store.spawnTask(t.Context(), pinned.RequestID); err == nil {
			t.Fatalf("%s: a refused pin was stored", name)
		}
	}
}

func TestHubSpawnMarkerIsNotReachableFromArgs(t *testing.T) {
	for _, operator := range []bool{false, true} {
		for _, args := range [][]string{
			{"--retry-host", "desktop"},
			{"--hub-retry-host", "desktop"},
			{"--retry-host", "desktop", "--host", "desktop"},
		} {
			if validHubSpawnArgsForMode(args, operator) {
				t.Fatalf("operator=%v: the allowlist accepts %v", operator, args)
			}
		}
		if !operator && validHubSpawnArgs([]string{"--host", "desktop"}) {
			t.Fatal("a non-operator submission may pin a host")
		}
	}
}
