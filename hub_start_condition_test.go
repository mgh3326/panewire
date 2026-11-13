package panewire

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestUnboundLegacyStartConditionCannotStartBeforeQueueCore(t *testing.T) {
	h := asyncSpawnHub(t)
	spawnAppCreated := false
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		spawnAppCreated = true
		return nil, nil
	}
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(1200)
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), task)
	if spawnAppCreated {
		t.Fatal("legacy conditioned request reached spawn execution")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "failed" || saved.Outcome.Reason != "start_conditions_unsupported" {
		t.Fatalf("legacy conditioned request = %#v, %v; want terminal unsupported outcome", saved, err)
	}
}

func TestBoundLegacyStartConditionWaitsBeforeReplayUntilMet(t *testing.T) {
	reads := 0
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks/42" {
			t.Errorf("unexpected HK request %s %s", r.Method, r.URL.Path)
		}
		reads++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42,"state":"ready","relations":[]}`))
	}))
	defer hk.Close()
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "start-condition-test")
	h := asyncSpawnHub(t)
	spawnAppCreated := false
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		spawnAppCreated = true
		return nil, nil
	}
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(1201)
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, hubSpawnRequest{
		RequestID: t14RequestID(1202), Machine: "machine-a", WaitSeconds: 600,
	}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	h.executeSpawnTask(t.Context(), task)
	if spawnAppCreated {
		t.Fatal("bound conditioned request was replayed before its condition was met")
	}
	if reads == 0 {
		t.Fatal("bound condition was not read before replay")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "dispatching" {
		t.Fatalf("bound conditioned request = %#v, %v; want pending dispatching row", saved, err)
	}
}
