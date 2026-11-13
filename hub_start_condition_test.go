package panewire

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestSpawnWaitsUntilStartConditionIsMet(t *testing.T) {
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tasks/42" || r.Method != http.MethodGet {
			t.Errorf("unexpected hk request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer start-condition-test" {
			t.Errorf("hk auth = %q", r.Header.Get("Authorization"))
		}
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
	task := hubSpawnTask{
		Request: fleet.Submission{
			RequestID:       t14RequestID(1200),
			StartConditions: []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}},
		},
		Status:   "accepted",
		Deadline: time.Now().Add(time.Hour),
	}
	h.executeSpawnTask(t.Context(), task)
	if spawnAppCreated {
		t.Fatal("spawn execution began before the start condition was met")
	}
}
