package panewire

// hk 4270 (decision 152): a request whose agent name herdr would refuse, or
// whose --tags no configured host declares, is refused at submit with a 400
// that names the reason and the fix. Nothing is reserved, so the submitter's
// terminal is the only place it is reported, and no SPAWN_FAILED follows.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestSpawnSubmitRefusesInputTheNodeCouldNotLaunch_t4270(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(args []string) []string
		reason string
	}{
		{"agent name too long", func(a []string) []string { return replaceSpawnArg(a, "-l", strings.Repeat("a", 33)) }, "label_invalid"},
		{"agent name uppercase", func(a []string) []string { return replaceSpawnArg(a, "-l", "Test-Job") }, "label_invalid"},
		{"no host declares the cap", func(a []string) []string { return append(a, "--tags", "unity") }, "no_eligible_host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			req := durableSpawnRequest()
			req.Args = tc.mutate(append([]string(nil), req.Args...))
			raw, _ := json.Marshal(req)
			r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
			r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
			w := httptest.NewRecorder()
			h.Handler().ServeHTTP(w, r)
			var body map[string]string
			_ = json.Unmarshal(w.Body.Bytes(), &body)
			if w.Code != http.StatusBadRequest || body["reason"] != tc.reason || body["fix"] == "" {
				t.Fatalf("submit=%d %s, want 400 with reason %s and a fix", w.Code, w.Body, tc.reason)
			}
			if _, err := h.inbox.store.spawnTask(t.Context(), req.RequestID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("a refused request left an operation behind: %v", err)
			}
		})
	}
}

func TestSpawnFeaturesAdvertiseQueueFeatures(t *testing.T) {
	h := asyncSpawnHub(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/spawns/features", nil)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	var response struct {
		Features []string `json:"features"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != http.StatusOK || !slices.Equal(response.Features, []string{"start_conditions_v1", "spawn_expires_v1"}) {
		t.Fatalf("features=%d %s, want start_conditions_v1 and spawn_expires_v1 (hk 4660)", w.Code, w.Body)
	}
}

// A request with a start condition is accepted into the queue as waiting
// (hk 4660); before queue-core support the hub refused it.
func TestSpawnSubmitQueuesStartConditionsAsWaiting(t *testing.T) {
	h := asyncSpawnHub(t)
	req := durableSpawnRequest()
	req.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/spawns", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != http.StatusAccepted || body["status"] != "waiting" || body["expires_at"] == "" {
		t.Fatalf("submit=%d %s, want 202 waiting with expires_at", w.Code, w.Body)
	}
	task, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil || task.Status != "waiting" || task.Node != nil {
		t.Fatalf("stored %+v %v, want a waiting row", task, err)
	}
	if held, err := h.inbox.store.spawnJobReserved(t.Context(), "test-job"); err != nil || !held {
		t.Fatalf("a waiting request must hold its reservation: %v %v", held, err)
	}
}

func replaceSpawnArg(args []string, flag, value string) []string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			args[i+1] = value
		}
	}
	return args
}
