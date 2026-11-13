package panewire

// hk 2861: a dry run asks the hub for its own plan. The plan route shares the
// submit route's auth and validation, but must leave no operation row behind.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSpawnPlanRouteLeavesNoOperation_t2861(t *testing.T) {
	h := asyncSpawnHub(t)
	req := durableSpawnRequest()
	raw, _ := json.Marshal(req)
	plan := func(token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/spawns/plan", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	if w := plan("node-token"); w.Code != http.StatusUnauthorized {
		t.Fatalf("node plan=%d, want 401", w.Code)
	}
	if w := plan(t14OperatorToken); w.Code == http.StatusAccepted {
		t.Fatalf("plan was accepted: %d %s", w.Code, w.Body)
	}
	if _, err := h.inbox.store.spawnTask(t.Context(), req.RequestID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("a plan left an operation behind: %v", err)
	}
}
