package fleet

// hk 2861: the client's plan request. A 200 is the hub's placement; a 404 or
// 405 means the hub predates the route; a refusal keeps the hub's reason.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func planClientFor(t *testing.T, status int, body any) hubHTTP {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/spawns/plan" {
			t.Errorf("plan request went to %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(server.Close)
	return hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
}

func TestHubPlanReturnsTheHubsPlacement_t2861(t *testing.T) {
	c := planClientFor(t, http.StatusOK, SpawnPlan{Host: "hub-host", Profile: "codex-terra61", Role: "worker", Job: "job-a"})
	plan, err := c.Plan(context.Background(), Submission{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Host != "hub-host" || plan.Profile != "codex-terra61" {
		t.Fatalf("plan chose %s/%s, want the hub's hub-host/codex-terra61", plan.Host, plan.Profile)
	}
}

func TestHubPlanFromOldHubIsUnsupported_t2861(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		c := planClientFor(t, status, map[string]string{"error": "not_found"})
		if _, err := c.Plan(context.Background(), Submission{}); !errors.Is(err, errHubPlanUnsupported) {
			t.Fatalf("status %d: err=%v, want errHubPlanUnsupported", status, err)
		}
	}
}
