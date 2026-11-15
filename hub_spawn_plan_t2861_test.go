package panewire

// hk 2861: a dry run asks the hub for its own plan. The plan route shares the
// submit route's auth and validation, must place as a submission would, and
// must leave no operation row, no reservation and no hk write behind.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// planJobs is the hk job-record fixture: a plan must not claim or write a record.
type planJobs struct {
	writes   atomic.Int32
	existing map[string]any
}

func (j *planJobs) Claim(context.Context, string, string, string) error {
	j.writes.Add(1)
	return nil
}
func (j *planJobs) Put(context.Context, string, map[string]any) error {
	j.writes.Add(1)
	return nil
}
func (j *planJobs) Get(context.Context, string) (map[string]any, error) {
	if j.existing != nil {
		return j.existing, nil
	}
	return nil, fleet.ErrJobNotFound
}

// planHK is the svc-panewire hk the hub reaches over HTTP. It serves task 42
// and counts every non-GET request, the writes a plan must not make.
func planHK(t *testing.T) *atomic.Int32 {
	t.Helper()
	writes := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			writes.Add(1)
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/42" {
			_, _ = io.WriteString(w, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"not_found"}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "svc-panewire-plan-token")
	return writes
}

func TestSpawnPlanRouteLeavesNoOperation_t2861(t *testing.T) {
	// The placement writes fleet events under the state home; keep them out of the tree.
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "machine-a", true, true)
	t14NamedNode(t, h, server, "desktop", true, true)
	hkWrites := planHK(t)
	jobs := &planJobs{}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.JobStore = jobs
		return app, nil
	}
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
	w := plan(t14OperatorToken)
	if w.Code != http.StatusOK {
		t.Fatalf("operator plan=%d %s, want 200", w.Code, w.Body)
	}
	var got fleet.SpawnPlan
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("plan body=%s err=%v", w.Body, err)
	}
	if got.Host == "" || got.Profile != "sonnet" || !strings.Contains(got.Output, "hub plan:") {
		t.Fatalf("plan must name the placed host and profile under the hub label: %+v", got)
	}
	if _, err := h.inbox.store.spawnTask(t.Context(), req.RequestID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("a plan left an operation behind: %v", err)
	}
	var reservations int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("a plan left %d reservations: %v", reservations, err)
	}
	if hkWrites.Load() != 0 || jobs.writes.Load() != 0 {
		t.Fatalf("a plan wrote to hk: http=%d job-records=%d", hkWrites.Load(), jobs.writes.Load())
	}
}

// A plan gives the refusals a submission gives before placement: an invalid
// label and a job id hk already holds. Neither reaches placement or writes.
func TestSpawnPlanRefusesWhatSubmitRefuses_t2861(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	hkWrites := planHK(t)
	jobs := &planJobs{existing: map[string]any{"state": "completed", "host": "machine-a"}}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.JobStore = jobs
		return app, nil
	}
	plan := func(mutate func(*fleet.Submission)) *httptest.ResponseRecorder {
		request := durableSpawnRequest()
		if mutate != nil {
			mutate(&request)
		}
		raw, _ := json.Marshal(request)
		r := httptest.NewRequest("POST", "/v1/spawns/plan", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	w := plan(nil)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "job_already_claimed") {
		t.Fatalf("a plan for a claimed job must refuse like submit: %d %s", w.Code, w.Body)
	}
	w = plan(func(request *fleet.Submission) {
		for i, arg := range request.Args {
			if arg == "-l" && i+1 < len(request.Args) {
				request.Args[i+1] = "job.a"
			}
		}
	})
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "label_invalid") {
		t.Fatalf("a plan for an invalid label must refuse like submit: %d %s", w.Code, w.Body)
	}
	if hkWrites.Load() != 0 || jobs.writes.Load() != 0 {
		t.Fatalf("a refusing plan wrote: hk=%d job-records=%d", hkWrites.Load(), jobs.writes.Load())
	}
}
