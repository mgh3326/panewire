package panewire

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// liveOp claims one hub spawn operation for job and returns it. profile is the
// -m the submitter named ("auto" leaves placement to pick one).
func liveOp(t *testing.T, h *HubServer, n int, job, profile string, claimedAt time.Time) hubSpawnTask {
	t.Helper()
	req := durableSpawnRequest()
	req.RequestID = fmt.Sprintf("d676220b-e763-4af1-aefd-%012d", n)
	req.Args = append([]string(nil), req.Args...)
	for i := range req.Args {
		switch req.Args[i] {
		case "-m":
			req.Args[i+1] = profile
		case "-l", "--job":
			req.Args[i+1] = job
		case "--task":
			req.Args[i+1] = fmt.Sprint(100 + n)
		}
	}
	if profile != "auto" {
		req.Args = append(req.Args, "--containment", "hk:1")
	}
	req.Repository.Worktree = "project." + job
	req.Repository.Checkout.Dir = req.Repository.Worktree
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, "parent", claimedAt)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func succeedOn(t *testing.T, h *HubServer, task hubSpawnTask, profile string) {
	t.Helper()
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Profile: profile}, time.Now()); err != nil || !changed {
		t.Fatalf("complete %s: %v %v", task.Job, changed, err)
	}
}

func nodeListsActive(h *HubServer, jobs ...string) {
	record := &hubNodeRecord{machineID: "machine-a", state: "connected", activeJobs: map[string]HubActiveJob{}}
	for _, job := range jobs {
		record.activeJobs[job] = HubActiveJob{JobID: job, AgentLabel: job, Epoch: 1}
	}
	h.mu.Lock()
	h.nodes["machine-a"] = record
	h.mu.Unlock()
}

func liveProfilesOf(t *testing.T, h *HubServer, except string) map[string]int {
	t.Helper()
	got, err := h.liveSpawnProfiles(t.Context(), except)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// (c) hub side: a succeeded spawn counts while a node lists its job and for a
// short settle window; a completed or lost job (no node lists it) does not.
func TestLiveSpawnProfilesSkipsEndedJobs(t *testing.T) {
	h := asyncSpawnHub(t)
	long := time.Now().Add(-3 * time.Hour)
	running := liveOp(t, h, 1, "running", "auto", long)
	succeedOn(t, h, running, "omp-sol")
	ended := liveOp(t, h, 2, "ended", "auto", long)
	succeedOn(t, h, ended, "omp-sol")
	nodeListsActive(h, "running")
	if got := liveProfilesOf(t, h, ""); got["omp-sol"] != 1 || len(got) != 1 {
		t.Fatalf("live = %v, want only the job a node still lists", got)
	}

	// Just landed, not in a heartbeat yet: still counted.
	fresh := liveOp(t, h, 3, "fresh", "auto", time.Now())
	succeedOn(t, h, fresh, "omp-sol")
	if got := liveProfilesOf(t, h, ""); got["omp-sol"] != 2 {
		t.Fatalf("live = %v, want a just-landed spawn counted", got)
	}
}

func TestLiveSpawnProfilesCountsAcceptedAndDispatchingByPlacedProfile(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	liveOp(t, h, 1, "named", "omp-sol", now)
	liveOp(t, h, 2, "unplaced", "auto", now)
	bound := liveOp(t, h, 3, "bound", "auto", now)
	node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-0000000000b3", Machine: "machine-a", Args: []string{"-m", "builder-sol"}}
	if _, err := h.inbox.store.bindSpawn(t.Context(), bound.Request.RequestID, bound.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	got := liveProfilesOf(t, h, "")
	if got["omp-sol"] != 1 || got["builder-sol"] != 1 || len(got) != 2 {
		t.Fatalf("live = %v, want the named and the bound spawn, not the unplaced one", got)
	}
}

func TestLiveSpawnProfilesLeavesOutFinishedAndExceptedOperations(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	failed := liveOp(t, h, 1, "failed", "omp-sol", now)
	if _, err := h.inbox.store.completeSpawn(t.Context(), failed, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "refused"}, now); err != nil {
		t.Fatal(err)
	}
	self := liveOp(t, h, 2, "self", "omp-sol", now)
	liveOp(t, h, 3, "other", "omp-sol", now)
	if got := liveProfilesOf(t, h, ""); got["omp-sol"] != 2 {
		t.Fatalf("live = %v, want the failed spawn out", got)
	}
	if got := liveProfilesOf(t, h, self.Request.RequestID); got["omp-sol"] != 1 {
		t.Fatalf("live = %v, want the spawn being placed out", got)
	}
}

func TestSpawnLiveEndpointServesOperatorOnly(t *testing.T) {
	h := asyncSpawnHub(t)
	liveOp(t, h, 1, "one", "omp-sol", time.Now())
	liveOp(t, h, 2, "two", "omp-sol", time.Now())
	get := func(path, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	if w := get("/v1/spawns/live", "node-token"); w.Code != http.StatusUnauthorized {
		t.Fatalf("node read the live count: %d", w.Code)
	}
	var body struct {
		Profiles map[string]int `json:"profiles"`
	}
	w := get("/v1/spawns/live", t14OperatorToken)
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Profiles["omp-sol"] != 2 {
		t.Fatalf("live = %d %s", w.Code, w.Body)
	}
	w = get("/v1/spawns/live?except=d676220b-e763-4af1-aefd-000000000001", t14OperatorToken)
	if json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Profiles["omp-sol"] != 1 {
		t.Fatalf("except ignored: %s", w.Body)
	}
}

// The hub's spawn App reads the count from the hub's own operations, never
// over HTTP.
func TestHubSpawnAppCountsFromOwnOperations(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLEET_HOSTS_CONFIG", "")
	t.Setenv("WRK_HOSTS_CONFIG", "")
	t.Setenv("PANEWIRE_POLICY", "")
	h := asyncSpawnHub(t)
	h.spawnNewApp = nil
	liveOp(t, h, 1, "one", "omp-sol", time.Now())
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if app.LiveSpawnProfiles == nil {
		t.Fatal("the hub's spawn app has no live count")
	}
	got, err := app.LiveSpawnProfiles(t.Context(), "")
	if err != nil || got["omp-sol"] != 1 {
		t.Fatalf("live = %v %v", got, err)
	}
}
