package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
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

// nodeReportsJob adds one active job to a node's presence record, the way the
// node's next heartbeat's active_jobs does.
func nodeReportsJob(h *HubServer, machine string, job HubActiveJob) {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	if record == nil {
		record = &hubNodeRecord{machineID: machine, state: "connected", activeJobs: map[string]HubActiveJob{}}
		h.nodes[machine] = record
	}
	if record.activeJobs == nil {
		record.activeJobs = map[string]HubActiveJob{}
	}
	record.activeJobs[job.JobID] = job
}

// hk 3399: a direct `panewire spawn` on a node is a live job on its profile
// even though the hub never saw the request. The node's heartbeat names the
// job and the profile its own job.spawned receipt recorded.
func TestLiveSpawnProfilesCountsDirectJobsOnNodes(t *testing.T) {
	h := asyncSpawnHub(t)
	liveOp(t, h, 1, "hub-job", "omp-sol", time.Now())
	nodeReportsJob(h, "machine-a", HubActiveJob{JobID: "direct-job", AgentLabel: "direct-job", Epoch: 1, Profile: "omp-sol"})
	nodeReportsJob(h, "machine-a", HubActiveJob{JobID: "direct-free", AgentLabel: "direct-free", Epoch: 1, Profile: "devin-swe2"})
	got := liveProfilesOf(t, h, "")
	if got["omp-sol"] != 2 || got["devin-swe2"] != 1 || len(got) != 2 {
		t.Fatalf("live = %v, want the hub spawn and both direct jobs", got)
	}
}

// A hub spawn that has become a node job is one live job: its operation counts
// it, and the node's entry for the same job id is skipped.
func TestLiveSpawnProfilesCountsALandedHubSpawnOnce(t *testing.T) {
	h := asyncSpawnHub(t)
	task := liveOp(t, h, 1, "landed", "auto", time.Now())
	succeedOn(t, h, task, "omp-sol")
	nodeReportsJob(h, "machine-a", HubActiveJob{JobID: task.Job, AgentLabel: task.Job, Epoch: 1, Profile: "omp-sol"})
	if got := liveProfilesOf(t, h, ""); got["omp-sol"] != 1 || len(got) != 1 {
		t.Fatalf("live = %v, want one live job for the landed spawn", got)
	}
	if got := liveProfilesOf(t, h, task.Request.RequestID); len(got) != 0 {
		t.Fatalf("live = %v, want the spawn being placed out, its node entry included", got)
	}
}

// The node's own scan reads the profile from the job's job.spawned receipt and
// puts it on the presence entry the hub counts.
func TestScanActiveJobsCarriesTheSpawnedProfile(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	writeR20T6Job(t, root, "job-placed", "w1:p1", now)
	writeR20T6Job(t, root, "job-claimed", "", now)
	jobs := scanHubActiveJobsWithPanes(context.Background(), root, nil)
	profiles := map[string]string{}
	for _, job := range jobs {
		profiles[job.JobID] = job.Profile
	}
	if profiles["job-placed"] != "profile-a" || profiles["job-claimed"] != "" || len(profiles) != 2 {
		t.Fatalf("scanned profiles = %v, want job-placed on profile-a and job-claimed with none", profiles)
	}
}

// The heartbeat accepts the profile on an active job, and an unusable one is
// dropped rather than refusing the whole heartbeat.
func TestDecodeHeartbeatCarriesActiveJobProfile(t *testing.T) {
	withProfile := []byte(`{"status":"alive","active_jobs":[{"job_id":"job-a","agent_label":"worker-a","epoch":1,"profile":"omp-sol"}]}`)
	heartbeat, ok := decodeHubHeartbeatPayload(withProfile)
	if !ok || len(heartbeat.ActiveJobs) != 1 || heartbeat.ActiveJobs[0].Profile != "omp-sol" {
		t.Fatalf("decode = %v %+v, want the profile kept", ok, heartbeat.ActiveJobs)
	}
	invalid := []byte(`{"status":"alive","active_jobs":[{"job_id":"job-b","agent_label":"worker-b","epoch":1,"profile":"bad profile"}]}`)
	heartbeat, ok = decodeHubHeartbeatPayload(invalid)
	if !ok || len(heartbeat.ActiveJobs) != 1 || heartbeat.ActiveJobs[0].Profile != "" {
		t.Fatalf("decode = %v %+v, want an unusable profile dropped", ok, heartbeat.ActiveJobs)
	}
}

// capPlacementQuota is a placement whose catalog is one row on pool p, so a
// pool cap drop is the only way the submitted spawn cannot leave placement.
type capPlacementQuota struct {
	row    fleet.CatalogRow
	launch fleet.Launch
}

func (q capPlacementQuota) Launch(context.Context, fleet.QuotaRequest) (fleet.Launch, error) {
	return q.launch, nil
}
func (capPlacementQuota) Gate(context.Context, fleet.QuotaRequest) (string, error) {
	return "pool=p\n", nil
}
func (q capPlacementQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{q.row}, nil
}
func (capPlacementQuota) Pools(context.Context) (map[string]fleet.PoolView, error) {
	zero := 0.0
	return map[string]fleet.PoolView{"p": {Status: "ok", Class: "spend", Blocking: &zero}}, nil
}

// hk 3399, closed contract: a direct job on pool P plus one hub spawn on P with
// cap 2 refuses the third. The direct job exists only in the node's presence
// list; the hub spawn is a bound operation. The reserved devin-free key reads
// the same count, so one direct free rung fills it.
func TestDirectNodeJobCountsTowardPoolCap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		direct   string // the direct job's placed profile
		row      fleet.CatalogRow
		launch   fleet.Launch
		capKey   string
		capLimit int64
		hubSpawn bool // one bound hub spawn on the same profile
	}{
		{
			name:     "pool cap 2: one hub spawn plus one direct job",
			direct:   "sonnet",
			row:      fleet.CatalogRow{Profile: "sonnet", Effort: "high", Grade: "B", ModelID: "m", Pool: "p", Gate: "default"},
			launch:   fleet.Launch{Profile: "sonnet", ModelID: "m", Effort: "high", Grade: "B", Gate: "default"},
			capKey:   "p",
			capLimit: 2,
			hubSpawn: true,
		},
		{
			name:     "devin-free 1: one direct free rung fills the reserved key",
			direct:   "devin-swe2",
			row:      fleet.CatalogRow{Profile: "devin-swe2", Effort: "high", Grade: "B", ModelID: "swe-2", Pool: "p", Gate: "default"},
			launch:   fleet.Launch{Profile: "devin-swe2", ModelID: "swe-2", Effort: "high", Grade: "B", Gate: "default"},
			capKey:   "devin-free",
			capLimit: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			retryHK(t)
			h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
				app := retryDispatchApp(t, out, errout)
				app.Quota = capPlacementQuota{row: tc.row, launch: tc.launch}
				app.LiveSpawnProfiles = h.liveSpawnProfiles
				app.Config.Quota = map[string]any{"pool_max_concurrent": map[string]any{tc.capKey: tc.capLimit}}
				return app, nil
			}
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			busy := t14NamedNode(t, h, server, "machine-a", true, true)
			t14NamedNode(t, h, server, "desktop", true, true)
			var bound hubSpawnTask
			if tc.hubSpawn {
				bound = boundDispatchFixture(t, h, 700, "machine-a")
			}
			nodeReportsJob(h, "machine-a", HubActiveJob{JobID: "direct-job", AgentLabel: "direct-job", Epoch: 1, Profile: tc.direct})
			h.startSpawnCoordinator(t.Context())
			if tc.hubSpawn {
				if message := t14ReadNodeSpawn(t, busy); message.RequestID != bound.Node.RequestID {
					t.Fatalf("first dispatch = %s, want the bound fixture %s", message.RequestID, bound.Node.RequestID)
				}
			}
			request := durableSpawnRequest()
			status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
			if status != http.StatusAccepted {
				t.Fatalf("submission rejected before placement: status=%d body=%s", status, body)
			}
			var saved hubSpawnTask
			eventually(t, 3*time.Second, "the third spawn parked at "+tc.capKey, func() bool {
				var err error
				saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
				return err == nil && saved.ParkReason != ""
			})
			if saved.Status != "accepted" || saved.ParkReason != spawnParkPoolCap || saved.Node != nil {
				t.Fatalf("third spawn was not parked pool_cap: status=%s park=%q node=%+v", saved.Status, saved.ParkReason, saved.Node)
			}
		})
	}
}
