package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reviewPassRequest is a reviewer spawn for task 42 under its own request id
// and job. pass "" sends no --review-pass, the shape every reviewer had before
// the flag.
func reviewPassRequest(n int, job, pass string) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = "d676220b-e763-4af1-aefd-00000000010" + string(rune('0'+n))
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", "reviewer", "--host", "auto"}
	if pass != "" {
		request.Args = append(request.Args, "--review-pass", pass)
	}
	return request
}

func reviewPassHub(t *testing.T, hk map[string]string) (*HubServer, *httptest.Server, *roleJobs) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{}}
	for job, state := range hk {
		jobs.records[job] = map[string]any{"state": state}
	}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	return hub, server, jobs
}

func reviewPassSubmit(t *testing.T, server *httptest.Server, request fleet.Submission) t14HTTPResult {
	t.Helper()
	return t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request))
}

func reviewPassOwner(t *testing.T, hub *HubServer, key string) string {
	t.Helper()
	owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), key)
	if err != nil {
		return ""
	}
	return owner
}

// holdFinished stores a reviewer operation for task 42 that ended failed with
// a recorded pane, the holder a replacement has to prove finished.
func holdFinished(t *testing.T, hub *HubServer, request fleet.Submission) {
	t.Helper()
	job := hubSpawnArg(request.Args, "--job")
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// The reservation is per (task, role, review pass): p1 and p2 of one task
// hold at once, a second p1 is refused with the 409 every held role gets, and
// a reviewer without the flag keeps the "task:role" key.
func TestReviewPassReservationsAreIndependent(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	p1 := reviewPassSubmit(t, server, reviewPassRequest(1, "rev-p1", "1"))
	p2 := reviewPassSubmit(t, server, reviewPassRequest(2, "rev-p2", "2"))
	if p1.status != http.StatusAccepted || p2.status != http.StatusAccepted {
		t.Fatalf("p1=%d %s p2=%d %s", p1.status, p1.body, p2.status, p2.body)
	}
	dup := reviewPassSubmit(t, server, reviewPassRequest(3, "rev-p1-again", "1"))
	var body map[string]string
	_ = json.Unmarshal(dup.body, &body)
	if dup.status != http.StatusConflict || body["error"] != "job_already_claimed" || body["request_id"] != reviewPassRequest(1, "", "").RequestID || body["job"] != "rev-p1" {
		t.Fatalf("second p1: status=%d body=%s", dup.status, dup.body)
	}
	plain := reviewPassSubmit(t, server, reviewPassRequest(4, "rev-plain", ""))
	if plain.status != http.StatusAccepted {
		t.Fatalf("reviewer without the flag: status=%d body=%s", plain.status, plain.body)
	}
	if again := reviewPassSubmit(t, server, reviewPassRequest(5, "rev-plain-again", "")); again.status != http.StatusConflict {
		t.Fatalf("second reviewer without the flag: status=%d body=%s", again.status, again.body)
	}
	for key, job := range map[string]string{"42:reviewer:p1": "rev-p1", "42:reviewer:p2": "rev-p2", "42:reviewer": "rev-plain"} {
		if _, got, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), key); err != nil || got != job {
			t.Fatalf("key %s held by %q (%v), want %s", key, got, err, job)
		}
	}
}

// A worker keeps "task:worker" and is unaffected by reviewer passes.
func TestReviewPassLeavesWorkerKeyUnchanged(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	if result := reviewPassSubmit(t, server, reviewPassRequest(1, "rev-p1", "1")); result.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if result := reviewPassSubmit(t, server, durableSpawnRequest()); result.status != http.StatusAccepted {
		t.Fatalf("worker status=%d body=%s", result.status, result.body)
	}
	if _, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job" {
		t.Fatalf("worker key held by %q (%v)", job, err)
	}
}

// Reclaim frees the pass it resubmits for and no other: p1's holder finished,
// p2's did not.
func TestReviewPassReclaimFreesOnlyItsPass(t *testing.T) {
	hub, server, _ := reviewPassHub(t, map[string]string{"rev-p1": "completed", "rev-p2": "claimed"})
	p1, p2 := reviewPassRequest(1, "rev-p1", "1"), reviewPassRequest(2, "rev-p2", "2")
	holdFinished(t, hub, p1)
	// p2's holder is still accepted: its operation has not finished.
	if _, _, err := hub.inbox.store.claimSpawn(t.Context(), p2, "rev-p2", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	if result := reviewPassSubmit(t, server, reviewPassRequest(3, "rev-p1-r2", "1")); result.status != http.StatusAccepted {
		t.Fatalf("p1 replacement: status=%d body=%s", result.status, result.body)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p1"); owner != reviewPassRequest(3, "", "").RequestID {
		t.Fatalf("p1 not moved to the replacement: %s", owner)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p2"); owner != p2.RequestID {
		t.Fatalf("p2 reservation disturbed: %s", owner)
	}
	if result := reviewPassSubmit(t, server, reviewPassRequest(4, "rev-p2-r2", "2")); result.status != http.StatusConflict {
		t.Fatalf("p2 replacement while p2 is live: status=%d body=%s", result.status, result.body)
	}
}

// Release drops one pass's reservation and leaves the other held.
func TestReviewPassReleaseFreesOnlyItsPass(t *testing.T) {
	hub, _, _ := reviewPassHub(t, nil)
	p1, p2 := reviewPassRequest(1, "rev-p1", "1"), reviewPassRequest(2, "rev-p2", "2")
	holdFinished(t, hub, p1)
	holdFinished(t, hub, p2)
	if err := hub.inbox.store.releaseCompletedSpawnRole(t.Context(), p1.RequestID, "42:reviewer:p1"); err != nil {
		t.Fatal(err)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p1"); owner != "" {
		t.Fatalf("p1 still held by %s", owner)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p2"); owner != p2.RequestID {
		t.Fatalf("p2 released with p1: %s", owner)
	}
	// A release that names the wrong pass frees nothing.
	if err := hub.inbox.store.releaseCompletedSpawnRole(t.Context(), p2.RequestID, "42:reviewer:p1"); err != nil {
		t.Fatal(err)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p2"); owner != p2.RequestID {
		t.Fatalf("p2 released by a p1 key: %s", owner)
	}
}

// A retryable holder is superseded only by a submission for its own pass.
func TestReviewPassSupersedeMatchesPass(t *testing.T) {
	retryable := hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}
	// The holder ran on the hub's own host, so the hub writes its lost record
	// itself (a remote holder's node writes it: hk 3235).
	env := filepath.Join(t.TempDir(), "hub-node.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=machine-a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_NODE_ENV", env)
	hub, server, jobs := reviewPassHub(t, map[string]string{"rev-p1": "claimed"})
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), reviewPassRequest(1, "rev-p1", "1"), "rev-p1", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, retryable, time.Now()); err != nil {
		t.Fatal(err)
	}
	if result := reviewPassSubmit(t, server, reviewPassRequest(2, "rev-p2", "2")); result.status != http.StatusAccepted || len(jobs.writes()) != 0 {
		t.Fatalf("p2 touched the p1 holder: status=%d writes=%v", result.status, jobs.writes())
	}
	if result := reviewPassSubmit(t, server, reviewPassRequest(3, "rev-p1-r2", "1")); result.status != http.StatusAccepted {
		t.Fatalf("p1 replacement: status=%d body=%s", result.status, result.body)
	}
	if !slices.Equal(jobs.writes(), []string{"rev-p1 lost"}) {
		t.Fatalf("writes=%v", jobs.writes())
	}
}

// A request that resumes after cleanup takes the same pass key it first held.
func TestReviewPassResumeKeepsItsKey(t *testing.T) {
	hub, _, _ := reviewPassHub(t, nil)
	request := reviewPassRequest(1, "rev-p1", "1")
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "rev-p1", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	held, err = hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || held.Status != "retryable" {
		t.Fatalf("holder %+v %v", held, err)
	}
	if reviewPassOwner(t, hub, "42:reviewer:p1") != "" {
		t.Fatal("a retryable operation still holds its pass")
	}
	if _, err = hub.inbox.store.resumeCleanedSpawn(t.Context(), held, time.Now()); err != nil {
		t.Fatal(err)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p1"); owner != request.RequestID {
		t.Fatalf("resume did not take the p1 key: %q", owner)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer"); owner != "" {
		t.Fatalf("resume took the plain reviewer key: %q", owner)
	}
}

// --review-pass crosses the hub allowlist for 1..9 and nothing else.
func TestHubSpawnAllowlistCarriesReviewPass(t *testing.T) {
	base := []string{"-l", "rev", "--job", "rev", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "reviewer"}
	for _, tc := range []struct {
		pass string
		want bool
	}{{"1", true}, {"9", true}, {"0", false}, {"10", false}, {"p1", false}, {"", false}} {
		if got := validHubSpawnArgs(append(slices.Clone(base), "--review-pass", tc.pass)); got != tc.want {
			t.Errorf("--review-pass %q accepted=%v, want %v", tc.pass, got, tc.want)
		}
	}
	if validHubSpawnArgs(append(slices.Clone(base), "--review-pass", "1", "--review-pass", "2")) {
		t.Error("a repeated --review-pass was accepted")
	}
}
