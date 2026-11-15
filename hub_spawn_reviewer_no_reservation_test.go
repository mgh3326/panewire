package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reviewPassRequest is a reviewer spawn for task 42 under its own request id
// and job. pass "" sends no --review-pass.
func reviewPassRequest(n int, job, pass string) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = "d676220b-e763-4af1-aefd-00000000010" + string(rune('0'+n))
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", "reviewer", "--host", "auto"}
	if pass != "" {
		request.Args = append(request.Args, "--review-pass", pass)
	}
	return request
}

func reviewPassHubAt(t *testing.T, path string, hk map[string]string) (*HubServer, *httptest.Server, *roleJobs) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{}}
	for job, state := range hk {
		jobs.records[job] = map[string]any{"state": state}
	}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout, Now: time.Now}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	return hub, server, jobs
}

func reviewPassHub(t *testing.T, hk map[string]string) (*HubServer, *httptest.Server, *roleJobs) {
	return reviewPassHubAt(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk)
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

// A reviewer holds no (task, role, pass) reservation (hk 4737), so two
// submissions for the same task and pass under different job ids both place.
func TestReviewerSubmissionsShareTaskAndPass(t *testing.T) {
	hub, server, jobs := reviewPassHub(t, nil)
	p1 := reviewPassSubmit(t, server, reviewPassRequest(1, "rev-p1", "1"))
	p1again := reviewPassSubmit(t, server, reviewPassRequest(2, "rev-p1-r2", "1"))
	plain := reviewPassSubmit(t, server, reviewPassRequest(3, "rev-plain", ""))
	plainAgain := reviewPassSubmit(t, server, reviewPassRequest(4, "rev-plain-r2", ""))
	for name, result := range map[string]t14HTTPResult{"p1": p1, "p1 again": p1again, "plain": plain, "plain again": plainAgain} {
		if result.status != http.StatusAccepted {
			t.Fatalf("%s: status=%d body=%s", name, result.status, result.body)
		}
	}
	if len(jobs.writes()) != 0 {
		t.Fatalf("accepted reviewers touched hk: %v", jobs.writes())
	}
	for _, key := range []string{"42:reviewer:p1", "42:reviewer"} {
		if owner := reviewPassOwner(t, hub, key); owner != "" {
			t.Fatalf("%s held by %s, want no reservation", key, owner)
		}
	}
}

// A replay of one request id is matched by that id and returns the same
// operation, not a second job.
func TestReviewerReplayReturnsTheSameOperation(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	request := reviewPassRequest(1, "rev-p1", "1")
	first := reviewPassSubmit(t, server, request)
	again := reviewPassSubmit(t, server, request)
	if first.status != http.StatusAccepted || again.status != http.StatusAccepted {
		t.Fatalf("first=%d %s again=%d %s", first.status, first.body, again.status, again.body)
	}
	var firstBody, againBody map[string]string
	if err := json.Unmarshal(first.body, &firstBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(again.body, &againBody); err != nil {
		t.Fatal(err)
	}
	if firstBody["request_id"] != request.RequestID || againBody["request_id"] != request.RequestID || firstBody["job_id"] != againBody["job_id"] {
		t.Fatalf("replay did not return the same operation: %s then %s", first.body, again.body)
	}
	if owner := reviewPassOwner(t, hub, "42:reviewer:p1"); owner != "" {
		t.Fatalf("replay took a reservation: %s", owner)
	}
}

// An implementer still takes its one-holder (task, role) slot.
func TestImplementerStillHoldsItsReservation(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	if result := reviewPassSubmit(t, server, durableSpawnRequest()); result.status != http.StatusAccepted {
		t.Fatalf("first implementer: status=%d body=%s", result.status, result.body)
	}
	second := reviewPassSubmit(t, server, roleSecondRequest())
	var body map[string]string
	_ = json.Unmarshal(second.body, &body)
	if second.status != http.StatusConflict || body["error"] != "task_role_claimed" {
		t.Fatalf("second implementer: status=%d body=%s", second.status, second.body)
	}
	if owner := reviewPassOwner(t, hub, "42:implementer"); owner != durableSpawnRequest().RequestID {
		t.Fatalf("implementer slot held by %q", owner)
	}
}

// A reviewer no longer holds a slot, so a new reviewer submission must not
// close the hk record of a retryable reviewer that failed pre-landing: that
// record is not displaced by a reservation (hk 4737).
func TestReviewerSubmissionDoesNotSupersedeARetryableReviewer(t *testing.T) {
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
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if result := reviewPassSubmit(t, server, reviewPassRequest(2, "rev-p2", "1")); result.status != http.StatusAccepted {
		t.Fatalf("second reviewer: status=%d body=%s", result.status, result.body)
	}
	if writes := jobs.writes(); len(writes) != 0 {
		t.Fatalf("a reviewer submission wrote %v", writes)
	}
}

// Startup clears a reviewer reservation an older hub wrote and leaves an
// implementer's alone. It is idempotent: a second start clears nothing.
func TestStartupClearsReviewerReservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	hub, _, _ := reviewPassHubAt(t, path, nil)
	store := hub.inbox.store
	ctx := t.Context()
	for _, row := range []struct{ request, job, key string }{
		{"req-rev", "job-rev", "42:reviewer:p2"},
		{"req-imp", "job-imp", "42:implementer"},
	} {
		if _, err := store.db.ExecContext(ctx, store.q(`INSERT INTO spawn_operations(request_id,job,owner,fingerprint,payload,status,created_ms,deadline_ms) VALUES(?,?,?,?,?,?,?,?)`), row.request, row.job, "parent", "fp", "{}", "succeeded", 1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, store.q(`INSERT INTO spawn_reservations(job,request_id,task_role) VALUES(?,?,?)`), row.job, row.request, row.key); err != nil {
			t.Fatal(err)
		}
	}
	hub.Close()

	for start := 1; start <= 2; start++ {
		reopened, _, _ := reviewPassHubAt(t, path, nil)
		if owner := reviewPassOwner(t, reopened, "42:reviewer:p2"); owner != "" {
			t.Fatalf("start %d: reviewer slot held by %q", start, owner)
		}
		owner, job, _, err := reopened.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
		if err != nil || owner != "req-imp" || job != "job-imp" {
			t.Fatalf("start %d: implementer slot %q/%q (%v)", start, owner, job, err)
		}
		reopened.Close()
	}
}
