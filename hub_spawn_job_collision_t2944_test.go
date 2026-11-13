package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A second request for the same job id and the same (task, role) is a job-id
// duplicate, and the hub must say so. It can arrive while the first is accepted
// and its hk job is not written yet, so no hk record refuses it earlier; the
// role hold it also collides with must not turn the answer into
// task_role_claimed (hk 2944 review).
func TestJobIDCollisionAmongRoleHoldAnswersJobAlreadyClaimed(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout, Now: time.Now}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	// The holder is accepted: it holds test-job and the 42:worker role, and
	// its hk job is not written yet.
	if _, _, err := hub.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	// Same job id and role, a new request id.
	dup := roleSecondRequest()
	for i, arg := range dup.Args {
		if arg == "test-job-r2" {
			dup.Args[i] = "test-job"
		}
	}
	result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, dup))
	var body map[string]string
	if err := json.Unmarshal(result.body, &body); err != nil {
		t.Fatal(err)
	}
	if result.status != http.StatusConflict || body["error"] != "job_already_claimed" {
		t.Fatalf("status=%d body=%s, want 409 job_already_claimed", result.status, result.body)
	}
}
