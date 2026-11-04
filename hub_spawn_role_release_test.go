package panewire

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/mgh3326/panewire/internal/fleet"
)

// roleJobs is an hk fake that remembers every state write.
type roleJobs struct {
	mu      sync.Mutex
	records map[string]map[string]any
}

func (r *roleJobs) Claim(context.Context, string, string, string) error { return nil }
func (r *roleJobs) Put(_ context.Context, id string, fields map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, value := range fields {
		r.records[id][key] = value
	}
	return nil
}
func (r *roleJobs) Get(_ context.Context, id string) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if record, ok := r.records[id]; ok {
		return map[string]any{"state": record["state"]}, nil
	}
	return nil, fleet.ErrJobNotFound
}
func (r *roleJobs) state(id string) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.records[id]["state"]
}

const roleHeldPane = "w2:p3"

// roleReleaseHub holds a failed operation (rc70, pane cleanup unverified) for
// task 42 role worker; its hk job is still claimed.
func roleReleaseHub(t *testing.T) (*HubServer, *httptest.Server, *roleJobs, hubSpawnTask) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{"test-job": {"state": "claimed"}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}, time.Now()); err != nil {
		t.Fatal(err)
	}
	held, err = hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID)
	if err != nil || held.Status != "failed" {
		t.Fatalf("holder %+v %v", held, err)
	}
	return hub, server, jobs, held
}

// roleSecondRequest is the same task and role under a new job id.
func roleSecondRequest() fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = "d676220b-e763-4af1-aefd-000000000002"
	request.Args = append([]string(nil), request.Args...)
	for i, arg := range request.Args {
		if arg == "test-job" {
			request.Args[i] = "test-job-r2"
		}
	}
	return request
}

func roleReplyPanes(t *testing.T, node *websocket.Conn, command map[string]json.RawMessage, panes []nodeAgentRow, panesOK bool) {
	t.Helper()
	result := map[string]any{"agents": []nodeAgentRow{}, "panes_ok": panesOK}
	if panesOK {
		result["panes"] = panes
	}
	nodeCommandReplyTo(t, node, command, map[string]any{"result": result})
}

func TestRoleReleaseAfterFailedSpawn(t *testing.T) {
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("pane absent releases and marks the prior job lost", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		command := nodeCommandRead(t, node)
		if string(command["op"]) != `"agents.list"` {
			t.Fatalf("probe=%s", command["op"])
		}
		roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		result := t14Result(t, response)
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "lost" {
			t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
		}
		owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42", "worker")
		if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
		}
	})
	t.Run("pane present keeps the reservation and names the holder", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		command := nodeCommandRead(t, node)
		roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: roleHeldPane, Cwd: "/work/project.test-job"}}, true)
		result := t14Result(t, response)
		var body map[string]string
		if err := json.Unmarshal(result.body, &body); err != nil {
			t.Fatal(err)
		}
		if result.status != http.StatusConflict || body["request_id"] != held.Request.RequestID || body["job"] != "test-job" || body["host"] != "machine-a" || body["pane"] != roleHeldPane || !strings.Contains(body["fix"], "close pane "+roleHeldPane) {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "claimed" {
			t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("herdr cannot list panes", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		roleReplyPanes(t, node, nodeCommandRead(t, node), nil, false)
		if result := t14Result(t, response); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "claimed" {
			t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("node unreachable releases nothing", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		result := t14Result(t, submit(t, server))
		var body map[string]string
		_ = json.Unmarshal(result.body, &body)
		if result.status != http.StatusConflict || body["pane"] != roleHeldPane || !strings.Contains(body["fix"], "close pane "+roleHeldPane) || !strings.Contains(body["fix"], "machine-a") {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "claimed" {
			t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
	t.Run("node answers with an error", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		nodeCommandReplyTo(t, node, nodeCommandRead(t, node), map[string]any{"error": "herdr_failed"})
		if result := t14Result(t, response); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if jobs.state("test-job") != "claimed" {
			t.Fatalf("prior hk job state=%v", jobs.state("test-job"))
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
}

func assertRoleHeldBy(t *testing.T, hub *HubServer, request string) {
	t.Helper()
	owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42", "worker")
	if err != nil || owner != request {
		t.Fatalf("reservation moved to %s (%v)", owner, err)
	}
}
