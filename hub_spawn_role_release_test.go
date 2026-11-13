package panewire

import (
	"context"
	"encoding/json"
	"errors"
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
	getErr  map[string]error // a job whose Get fails with the mapped error
	puts    []string         // "<job> <state>" for every Put, in order
	// completeAfterGet makes the holder complete right after its first Get,
	// the way a job finishing during the pane probe does.
	completeAfterGet bool
	putErr           error // when set, every Put fails with it
}

func (r *roleJobs) Claim(context.Context, string, string, string) error { return nil }
func (r *roleJobs) Put(_ context.Context, id string, fields map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.puts = append(r.puts, id+" "+scalarString(fields["state"]))
	if r.putErr != nil {
		return r.putErr
	}
	// hk never rewrites a terminal record.
	if state := r.records[id]["state"]; state == "completed" || state == "lost" || state == "escalated" {
		return errors.New("job_state_conflict")
	}
	for key, value := range fields {
		r.records[id][key] = value
	}
	return nil
}
func (r *roleJobs) Get(_ context.Context, id string) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.getErr[id]; err != nil {
		return nil, err
	}
	if record, ok := r.records[id]; ok {
		out := map[string]any{"state": record["state"]}
		if r.completeAfterGet && id == "test-job" {
			record["state"] = "completed"
		}
		return out, nil
	}
	return nil, fleet.ErrJobNotFound
}
func scalarString(v any) string {
	s, _ := v.(string)
	return s
}
func (r *roleJobs) writes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.puts...)
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
	return roleReleaseHubFor(t, hubSpawnOutcome{RC: 70, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane})
}

// roleReleaseHubFor is roleReleaseHub with the holder's recorded outcome chosen.
func roleReleaseHubFor(t *testing.T, outcome hubSpawnOutcome) (*HubServer, *httptest.Server, *roleJobs, hubSpawnTask) {
	return roleHolderHub(t, outcome, "claimed", "failed")
}

// roleHolderHub holds task 42 role worker with an operation that ended in
// wantStatus with outcome, and an hk job in hkState.
func roleHolderHub(t *testing.T, outcome hubSpawnOutcome, hkState, wantStatus string) (*HubServer, *httptest.Server, *roleJobs, hubSpawnTask) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{"test-job": {"state": hkState}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout, Now: time.Now}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, outcome, time.Now()); err != nil {
		t.Fatal(err)
	}
	held, err = hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID)
	if err != nil || held.Status != wantStatus {
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

// roleReplyMarkLost reads the hub's job.mark_lost command and answers it.
func roleReplyMarkLost(t *testing.T, node *websocket.Conn, outcome string) map[string]json.RawMessage {
	t.Helper()
	command := nodeCommandRead(t, node)
	if string(command["op"]) != `"job.mark_lost"` {
		t.Fatalf("mark lost op=%s", command["op"])
	}
	nodeCommandReplyTo(t, node, command, map[string]any{"result": map[string]any{"outcome": outcome}})
	return command
}

// roleMarkLostArgs decodes the arguments of a job.mark_lost command.
func roleMarkLostArgs(t *testing.T, command map[string]json.RawMessage) map[string]any {
	t.Helper()
	var args map[string]any
	if err := json.Unmarshal(command["args"], &args); err != nil {
		t.Fatal(err)
	}
	return args
}

func TestRoleReleaseAfterFailedSpawn(t *testing.T) {
	submit := func(t *testing.T, server *httptest.Server) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	}
	t.Run("pane absent releases and the holder node marks the prior job lost", func(t *testing.T) {
		hub, server, jobs, held := roleReleaseHub(t)
		node := t14Node(t, hub, server, true)
		response := submit(t, server)
		command := nodeCommandRead(t, node)
		if string(command["op"]) != `"agents.list"` {
			t.Fatalf("probe=%s", command["op"])
		}
		roleReplyPanes(t, node, command, []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
		mark := roleReplyMarkLost(t, node, nodeJobMarkLostLost)
		args := roleMarkLostArgs(t, mark)
		if args["job"] != "test-job" || args["pane_id"] != roleHeldPane || args["label"] != "test-job" {
			t.Fatalf("mark lost args=%v", args)
		}
		result := t14Result(t, response)
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		// The hub's client never writes another host's job record.
		if len(jobs.writes()) != 0 || jobs.state("test-job") != "claimed" {
			t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
		}
		owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
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
	owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || owner != request {
		t.Fatalf("reservation moved to %s (%v)", owner, err)
	}
}

// A spawn refused before any pane existed (checkout_refused at
// worktree.prepare) recorded no pane and never claimed its hk job, so nothing
// can hold the role: a new submission takes it at once, without probing a
// node. Before this, the reservation was held for good (rc 74
// job_already_claimed on every retry).
func TestRoleReleaseAfterPrePaneRefusal(t *testing.T) {
	setup := func(t *testing.T, jobRecord bool) (*HubServer, *httptest.Server, hubSpawnTask) {
		t.Helper()
		t.Setenv(crashReportDSNEnv, "")
		hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { hub.Close() })
		jobs := &roleJobs{records: map[string]map[string]any{}}
		if jobRecord {
			jobs.records["test-job"] = map[string]any{"state": "claimed"}
		}
		hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
			return &fleet.App{JobStore: jobs, Out: out, Err: errout, Now: time.Now}, nil
		}
		server := httptest.NewServer(hub.Handler())
		t.Cleanup(server.Close)
		held, _, err := hub.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 81, Stage: "placement", Reason: "checkout_refused:branch-elsewhere", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		held, err = hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID)
		if err != nil || held.Status != "failed" || held.Outcome.Pane != "" {
			t.Fatalf("holder %+v %v", held, err)
		}
		return hub, server, held
	}
	t.Run("no pane and no hk job releases at once", func(t *testing.T) {
		hub, server, held := setup(t, false)
		result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest()))
		if result.status != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
		if err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
			t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
		}
	})
	t.Run("no pane but a claimed hk job keeps the reservation", func(t *testing.T) {
		hub, server, held := setup(t, true)
		if result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())); result.status != http.StatusConflict {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		assertRoleHeldBy(t, hub, held.Request.RequestID)
	})
}
