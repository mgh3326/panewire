package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// retryableStoredHostTask is a stored operation whose first attempt failed on
// desktop and was resumed as a second attempt, the state the hub's
// verified-cleanup retry rewrites.
func retryableStoredHostTask(t *testing.T, h *HubServer, request fleet.Submission) hubSpawnTask {
	t.Helper()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	failed := hubSpawnOutcome{RC: 124, Stage: "admission", Reason: "timeout", Host: "desktop", Profile: "codex-luna", Retryable: true}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, failed, time.Now()); err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := h.inbox.store.resumeCleanedSpawn(t.Context(), saved, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return retried
}

// retryQuota admits one sonnet seat on pool "p" for every request.
type retryQuota struct{}

func (retryQuota) Launch(context.Context, fleet.QuotaRequest) (fleet.Launch, error) {
	return fleet.Launch{Profile: "sonnet", ModelID: "m", Effort: "high", Grade: "B", Gate: "default"}, nil
}
func (retryQuota) Gate(context.Context, fleet.QuotaRequest) (string, error) { return "pool=p\n", nil }
func (retryQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "B", ModelID: "m", Pool: "p", Gate: "default"}}, nil
}
func (retryQuota) Pools(context.Context) (map[string]fleet.PoolView, error) {
	zero := 0.0
	return map[string]fleet.PoolView{"p": {Status: "ok", Class: "spend", Blocking: &zero}}, nil
}

type retrySource struct{ now time.Time }

func (retrySource) Hosts(context.Context, string) ([]fleet.HostView, error) { return nil, nil }
func (s retrySource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{{HostID: "desktop", AccountKey: "p:0000000000000001", LastSeen: s.now}}, nil
}
func (s retrySource) SharedAccount(_ context.Context, key string) (fleet.SharedAccount, error) {
	zero := 0.0
	return fleet.SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: s.now, Quota: &fleet.PoolView{Status: "ok", Class: "spend", Blocking: &zero}}, nil
}

// retryRunner runs nothing: the hub path's hk reads and brief upload go over
// HTTP to retryHK.
type retryRunner struct{}

func (retryRunner) Run(_ context.Context, c fleet.Command) fleet.Result {
	if c.Name == "handoffkeep" {
		return fleet.Result{Code: 1, Err: "the spawn path must not run the hk CLI: " + strings.Join(c.Args, " ")}
	}
	return fleet.Result{}
}

// retryHK is the svc-panewire hk the hub path reaches over HTTP: task 42 and
// the brief upload; anything else is not_found.
func retryHK(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/42":
			io.WriteString(w, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			io.WriteString(w, `{"document":{},"changed":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/placements":
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error":"unavailable"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":"not_found"}`)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "svc-panewire-retry-token")
}

// retryDispatchApp is an App whose auto placement can select a seat, so a
// replay runs through to the hub transport.
func retryDispatchApp(t *testing.T, out, errout io.Writer) *fleet.App {
	t.Helper()
	policy, err := fleet.LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	clock := time.Unix(1_800_000_000, 0)
	return &fleet.App{
		Policy: policy, Runner: retryRunner{}, Quota: retryQuota{}, Source: retrySource{now: clock},
		Config:   fleet.Config{Hosts: map[string]map[string]any{"desktop": {}, "machine-a": {}}, HostOrder: []string{"desktop", "machine-a"}},
		JobStore: spawnJobsFixture{}, Home: root, Jobs: filepath.Join(root, "jobs"), Out: out, Err: errout,
		Now:   func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) error { clock = clock.Add(d); return nil },
	}
}

func TestHubSpawnRetryReplayIsNotAPlacementPin(t *testing.T) {
	// The spawn writes fleet events under the state home; keep them out of the tree.
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	// A second connected node the retry must not choose: only the stored
	// outcome's host (desktop) may receive the job.
	t14NamedNode(t, h, server, "machine-a", true, true)
	node := t14NamedNode(t, h, server, "desktop", true, true)
	retryHK(t)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) { return retryDispatchApp(t, out, errout), nil }
	request := durableSpawnRequest()
	task := retryableStoredHostTask(t, h, request)
	if task.Outcome.Host != "desktop" || !task.Outcome.Retryable {
		t.Fatalf("fixture lost the stored outcome: %+v", task.Outcome)
	}
	h.startSpawnCoordinator(t.Context())
	sent := t14ReadNodeSpawn(t, node)
	// The node is the stored outcome's host; the rewrite keeps the override
	// reason and the logical request key.
	if sent.WaitSeconds == 0 || hubSpawnArg(sent.Args, "--override-reason") != "verified-cleanup-retry" || hubSpawnArg(sent.Args, "--job") != "test-job" {
		t.Fatalf("dispatched args: %v", sent.Args)
	}
	t14WriteSpawnResult(t, node, sent.RequestID, 1, "test-job", "", "", "spawn_disabled")
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Attempt != 2 || saved.Node == nil || saved.Node.Machine != "desktop" || saved.Node.RequestID != sent.RequestID {
		t.Fatalf("the retry was not bound to the stored host's dispatch: %+v %v", saved, err)
	}
	if strings.Contains(saved.Outcome.Reason, "containment_required") {
		t.Fatalf("the hub's own retry was refused as an operator pin: %+v", saved.Outcome)
	}
	// Same request, same idempotency key: the retry rewrites an in-memory copy.
	raw, err := json.Marshal(saved.Request)
	if err != nil || saved.Request.RequestID != request.RequestID || !strings.Contains(string(raw), `"--host","auto"`) || strings.Contains(string(raw), `"--host","desktop"`) {
		t.Fatalf("the stored request changed: %s %v", raw, err)
	}
}

func TestHubSpawnSubmitterPinStaysRefused(t *testing.T) {
	h := asyncSpawnHub(t)
	submit := func(request fleet.Submission, body func(map[string]any)) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(request)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		if body != nil {
			body(fields)
		}
		raw, _ = json.Marshal(fields)
		r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	pinned := durableSpawnRequest()
	pinned.Args = append([]string(nil), pinned.Args...)
	for i := range pinned.Args {
		if pinned.Args[i] == "--host" {
			pinned.Args[i+1] = "desktop"
		}
	}
	if _, err := fleet.ValidateSubmission(pinned); err == nil || !strings.Contains(err.Error(), "containment_required") {
		t.Fatalf("submitter pin: %v", err)
	}
	for name, body := range map[string]func(map[string]any){
		"args only":       nil,
		"RetryHost field": func(f map[string]any) { f["RetryHost"] = "desktop" },
		"retry_host key":  func(f map[string]any) { f["retry_host"] = "desktop" },
	} {
		if w := submit(pinned, body); w.Code != 400 {
			t.Fatalf("%s: pin accepted through the hub API: %d %s", name, w.Code, w.Body)
		}
		if _, err := h.inbox.store.spawnTask(t.Context(), pinned.RequestID); err == nil {
			t.Fatalf("%s: a refused pin was stored", name)
		}
	}
}

func TestHubSpawnMarkerIsNotReachableFromArgs(t *testing.T) {
	for _, operator := range []bool{false, true} {
		for _, args := range [][]string{
			{"--retry-host", "desktop"},
			{"--hub-retry-host", "desktop"},
			{"--retry-host", "desktop", "--host", "desktop"},
		} {
			if validHubSpawnArgsForMode(args, operator) {
				t.Fatalf("operator=%v: the allowlist accepts %v", operator, args)
			}
		}
		if !operator && validHubSpawnArgs([]string{"--host", "desktop"}) {
			t.Fatal("a non-operator submission may pin a host")
		}
	}
}
