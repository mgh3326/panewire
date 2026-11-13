package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestAsyncSpawnUndispatchedTimeoutReleasesReservation(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, phase := range []string{"queued", "placement"} {
		t.Run(phase, func(t *testing.T) {
			h := asyncSpawnHub(t)
			request := durableSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if phase == "queued" {
				task.Deadline = time.Now().Add(-time.Second)
			} else {
				task.Deadline = time.Now().Add(10 * time.Millisecond)
				original := h.spawnNewApp
				h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
					time.Sleep(30 * time.Millisecond)
					return original(out, errout)
				}
			}
			h.executeSpawnTask(t.Context(), task)
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "retryable" || !saved.Outcome.Retryable || saved.Outcome.Reason != "timeout" || saved.Node != nil {
				t.Fatalf("undispatched timeout retained ownership: %+v %v", saved, err)
			}
			pending, err := h.inbox.store.pendingSpawns(t.Context(), time.Now())
			if err != nil || len(pending) != 0 {
				t.Fatalf("reselected: %+v %v", pending, err)
			}
			retried, err := h.inbox.store.resumeCleanedSpawn(t.Context(), saved, time.Now())
			if err != nil || retried.Attempt != 2 || retried.Request.RequestID != request.RequestID || !retried.Deadline.After(time.Now()) {
				t.Fatalf("same-key retry failed: %+v %v", retried, err)
			}
		})
	}
}

func TestSpawnBindingRefusesDifferentNodeRequest(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(988), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:busy", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	node.RequestID = t14RequestID(989)
	if _, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("new request silently reused old binding: %v", err)
	}
	transport := directSpawnTransport{h: h, task: task, bound: true}
	if _, err = transport.Spawn(t.Context(), fleet.HubSpawnRequest{RequestID: node.RequestID, Machine: node.Machine}); err == nil {
		t.Fatal("busy retry dispatched a second request within a completed attempt")
	}
}

func TestSpawnOperatorFlagsRequireAuthorizedDurableMessage(t *testing.T) {
	_, server := t14Hub(t)
	_, scopedServer := pw2Hub(t, map[string]string{"svc-machine": "spawn@node-a"}, "", nil)
	service := pw2Caller{"svc-machine", "svc-machine", pw2ServiceToken("svc-machine")}
	flags := [][]string{{"--task-hk-bypass"}, {"--job-dup-ok"}, {"--allow-escalation"}, {"--keep"}, {"--grade", "A"}, {"--role", "consult"}, {"--override-reason", "operator request"}, {"--operator-request", "request-a"}, {"--requested-by", "operator"}, {"--purpose", "inspect launch"}, {"-w", "worker pool"}}
	for _, args := range flags {
		body := t14SpawnBody(t14RequestID(985))
		body["args"] = args
		status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
		if status != http.StatusBadRequest {
			t.Fatalf("legacy endpoint admitted %v: %d", args, status)
		}
		body["machine"] = "node-a"
		serviceRaw, _ := json.Marshal(body)
		if status, _ := service.do(t, scopedServer, http.MethodPost, "/v1/spawn", string(serviceRaw)); status != http.StatusBadRequest {
			t.Fatalf("scoped service admitted %v: %d", args, status)
		}
		message := map[string]any{"type": "job.spawn", "request_id": t14RequestID(985), "cwd_key": "repo-a", "brief": map[string]any{"inline": "brief"}, "args": args, "wait_seconds": 600, "deadline_ms": time.Now().Add(time.Minute).UnixMilli()}
		raw, _ := json.Marshal(message)
		if _, ok := parseHubOutbound(raw); ok {
			t.Fatalf("unmarked node command admitted %v", args)
		}
		message["operator_authorized"] = true
		raw, _ = json.Marshal(message)
		if parsed, ok := parseHubOutbound(raw); !ok || !parsed.OperatorAuthorized {
			t.Fatalf("operator command refused %v", args)
		}
		body["operator_authorized"] = true
		status, _, _ = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
		if status != http.StatusBadRequest {
			t.Fatal("legacy caller forged operator marker")
		}
		serviceRaw, _ = json.Marshal(body)
		if status, _ := service.do(t, scopedServer, http.MethodPost, "/v1/spawn", string(serviceRaw)); status != http.StatusBadRequest {
			t.Fatal("scoped service forged operator marker")
		}
	}
}

func TestNodeSpawnLegacyResultKeepsOldWireShape(t *testing.T) {
	result := hubSpawnResult{RequestID: t14RequestID(986), RC: 70, Error: "spawn_failed", Receipt: &fleet.ExecutionReceipt{Job: "test-job"}, Stage: "launch", StageMS: map[string]int64{"launch": 12}, Retryable: true}
	for _, extended := range []bool{false, true} {
		raw, err := json.Marshal(spawnResultWire(result, extended))
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			t.Fatal(string(raw))
		}
		for _, key := range []string{"receipt", "stage", "stage_ms", "retryable"} {
			if _, ok := fields[key]; ok != extended {
				t.Fatalf("extended=%v field %s: %s", extended, key, raw)
			}
		}
		if !extended && len(fields) != 7 {
			t.Fatalf("legacy hub rejects %d fields: %s", len(fields), raw)
		}
	}
	for _, reason := range []string{"reconciling", "journal_unavailable", "journal_conflict"} {
		result.Error = reason
		raw, _ := json.Marshal(spawnResultWire(result, false))
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if string(fields["error"]) != `"timeout"` {
			t.Fatalf("legacy hub cannot decode uncertain journal result: %s", raw)
		}
	}
}

func TestNodeSpawnSuccessfulReceiptSurvivesContextExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		cancel()
		return t14EngineOK()
	}}
	result := client.runHubSpawn(ctx, hubOutboundMessage{RequestID: t14RequestID(987), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1})
	if result.RC != 0 || result.Error != "" || result.Pane == "" {
		t.Fatalf("success overwritten by deadline: %+v", result)
	}
}

func TestNodeSpawnRetiredSettingRefusesAtStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(path, []byte(`{"runtug_path":"/opt/bin/runtug"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var warning string
	client := &HubClient{spawnConfigPath: path, warn: func(s string) { warning = s }}
	client.Run(t.Context())
	if !strings.Contains(warning, "node startup refused") || !strings.Contains(warning, "remove retired runtug_path") {
		t.Fatalf("startup migration not reported: %q", warning)
	}
}

// The hub spawn allowlist no longer admits the retired builder seat, and still
// carries the author family and job a reviewer's independence check reads.
func TestHubSpawnArgsRefuseBuilderRoleKeepReviewerAuthorFlags(t *testing.T) {
	if validHubSpawnArgs([]string{"--role", "builder", "--lane", "job", "--parent", "caller"}) {
		t.Fatal("--role builder must not pass the hub allowlist")
	}
	if !validHubSpawnArgs([]string{"--role", "reviewer", "--author-family", "anthropic", "--builder-job", "author-job"}) {
		t.Fatal("a reviewer must keep --author-family and --builder-job")
	}
}
