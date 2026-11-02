package panewire

import (
	"context"
	"errors"
	"github.com/coder/websocket/wsjson"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestOperatorSpawnGradeAndRoleBoundary(t *testing.T) {
	for _, grade := range []string{"C", "B", "A", "A+", "S", "S+"} {
		args := []string{"--grade", grade, "--role", "counsel"}
		if !validHubSpawnArgsForMode(args, true) {
			t.Fatalf("operator grade rejected: %s", grade)
		}
		if validHubSpawnArgs(args) {
			t.Fatal("legacy request gained operator authority")
		}
	}
	for _, args := range [][]string{{"--grade", "A++"}, {"--grade", "Z"}, {"--job", "job+"}, {"--grade", "S+\n"}} {
		if validHubSpawnArgsForMode(args, true) {
			t.Fatalf("invalid argument accepted: %q", args)
		}
	}
}

func TestNodeSpawnJournalUsesWritableStateWithReadOnlyConfig(t *testing.T) {
	config := t14SpawnConfig(t, t.TempDir())
	if err := os.Chmod(filepath.Dir(config), 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Dir(config), 0700) })
	root := filepath.Join(t.TempDir(), "node-state", "spawn")
	client := &HubClient{spawnConfigPath: config, spawnJournalDir: root}
	if err := client.spawnStartupError(); err != nil {
		t.Fatal(err)
	}
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	client.releaseSpawnJournal()
	if journal == nil {
		t.Fatal("no journal")
	}
	if _, err := os.Stat(filepath.Join(root, "spawn-inbox.sqlite3")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(config), "spawn-inbox.sqlite3")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("wrote into config directory")
	}
}

func TestNodeSpawnJournalStartupRefusesUnavailableOrUnmigratedState(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unwritable-state", true: "legacy-journal"}[legacy], func(t *testing.T) {
			config := t14SpawnConfig(t, t.TempDir())
			root := filepath.Join(t.TempDir(), "spawn")
			if legacy {
				old, err := openNodeSpawnJournal(filepath.Dir(config))
				if err != nil {
					t.Fatal(err)
				}
				old.db.Close()
			} else {
				if err := os.WriteFile(root, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			client := &HubClient{spawnConfigPath: config, spawnJournalDir: root}
			err := client.spawnStartupError()
			if err == nil || !strings.Contains(err.Error(), "node startup refused") {
				t.Fatalf("unsafe startup: %v", err)
			}
			if legacy && !strings.Contains(err.Error(), "SQLite backup") {
				t.Fatalf("missing migration fix: %v", err)
			}
		})
	}
}

func TestSpawnBoundTimeoutCleanupAttestationContract(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, attested := range []bool{false, true} {
				t.Run(map[bool]string{false: "hub-observation", true: "node-cleanup"}[attested], func(t *testing.T) {
					s := backend.fresh(t)()
					if err := s.ensureSpawnTasks(); err != nil {
						t.Fatal(err)
					}
					req := durableSpawnRequest()
					now := time.Now()
					task, _, err := s.claimSpawn(t.Context(), req, "test-job", "parent", now)
					if err != nil {
						t.Fatal(err)
					}
					task, err = s.bindSpawn(t.Context(), req.RequestID, task.Attempt, hubSpawnRequest{RequestID: t14RequestID(998), Machine: "machine-a"})
					if err != nil {
						t.Fatal(err)
					}
					// First a hub deadline passes while the node is still running.
					if _, err = s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Reason: "timeout", Retryable: true}, now); err != nil {
						t.Fatal(err)
					}
					if attested {
						if changed, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Reason: "timeout", Retryable: true, NodeResult: true}, now); err != nil || !changed {
							t.Fatalf("cleanup result ignored: %v %v", changed, err)
						}
					}
					task, err = s.spawnTask(t.Context(), req.RequestID)
					if err != nil {
						t.Fatal(err)
					}
					want := "deadline_exceeded"
					if attested {
						want = "retryable"
					}
					if task.Status != want || task.Outcome.Retryable != attested {
						t.Fatalf("ownership status: %+v", task)
					}
					req.RequestID = t14RequestID(997)
					_, fresh, err := s.claimSpawn(t.Context(), req, "test-job", "parent", now)
					if attested {
						if err != nil || !fresh {
							t.Fatalf("verified cleanup blocked retry: %v", err)
						}
					} else if !errors.Is(err, errSpawnClaimed) {
						t.Fatalf("unproven timeout freed ownership: %v", err)
					}
				})
			}
		})
	}
}

func TestSubmissionRejectsSubdirectoryAndConflictingTaskAuthority(t *testing.T) {
	for _, change := range []func(*fleet.Submission){
		func(r *fleet.Submission) { r.Repository.Relative = "src" },
		func(r *fleet.Submission) { r.Args = append(r.Args, "--parent-job", "parent-job") },
	} {
		h := asyncSpawnHub(t)
		req := durableSpawnRequest()
		change(&req)
		if _, err := fleet.ValidateSubmission(req); err == nil {
			t.Fatal("invalid submission accepted")
		}
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now()); err == nil {
			t.Fatal("invalid submission reserved")
		}
	}
}

func TestAsyncSpawnCompletedNodeRefusalsRemainTerminal(t *testing.T) {
	for _, reason := range []string{"spawn_failed", "cwd_unmapped", "checkout_refused:origin_mismatch", ""} {
		t.Run(reason, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			req := durableSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			dispatch := hubSpawnRequest{RequestID: t14RequestID(996), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
			task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, dispatch)
			if err != nil {
				t.Fatal(err)
			}
			transport := directSpawnTransport{h: h, task: task}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := transport.dispatch(ctx, dispatch); done <- err }()
			sent := t14ReadNodeSpawn(t, node)
			message := map[string]any{"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 78, "job_id": "test-job", "pane": "", "stdout_tail": "", "stage": "launch", "retryable": false}
			if reason != "" {
				message["error"] = reason
			}
			if err := wsjson.Write(t.Context(), node, message); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
			want := reason
			if want == "" {
				want = "spawn_failed"
			}
			if err != nil || saved.Status != "failed" || saved.Outcome.Reason != want || !saved.Outcome.NodeResult {
				t.Fatalf("definite refusal became unknown: %+v %v", saved, err)
			}
			req.RequestID = t14RequestID(995)
			if _, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now()); !errors.Is(err, errSpawnClaimed) {
				t.Fatalf("unproven cleanup freed ownership: %v", err)
			}
		})
	}
}
