package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func durableSpawnRequest() fleet.Submission {
	return fleet.Submission{RequestID: "d676220b-e763-4af1-aefd-000000000001", Args: []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "test-job", "--job", "test-job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer", "--host", "auto"}, Brief: "Implement the task", Repository: fleet.Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/project", Worktree: "project.test-job", Checkout: &fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/task", Sha: strings.Repeat("a", 40), Dir: "project.test-job"}}, OriginMachine: "machine-a", DeadlineSeconds: 600}
}
func openSpawnStore(t *testing.T, path string) *laneInboxStore {
	t.Helper()
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	if err = s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestSpawnReservationSurvivesRestartAndReplaysLostResponse(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s := openSpawnStore(t, path)
	req := durableSpawnRequest()
	now := time.Now()
	first, fresh, err := s.claimSpawn(ctx, req, "test-job", "parent", now)
	if err != nil || !fresh {
		t.Fatalf("claim: %v %v", fresh, err)
	}
	recovered := openSpawnStore(t, path)
	replay, fresh, err := recovered.claimSpawn(ctx, req, "test-job", "parent", now)
	if err != nil || fresh || replay.Request.RequestID != first.Request.RequestID {
		t.Fatalf("lost-response replay: %+v %v %v", replay, fresh, err)
	}
	changed := req
	changed.Brief = "different payload"
	if _, _, err = recovered.claimSpawn(ctx, changed, "test-job", "parent", now); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("payload conflict accepted: %v", err)
	}
	changed = req
	changed.RequestID = "d676220b-e763-4af1-aefd-000000000002"
	if _, _, err = recovered.claimSpawn(ctx, changed, "test-job", "parent", now); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("duplicate job accepted: %v", err)
	}
	changed.Args = append([]string(nil), req.Args...)
	for i, v := range changed.Args {
		if v == "test-job" {
			changed.Args[i] = "second-job"
		}
	}
	if _, _, err = recovered.claimSpawn(ctx, changed, "second-job", "parent", now); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("duplicate task/role accepted: %v", err)
	}
	for i, arg := range changed.Args {
		if arg == "--task" {
			changed.Args = append(changed.Args[:i], changed.Args[i+2:]...)
			break
		}
	}
	changed.Args = append(changed.Args, "--parent-job", "test-job")
	if _, fresh, err = recovered.claimSpawn(ctx, changed, "second-job", "parent", now); err != nil || !fresh {
		t.Fatalf("legal inherited child blocked: %v", err)
	}
}
func TestSpawnUnknownRetainsOwnershipAndLateResultCommitsOutbox(t *testing.T) {
	for _, reason := range []string{"timeout", "disconnected", "unknown"} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			s := openSpawnStore(t, path)
			req := durableSpawnRequest()
			now := time.Now()
			task, _, err := s.claimSpawn(ctx, req, "test-job", "parent", now)
			if err != nil {
				t.Fatal(err)
			}
			node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-000000000003", Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 600}
			bound, err := s.bindSpawn(ctx, req.RequestID, task.Attempt, node, time.Now().UTC())
			if err != nil || bound.Node == nil {
				t.Fatal(err)
			}
			other := node
			other.Machine = "machine-b"
			again, err := s.bindSpawn(ctx, req.RequestID, task.Attempt, other, time.Now().UTC())
			if !errors.Is(err, errSpawnClaimed) || again.Node.Machine != "machine-a" {
				t.Fatalf("destination changed: %+v %v", again, err)
			}
			if changed, err := s.completeSpawn(ctx, task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: reason, Retryable: true}, now); err != nil || !changed {
				t.Fatal(err)
			}
			recovered := openSpawnStore(t, path)
			blocked := req
			blocked.RequestID = "d676220b-e763-4af1-aefd-000000000004"
			if _, _, err = recovered.claimSpawn(ctx, blocked, "test-job", "parent", now); !errors.Is(err, errSpawnClaimed) {
				t.Fatalf("unknown freed ownership: %v", err)
			}
			task, err = recovered.spawnTaskByNode(ctx, node.RequestID)
			if err != nil || task.Outcome.Retryable {
				t.Fatalf("recovery: %+v %v", task, err)
			}
			success := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1"}
			if changed, err := recovered.completeSpawn(ctx, task, success, now); err != nil || !changed {
				t.Fatalf("late result: %v %v", changed, err)
			}
			if changed, err := recovered.completeSpawn(ctx, task, hubSpawnOutcome{RC: 124, Reason: "timeout"}, now); err != nil || changed {
				t.Fatalf("terminal result regressed: %v %v", changed, err)
			}
			// A reconciling step notifies nothing (hk 3676). A timeout also leaves its
			// SPAWN_FAILED row before the late SPAWNED; the other two leave only SPAWNED.
			wantRows := 1
			if reason == "timeout" {
				wantRows = 2
			}
			var count int
			if err = recovered.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE delivered=0`).Scan(&count); err != nil || count != wantRows {
				t.Fatalf("atomic outbox: %d undelivered, want %d: %v", count, wantRows, err)
			}
			if changed, err := recovered.completeSpawn(ctx, task, success, now); err != nil || changed {
				t.Fatalf("duplicate success: %v %v", changed, err)
			}
		})
	}
}
func TestSpawnOnlyVerifiedCleanupAllowsSameJobRetry(t *testing.T) {
	ctx := context.Background()
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	req := durableSpawnRequest()
	now := time.Now()
	task, _, err := s.claimSpawn(ctx, req, "test-job", "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.completeSpawn(ctx, task, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Retryable: true}, now); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "d676220b-e763-4af1-aefd-000000000002"
	if _, fresh, err := s.claimSpawn(ctx, req, "test-job", "parent", now); err != nil || !fresh {
		t.Fatalf("safe retry refused: %v", err)
	}
}
func TestNodeSpawnJournalRestartDedupAndLostResultReplay(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	message := hubOutboundMessage{RequestID: "d676220b-e763-4af1-aefd-000000000001", CWDKey: "project", BriefInline: "task", Args: []string{"--job", "test-job"}, WaitSeconds: 600}
	claimed, cached, err := j.claim(ctx, message)
	if err != nil || !claimed || cached != nil {
		t.Fatal(err)
	}
	j.db.Close()
	j, err = openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()
	claimed, cached, err = j.claim(ctx, message)
	if err != nil || claimed || cached != nil {
		t.Fatalf("restart reran uncertain execution: %v %v %v", claimed, cached, err)
	}
	result := hubSpawnResult{RequestID: message.RequestID, RC: 0, JobID: "test-job", Pane: "pane-1"}
	if err = j.complete(ctx, result); err != nil {
		t.Fatal(err)
	}
	claimed, cached, err = j.claim(ctx, message)
	a, _ := json.Marshal(result)
	b, _ := json.Marshal(cached)
	if err != nil || claimed || string(a) != string(b) {
		t.Fatalf("lost result not replayed: %v %s %v", claimed, b, err)
	}
	message.BriefInline = "changed"
	if _, _, err = j.claim(ctx, message); err == nil {
		t.Fatal("changed-payload duplicate accepted")
	}
}

func TestSpawnLostBindResponseDoesNotReleaseOrMoveOwnership(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := directSpawnTransport{h: h, task: task}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = transport.Spawn(ctx, fleet.HubSpawnRequest{RequestID: t14RequestID(999), Machine: "machine-a", Args: []string{"--job", "test-job"}})
	if err == nil || !transport.bound {
		t.Fatalf("uncertain bind treated as unowned: %v %+v", err, transport)
	}
	another := request
	another.RequestID = t14RequestID(998)
	if _, _, err = h.inbox.store.claimSpawn(t.Context(), another, "test-job", "parent", time.Now()); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("bind failure released claim: %v", err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "unknown"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	recovered, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err = h.inbox.store.resumeUndispatchedSpawn(t.Context(), recovered)
	if err != nil || recovered.Status != "accepted" || recovered.Request.RequestID != request.RequestID || recovered.Attempt != task.Attempt {
		t.Fatalf("undispatched recovery %+v %v", recovered, err)
	}
}
