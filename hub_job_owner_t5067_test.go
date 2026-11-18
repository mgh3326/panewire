package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

type ownerFenceHK struct {
	mu             sync.Mutex
	record         map[string]any
	legacy         bool
	terminalOwners bool
	updates        []map[string]any
}

func (s *ownerFenceHK) Claim(context.Context, string, string, string) error { return nil }
func (s *ownerFenceHK) Get(context.Context, string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]any, len(s.record))
	for key, value := range s.record {
		if !s.legacy || (key != "owner_node" && key != "owner_epoch") {
			result[key] = value
		}
	}
	return result, nil
}
func (s *ownerFenceHK) Put(_ context.Context, _ string, fields map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var update map[string]any
	if err := json.Unmarshal(body, &update); err != nil {
		return err
	}
	s.updates = append(s.updates, update)
	if s.legacy && (update["owner"] != nil || update["expected_owner"] != nil) {
		return &fleet.BackendError{Status: http.StatusBadRequest, Code: "invalid_context"}
	}
	node, _ := s.record["owner_node"].(string)
	epoch, _ := s.record["owner_epoch"].(float64)
	if expected, ok := update["expected_owner"].(map[string]any); ok && (expected["node"] != node || expected["epoch"] != epoch) {
		return &fleet.BackendError{Status: http.StatusConflict, Code: "job_owner_mismatch", OwnerNode: node, OwnerEpoch: uint64(epoch)}
	}
	if owner, ok := update["owner"].(map[string]any); ok {
		if next, _ := owner["epoch"].(float64); next <= epoch {
			return &fleet.BackendError{Status: http.StatusConflict, Code: "job_owner_mismatch", OwnerNode: node, OwnerEpoch: uint64(epoch)}
		}
		if state := s.record["state"]; !s.terminalOwners && (state == "completed" || state == "lost" || state == "escalated") {
			return &fleet.BackendError{Status: http.StatusConflict, Code: "job_state_conflict"}
		}
		s.record["owner_node"], s.record["owner_epoch"] = owner["node"], owner["epoch"]
	}
	for key, value := range update {
		if key != "expected_owner" && key != "owner" {
			s.record[key] = value
		}
	}
	return nil
}

func ownerFenceHub(t *testing.T, hk *ownerFenceHK) (*HubServer, *hubAgent) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.ownerTransferDir = t.TempDir()
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-a", Epoch: 1}, false); err != nil {
		t.Fatal(err)
	}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: hk, Out: out, Err: errout}, nil
	}
	agent := &hubAgent{assignments: make(chan hubJobAssignedEvent, 1)}
	hub.mu.Lock()
	hub.jobs["fenced-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "fenced-job", Epoch: 1}, Node: "machine-a", Orphaned: true}
	hub.nodes["machine-b"] = &hubNodeRecord{agent: agent}
	hub.mu.Unlock()
	return hub, agent
}

func waitOwnerWork(t *testing.T, hub *HubServer, key string, seed bool) {
	t.Helper()
	eventually(t, 3*time.Second, "owner background work", func() bool {
		hub.ownerWork.mu.Lock()
		defer hub.ownerWork.mu.Unlock()
		if seed {
			return !hub.ownerWork.seeding[key]
		}
		return !hub.ownerWork.recovering[key]
	})
}

// The old pane has not seen a revocation when it attempts its lost write.
func TestJobOwnerFenceTransferRejectsOldNode_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	result, err := hub.reassignJob("fenced-job", "machine-b")
	if err != nil || result.Epoch != 2 {
		t.Fatalf("transfer=%+v err=%v", result, err)
	}
	select {
	case assignment := <-agent.assignments:
		if assignment.Epoch != 2 {
			t.Fatalf("assignment=%+v", assignment)
		}
	default:
		t.Fatal("successor was not assigned")
	}
	jobs := filepath.Join(t.TempDir(), "jobs")
	err = fleet.PutOwnedJob(t.Context(), hk, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil)
	if !fleet.IsJobOwnerMismatch(err) {
		t.Fatalf("old node's write error=%v, want job_owner_mismatch", err)
	}
	if err = fleet.PutOwnedJob(t.Context(), hk, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil); err == nil {
		t.Fatal("a second stale write was accepted")
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if hk.record["state"] != "spawned" || hk.record["owner_node"] != "machine-b" || hk.record["owner_epoch"] != float64(2) {
		t.Fatalf("successor record was changed: %v", hk.record)
	}
	if len(hk.updates) != 1 {
		t.Fatalf("updates=%v, want the transfer and no stale write", hk.updates)
	}
}

func TestJobOwnerFenceTransfersUnfencedOrphan_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, agent := ownerFenceHub(t, hk)
	result, err := hub.reassignJob("fenced-job", "machine-b")
	if err != nil || result.Epoch != 2 || len(agent.assignments) != 1 {
		t.Fatalf("unfenced reassignment=%+v err=%v assignments=%d", result, err, len(agent.assignments))
	}
	if hk.record["owner_node"] != "machine-b" || hk.record["owner_epoch"] != float64(2) {
		t.Fatalf("owner after transfer=%v", hk.record)
	}
}

// seedWindowStore pauses the first owner update and the old node's write so
// the test observes whether that write was fenced before the transfer.
type seedWindowStore struct {
	*ownerFenceHK
	firstOwner sync.Once
	ownerReady chan struct{}
	oldReady   chan struct{}
	ownerGo    chan struct{}
	oldGo      chan struct{}
}

func (s *seedWindowStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if owner, ok := fields["owner"].(fleet.JobOwner); ok {
		first := false
		s.firstOwner.Do(func() { first = true })
		if first {
			// In the correct path A's seed is visible before A reads. The
			// no-seed mutant pauses B's update while the record is still empty.
			if owner.Node == "machine-a" {
				if err := s.ownerFenceHK.Put(ctx, id, fields); err != nil {
					return err
				}
			}
			close(s.ownerReady)
			select {
			case <-s.ownerGo:
			case <-ctx.Done():
				return ctx.Err()
			}
			if owner.Node == "machine-a" {
				return nil
			}
		}
	}
	if fields["state"] == "lost" {
		close(s.oldReady)
		select {
		case <-s.oldGo:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFenceSeedsBeforeTransfer_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	store := &seedWindowStore{ownerFenceHK: hk, ownerReady: make(chan struct{}), oldReady: make(chan struct{}), ownerGo: make(chan struct{}), oldGo: make(chan struct{})}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOwner, releaseOld sync.Once
	t.Cleanup(func() { releaseOwner.Do(func() { close(store.ownerGo) }); releaseOld.Do(func() { close(store.oldGo) }) })
	transfer := make(chan error, 1)
	go func() { _, err := hub.reassignJob("fenced-job", "machine-b"); transfer <- err }()
	eventually(t, 3*time.Second, "first owner update", func() bool {
		select {
		case <-store.ownerReady:
			return true
		default:
			return false
		}
	})
	jobs := filepath.Join(t.TempDir(), "jobs")
	oldWrite := make(chan error, 1)
	go func() {
		oldWrite <- fleet.PutOwnedJob(ctx, store, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil)
	}()
	eventually(t, 3*time.Second, "old write prepared", func() bool {
		select {
		case <-store.oldReady:
			return true
		default:
			return false
		}
	})
	releaseOwner.Do(func() { close(store.ownerGo) })
	var transferErr error
	var transferred bool
	eventually(t, 3*time.Second, "owner transfer", func() bool {
		select {
		case transferErr = <-transfer:
			transferred = true
		default:
		}
		return transferred
	})
	if transferErr != nil {
		t.Fatalf("reassignment: %v", transferErr)
	}
	releaseOld.Do(func() { close(store.oldGo) })
	var oldErr error
	var wrote bool
	eventually(t, 3*time.Second, "old write refusal", func() bool {
		select {
		case oldErr = <-oldWrite:
			wrote = true
		default:
		}
		return wrote
	})
	if !fleet.IsJobOwnerMismatch(oldErr) {
		t.Fatalf("old write after transfer error=%v, record=%v", oldErr, hk.record)
	}
	if hk.record["state"] != "spawned" || hk.record["owner_node"] != "machine-b" || len(hk.updates) != 3 {
		t.Fatalf("seed, transfer, and refusal: record=%v updates=%v", hk.record, hk.updates)
	}
	seed, moved := hk.updates[0], hk.updates[1]
	if seed["owner"].(map[string]any)["node"] != "machine-a" || seed["expected_owner"].(map[string]any)["epoch"] != float64(0) || moved["expected_owner"].(map[string]any)["node"] != "machine-a" || moved["owner"].(map[string]any)["node"] != "machine-b" {
		t.Fatalf("owner updates out of order: %v", hk.updates)
	}
}

type competingSeedStore struct{ *ownerFenceHK }

func (s competingSeedStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if owner, ok := fields["owner"].(fleet.JobOwner); ok && owner.Node == "machine-a" {
		s.mu.Lock()
		s.record["owner_node"], s.record["owner_epoch"] = "machine-c", float64(2)
		s.mu.Unlock()
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFenceRefusesCompetingSeed_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, agent := ownerFenceHub(t, hk)
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: competingSeedStore{hk}, Out: out, Err: errout}, nil
	}
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err == nil || !strings.Contains(err.Error(), "machine-c epoch 2") {
		t.Fatalf("competing seed error=%v", err)
	}
	if len(agent.assignments) != 0 || hub.jobs["fenced-job"].Node != "machine-a" || hk.record["owner_node"] != "machine-c" {
		t.Fatalf("competing seed exposed successor: job=%+v record=%v", hub.jobs["fenced-job"], hk.record)
	}
}

func TestJobOwnerFenceLandingSeedsAssignment_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	hub.seedLandedOwner("fenced-job", "machine-a")
	var record map[string]any
	eventually(t, 3*time.Second, "landing owner seed", func() bool {
		record, _ = hk.Get(t.Context(), "fenced-job")
		return record["owner_node"] == "machine-a" && record["owner_epoch"] == float64(1)
	})
}

type preSeedLegacyStore struct {
	*ownerFenceHK
	started chan struct{}
	release chan struct{}
}

func (s *preSeedLegacyStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if fields["state"] == "lost" {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFencePreSeedWriteIsRefused_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	store := &preSeedLegacyStore{ownerFenceHK: hk, started: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(store.release) }) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := filepath.Join(t.TempDir(), "jobs")
	done := make(chan error, 1)
	go func() {
		done <- fleet.PutOwnedJob(ctx, store, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil)
	}()
	eventually(t, 3*time.Second, "legacy write waiting", func() bool {
		select {
		case <-store.started:
			return true
		default:
			return false
		}
	})
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(store.release) })
	var err error
	var finished bool
	eventually(t, 3*time.Second, "legacy write refusal", func() bool {
		select {
		case err = <-done:
			finished = true
		default:
		}
		return finished
	})
	if !fleet.IsJobOwnerMismatch(err) || hk.record["owner_node"] != "machine-b" || hk.record["state"] != "spawned" {
		t.Fatalf("pre-seed write error=%v record=%v updates=%v", err, hk.record, hk.updates)
	}
}

type seedDuringWrite struct {
	*ownerFenceHK
	owner fleet.JobOwner
	done  bool
}

func (s *seedDuringWrite) Put(ctx context.Context, id string, fields map[string]any) error {
	if !s.done {
		s.done = true
		s.mu.Lock()
		s.record["owner_node"], s.record["owner_epoch"] = s.owner.Node, float64(s.owner.Epoch)
		s.mu.Unlock()
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFenceRetriesSameNodeSeedOnce_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	store := &seedDuringWrite{ownerFenceHK: hk, owner: fleet.JobOwner{Node: "machine-a", Epoch: 1}}
	jobs := filepath.Join(t.TempDir(), "jobs")
	if err := fleet.PutOwnedJob(t.Context(), store, jobs, "machine-a", "fenced-job", map[string]any{"model": "updated"}, nil); err != nil {
		t.Fatal(err)
	}
	if hk.record["model"] != "updated" || len(hk.updates) != 2 {
		t.Fatalf("same-node seed retry: record=%v updates=%v", hk.record, hk.updates)
	}
	first, retry := hk.updates[0], hk.updates[1]
	if first["expected_owner"].(map[string]any)["node"] != "" || first["expected_owner"].(map[string]any)["epoch"] != float64(0) || retry["expected_owner"].(map[string]any)["node"] != "machine-a" || retry["expected_owner"].(map[string]any)["epoch"] != float64(1) || first["owner"] != nil || retry["owner"] != nil {
		t.Fatalf("write conditions or owner claim: %v", hk.updates)
	}
	markers, err := filepath.Glob(filepath.Join(jobs, "fenced-job", "events", "*-job.revoked.json"))
	if err != nil || len(markers) != 0 {
		t.Fatalf("same-node seed revoked the grant: markers=%v err=%v", markers, err)
	}
}

type repeatedSelfMismatch struct {
	*ownerFenceHK
	puts int
}

func (s *repeatedSelfMismatch) Put(context.Context, string, map[string]any) error {
	s.puts++
	return &fleet.BackendError{Status: http.StatusConflict, Code: "job_owner_mismatch", OwnerNode: "machine-a", OwnerEpoch: 1}
}

func TestJobOwnerFenceSameNodeMismatchRetriesOnlyOnce_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	store := &repeatedSelfMismatch{ownerFenceHK: hk}
	jobs := filepath.Join(t.TempDir(), "jobs")
	err := fleet.PutOwnedJob(t.Context(), store, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil)
	if !fleet.IsJobOwnerMismatch(err) || store.puts != 2 {
		t.Fatalf("same-node mismatch error=%v puts=%d", err, store.puts)
	}
	markers, err := filepath.Glob(filepath.Join(jobs, "fenced-job", "events", "*-job.revoked.json"))
	if err != nil || len(markers) != 1 {
		t.Fatalf("exhausted retry revocation markers=%v err=%v", markers, err)
	}
}

func TestJobOwnerFenceDoesNotRetryAnotherOwner_t5067(t *testing.T) {
	for _, owner := range []fleet.JobOwner{{Node: "machine-b", Epoch: 2}, {Node: "machine-a", Epoch: 3}} {
		t.Run(fmt.Sprintf("%s-%d", owner.Node, owner.Epoch), func(t *testing.T) {
			hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
			store := &seedDuringWrite{ownerFenceHK: hk, owner: owner}
			jobs := filepath.Join(t.TempDir(), "jobs")
			err := fleet.PutOwnedJob(t.Context(), store, jobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil)
			if err == nil || len(hk.updates) != 1 || hk.record["state"] != "spawned" || hk.record["owner_node"] != owner.Node {
				t.Fatalf("another-owner write error=%v record=%v updates=%v", err, hk.record, hk.updates)
			}
			if owner.Node == "machine-a" && fleet.IsJobReassigned(err) || owner.Node != "machine-a" && !fleet.IsJobOwnerMismatch(err) {
				t.Fatalf("wrong refusal for owner %+v: %v", owner, err)
			}
			markers, err := filepath.Glob(filepath.Join(jobs, "fenced-job", "events", "*-job.revoked.json"))
			wantMarkers := 1
			if owner.Node == "machine-a" {
				wantMarkers = 0 // The newer same-node grant has not reached the ledger yet.
			}
			if err != nil || len(markers) != wantMarkers {
				t.Fatalf("another-owner revocation markers=%v err=%v, want %d", markers, err, wantMarkers)
			}
		})
	}
}

func TestJobOwnerFenceSeedsCurrentEpochOrphan_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	hub.jobs["fenced-job"].Epoch = 2
	path := filepath.Join(hub.ownerTransferDir, "_assignments", "fenced-job.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"job":"fenced-job","node":"machine-a","epoch":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatal(err)
	}
	if hk.record["owner_node"] != "machine-b" || hk.record["owner_epoch"] != float64(3) || len(hk.updates) != 2 {
		t.Fatalf("epoch-2 orphan owner=%v updates=%v", hk.record, hk.updates)
	}
	seed, moved := hk.updates[0], hk.updates[1]
	if seed["expected_owner"].(map[string]any)["epoch"] != float64(0) ||
		seed["owner"].(map[string]any)["node"] != "machine-a" || seed["owner"].(map[string]any)["epoch"] != float64(2) ||
		moved["expected_owner"].(map[string]any)["node"] != "machine-a" || moved["expected_owner"].(map[string]any)["epoch"] != float64(2) ||
		moved["owner"].(map[string]any)["node"] != "machine-b" || moved["owner"].(map[string]any)["epoch"] != float64(3) {
		t.Fatalf("epoch-2 seed and transfer=%v", hk.updates)
	}
}

func TestJobOwnerFenceHubSeedsBeforeStaleNodeWrite_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2}, false); err != nil {
		t.Fatal(err)
	}
	aJobs, bJobs := filepath.Join(t.TempDir(), "jobs"), filepath.Join(t.TempDir(), "jobs")
	if err := fleet.PutOwnedJob(t.Context(), hk, aJobs, "machine-a", "fenced-job", map[string]any{"model": "legacy"}, nil); err != nil {
		t.Fatal(err)
	}
	if hk.record["owner_node"] != "" {
		t.Fatalf("legacy node claimed owner: %v", hk.record)
	}
	if err := jobowner.Assign(bJobs, "fenced-job", 2); err != nil {
		t.Fatal(err)
	}
	hub.seedOwnerAssignment(t.Context(), hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2})
	if err := fleet.PutOwnedJob(t.Context(), hk, bJobs, "machine-b", "fenced-job", map[string]any{"model": "current"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := fleet.PutOwnedJob(t.Context(), hk, aJobs, "machine-a", "fenced-job", map[string]any{"state": "lost"}, nil); !fleet.IsJobOwnerMismatch(err) {
		t.Fatalf("stale node write error=%v", err)
	}
	if err := fleet.PutOwnedJob(t.Context(), hk, bJobs, "machine-b", "fenced-job", map[string]any{"model": "still-current"}, nil); err != nil {
		t.Fatalf("current owner was revoked: %v", err)
	}
	if hk.record["owner_node"] != "machine-b" || hk.record["state"] != "spawned" {
		t.Fatalf("record=%v", hk.record)
	}
}

func TestJobOwnerFenceUnheldLegacyStaysUnfenced_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, agent := ownerFenceHub(t, hk)
	path := filepath.Join(hub.ownerTransferDir, "_assignments", "fenced-job.json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, hub.now())
	hub.sendHeartbeatDirectives("machine-a", agent)
	waitOwnerWork(t, hub, "fenced-job", true)
	waitOwnerWork(t, hub, "machine-a", false)
	if hk.record["owner_node"] != "" || len(hk.updates) != 0 {
		t.Fatalf("unheld legacy job was seeded: record=%v updates=%v", hk.record, hk.updates)
	}
	if hub.jobs["fenced-job"] == nil {
		t.Fatal("legacy job was not admitted")
	}
	hub.jobs["fenced-job"].Orphaned = true
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err == nil || !strings.Contains(err.Error(), "durable assignment unavailable") {
		t.Fatalf("unheld legacy reassignment error=%v", err)
	}
	if hk.record["owner_node"] != "" || len(hk.updates) != 0 {
		t.Fatalf("unheld legacy reassignment was seeded: record=%v updates=%v", hk.record, hk.updates)
	}
}

func TestJobOwnerFenceRestartUsesDurableAssignment_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	path := filepath.Join(hub.ownerTransferDir, "_assignments", "fenced-job.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"job":"fenced-job","node":"machine-b","epoch":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, hub.now())
	if hub.jobs["fenced-job"] != nil || hk.record["owner_node"] != "" {
		t.Fatalf("stale heartbeat won: job=%+v record=%v", hub.jobs["fenced-job"], hk.record)
	}
	hub.observeActiveJobs("machine-b", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-b", Epoch: 2}}, hub.now())
	eventually(t, 3*time.Second, "durable assignment seeded", func() bool {
		hk.mu.Lock()
		defer hk.mu.Unlock()
		return hk.record["owner_node"] == "machine-b" && hk.record["owner_epoch"] == float64(2)
	})
	if job := hub.jobs["fenced-job"]; job == nil || job.Node != "machine-b" || job.Epoch != 2 || hk.record["owner_node"] != "machine-b" || hk.record["owner_epoch"] != float64(2) {
		t.Fatalf("durable assignment not recovered: job=%+v record=%v", job, hk.record)
	}
}

func TestJobOwnerFenceStartupSeedsOnlyHeldLiveAssignments_t5067(t *testing.T) {
	for _, state := range []string{"spawned", "completed"} {
		t.Run(state, func(t *testing.T) {
			hk := &ownerFenceHK{record: map[string]any{"state": state, "owner_node": "", "owner_epoch": float64(0)}}
			hub, _ := ownerFenceHub(t, hk)
			if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2}, false); err != nil {
				t.Fatal(err)
			}
			path := hub.ownerTransferDir
			restarted, _ := ownerFenceHub(t, hk)
			restarted.ownerTransferDir = path
			delete(restarted.jobs, "fenced-job")
			restarted.sweepOwnerAssignments(t.Context())
			waitOwnerWork(t, restarted, "fenced-job", true)
			want := "machine-b"
			if state == "completed" {
				want = ""
			}
			if hk.record["owner_node"] != want {
				t.Fatalf("state=%s owner=%v, want %s", state, hk.record, want)
			}
		})
	}
}

func TestJobOwnerFenceCompletedAssignmentIsNotReseeded_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
	hub, _ := ownerFenceHub(t, hk)
	completion := hubJobEventPayload{JobID: "fenced-job", Epoch: 1}
	if !hub.observeJobCompletion("machine-a", completion, hub.now()) {
		t.Fatal("current completion was refused")
	}
	restarted, _ := ownerFenceHub(t, hk)
	restarted.ownerTransferDir = hub.ownerTransferDir
	delete(restarted.jobs, "fenced-job")
	restarted.sweepOwnerAssignments(t.Context())
	if hk.record["owner_node"] != "" || len(hk.updates) != 0 {
		t.Fatalf("completed assignment was seeded after restart: record=%v updates=%v", hk.record, hk.updates)
	}
}

func TestJobOwnerFenceEndedAssignmentDoesNotReturn_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, _ := ownerFenceHub(t, hk)
	if !hub.observeJobCompletion("machine-a", hubJobEventPayload{JobID: "fenced-job", Epoch: 1}, hub.now()) {
		t.Fatal("current completion was refused")
	}
	restarted, _ := ownerFenceHub(t, hk)
	restarted.ownerTransferDir = hub.ownerTransferDir
	delete(restarted.jobs, "fenced-job")
	restarted.nodes["machine-a"] = &hubNodeRecord{}
	restarted.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, restarted.now())
	if job := restarted.jobs["fenced-job"]; job != nil {
		t.Fatalf("completed assignment returned on heartbeat: %+v", job)
	}
}

func TestJobOwnerFenceCompletionKeepsProducingEpoch_t5067(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00001-job.completed.json"), []byte(`{"type":"job.completed","epoch":1,"owner_lane":"lane-a"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(filepath.Join(root, "jobs"), "job-a", 3); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: make(map[string]uint64), assignedJobs: map[string]uint64{"job-a": 3}}
	events := client.jobCompletionEvents()
	if len(events) != 1 || !strings.Contains(string(events[0].Payload), `"epoch":1`) {
		t.Fatalf("old completion was relabeled with a newer grant: %+v", events)
	}
}

func TestJobOwnerFenceLifecycleEnvelopeKeepsEpoch_t5067(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	body := `{"kind":"job.completed","created_at":"` + old + `","payload":{"owner_lane":"lane-a","report":"report.md","owner_epoch":3}}`
	if err := os.WriteFile(filepath.Join(dir, "00001-job.completed.json"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	events := scanHubRelayEvents(root)
	if len(events) != 1 || events[0].Epoch != 3 {
		t.Fatalf("lifecycle completion epoch: %+v", events)
	}
}

type committedTransferLostAnswer struct {
	*ownerFenceHK
	lost bool
}

func (s *committedTransferLostAnswer) Put(ctx context.Context, id string, fields map[string]any) error {
	err := s.ownerFenceHK.Put(ctx, id, fields)
	if err == nil && fields["owner"] != nil && !s.lost {
		s.lost = true
		return errors.New("transfer response lost")
	}
	return err
}

func TestJobOwnerFenceRecoversLostTransferAnswer_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	store := &committedTransferLostAnswer{ownerFenceHK: hk}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	result, err := hub.reassignJob("fenced-job", "machine-b")
	if err != nil || result.Epoch != 2 || len(agent.assignments) != 1 {
		t.Fatalf("committed transfer not recovered: result=%+v err=%v assignments=%d", result, err, len(agent.assignments))
	}
}

type transferAnswerAndReadLost struct {
	*committedTransferLostAnswer
	unavailable bool
}

func (s *transferAnswerAndReadLost) Get(ctx context.Context, id string) (map[string]any, error) {
	if s.unavailable {
		return nil, errors.New("confirmation unavailable")
	}
	return s.ownerFenceHK.Get(ctx, id)
}

func (s *transferAnswerAndReadLost) Put(ctx context.Context, id string, fields map[string]any) error {
	err := s.committedTransferLostAnswer.Put(ctx, id, fields)
	if err != nil {
		s.unavailable = true
	}
	return err
}

func TestJobOwnerFenceRetriesUnconfirmedTransfer_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	store := &transferAnswerAndReadLost{committedTransferLostAnswer: &committedTransferLostAnswer{ownerFenceHK: hk}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err == nil {
		t.Fatal("transfer with unavailable confirmation should report an error")
	}
	store.unavailable = false
	result, err := hub.reassignJob("fenced-job", "machine-b")
	if err != nil || result.Epoch != 2 || len(agent.assignments) != 1 {
		t.Fatalf("retry did not recover committed transfer: result=%+v err=%v assignments=%d", result, err, len(agent.assignments))
	}
}

type earlierTransferWins struct {
	*ownerFenceHK
	won bool
}

func (s *earlierTransferWins) Put(ctx context.Context, id string, fields map[string]any) error {
	if fields["owner"] != nil && !s.won {
		s.won = true
		if err := s.ownerFenceHK.Put(ctx, id, fields); err != nil {
			return err
		}
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFenceRetryReconcilesConcurrentCommit_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	store := &earlierTransferWins{ownerFenceHK: hk}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	result, err := hub.reassignJob("fenced-job", "machine-b")
	if err != nil || result.Epoch != 2 || len(agent.assignments) != 1 {
		t.Fatalf("concurrently committed transfer not recovered: result=%+v err=%v assignments=%d", result, err, len(agent.assignments))
	}
}

func TestJobOwnerFenceRedeliversAfterHubRestart_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatal(err)
	}
	<-agent.assignments // The hub can stop before the node receives this frame.
	journal := hub.ownerTransferDir
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, newAgent := ownerFenceHub(t, hk)
	restarted.ownerTransferDir = journal
	restarted.mu.Lock()
	delete(restarted.jobs, "fenced-job")
	restarted.mu.Unlock()
	restarted.sendHeartbeatDirectives("machine-b", newAgent)
	var assignment hubJobAssignedEvent
	if !waitUntil(3*time.Second, func() bool {
		select {
		case assignment = <-newAgent.assignments:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("hub restart lost the successor assignment")
	}
	if assignment.JobID != "fenced-job" || assignment.Epoch != 2 {
		t.Fatalf("recovered assignment=%+v", assignment)
	}
}

func TestJobOwnerFenceDoesNotRedeliverUncommittedTransfer_t5067(t *testing.T) {
	for _, owner := range []struct {
		node    string
		epoch   float64
		retains bool
	}{{"machine-a", 1, true}, {"machine-c", 3, false}} {
		t.Run(owner.node, func(t *testing.T) {
			hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": owner.node, "owner_epoch": owner.epoch}}
			hub, agent := ownerFenceHub(t, hk)
			if err := hub.saveOwnerTransfer(hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}); err != nil {
				t.Fatal(err)
			}
			delete(hub.jobs, "fenced-job")
			hub.sendHeartbeatDirectives("machine-b", agent)
			waitOwnerWork(t, hub, "machine-b", false)
			if len(agent.assignments) != 0 || hub.jobs["fenced-job"] != nil {
				t.Fatalf("journal without matching hk owner granted a job: %+v", hub.jobs["fenced-job"])
			}
			found, err := hub.hasOwnerTransfer(hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1})
			if err != nil || found != owner.retains {
				t.Fatalf("journal retained=%v err=%v, want %v", found, err, owner.retains)
			}
		})
	}
}

func TestJobOwnerFenceClearsTransferAfterGrantHeartbeat_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatal(err)
	}
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if found, err := hub.hasOwnerTransfer(intent); err != nil || !found {
		t.Fatalf("pending transfer journal found=%v err=%v", found, err)
	}
	hub.mu.Lock()
	hub.nodes["machine-b"].activeJobs = map[string]HubActiveJob{"fenced-job": {JobID: "fenced-job", Epoch: 2}}
	hub.mu.Unlock()
	hub.sendHeartbeatDirectives("machine-b", agent)
	eventually(t, 3*time.Second, "transfer journal removed", func() bool {
		found, err := hub.hasOwnerTransfer(intent)
		return err == nil && !found
	})
	if found, err := hub.hasOwnerTransfer(intent); err != nil || found {
		t.Fatalf("acknowledged transfer journal found=%v err=%v", found, err)
	}
}

func TestJobOwnerFenceRestoresPredecessorBeforeJournalCleanup_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, agent := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	hub.jobs["fenced-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "fenced-job", Epoch: 2}, Node: "machine-b", FencedNodes: make(map[string]uint64)}
	hub.nodes["machine-b"].activeJobs = map[string]HubActiveJob{"fenced-job": {JobID: "fenced-job", Epoch: 2}}
	hub.sendHeartbeatDirectives("machine-b", agent)
	eventually(t, 3*time.Second, "predecessor revocation restored", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.jobs["fenced-job"].FencedNodes["machine-a"] == 2 && hub.pendingRevocations["machine-a"]["fenced-job"].Epoch == 2
	})
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if got := hub.jobs["fenced-job"].FencedNodes["machine-a"]; got != 2 || hub.pendingRevocations["machine-a"]["fenced-job"].Epoch != 2 {
		t.Fatalf("predecessor fence not restored: fence=%d pending=%v", got, hub.pendingRevocations["machine-a"])
	}
}

func TestJobOwnerFenceRecoveryClearsPredecessorCompletion_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, agent := ownerFenceHub(t, hk)
	if err := hub.saveOwnerTransfer(hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if !hub.observeJobCompletion("machine-a", hubJobEventPayload{JobID: "fenced-job", Epoch: 1}, hub.now()) {
		t.Fatal("predecessor completion was not recorded")
	}
	hub.sendHeartbeatDirectives("machine-b", agent)
	eventually(t, 3*time.Second, "successor recovered", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		job := hub.jobs["fenced-job"]
		return job.Node == "machine-b" && job.Epoch == 2 && !job.Completed && len(agent.assignments) == 1
	})
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if job := hub.jobs["fenced-job"]; job.Node != "machine-b" || job.Epoch != 2 || job.Completed || len(agent.assignments) != 1 {
		t.Fatalf("successor remained completed: job=%+v assignments=%d", job, len(agent.assignments))
	}
}

func TestJobOwnerFenceDoesNotRecoverTerminalJob_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "completed", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, agent := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.sendHeartbeatDirectives("machine-b", agent)
	eventually(t, 3*time.Second, "terminal transfer journal removed", func() bool {
		found, err := hub.hasOwnerTransfer(intent)
		return err == nil && !found
	})
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if job := hub.jobs["fenced-job"]; job != nil || len(agent.assignments) != 0 {
		t.Fatalf("terminal job was redelivered: job=%+v assignments=%d", job, len(agent.assignments))
	}
}

func TestJobOwnerFenceRefusesStaleTransfer_t5067(t *testing.T) {
	for _, tc := range []struct {
		name  string
		epoch float64
		want  string
	}{
		{"different node at the same epoch", 1, "epoch 1"},
		{"newer owner epoch", 2, "epoch 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": tc.epoch}}
			hub, agent := ownerFenceHub(t, hk)
			_, err := hub.reassignJob("fenced-job", "machine-b")
			if err == nil || !strings.Contains(err.Error(), "machine-b") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("stale transfer error=%v, want current owner and %s", err, tc.want)
			}
			if len(agent.assignments) != 0 || hub.jobs["fenced-job"].Node != "machine-a" {
				t.Fatal("stale transfer exposed a successor")
			}
		})
	}
}

func TestJobOwnerFenceOldHandoffkeepGetsNoFields_t5067(t *testing.T) {
	hk := &ownerFenceHK{legacy: true, record: map[string]any{"state": "spawned"}}
	hub, _ := ownerFenceHub(t, hk)
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatalf("legacy reassignment: %v", err)
	}
	jobs := filepath.Join(t.TempDir(), "jobs")
	if err := fleet.PutOwnedJob(t.Context(), hk, jobs, "machine-b", "fenced-job", map[string]any{"state": "lost"}, nil); err != nil {
		t.Fatalf("legacy write: %v", err)
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if len(hk.updates) != 1 || len(hk.updates[0]) != 1 || hk.updates[0]["state"] != "lost" {
		t.Fatalf("legacy hk received fence fields: %v", hk.updates)
	}
}

func TestJobOwnerFenceTerminalTransferFailsClosed_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "lost", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	_, err := hub.reassignJob("fenced-job", "machine-b")
	if err == nil || !strings.Contains(err.Error(), "job_state_conflict") {
		t.Fatalf("terminal transfer error=%v, want job_state_conflict", err)
	}
	if hub.jobs["fenced-job"].Node != "machine-a" || len(agent.assignments) != 0 {
		t.Fatal("terminal refusal exposed the successor")
	}
}

func TestJobOwnerFenceTerminalTransferFutureHandoffkeep_t5067(t *testing.T) {
	t.Skip("hk 5303: waiting for the deployed handoffkeep version supporting conditional terminal owner-only transfers")
	hk := &ownerFenceHK{terminalOwners: true, record: map[string]any{"state": "lost", "owner_node": "machine-a", "owner_epoch": float64(1), "result": "immutable"}}
	hub, _ := ownerFenceHub(t, hk)
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err != nil {
		t.Fatal(err)
	}
	if hk.record["owner_node"] != "machine-b" || hk.record["owner_epoch"] != float64(2) || hk.record["state"] != "lost" || hk.record["result"] != "immutable" {
		t.Fatalf("terminal transfer changed result or state: %v", hk.record)
	}
}

var _ fleet.JobStore = (*ownerFenceHK)(nil)

// This exercises the durable pool action through a real hub restart. The fake
// already carries the new owner: terminal transfer support is tracked by the
// separately skipped future-server test above.
func TestJobOwnerFenceRestartSuppressesOldNotice_t5067(t *testing.T) {
	for _, state := range []string{"lost", "spawned"} {
		t.Run(state, func(t *testing.T) {
			f, action := poolActionFixture(t)
			if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_reservations SET task_role='legacy-reviewer' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
				t.Fatal(err)
			}
			action.Phase = poolActionSettled
			if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
				t.Fatal(err)
			}
			f.jobs.mu.Lock()
			f.jobs.records[f.job]["state"] = state
			f.jobs.records[f.job]["end_reason"] = action.Data.EndReason
			f.jobs.records[f.job]["owner_node"] = "machine-b"
			f.jobs.records[f.job]["owner_epoch"] = float64(2)
			f.jobs.mu.Unlock()
			restartPoolActionHub(t, f)
			f.hub.mu.Lock()
			_, present := f.hub.jobs[action.Job]
			f.hub.mu.Unlock()
			if present {
				t.Fatal("restart unexpectedly loaded an in-memory job")
			}
			if err := f.hub.finishPoolAction(t.Context(), action); err != nil {
				t.Fatal(err)
			}
			if events := spawnFailedInjections(f.destination); len(events) != 0 {
				t.Fatalf("restart published an old-owner notice: %+v", events)
			}
			if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "legacy-reviewer"); err != nil {
				t.Fatalf("restart released the reassigned role: %v", err)
			}
		})
	}
}

func TestJobOwnerFenceRestartOwnerDecision_t5067(t *testing.T) {
	action := poolAction{Node: "machine-a", Epoch: 1}
	for _, tc := range []struct {
		name   string
		record map[string]any
		moved  bool
	}{
		{"legacy", map[string]any{"state": "lost"}, false},
		{"unfenced", map[string]any{"owner_node": "", "owner_epoch": float64(0)}, false},
		{"same owner", map[string]any{"owner_node": "machine-a", "owner_epoch": float64(1)}, false},
		{"new node", map[string]any{"owner_node": "machine-b", "owner_epoch": float64(2)}, true},
		{"new epoch same node", map[string]any{"owner_node": "machine-a", "owner_epoch": float64(3)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			moved, err := poolActionOwnerMoved(action, tc.record)
			if err != nil || moved != tc.moved {
				t.Fatalf("moved=%v err=%v, want %v", moved, err, tc.moved)
			}
		})
	}
}

func TestJobOwnerFenceAssignmentSurvivesNodeRestart_t5067(t *testing.T) {
	inbox := t.TempDir()
	first := &HubClient{jobsInboxRoot: inbox, assignedJobs: make(map[string]uint64)}
	message := hubOutboundMessage{Type: "job.assigned", JobID: "fenced-job", Epoch: 3}
	if err := first.handleHubInboundMessage(t.Context(), nil, message); err != nil {
		t.Fatal(err)
	}
	// A new client has no in-memory grant. The on-disk grant must still keep
	// a close command from an earlier hub epoch from reaching the pane.
	restarted := &HubClient{jobsInboxRoot: inbox, assignedJobs: make(map[string]uint64)}
	_, code := restarted.nodePaneCloseOp(t.Context(), nodePaneCloseOpArgs{Job: "fenced-job", Epoch: 1})
	if code != "epoch_mismatch" {
		t.Fatalf("stale close after restart: code=%q", code)
	}
}

func TestJobOwnerFenceBadAssignmentDoesNotEndSession_t5067(t *testing.T) {
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	badDir := filepath.Join(jobs, "bad-job")
	if err := os.MkdirAll(badDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(badDir, ".panewire-owner-epoch.json")
	if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	var warnings []string
	client := &HubClient{jobsInboxRoot: inbox, assignedJobs: make(map[string]uint64), warn: func(s string) { warnings = append(warnings, s) }}
	bad := hubOutboundMessage{Type: "job.assigned", JobID: "bad-job", Epoch: 2}
	if err := client.handleHubInboundMessage(t.Context(), nil, bad); err != nil {
		t.Fatalf("bad assignment ended the session: %v", err)
	}
	if client.assignedJobs["bad-job"] != 0 || len(warnings) != 1 {
		t.Fatalf("bad assignment advanced or was not warned: grants=%v warnings=%v", client.assignedJobs, warnings)
	}
	good := hubOutboundMessage{Type: "job.assigned", JobID: "good-job", Epoch: 2}
	if err := client.handleHubInboundMessage(t.Context(), nil, good); err != nil || client.assignedJobs["good-job"] != 2 {
		t.Fatalf("other job could not receive a grant: grants=%v err=%v", client.assignedJobs, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := client.handleHubInboundMessage(t.Context(), nil, bad); err != nil || client.assignedJobs["bad-job"] != 2 {
		t.Fatalf("assignment was not retried: grants=%v err=%v", client.assignedJobs, err)
	}
	client.jobsInboxRoot = ""
	if err := client.handleHubInboundMessage(t.Context(), nil, hubOutboundMessage{Type: "job.assigned", JobID: "missing-root", Epoch: 2}); err != nil || client.assignedJobs["missing-root"] != 0 || len(warnings) != 2 {
		t.Fatalf("missing root ended session or advanced grant: err=%v grants=%v warnings=%v", err, client.assignedJobs, warnings)
	}
}

func TestJobOwnerFenceRestartRecognizesOnlyStoredOwner_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	hub.mu.Lock()
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.mu.Unlock()
	old := HubActiveJob{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}
	hub.observeActiveJobs("machine-a", []HubActiveJob{old}, hub.now())
	if hub.jobs["fenced-job"] != nil {
		t.Fatal("the old owner reclaimed a job after hub restart")
	}
	newOwner := HubActiveJob{JobID: "fenced-job", AgentLabel: "worker-b", Epoch: 2}
	hub.observeActiveJobs("machine-b", []HubActiveJob{newOwner}, hub.now())
	if job := hub.jobs["fenced-job"]; job == nil || job.Node != "machine-b" || job.Epoch != 2 {
		t.Fatalf("stored owner not restored after restart: %+v", job)
	}
}

// An old epoch-1 heartbeat cannot become authoritative while hk is down:
// doing so would make the real epoch-2 owner's next heartbeat look stale.
func TestJobOwnerFenceRestartWaitsForOwnerRead_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerTransfer(hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.mu.Unlock()
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: unavailableOwnerStore{hk}, Out: out, Err: errout}, nil
	}
	old := HubActiveJob{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}
	hub.observeActiveJobs("machine-a", []HubActiveJob{old}, hub.now())
	if hub.jobs["fenced-job"] != nil {
		t.Fatal("unreadable hk let an old owner claim the job")
	}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: hk, Out: out, Err: errout}, nil
	}
	current := HubActiveJob{JobID: "fenced-job", AgentLabel: "worker-b", Epoch: 2}
	hub.observeActiveJobs("machine-b", []HubActiveJob{current}, hub.now())
	if job := hub.jobs["fenced-job"]; job == nil || job.Node != "machine-b" || job.Epoch != 2 {
		t.Fatalf("stored owner was not admitted after hk recovered: %+v", job)
	}
}

func TestJobOwnerFenceDefaultClientReadFailureDoesNotAdmit_t5067(t *testing.T) {
	hub, _ := ownerFenceHub(t, &ownerFenceHK{})
	hub.spawnNewApp = nil
	hub.handoffkeep = nil
	if hub.firstSeenJobOwned(t.Context(), "missing-job", "machine-a", 1, false) {
		t.Fatal("an unreadable default job store admitted an unverified owner")
	}
}

func TestJobOwnerFenceCloseRechecksGrantAfterSettle_t5067(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	run := fake.run
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "agent" && args[1] == "list" && fake.called("agent", "list") == 0 {
			if err := jobowner.Assign(filepath.Join(inbox, "jobs"), closeOpJob, 3); err != nil {
				t.Fatal(err)
			}
		}
		return run(ctx, args...)
	}
	_, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-epoch"))
	if code != "epoch_mismatch" || fake.called("pane", "close") != 0 {
		t.Fatalf("close with newer grant: code=%q close calls=%d", code, fake.called("pane", "close"))
	}
}

func TestJobOwnerFenceHTTPMismatchNamesCurrentOwner_t5067(t *testing.T) {
	var update map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `{"state":"spawned","owner_node":"machine-b","owner_epoch":2}`)
			return
		}
		if r.Method != http.MethodPut || json.NewDecoder(r.Body).Decode(&update) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"job_owner_mismatch","owner_node":"machine-b","owner_epoch":2}`)
	}))
	t.Cleanup(server.Close)
	store, err := fleet.NewJobStore(server.URL, "test-token", "", "")
	if err != nil {
		t.Fatal(err)
	}
	supported, err := fleet.TransferJobOwner(t.Context(), store, "fenced-job", fleet.JobOwner{Node: "machine-a", Epoch: 1}, fleet.JobOwner{Node: "machine-b", Epoch: 2})
	if !supported || err == nil || !strings.Contains(err.Error(), "machine-b epoch 2") {
		t.Fatalf("supported=%v err=%v", supported, err)
	}
	if update["owner"] == nil || update["expected_owner"] == nil {
		t.Fatalf("transfer request omitted its atomic owner pair: %v", update)
	}
}

type unavailableOwnerStore struct{ fleet.JobStore }

func (unavailableOwnerStore) Get(context.Context, string) (map[string]any, error) {
	return nil, errors.New("handoffkeep unavailable")
}

func TestJobOwnerFenceUnavailableStoreFailsClosed_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: unavailableOwnerStore{hk}, Out: out, Err: errout}, nil
	}
	if _, err := hub.reassignJob("fenced-job", "machine-b"); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("unreadable owner error=%v", err)
	}
	if len(agent.assignments) != 0 || hub.jobs["fenced-job"].Node != "machine-a" {
		t.Fatal("unreadable owner exposed the successor")
	}
}

// legacyOwnerStore represents an old hk for tests of the pre-existing hub
// reassignment and pool-action protocols.
type legacyOwnerStore struct{}

func (legacyOwnerStore) Claim(context.Context, string, string, string) error { return nil }
func (legacyOwnerStore) Put(context.Context, string, map[string]any) error   { return nil }
func (legacyOwnerStore) Get(context.Context, string) (map[string]any, error) {
	return map[string]any{"state": "spawned"}, nil
}

func useLegacyOwnerStore(h *HubServer) {
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: legacyOwnerStore{}, Out: out, Err: errout}, nil
	}
}

func TestJobOwnerFenceNodeMarkLostDoesNotRetry_t5067(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, _, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	client.machineID, client.nodeJobs = "machine-a", hk
	args := nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"}
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, args)
	if code != "" || result["outcome"] != nodeJobMarkLostNewerClaim {
		t.Fatalf("stale mark lost: result=%v code=%q", result, code)
	}
	_, _ = nodeRun(t, client, nodeCommandJobMarkLost, args)
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if len(hk.updates) != 0 || hk.record["state"] != "spawned" {
		t.Fatalf("stale node retried or changed the successor: updates=%v record=%v", hk.updates, hk.record)
	}
}

func TestJobOwnerFenceReviewerResultDoesNotEndSuccessor_t5067(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Minute)
	client, _, _ := postedResultClient(t, inbox, "idle")
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	client.machineID, client.nodeJobs = "machine-a", hk
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
	if code != "" || result["outcome"] != nodeJobMarkLostNewerClaim {
		t.Fatalf("stale posted-result end: result=%v code=%q", result, code)
	}
	if len(hk.updates) != 0 || hk.record["state"] != "spawned" || len(jobLostEvents(t, inbox)) != 0 {
		t.Fatalf("successor changed: updates=%v record=%v", hk.updates, hk.record)
	}
}

type blockedOwnerTransfer struct {
	*ownerFenceHK
	started chan struct{}
	release chan struct{}
}

func (s *blockedOwnerTransfer) Put(ctx context.Context, id string, fields map[string]any) error {
	if fields["owner"] != nil {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.ownerFenceHK.Put(ctx, id, fields)
}

func TestJobOwnerFenceTransferExcludesReturningOldNode_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	store := &blockedOwnerTransfer{ownerFenceHK: hk, started: make(chan struct{}), release: make(chan struct{})}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	hub.mu.Lock()
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(store.release) }) }
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := hub.reassignJob("fenced-job", "machine-b")
		done <- err
	}()
	var started bool
	eventually(t, time.Second, "transfer starts", func() bool {
		select {
		case <-store.started:
			started = true
		default:
		}
		return started
	})
	old := HubActiveJob{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}
	hub.observeActiveJobs("machine-a", []HubActiveJob{old}, hub.now())
	if hub.observeJobCompletion("machine-a", hubJobEventPayload{JobID: "fenced-job", Epoch: 1}, hub.now()) {
		t.Fatal("old completion was accepted while the transfer was in progress")
	}
	if len(agent.assignments) != 0 {
		t.Fatal("successor was assigned before hk confirmed the transfer")
	}
	release()
	var result error
	var finished bool
	eventually(t, time.Second, "transfer finishes", func() bool {
		select {
		case result = <-done:
			finished = true
		default:
		}
		return finished
	})
	if result != nil || hub.jobs["fenced-job"].Node != "machine-b" || hub.jobs["fenced-job"].Epoch != 2 {
		t.Fatalf("transfer err=%v job=%+v", result, hub.jobs["fenced-job"])
	}
}

func TestJobOwnerFenceReassignHTTPResponse_t5067(t *testing.T) {
	for _, tc := range []struct {
		name      string
		owner     string
		wantCode  int
		wantOwner string
	}{
		{"current owner", "machine-a", http.StatusOK, "machine-b"},
		{"stale owner", "machine-b", http.StatusConflict, "machine-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": tc.owner, "owner_epoch": float64(1)}}
			hub, _ := ownerFenceHub(t, hk)
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs/reassign", strings.NewReader(`{"job_id":"fenced-job","to":"machine-b"}`))
			req.Header.Set(hubAuthorizationHeader, "Bearer "+t14OperatorToken)
			rec := httptest.NewRecorder()
			hub.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status=%d body=%s, want %d", rec.Code, rec.Body.String(), tc.wantCode)
			}
			if tc.wantCode == http.StatusConflict && !strings.Contains(rec.Body.String(), tc.wantOwner) {
				t.Fatalf("conflict did not name the current owner: %s", rec.Body.String())
			}
			if tc.wantCode == http.StatusOK && hk.record["owner_node"] != tc.wantOwner {
				t.Fatalf("owner=%v, want %s", hk.record["owner_node"], tc.wantOwner)
			}
		})
	}
}

func TestJobOwnerFenceNodeCommandKeepsOriginalGrant_t5067(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, _, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	client.machineID, client.nodeJobs = "machine-a", hk
	moved := false
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if !moved {
			moved = true
			if err := jobowner.Assign(filepath.Join(inbox, "jobs"), "job-a", 3); err != nil {
				return nil, err
			}
			hk.mu.Lock()
			hk.record["owner_node"], hk.record["owner_epoch"] = "machine-a", float64(3)
			hk.mu.Unlock()
		}
		return fake.run(ctx, args...)
	}
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostNewerClaim || len(hk.updates) != 0 || hk.record["state"] != "spawned" {
		t.Fatalf("stale command: result=%v code=%q updates=%v record=%v", result, code, hk.updates, hk.record)
	}
	files, err := filepath.Glob(filepath.Join(inbox, "jobs", "job-a", "events", "*-job.revoked.json"))
	if err != nil || len(files) != 0 {
		t.Fatalf("new grant was revoked: files=%v err=%v", files, err)
	}
}
