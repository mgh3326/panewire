package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	if len(hk.updates) != 2 {
		t.Fatalf("updates=%v, want one transfer and one rejected write", hk.updates)
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
	select {
	case assignment := <-newAgent.assignments:
		if assignment.JobID != "fenced-job" || assignment.Epoch != 2 {
			t.Fatalf("recovered assignment=%+v", assignment)
		}
	default:
		t.Fatal("hub restart lost the successor assignment")
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
	if found, err := hub.hasOwnerTransfer(intent); err != nil || found {
		t.Fatalf("acknowledged transfer journal found=%v err=%v", found, err)
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
	t.Skip("hk 5067 comment 25973: waiting for the handoffkeep task allowing conditional terminal owner-only transfers")
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
	if len(hk.updates) != 1 || hk.record["state"] != "spawned" {
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
	if len(hk.updates) != 1 || hk.record["state"] != "spawned" || len(jobLostEvents(t, inbox)) != 0 {
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
	if code != "" || result["outcome"] != nodeJobMarkLostNewerClaim || len(hk.updates) != 1 || hk.record["state"] != "spawned" {
		t.Fatalf("stale command: result=%v code=%q updates=%v record=%v", result, code, hk.updates, hk.record)
	}
	files, err := filepath.Glob(filepath.Join(inbox, "jobs", "job-a", "events", "*-job.revoked.json"))
	if err != nil || len(files) != 0 {
		t.Fatalf("new grant was revoked: files=%v err=%v", files, err)
	}
}
