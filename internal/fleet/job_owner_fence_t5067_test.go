package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// fenceJobStore models hk's conditional update: an omitted condition remains
// an unfenced legacy update, while a stale condition changes nothing.
type fenceJobStore struct {
	*fakeJobStore
	puts       int
	mismatches int
}

func (s *fenceJobStore) Claim(ctx context.Context, id, task, host string) error {
	if err := s.fakeJobStore.Claim(ctx, id, task, host); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[id]["owner_node"]; !exists {
		s.records[id]["owner_node"], s.records[id]["owner_epoch"] = "", float64(0)
	}
	return nil
}

func (s *fenceJobStore) Put(ctx context.Context, id string, fields map[string]any) error {
	s.puts++
	var update struct {
		Expected *JobOwner `json:"expected_owner"`
		Owner    *JobOwner `json:"owner"`
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &update); err != nil {
		return err
	}
	if update.Expected != nil {
		record, err := s.fakeJobStore.Get(ctx, id)
		if err != nil {
			return err
		}
		if update.Expected.Node != record["owner_node"] || float64(update.Expected.Epoch) != record["owner_epoch"] {
			s.mismatches++
			return &BackendError{Status: http.StatusConflict, Code: "job_owner_mismatch", OwnerNode: str(record, "owner_node"), OwnerEpoch: uint64(record["owner_epoch"].(float64))}
		}
	}
	copyFields := make(map[string]any, len(fields))
	for key, value := range fields {
		if key != "expected_owner" && key != "owner" {
			copyFields[key] = value
		}
	}
	if len(copyFields) != 0 {
		if err := s.fakeJobStore.Put(ctx, id, copyFields); err != nil {
			return err
		}
	}
	if update.Owner != nil {
		s.mu.Lock()
		s.records[id]["owner_node"], s.records[id]["owner_epoch"] = update.Owner.Node, float64(update.Owner.Epoch)
		s.mu.Unlock()
	}
	return nil
}

// The hub has transferred the shared record to B but A has not yet received
// the revocation. A's reaper must get a 409 rather than end B's record.
func TestJobOwnerFenceReapBeforeRevocation_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	putEvent(t, a, "j", "job.lost", map[string]any{})
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	base.records["j"]["owner_node"] = "machine-b"
	base.records["j"]["owner_epoch"] = float64(2)
	store := &fenceJobStore{fakeJobStore: base}
	a.JobStore = store

	a.markEndedJobLost(t.Context(), job)
	if got := base.record(t, "j")["state"]; got != "spawned" {
		t.Fatalf("successor record state=%v, want spawned after stale node's lost write", got)
	}
	if store.puts != 1 || store.mismatches != 1 {
		t.Fatalf("puts=%d owner mismatches=%d, want one rejected PUT and no retry", store.puts, store.mismatches)
	}
}

func staleFenceJob(t *testing.T) (*App, *fenceJobStore, Job) {
	t.Helper()
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "machine-b", float64(2)
	store := &fenceJobStore{fakeJobStore: base}
	a.JobStore = store
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	return a, store, j
}

func TestJobOwnerFenceClaimAndLanding_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	store := &fenceJobStore{fakeJobStore: a.JobStore.(*fakeJobStore)}
	a.JobStore = store
	s := Spawn{Job: "j", Task: "42", Role: "implementer", Label: "agent-a", Profile: "cmd-dsflash41"}
	if err := a.claimJobRecord(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	a.spawnedJobRecord(t.Context(), s, Seat{Family: "openai"}, Launch{}, "w:p1", "pool", "owner")
	record := store.record(t, "j")
	if record["state"] != "spawned" || record["owner_node"] != "test-host" || record["owner_epoch"] != float64(1) {
		t.Fatalf("landed record=%v, want spawned owned by test-host epoch 1", record)
	}
}

// A job can already be running when an older hk is upgraded to the fence API.
func TestJobOwnerFenceAdoptsRunningLegacyJob_t5067(t *testing.T) {
	for _, epoch := range []uint64{1, 3} {
		t.Run(strconv.FormatUint(epoch, 10), func(t *testing.T) {
			jobs := filepath.Join(t.TempDir(), "jobs")
			base := newFakeJobStore()
			if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
				t.Fatal(err)
			}
			base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "", float64(0)
			if epoch > 1 {
				if err := jobowner.Assign(jobs, "j", epoch); err != nil {
					t.Fatal(err)
				}
			}
			store := &fenceJobStore{fakeJobStore: base}
			if err := PutOwnedJob(t.Context(), store, jobs, "test-host", "j", map[string]any{"model": "continued"}, nil); err != nil {
				t.Fatalf("running job was not adopted: %v", err)
			}
			if record := base.record(t, "j"); record["owner_node"] != "test-host" || record["owner_epoch"] != float64(epoch) || record["model"] != "continued" {
				t.Fatalf("record=%v, want adopted owner and update", record)
			}
		})
	}
}

func TestJobOwnerFenceTransferRefusesDifferentOwner_t5067(t *testing.T) {
	base := newFakeJobStore()
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "machine-c", float64(1)
	store := &fenceJobStore{fakeJobStore: base}
	supported, err := TransferJobOwner(t.Context(), store, "j", JobOwner{Node: "machine-a", Epoch: 1}, JobOwner{Node: "machine-b", Epoch: 2})
	if !supported || err == nil || !strings.Contains(err.Error(), "machine-c epoch 1") {
		t.Fatalf("stale transfer supported=%v err=%v, want current owner machine-c epoch 1", supported, err)
	}
	if record := base.record(t, "j"); record["owner_node"] != "machine-c" || record["owner_epoch"] != float64(1) {
		t.Fatalf("stale transfer changed current owner: %v", record)
	}
}

type mismatchThenRegrant struct {
	*fenceJobStore
	jobs string
}

func (s mismatchThenRegrant) Put(ctx context.Context, id string, fields map[string]any) error {
	err := s.fenceJobStore.Put(ctx, id, fields)
	if IsJobOwnerMismatch(err) {
		if grantErr := jobowner.Assign(s.jobs, id, 3); grantErr != nil {
			return grantErr
		}
		s.mu.Lock()
		s.records[id]["owner_node"], s.records[id]["owner_epoch"] = "test-host", float64(3)
		s.mu.Unlock()
	}
	return err
}

// The response still describes B/2 even though A has already been granted 3.
func TestJobOwnerFenceDelayedMismatchDoesNotRevokeNewGrant_t5067(t *testing.T) {
	a := testApp(t)
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "machine-b", float64(2)
	store := mismatchThenRegrant{fenceJobStore: &fenceJobStore{fakeJobStore: base}, jobs: a.Jobs}
	if err := PutOwnedJob(t.Context(), store, a.Jobs, "test-host", "j", map[string]any{"model": "old"}, nil); !IsJobOwnerMismatch(err) {
		t.Fatalf("old write error=%v, want owner mismatch", err)
	}
	if err := PutOwnedJob(t.Context(), store.fenceJobStore, a.Jobs, "test-host", "j", map[string]any{"model": "new"}, nil); err != nil {
		t.Fatalf("delayed mismatch revoked the new grant: %v", err)
	}
}

func TestJobOwnerFenceUsageAndSweepWrites_t5067(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*App, Job)
	}{
		{"usage", func(a *App, j Job) { a.writeJobSession(t.Context(), j, jobSession{}) }},
		{"sweep", func(a *App, j Job) { a.markSweptJob(t.Context(), j, "lost") }},
		{"lost", func(a *App, j Job) { _ = a.MarkJobLost(t.Context(), j.ID) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, store, job := staleFenceJob(t)
			test.write(a, job)
			record := store.record(t, job.ID)
			if record["state"] != "spawned" || store.puts != 1 || store.mismatches != 1 {
				t.Fatalf("record=%v puts=%d mismatches=%d, want one refused write", record, store.puts, store.mismatches)
			}
		})
	}
}

func TestJobOwnerFenceCompletionNeverRetriesMismatch_t5067(t *testing.T) {
	a, store, _ := staleFenceJob(t)
	_, err := a.strictCompletedPutUntil(t.Context(), "j", map[string]any{"state": "completed"}, nil, nil, time.Now().Add(time.Second))
	if !isDoneResultFailure(err, "job_reassigned") {
		t.Fatalf("completion error=%v, want reassigned", err)
	}
	if store.puts != 1 || store.mismatches != 1 || store.record(t, "j")["state"] != "spawned" {
		t.Fatalf("puts=%d mismatches=%d record=%v; want one refusal", store.puts, store.mismatches, store.record(t, "j"))
	}
}

type moveOwnerAfterUsage struct{ *fenceJobStore }

func (s moveOwnerAfterUsage) Put(ctx context.Context, id string, fields map[string]any) error {
	err := s.fenceJobStore.Put(ctx, id, fields)
	if err == nil && fields["usage"] != nil {
		s.mu.Lock()
		s.records[id]["owner_node"], s.records[id]["owner_epoch"] = "machine-b", float64(2)
		s.mu.Unlock()
	}
	return err
}

func TestJobOwnerFenceModelWrite_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "test-host", float64(1)
	base.records["j"]["model"] = "successor-model"
	store := &fenceJobStore{fakeJobStore: base}
	a.JobStore = moveOwnerAfterUsage{store}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	a.recordJobUsage(t.Context(), j, false)
	if store.puts != 2 || store.mismatches != 1 || base.record(t, "j")["model"] != "successor-model" {
		t.Fatalf("puts=%d mismatches=%d model=%v, want the stale model refused", store.puts, store.mismatches, base.record(t, "j")["model"])
	}
}

type switchingFenceStore struct {
	*fenceJobStore
	legacy bool
}

func (s *switchingFenceStore) Get(ctx context.Context, id string) (map[string]any, error) {
	record, err := s.fenceJobStore.Get(ctx, id)
	if err == nil && s.legacy {
		delete(record, "owner_node")
		delete(record, "owner_epoch")
	}
	return record, err
}

func TestJobOwnerFenceOldServerWarningOncePerStreak_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "test-host", float64(1)
	store := &switchingFenceStore{fenceJobStore: &fenceJobStore{fakeJobStore: base}, legacy: true}
	a.JobStore = store
	for _, legacy := range []bool{true, true, false, false, true} {
		store.legacy = legacy
		if err := a.putJobRecord(t.Context(), "j", map[string]any{"model": "model-a"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(a.Err.(*bytes.Buffer).String(), "job owner fencing unavailable"); got != 2 {
		t.Fatalf("warnings=%d, want one for each legacy streak", got)
	}
}

func TestJobOwnerFenceLatePinRefusesMovedOwner_t5067(t *testing.T) {
	a, store, _ := staleFenceJob(t)
	store.records["j"]["state"] = "completed"
	_, err := a.pinReport(t.Context(), "j", "reports/j/result.md", strings.Repeat("a", 64))
	if !isDoneResultFailure(err, "job_reassigned") || store.puts != 1 || store.mismatches != 1 {
		t.Fatalf("pin error=%v puts=%d mismatches=%d", err, store.puts, store.mismatches)
	}
	if _, pinned := store.records["j"]["report_doc"]; pinned {
		t.Fatal("stale node pinned a report on the successor record")
	}
}

func TestJobOwnerFenceMismatchClosesLocalPane_t5067(t *testing.T) {
	a := testApp(t)
	runner := ledgerRunner(t, a)
	baseRunner := runner.Do
	runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"idle"}]}}`}
		}
		return baseRunner(c)
	}
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "reviewer")
	putEvent(t, a, "j", "job.lost", map[string]any{})
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "machine-b", float64(2)
	store := &fenceJobStore{fakeJobStore: base}
	a.JobStore = store
	reapJob(t, a)
	if closed := closedPanes(runner); len(closed) != 1 {
		t.Fatalf("closed panes=%v, want the stale local pane closed", closed)
	}
	if store.puts != 1 || store.mismatches != 1 || base.record(t, "j")["state"] != "spawned" {
		t.Fatalf("puts=%d mismatches=%d record=%v", store.puts, store.mismatches, base.record(t, "j"))
	}
}

type ownerMoveDuringRead struct {
	*fenceJobStore
	jobs  string
	moved bool
}

func (s *ownerMoveDuringRead) Get(ctx context.Context, id string) (map[string]any, error) {
	record, err := s.fenceJobStore.Get(ctx, id)
	if err == nil && !s.moved {
		s.moved = true
		if err := jobowner.Assign(s.jobs, id, 3); err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.records[id]["owner_node"], s.records[id]["owner_epoch"] = "test-host", float64(3)
		s.mu.Unlock()
	}
	return record, err
}

// A stale operation started at epoch 1 must not borrow the new epoch after the
// hub moves the job A→B→A while that operation is reading hk.
func TestJobOwnerFenceOldOperationCannotBorrowNewGrant_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	putEvent(t, a, "j", "job.lost", map[string]any{})
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "test-host", float64(1)
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	store := &ownerMoveDuringRead{fenceJobStore: &fenceJobStore{fakeJobStore: base}, jobs: a.Jobs}
	a.JobStore = store
	a.markEndedJobLost(t.Context(), job)
	if record := base.record(t, "j"); record["state"] != "spawned" || store.mismatches != 1 {
		t.Fatalf("old operation borrowed newer epoch: record=%v mismatches=%d", record, store.mismatches)
	}
	current, err := a.job("j")
	if err != nil || current.Reassigned || current.OwnerEpoch != 3 {
		t.Fatalf("new grant was revoked: job=%+v err=%v", current, err)
	}
	if err := a.putJobRecord(withJobEpoch(t.Context(), current), "j", map[string]any{"model": "new-owner"}); err != nil {
		t.Fatalf("newly granted owner could not write: %v", err)
	}
}

func TestJobOwnerFenceOldUsageDoesNotWriteModelAfterMismatch_t5067(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	base.records["j"]["owner_node"], base.records["j"]["owner_epoch"] = "test-host", float64(1)
	base.records["j"]["model"] = "new-owner-model"
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	store := &ownerMoveDuringRead{fenceJobStore: &fenceJobStore{fakeJobStore: base}, jobs: a.Jobs}
	a.JobStore = store
	err = a.recordJobUsage(t.Context(), j, false)
	if !IsJobReassigned(err) || store.puts != 1 || store.mismatches != 1 || base.record(t, "j")["model"] != "new-owner-model" {
		t.Fatalf("err=%v puts=%d mismatches=%d record=%v", err, store.puts, store.mismatches, base.record(t, "j"))
	}
}

func TestJobOwnerFencePreLandingAbort_t5067(t *testing.T) {
	a := testApp(t)
	store := &fenceJobStore{fakeJobStore: a.JobStore.(*fakeJobStore)}
	a.JobStore = store
	if err := a.claimJobRecord(t.Context(), Spawn{Job: "j", Task: "42"}); err != nil {
		t.Fatal(err)
	}
	a.abandonJobRecord(t.Context(), "j")
	record := store.record(t, "j")
	if record["state"] != "lost" || record["owner_node"] != "test-host" || record["owner_epoch"] != float64(1) || store.puts != 1 {
		t.Fatalf("pre-landing abort=%v puts=%d", record, store.puts)
	}
}
