package panewire

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPoolActionDisabledStoreCannotProveNoPriorAction(t *testing.T) {
	f, action := poolActionFixture(t)
	path := filepath.Join(filepath.Dir(f.hub.inbox.deferredPath), "inbox.sqlite3")
	_ = f.node.CloseNow()
	if err := f.hub.Close(); err != nil {
		t.Fatal(err)
	}
	// Starting without the store does not erase its previously persisted row.
	oldStore, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldStore.close() })
	if id, err := oldStore.pendingPoolAction(t.Context(), action.Job); err != nil || id != action.ID {
		t.Fatalf("prior store has pending action %q, err=%v", id, err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.jobs[action.Job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: action.Job, Epoch: 1}, Node: action.Node, Orphaned: true}
	if _, err := hub.reassignJob(action.Job, "machine-b"); err == nil || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("reassignment without the prior store was not fenced: %v", err)
	}
}

func TestPoolActionRoleReleaseRejectsAReassignedHolder(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = poolActionRefused
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatal(err)
	}
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if released, probe := f.hub.releaseFinishedSpawnRole(ctx, app, f.holder, map[string]any{"state": "spawned"}, "legacy-reviewer"); released || probe.reason != "job_owner_changed" {
		t.Fatalf("released=%v probe=%+v; want reassigned holder refused", released, probe)
	}
}

func TestPoolActionReassignmentWaitsForRoleRelease(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = poolActionRefused
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	unlock, ok := f.hub.lockWithoutPoolAction(t.Context(), action.Job)
	if !ok {
		t.Fatal("could not start role release")
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := f.hub.reassignJob(action.Job, "machine-b")
		done <- err
	}()
	<-started
	var result error
	early := waitUntil(300*time.Millisecond, func() bool {
		select {
		case result = <-done:
			return true
		default:
			return false
		}
	})
	unlock()
	if !early {
		result = <-done
	}
	if early || result != nil {
		t.Fatalf("reassignment returned while role release held the job: early=%v err=%v", early, result)
	}
}

func TestPoolActionSettledReassignmentSuppressesOldNotice(t *testing.T) {
	f, action := poolActionFixture(t)
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_reservations SET task_role='legacy-reviewer' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	action.Phase = poolActionSettled
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = action.Data.EndReason
	f.jobs.mu.Unlock()
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("settled action must allow reassignment: %v", err)
	}
	if err := f.hub.finishPoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("old action notified after reassignment: %+v", events)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "legacy-reviewer"); err != nil {
		t.Fatalf("old action released the reassigned role: %v", err)
	}
}
