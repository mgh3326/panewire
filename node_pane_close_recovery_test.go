package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestCloseOpsRecoverySkipsCancelledContext(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client.runHubCloseSettle(ctx)
	_, err := os.Stat(filepath.Join(inbox, "jobs", ".close-ops-admission.lock"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled startup created the recovery lock: %v", err)
	}
}

type recoverySignalLedger struct {
	*fakeHubClose
	settled chan string
}

func (ledger *recoverySignalLedger) Settle(_ context.Context, job string) (fleet.HubCloseState, error) {
	ledger.settled <- job
	return fleet.HubCloseCancelled, nil
}

func TestCloseOpsRecoveryContinuesPastUnreadableJob(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	badEvents := filepath.Join(inbox, "jobs", "bad-job", "events")
	if err := os.MkdirAll(badEvents, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badEvents, "00001-job.claim.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"legacy-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a","end_reason":"quota_stop pool=devin-free"}}`)
	seedNodeCloseOp(t, inbox, "legacy-op", fleet.CloseOpRegistered)
	root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	op, _, err := fleet.ReadCloseOp(root, "legacy-op")
	if err != nil {
		t.Fatal(err)
	}
	op.Epoch = 0
	if err := fleet.WriteCloseOp(root, op); err != nil {
		t.Fatal(err)
	}
	root.Close()
	ledger := &recoverySignalLedger{fakeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}, settled: make(chan string, 4)}
	client.nodeHubClose = ledger
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.runHubCloseSettle(ctx) }()
	settled := false
	finished := waitUntil(3*time.Second, func() bool {
		select {
		case job := <-ledger.settled:
			settled = settled || job == closeOpJob
		default:
		}
		root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
		if err != nil {
			return false
		}
		defer root.Close()
		op, _, err := fleet.ReadCloseOp(root, "legacy-op")
		return settled && err == nil && op.State == fleet.CloseOpCancelled
	})
	cancel()
	<-done
	root, err = os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	op, _, err = fleet.ReadCloseOp(root, "legacy-op")
	if err != nil || !finished || !settled || op.State != fleet.CloseOpCancelled {
		t.Fatalf("healthy job settled=%v legacy state=%q err=%v", settled, op.State, err)
	}
}

func TestCloseOpsRecoveryContinuesPastBusyJob(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	badDir := filepath.Join(inbox, "jobs", "bad-job")
	badEvents := filepath.Join(badDir, "events")
	if err := os.MkdirAll(badEvents, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"00001-job.claim.json":            `{"kind":"job.claim"}`,
		"00002-job.spawned.json":          `{"kind":"job.spawned","payload":{"pane_id":"w2:p1","label":"bad","role":"reviewer"}}`,
		"00003-job.hub_close_intent.json": `{"kind":"job.hub_close_intent","payload":{"request_id":"bad-op","attempt":"00002-job.spawned.json","pane_id":"w2:p1","label":"bad","end_reason":"quota_stop pool=devin-free"}}`,
	} {
		if err := os.WriteFile(filepath.Join(badEvents, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"legacy-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a","end_reason":"quota_stop pool=devin-free"}}`)
	seedNodeCloseOp(t, inbox, "legacy-op", fleet.CloseOpRegistered)
	root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	op, _, err := fleet.ReadCloseOp(root, "legacy-op")
	if err != nil {
		t.Fatal(err)
	}
	op.Epoch = 0
	if err := fleet.WriteCloseOp(root, op); err != nil {
		t.Fatal(err)
	}
	root.Close()
	unlock, err := fleet.LockJobEnd(badDir)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &recoverySignalLedger{fakeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}, settled: make(chan string, 4)}
	client.nodeHubClose = ledger
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.runHubCloseSettle(ctx) }()
	settled := waitUntil(time.Second, func() bool {
		select {
		case job := <-ledger.settled:
			return job == closeOpJob
		default:
			return false
		}
	})
	recovered := waitUntil(4*time.Second, func() bool {
		root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
		if err != nil {
			return false
		}
		defer root.Close()
		op, _, err := fleet.ReadCloseOp(root, "legacy-op")
		return err == nil && op.State == fleet.CloseOpCancelled
	})
	unlock()
	cancel()
	<-done
	if !settled || !recovered {
		t.Fatalf("healthy job while another end lock was held: settled=%v recovered=%v", settled, recovered)
	}
}
