package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type revokingJobStore struct {
	JobStore
	onGet func() error
}

func (s revokingJobStore) Get(ctx context.Context, id string) (map[string]any, error) {
	if err := s.onGet(); err != nil {
		return nil, err
	}
	return s.JobStore.Get(ctx, id)
}

// A revocation visible after the settlement's initial fold still prevents an
// hk lost write. The raw event simulates an independent ledger writer.
func TestSettlementRechecksRevocationBeforeHKWrite_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	base := a.JobStore
	a.JobStore = revokingJobStore{JobStore: base, onGet: func() error {
		return os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99999-job.revoked.json"), []byte(`{"type":"job.revoked","job_id":"j","epoch":2}`), 0o600)
	}}
	state, err := (HubCloseSettlement{App: a}).Settle(context.Background(), "j")
	if err != nil || state != HubCloseSettled || str(h.record(t, "j"), "state") != "spawned" {
		t.Fatalf("settlement state=%v err=%v hk=%v; want superseded with live hk unchanged", state, err, h.record(t, "j"))
	}
}

// This durable intent is what a node crash between intent and journal leaves.
// A quiesced rollback resolves it without relying on a hub retry.
func TestRollbackDrainsUnregisteredIntentAfterRestart_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	// The replacement node has only the persisted ledger, not an in-flight
	// registration. Begin the same drain the operator CLI invokes.
	restarted := *a
	if err := BeginCloseOpsDrain(context.Background(), a.Jobs); err != nil {
		t.Fatal(err)
	}
	state, err := (HubCloseSettlement{App: &restarted}).Settle(context.Background(), "j")
	pending, scanErr := PendingHubCloseJobs(a.Jobs)
	if err != nil || scanErr != nil || state != HubCloseCancelled || len(pending) != 0 || str(h.record(t, "j"), "state") != "spawned" {
		t.Fatalf("settlement=%v err=%v pending=%v scan=%v hk=%v; want cancelled, no pending and live hk", state, err, pending, scanErr, h.record(t, "j"))
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	op, found, err := ReadCloseOp(root, in.RequestID)
	if err != nil || !found || op.State != CloseOpCancelled || op.Released == nil || op.Attempt != in.Attempt || op.PaneID != in.Pane {
		t.Fatalf("rollback tombstone=%+v found=%v err=%v", op, found, err)
	}
}

func TestRollbackDrainsRegisteredAndInterruptedCloses_t4632(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  string
	}{
		{CloseOpRegistered, CloseOpCancelled},
		{CloseOpClosing, CloseOpUnknown},
		{CloseOpUnknown, CloseOpUnknown},
		{CloseOpGone, CloseOpGone},
	} {
		t.Run(tc.state, func(t *testing.T) {
			a, h := hubCloseFixture(t)
			if err := a.event(context.Background(), "j", "job.spawned", map[string]any{
				"pane_id": "w:p1", "label": "agent-a", "role": "reviewer", "herdr_session": "old-session",
				"harness_session": map[string]any{"kind": "codex", "id": "session-old"},
			}); err != nil {
				t.Fatal(err)
			}
			runner := a.Runner.(*fakeRunner)
			previous := runner.Do
			runner.Do = func(c Command) Result {
				if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
					return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","name":"agent-a","agent_status":"idle","agent_session":{"value":"session-old"}}]}}`}
				}
				return previous(c)
			}
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: tc.state})
			if err := BeginCloseOpsDrain(context.Background(), a.Jobs); err != nil {
				t.Fatal(err)
			}
			// A listed pane is not proof that an unknown close stopped it.
			settlement := HubCloseSettlement{App: a, PaneListed: func(context.Context, string, string) (bool, error) { return true, nil }}
			state, err := settlement.Settle(context.Background(), "j")
			pending, scanErr := PendingHubCloseJobs(a.Jobs)
			if err != nil || scanErr != nil || state != HubCloseCancelled || len(pending) != 0 || str(h.record(t, "j"), "state") != "spawned" {
				t.Fatalf("settlement=%v err=%v pending=%v scan=%v hk=%v", state, err, pending, scanErr, h.record(t, "j"))
			}
			root, err := os.OpenRoot(filepath.Join(a.Jobs, "j"))
			if err != nil {
				t.Fatal(err)
			}
			op, _, err := ReadCloseOp(root, in.RequestID)
			root.Close()
			if err != nil || op.State != tc.want {
				t.Fatalf("journal=%+v err=%v, want %s", op, err, tc.want)
			}
		})
	}
}

func TestRollbackKeepsUnknownCloseWhenPaneWasReassigned_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	if err := a.event(context.Background(), "j", "job.spawned", map[string]any{
		"pane_id": "w:p1", "label": "agent-a", "role": "reviewer", "herdr_session": "old-session",
		"harness_session": map[string]any{"kind": "codex", "id": "session-old"},
	}); err != nil {
		t.Fatal(err)
	}
	runner := a.Runner.(*fakeRunner)
	previous := runner.Do
	runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","name":"agent-a","agent_status":"idle","agent_session":{"value":"session-new"}}]}}`}
		}
		return previous(c)
	}
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpUnknown})
	if err := BeginCloseOpsDrain(context.Background(), a.Jobs); err != nil {
		t.Fatal(err)
	}
	// The pane id is present, but its agent session belongs to a replacement.
	settlement := HubCloseSettlement{App: a, PaneListed: func(context.Context, string, string) (bool, error) { return true, nil }}
	state, err := settlement.Settle(context.Background(), "j")
	pending, scanErr := PendingHubCloseJobs(a.Jobs)
	if err != nil || scanErr != nil || state != HubClosePending || len(pending) != 1 || str(h.record(t, "j"), "state") != "spawned" {
		t.Fatalf("settlement=%v err=%v pending=%v scan=%v hk=%v; want unresolved without changing hk", state, err, pending, scanErr, h.record(t, "j"))
	}
}

func TestRollbackDrainBlocksUnreadableLedgerWithoutIntentName_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	if err := os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99999-broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	pending, err := PendingHubCloseJobs(a.Jobs)
	if err != nil || len(pending) != 1 || pending[0] != "j" {
		t.Fatalf("pending=%v err=%v; unreadable job must block rollback", pending, err)
	}
}
