package panewire

import (
	"testing"
	"time"
)

func TestPoolActionRetryBackoffIsBounded(t *testing.T) {
	previous := time.Duration(0)
	for n := uint8(0); n < 20; n++ {
		delay := poolRetryBackoff(n)
		if delay < previous || delay > 10*time.Minute {
			t.Fatalf("retry %d delay %v after %v", n, delay, previous)
		}
		previous = delay
	}
	if poolRetryBackoff(0) != 10*time.Second || previous != 10*time.Minute {
		t.Fatalf("retry bounds: first=%v last=%v", poolRetryBackoff(0), previous)
	}
}

// The name puts this contract in CI's Postgres migration and store job.
func TestLaneInboxStorePoolActionsSurviveRestart(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			open := backend.fresh(t)
			store := open()
			action := poolAction{ID: "action-1", Job: "job-a", Node: "node-a", Epoch: 3, Phase: poolActionPending, Data: poolActionData{EndReason: "quota_stop pool=codex"}}
			invalid := action
			invalid.ID, invalid.Phase = "invalid-phase", "unknown"
			if err := store.insertPoolAction(t.Context(), invalid); err == nil {
				t.Fatal("migration accepted an unknown action phase")
			}
			invalid.ID, invalid.Phase, invalid.Epoch = "invalid-epoch", poolActionPending, 0
			if err := store.insertPoolAction(t.Context(), invalid); err == nil {
				t.Fatal("migration accepted an unbound epoch")
			}
			if err := store.insertPoolAction(t.Context(), action); err != nil {
				t.Fatal(err)
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			store = open()
			id, err := store.pendingPoolAction(t.Context(), action.Job)
			if err != nil || id != action.ID {
				t.Fatalf("pending = %q, %v", id, err)
			}
			rows, err := store.activePoolActions(t.Context())
			if err != nil || len(rows) != 1 || rows[0].Epoch != 3 || rows[0].Data.EndReason != action.Data.EndReason {
				t.Fatalf("actions = %+v, %v", rows, err)
			}
			action.Phase = poolActionGuarded
			if err := store.savePoolAction(t.Context(), action); err != nil {
				t.Fatal(err)
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			store = open()
			if id, err := store.pendingPoolAction(t.Context(), action.Job); err != nil || id != action.ID {
				t.Fatalf("guard did not survive restart: %q, %v", id, err)
			}
			if rows, err := store.activePoolActions(t.Context()); err != nil || len(rows) != 1 || rows[0].Phase != poolActionGuarded {
				t.Fatalf("guard not reconciled: %+v, %v", rows, err)
			}
			action.Phase = poolActionSettled
			if err := store.savePoolAction(t.Context(), action); err != nil {
				t.Fatal(err)
			}
			if id, err := store.pendingPoolAction(t.Context(), action.Job); err != nil || id != "" {
				t.Fatalf("final action remains pending: %q, %v", id, err)
			}
		})
	}
}
