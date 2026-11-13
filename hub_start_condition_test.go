package panewire

import (
	"io"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestUnboundLegacyStartConditionCannotStartBeforeQueueCore(t *testing.T) {
	h := asyncSpawnHub(t)
	spawnAppCreated := false
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		spawnAppCreated = true
		return nil, nil
	}
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(1200)
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), task)
	if spawnAppCreated {
		t.Fatal("legacy conditioned request reached spawn execution")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "failed" || saved.Outcome.Reason != "start_conditions_unsupported" {
		t.Fatalf("legacy conditioned request = %#v, %v; want terminal unsupported outcome", saved, err)
	}
}
