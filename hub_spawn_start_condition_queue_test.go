package panewire

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestConditionedRowsDoNotStarveRunnableSpawnPastPendingPage(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	for i := range 33 {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(3000 + i)
		job := fmt.Sprintf("waiting-%02d", i)
		request.Args = replaceSpawnArg(request.Args, "--job", job)
		request.Args = replaceSpawnArg(request.Args, "--task", fmt.Sprintf("%d", 5000+i))
		request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "9000", State: "merged"}}
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", now.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatalf("claim conditioned row %d: %v", i, err)
		}
	}
	runnable := durableSpawnRequest()
	runnable.RequestID = t14RequestID(3100)
	runnableJob := "runnable-after-waiters"
	runnable.Args = replaceSpawnArg(runnable.Args, "--job", runnableJob)
	runnable.Args = replaceSpawnArg(runnable.Args, "--task", "9100")
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), runnable, runnableJob, "parent", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	page, err := h.inbox.store.pendingSpawns(t.Context())
	if err != nil || len(page) != 1 || page[0].Request.RequestID != runnable.RequestID {
		t.Fatalf("runnable page has %d rows, %v; waiting rows must not consume its slots", len(page), err)
	}
	conditioned, err := h.inbox.store.pendingStartConditionSpawns(t.Context())
	if err != nil || len(conditioned) != 32 {
		t.Fatalf("bounded conditioned pass has %d rows, %v; want 32", len(conditioned), err)
	}

	appStarted := make(chan struct{}, 1)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		select {
		case appStarted <- struct{}{}:
		default:
		}
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		h.runSpawnCoordinator(ctx)
		close(done)
	}()
	eventually(t, 5*time.Second, "runnable spawn reaching placement while 33 conditioned rows were pending", func() bool {
		select {
		case <-appStarted:
			return true
		default:
			return false
		}
	})
	cancel()
	<-done
}
