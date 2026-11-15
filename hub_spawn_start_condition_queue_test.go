package panewire

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const startConditionTestToken = "start-condition-test"

func TestConditionedRowsDoNotStarveRunnableSpawnPastPendingPage(t *testing.T) {
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks/9000" || r.Header.Get("Authorization") != "Bearer "+startConditionTestToken {
			t.Errorf("unexpected HK request %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":9000,"state":"ready","relations":[]}`))
	}))
	defer hk.Close()
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", startConditionTestToken)
	h := asyncSpawnHub(t)
	now := time.Now()
	for i := range 33 {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(3000 + i)
		job := fmt.Sprintf("waiting-%02d", i)
		request.Args = replaceSpawnArg(request.Args, "--job", job)
		request.Args = replaceSpawnArg(request.Args, "--task", fmt.Sprintf("%d", 5000+i))
		request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "9000", State: "merged"}}
		conditioned, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", now.Add(time.Duration(i)*time.Millisecond))
		if err != nil {
			t.Fatalf("claim conditioned row %d: %v", i, err)
		}
		if _, err := h.inbox.store.bindSpawn(t.Context(), request.RequestID, conditioned.Attempt, hubSpawnRequest{
			RequestID: t14RequestID(3200 + i), Machine: "machine-a", WaitSeconds: 600,
		}, time.Now().UTC()); err != nil {
			t.Fatalf("bind waiting row %d: %v", i, err)
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

	page, err := h.inbox.store.pendingSpawns(t.Context(), now.Add(time.Second))
	if err != nil || len(page) != 1 || page[0].Request.RequestID != runnable.RequestID {
		t.Fatalf("runnable page has %d rows, %v; waiting rows must not consume its slots", len(page), err)
	}
	conditioned, err := h.inbox.store.pendingStartConditionSpawns(t.Context(), now.Add(time.Second))
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
