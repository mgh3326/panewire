package fleet

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// Concurrent node spawns (hk 4578) run this engine in one process: two that
// both find the label absent must not each create a workspace.
func TestResolveWorkspaceCreatesMissingLabelOnceUnderConcurrency_t4578(t *testing.T) {
	var mu sync.Mutex
	var workspaces []string
	creates := 0
	runner := &fakeRunner{Do: func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "workspace list":
			mu.Lock()
			defer mu.Unlock()
			rows := make([]string, 0, len(workspaces))
			for _, id := range workspaces {
				rows = append(rows, `{"workspace_id":"`+id+`","label":"workers"}`)
			}
			return Result{Out: `{"result":{"workspaces":[` + strings.Join(rows, ",") + `]}}`}
		case "workspace create --label workers":
			// Simulated latency: wide enough for an unserialized second
			// resolve to list the label as absent.
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			creates++
			id := "w" + string(rune('0'+creates))
			workspaces = append(workspaces, id)
			return Result{Out: `{"result":{"workspace":{"workspace_id":"` + id + `"},"tab":{"tab_id":"` + id + `:t1"},"root_pane":{"pane_id":"` + id + `:p0"}}}`}
		}
		t.Errorf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	const n = 3
	got := make([]string, n)
	var wg sync.WaitGroup
	state := t.TempDir()
	for i := range n {
		a := testApp(t)
		// Separate apps sharing one state dir: the lock is the file.
		a.HerdrSession, a.StateDir = "worker", state
		a.Runner = runner
		wg.Go(func() {
			id, _, err := a.resolveWorkspace(context.Background(), "workers")
			if err != nil {
				t.Errorf("resolve %d: %v", i, err)
			}
			got[i] = id
		})
	}
	wg.Wait()
	if creates != 1 || got[0] != "w1" || got[1] != "w1" || got[2] != "w1" {
		t.Fatalf("creates=%d workspaces=%v", creates, got)
	}
}

// A spawn waiting on another's workspace resolution leaves when its own
// context ends, and the hold is a file lock another process shares.
func TestResolveWorkspaceWaitEndsWithContext_t4578(t *testing.T) {
	holder := testApp(t)
	holder.HerdrSession = "worker"
	release := make(chan struct{})
	entered := make(chan struct{})
	holder.Runner = &fakeRunner{Do: func(c Command) Result {
		close(entered)
		<-release
		return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"}]}}`}
	}}
	done := make(chan struct{})
	go func() { defer close(done); _, _, _ = holder.resolveWorkspace(context.Background(), "workers") }()
	<-entered
	waiter := testApp(t)
	waiter.HerdrSession, waiter.StateDir, waiter.Home = "worker", holder.stateDir(), holder.Home
	waiter.Runner = &fakeRunner{Do: func(c Command) Result {
		t.Errorf("waiter ran herdr %v while the label was held", c.Args)
		return Result{}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, _, err := waiter.resolveWorkspace(ctx, "workers"); err == nil {
		t.Fatal("waiter resolved while the label was held")
	}
	close(release)
	<-done
}
