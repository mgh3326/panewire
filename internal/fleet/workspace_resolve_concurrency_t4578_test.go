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
	for i := range n {
		a := testApp(t)
		a.HerdrSession = "worker"
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
