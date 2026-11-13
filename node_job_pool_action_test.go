package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func readPoolActionMarker(t *testing.T, inbox, job string) (map[string]any, bool) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(inbox, "jobs", job, nodePoolActionFile))
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var marker map[string]any
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	return marker, true
}

// job.pool_action writes the marker beside the job's ledger and removes it, so
// the holder's watcher can see the hub's action before its pane is closed.
func TestNodeJobPoolActionRegistersAndClearsTheMarker(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	const endReason = "quota_stop pool=devin reset_at=2026-11-14T10:00:00Z"
	result, code := nodeRun(t, client, nodeCommandJobPoolAction, nodeJobPoolActionArgs{
		Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionRegister, EndReason: endReason,
	})
	if code != "" || result["outcome"] != nodePoolActionRegistered {
		t.Fatalf("code=%q result=%v", code, result)
	}
	marker, found := readPoolActionMarker(t, inbox, "job-a")
	if !found {
		t.Fatal("no marker written")
	}
	if marker["action_id"] != "0b3f4b1e-0000-4000-8000-000000000001:1" || marker["end_reason"] != endReason || marker["at"] == nil {
		t.Fatalf("marker=%v", marker)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("registering wrote hk: %v", store.writes())
	}

	result, code = nodeRun(t, client, nodeCommandJobPoolAction, nodeJobPoolActionArgs{
		Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionClear,
	})
	if code != "" || result["outcome"] != nodePoolActionCleared {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if _, found := readPoolActionMarker(t, inbox, "job-a"); found {
		t.Fatal("the marker survived a clear")
	}
}

// A marker never outlives its attempt: a completed or reclaimed job refuses
// one, and a job with no local ledger has nowhere to hold it.
func TestNodeJobPoolActionRefusesAnotherAttemptOrNoLedger(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned", "job.completed")
	client, _, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobPoolAction, nodeJobPoolActionArgs{
		Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionRegister, EndReason: "quota_stop pool=devin",
	})
	if code != "" || result["outcome"] != nodePoolActionMoved {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if _, found := readPoolActionMarker(t, inbox, "job-a"); found {
		t.Fatal("a marker was written for a completed attempt")
	}

	empty := t.TempDir()
	client, _, _ = markLostClient(t, empty, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code = nodeRun(t, client, nodeCommandJobPoolAction, nodeJobPoolActionArgs{
		Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionRegister, EndReason: "quota_stop pool=devin",
	})
	if code != "" || result["outcome"] != nodePoolActionNoLedger {
		t.Fatalf("code=%q result=%v", code, result)
	}
}

// The command is a closed shape: it needs an attempt identity, an action id and
// a valid op, and a clear carries no end reason.
func TestNodeJobPoolActionValidatesItsArguments(t *testing.T) {
	base := nodeJobPoolActionArgs{
		Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionRegister, EndReason: "quota_stop pool=devin",
	}
	for _, test := range []struct {
		name string
		args nodeJobPoolActionArgs
		want bool
	}{
		{"register", base, true},
		{"no identity", func() nodeJobPoolActionArgs { a := base; a.PaneID, a.Label = "", ""; return a }(), false},
		{"bad action id", func() nodeJobPoolActionArgs { a := base; a.ActionID = "not an id"; return a }(), false},
		{"unknown op", func() nodeJobPoolActionArgs { a := base; a.Op = "drop"; return a }(), false},
		{"clear with a reason", func() nodeJobPoolActionArgs { a := base; a.Op = nodePoolActionClear; return a }(), false},
		{"clear", func() nodeJobPoolActionArgs { a := base; a.Op = nodePoolActionClear; a.EndReason = ""; return a }(), true},
	} {
		if got := validNodeJobPoolActionArgs(test.args); got != test.want {
			t.Fatalf("%s: valid=%v, want %v", test.name, got, test.want)
		}
	}
}

func TestNodeJobPoolActionFinishesTheMarker(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, _, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	args := nodeJobPoolActionArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: "register", EndReason: "quota_stop pool=devin"}
	if _, code := nodeRun(t, client, nodeCommandJobPoolAction, args); code != "" {
		t.Fatal(code)
	}
	args.Op = "finish"
	result, code := nodeRun(t, client, nodeCommandJobPoolAction, args)
	if code != "" || result["outcome"] != "finished" {
		t.Fatalf("code=%q result=%v", code, result)
	}
	marker, found := readPoolActionMarker(t, inbox, "job-a")
	if !found || marker["done"] != true {
		t.Fatalf("marker=%v found=%v", marker, found)
	}
}

func TestNodeJobPoolActionRefusesAfterWatcherLoss(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned", "job.lost")
	client, _, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobPoolAction, nodeJobPoolActionArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a", ActionID: "0b3f4b1e-0000-4000-8000-000000000001:1", Op: nodePoolActionRegister, EndReason: "quota_stop pool=devin"})
	if code != "" || result["outcome"] != nodePoolActionMoved {
		t.Fatalf("code=%q result=%v", code, result)
	}
}
