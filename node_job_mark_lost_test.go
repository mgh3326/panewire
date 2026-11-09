package panewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// markLostJobDir writes a job directory with the given ledger event kinds.
func markLostJobDir(t *testing.T, inbox, job string, kinds ...string) {
	t.Helper()
	events := make([]map[string]any, len(kinds))
	for index, kind := range kinds {
		events[index] = map[string]any{"kind": kind}
	}
	markLostJobEvents(t, inbox, job, events...)
}

// markLostJobEvents writes one events file per event, in order.
func markLostJobEvents(t *testing.T, inbox, job string, events ...map[string]any) {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for index, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("%05d-%v.json", index+1, event["kind"])
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func markLostClient(t *testing.T, inbox string, agents []string, panes string) (*HubClient, *roleJobs, *nodeFakeHerdr) {
	t.Helper()
	store := &roleJobs{records: map[string]map[string]any{"job-a": {"state": "spawned"}}}
	fake := &nodeFakeHerdr{agents: agents, panes: panes}
	return &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run, nodeJobs: store}, store, fake
}

const markLostEmptyPanes = `{"result":{"panes":[]}}`

// job.mark_lost re-checks herdr and the local ledger, then writes the hk
// record with this node's own client.
func TestNodeJobMarkLostWritesLost(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLost {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if store.state("job-a") != "lost" || len(store.writes()) != 1 || store.writes()[0] != "job-a lost" {
		t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
	}
	if fake.called("agent", "list") != 1 || fake.called("pane", "list") != 1 {
		t.Fatalf("herdr calls=%v", fake.calls)
	}
}

func TestNodeJobMarkLostRefusesAListedPaneOrLabel(t *testing.T) {
	for _, test := range []struct {
		name   string
		agents string
		args   nodeJobMarkLostArgs
		want   string
	}{
		{"pane listed", nodeAgentList(nodeAgentJSON("w2:p3", "other", "working", "/work/x")), nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"}, nodeJobMarkLostPaneListed},
		{"label listed under another pane", nodeAgentList(nodeAgentJSON("w3:p1", "job-a", "idle", "/work/x")), nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"}, nodeJobMarkLostLabelListed},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
			client, store, _ := markLostClient(t, inbox, []string{test.agents}, markLostEmptyPanes)
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, test.args)
			if code != "" || result["outcome"] != test.want {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 {
				t.Fatalf("wrote hk: %v", store.writes())
			}
		})
	}
}

func TestNodeJobMarkLostRefusesWithoutACompletePaneList(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	fake.panesErr = errors.New("herdr list failed")
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostPanesUnknown {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("wrote hk: %v", store.writes())
	}
}

// A label-only check needs no pane list: the agent list is the completer.
func TestNodeJobMarkLostLabelOnlyIgnoresAFailedPaneList(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	fake.panesErr = errors.New("herdr list failed")
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLost {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if store.state("job-a") != "lost" {
		t.Fatalf("hk state=%v", store.state("job-a"))
	}
}

// The local ledger is read before herdr: a completion or a reclaim after the
// holder's own attempt refuses and never writes.
func TestNodeJobMarkLostRefusesOnTheLocalLedger(t *testing.T) {
	claim := map[string]any{"kind": "job.claim", "payload": map[string]any{"agent_label": "job-a"}}
	spawned := map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w2:p3", "label": "job-a"}}
	for _, test := range []struct {
		name  string
		extra map[string]any
		want  string
	}{
		{"completion", map[string]any{"kind": "job.completed"}, nodeJobMarkLostCompleted},
		{"newer claim", map[string]any{"kind": "job.reclaim", "payload": map[string]any{"agent_label": "job-a"}}, nodeJobMarkLostNewerClaim},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", claim, spawned, test.extra)
			client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
			if code != "" || result["outcome"] != test.want {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 || len(fake.calls) != 0 {
				t.Fatalf("writes=%v herdr=%v", store.writes(), fake.calls)
			}
		})
	}
}

// A job id may be released and reclaimed, so an earlier cycle's completion and
// claim say nothing about the holder's own attempt.
func TestNodeJobMarkLostIgnoresAnEarlierCycle(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a",
		map[string]any{"kind": "job.claim", "payload": map[string]any{"agent_label": "job-a"}},
		map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w0:p1", "label": "job-a"}},
		map[string]any{"kind": "job.completed"},
		map[string]any{"kind": "job.reclaim", "payload": map[string]any{"agent_label": "job-a"}},
		map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w2:p3", "label": "job-a"}},
	)
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLost {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if store.state("job-a") != "lost" {
		t.Fatalf("hk state=%v", store.state("job-a"))
	}
}

func TestNodeJobMarkLostFailsClosed(t *testing.T) {
	t.Run("unreadable ledger", func(t *testing.T) {
		inbox := t.TempDir()
		dir := filepath.Join(inbox, "jobs", "job-a", "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte("{not json"), 0600); err != nil {
			t.Fatal(err)
		}
		client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
		if _, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3"}); code != "mark_lost_failed" {
			t.Fatalf("code=%q", code)
		}
		if len(store.writes()) != 0 {
			t.Fatalf("wrote hk: %v", store.writes())
		}
	})
	t.Run("no jobs root", func(t *testing.T) {
		if _, code := nodeRun(t, &HubClient{}, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3"}); code != "jobs_root_unset" {
			t.Fatalf("code=%q", code)
		}
	})
	t.Run("hk write fails", func(t *testing.T) {
		inbox := t.TempDir()
		markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
		client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
		store.putErr = errors.New("job_host_forbidden")
		if _, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3"}); code != "mark_lost_failed" {
			t.Fatalf("code=%q", code)
		}
	})
}

func TestNodeJobMarkLostArgValidation(t *testing.T) {
	for _, args := range []nodeJobMarkLostArgs{
		{Job: "../job-a", PaneID: "w2:p3"},
		{Job: "job-a"},
		{Job: "job-a", PaneID: "bad pane"},
		{Job: "job-a", Label: "-bad"},
		{Job: "job-a", Label: "bad/label"},
	} {
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		if _, code := (&HubClient{}).runNodeCommand(t.Context(), nodeCommandJobMarkLost, raw); code != "invalid_args" {
			t.Fatalf("args=%+v code=%q", args, code)
		}
	}
}

// The hub writes end_reason with the lost record when it replaces a
// quota-stopped job (hk 3334); the node writes it in the same PUT.
func TestNodeJobMarkLostWritesEndReason(t *testing.T) {
	inbox := t.TempDir()
	markLostJobDir(t, inbox, "job-a", "job.claim", "job.spawned")
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	args := nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a", EndReason: "quota_stop pool=claude replaced_by=job-a-q1"}
	if result, code := nodeRun(t, client, nodeCommandJobMarkLost, args); code != "" || result["outcome"] != nodeJobMarkLostLost {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if got := store.records["job-a"]["end_reason"]; store.state("job-a") != "lost" || got != args.EndReason {
		t.Fatalf("hk state=%v end_reason=%v", store.state("job-a"), got)
	}
}

func TestNodeJobMarkLostRefusesBadEndReason(t *testing.T) {
	for name, reason := range map[string]string{"newline": "quota_stop\nreplaced_by=x", "too long": strings.Repeat("a", 129), "control": "quota\x01stop", "non-ascii": "quota_stop 정지"} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a", EndReason: reason})
			if _, ok := decodeNodeCommandArgs(nodeCommandJobMarkLost, raw); ok {
				t.Fatalf("end_reason %q was accepted", reason)
			}
		})
	}
	old, _ := json.Marshal(nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
	if _, ok := decodeNodeCommandArgs(nodeCommandJobMarkLost, old); !ok {
		t.Fatal("the original three-field shape was refused")
	}
}
