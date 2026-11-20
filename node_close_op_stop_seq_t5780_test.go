package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5780 review: an implementer's screen-stop hand-over close is fenced to the
// pane state the stop was observed in, as pane.close is. A pane that resumed
// and settled idle again on the same clean, pushed branch has a newer sequence,
// so a late announcement can no longer close it and start a second worker.
func TestCloseOpFencedToTheStopsPaneState_t5780(t *testing.T) {
	for _, tc := range []struct {
		name   string
		seq    any
		closed bool
	}{
		{"still in the observed state", 5, true},
		{"resumed and idle again", 3, false},
		{"no fence", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, commit := handoverRepo(t)
			inbox := t.TempDir()
			row := `{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"` + work + `","tab_id":"t1","state_change_seq":5}`
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(row)}, read: nodeReapScreen("❯ ")}
			client := closeOpNode(t, inbox, fake)
			event := filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt)
			receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
			if err := os.WriteFile(event, receipt, 0600); err != nil {
				t.Fatal(err)
			}
			args := closeOpArgs("op-stop-seq")
			args["cwd"] = work
			args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
			if tc.seq != nil {
				args["stop_seq"] = tc.seq
			}
			result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
			if code != "" {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if closes := fake.called("pane", "close"); tc.closed != (closes == 1) || tc.closed != (result["outcome"] == fleet.CloseOpClosed) {
				t.Fatalf("outcome=%v closes=%d, want closed=%v", result["outcome"], closes, tc.closed)
			}
		})
	}
}

// pane.close_op decodes stop_seq beside the handover guard, each on its own.
func TestCloseOpArgsAcceptTheStopSequence_t5780(t *testing.T) {
	base := closeOpArgs("op-decode")
	with := func(extra map[string]any) json.RawMessage {
		args := map[string]any{}
		for k, v := range base {
			args[k] = v
		}
		for k, v := range extra {
			args[k] = v
		}
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	guard := map[string]any{"origin": "o", "branch": "wk/task", "commit": "0123456789abcdef0123456789abcdef01234567"}
	for name, tc := range map[string]struct {
		raw  json.RawMessage
		want bool
	}{
		"neither":          {with(nil), true},
		"stop_seq":         {with(map[string]any{"stop_seq": 7}), true},
		"handover":         {with(map[string]any{"handover": guard}), true},
		"both":             {with(map[string]any{"handover": guard, "stop_seq": 7}), true},
		"null stop_seq":    {with(map[string]any{"stop_seq": nil}), false},
		"negative seq":     {with(map[string]any{"stop_seq": -1}), false},
		"unknown optional": {with(map[string]any{"stop_seq": 7, "extra": 1}), false},
	} {
		t.Run(name, func(t *testing.T) {
			decoded, ok := decodeNodeCommandArgs(nodeCommandPaneCloseOp, tc.raw)
			if ok != tc.want {
				t.Fatalf("ok=%v want %v (%s)", ok, tc.want, tc.raw)
			}
			if ok && name != "neither" && name != "handover" {
				if args := decoded.(nodePaneCloseOpArgs); args.StopSeq == nil || strconv.FormatInt(*args.StopSeq, 10) != "7" {
					t.Fatalf("stop_seq not decoded: %+v", args.StopSeq)
				}
			}
		})
	}
}

// hk 5780 review: the worktree check runs after the close's last pane look and can
// take seconds. A pane that resumed during it, with its checkout still clean and
// pushed, has another sequence and must not be closed under the old one: the
// sequence is read again at the close boundary.
func TestCloseOpRechecksTheStopSequenceAfterTheWorktreeCheck_t5780(t *testing.T) {
	work, origin, commit := handoverRepo(t)
	inbox := t.TempDir()
	row := func(seq int) string {
		return `{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"` + work + `","tab_id":"t1","state_change_seq":` + strconv.Itoa(seq) + `}`
	}
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(row(5))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
	if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob, "events", closeOpAttempt), receipt, 0600); err != nil {
		t.Fatal(err)
	}
	// The pane resumes and idles again (sequence 6) once the worktree check has
	// started: after the second pane look, before the close.
	git := client.nodeGitRunner()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		fake.mu.Lock()
		if fake.agentRead >= 2 {
			fake.agents = []string{nodeAgentList(row(6))}
		}
		fake.mu.Unlock()
		return git(ctx, args...)
	}
	args := closeOpArgs("op-late-resume")
	args["cwd"] = work
	args["handover"] = map[string]any{"origin": origin, "branch": "wk/task", "commit": commit}
	args["stop_seq"] = 5
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if code != "" || result["outcome"] == fleet.CloseOpClosed || fake.called("pane", "close") != 0 {
		t.Fatalf("a pane that resumed during the worktree check was closed: result=%v code=%q closes=%d", result, code, fake.called("pane", "close"))
	}
}

// worktree.handover_snapshot publishes the worktree to origin before any close,
// so for a stop it is bound to the pane state itself: the node reads the job's
// own pane immediately before the push and publishes nothing for a pane that left
// the stopped state (hk 5780 review).
func TestHandoverSnapshotIsBoundToTheStopSequence_t5780(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seq       any
		paneSeq   int
		published bool
	}{
		{"still in the stopped state", 5, 5, true},
		{"resumed and idle again", 5, 6, false},
		{"no fence", nil, 6, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, origin, _ := handoverRepo(t)
			writeSnapshotFile(t, work, "a.txt", "tracked edit after the stop")
			inbox := t.TempDir()
			events := filepath.Join(inbox, "jobs", "job-a", "events")
			if err := os.MkdirAll(events, 0700); err != nil {
				t.Fatal(err)
			}
			receipt, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a", "role": "implementer", "herdr_session": "fleet", "cwd": work}})
			if err := os.WriteFile(filepath.Join(events, "00001-job.spawned.json"), receipt, 0600); err != nil {
				t.Fatal(err)
			}
			row := `{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"` + work + `","tab_id":"t1","state_change_seq":` + strconv.Itoa(tc.paneSeq) + `}`
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(row)}}
			client := &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run}
			args := snapshotArgs(t, work)
			if tc.seq != nil {
				args["stop_seq"] = tc.seq
			}
			result, code := nodeRun(t, client, nodeCommandHandoverSnapshot, args)
			if code != "" {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if tc.published != (result["outcome"] == handoverSnapshotOK) {
				t.Fatalf("outcome=%v, want published=%v", result["outcome"], tc.published)
			}
			if !tc.published {
				if result["outcome"] != handoverPaneChanged {
					t.Fatalf("outcome=%v, want %s", result["outcome"], handoverPaneChanged)
				}
				originHasNoSnapshotRef(t, origin)
			}
		})
	}
}

// worktree.handover_snapshot decodes stop_seq as its one optional field.
func TestSnapshotArgsAcceptTheStopSequence_t5780(t *testing.T) {
	work, _, _ := handoverRepo(t)
	for name, tc := range map[string]struct {
		extra map[string]any
		want  bool
	}{
		"none":     {nil, true},
		"stop_seq": {map[string]any{"stop_seq": 7}, true},
		"null":     {map[string]any{"stop_seq": nil}, false},
		"negative": {map[string]any{"stop_seq": -1}, false},
		"unknown":  {map[string]any{"extra": 1}, false},
	} {
		t.Run(name, func(t *testing.T) {
			args := snapshotArgs(t, work)
			for k, v := range tc.extra {
				args[k] = v
			}
			raw, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := decodeNodeCommandArgs(nodeCommandHandoverSnapshot, raw); ok != tc.want {
				t.Fatalf("ok=%v want %v (%s)", ok, tc.want, raw)
			}
		})
	}
}
