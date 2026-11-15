package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeLaneEventFile writes one lane-event record and stamps the mtime the
// question was asked at.
func writeLaneEventFile(t *testing.T, inbox, name string, at time.Time, event map[string]any) {
	t.Helper()
	dir := filepath.Join(inbox, "events-lane")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

// A pane.close that names a question_job refuses while the job's own lane-event
// records hold an unanswered question. The documented form is
// `--type decision` with ordinary text; the answer is a later event from the
// same pane. Every uncertainty keeps the pane.
func TestNodeCommandPaneCloseQuestionOpen(t *testing.T) {
	const cwd = "/work/a"
	at := time.Now().Add(-time.Minute)
	jobEvent := func(kind string, payload map[string]any) map[string]any {
		return map[string]any{"kind": kind, "created_at": at.Add(-time.Minute).Format(time.RFC3339Nano), "payload": payload}
	}
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a",
		jobEvent("job.claimed", map[string]any{"role": "reviewer"}),
		jobEvent("job.spawned", map[string]any{"pane_id": "w1:p1", "role": "reviewer"}),
	)
	question := func(id string, when time.Time, notifyType, text string) map[string]any {
		return map[string]any{"kind": "lane.event", "pane_id": "w1:p1", "event_id": id, "notify_type": notifyType, "text": text}
	}
	writeLaneEventFile(t, inbox, "0001.json", at, question("q1", at, "decision", "Which finding should I keep?"))

	args := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "done", "cwd": cwd, "question_job": "job-a"}
	client := func() (*HubClient, *nodeFakeHerdr) {
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "done", cwd))}, read: nodeReapScreen("❯ ")}
		return &HubClient{jobsInboxRoot: inbox, nodeCloseSettle: time.Millisecond, nodeHerdr: fake.run}, fake
	}

	t.Run("an open question refuses the close", func(t *testing.T) {
		client, fake := client()
		result, code := nodeRun(t, client, nodeCommandPaneClose, args)
		if code != "" || result["outcome"] != nodePaneQuestionOpen {
			t.Fatalf("result=%v code=%q", result, code)
		}
		if closes := fake.called("pane", "close"); closes != 0 {
			t.Fatalf("closed an open question: closes=%d", closes)
		}
	})

	t.Run("a later lane event answers it", func(t *testing.T) {
		writeLaneEventFile(t, inbox, "0002.json", at.Add(10*time.Second), question("q2", at.Add(10*time.Second), "progress", "kept both"))
		client, fake := client()
		result, code := nodeRun(t, client, nodeCommandPaneClose, args)
		if code != "" || result["outcome"] != nodePaneClosed || fake.called("pane", "close") != 1 {
			t.Fatalf("result=%v code=%q calls=%v", result, code, fake.calls)
		}
	})

	t.Run("no question_job keeps the old behaviour", func(t *testing.T) {
		writeLaneEventFile(t, inbox, "0003.json", at.Add(20*time.Second), question("q3", at.Add(20*time.Second), "decision", "Another question"))
		plain := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "done", "cwd": cwd}
		client, fake := client()
		result, code := nodeRun(t, client, nodeCommandPaneClose, plain)
		if code != "" || result["outcome"] != nodePaneClosed || fake.called("pane", "close") != 1 {
			t.Fatalf("result=%v code=%q", result, code)
		}
	})

	t.Run("a job whose spawned pane differs is refused", func(t *testing.T) {
		other := t.TempDir()
		markLostJobEvents(t, other, "job-a",
			jobEvent("job.claimed", map[string]any{"role": "reviewer"}),
			jobEvent("job.spawned", map[string]any{"pane_id": "w1:p9", "role": "reviewer"}),
		)
		writeLaneEventFile(t, other, "0001.json", at, question("q1", at, "decision", "Which?"))
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "done", cwd))}, read: nodeReapScreen("❯ ")}
		client := &HubClient{jobsInboxRoot: other, nodeCloseSettle: time.Millisecond, nodeHerdr: fake.run}
		result, code := nodeRun(t, client, nodeCommandPaneClose, args)
		if code != "" || result["outcome"] != nodePaneChanged || fake.called("pane", "close") != 0 {
			t.Fatalf("result=%v code=%q", result, code)
		}
	})

	t.Run("an unset jobs root is refused", func(t *testing.T) {
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "done", cwd))}, read: nodeReapScreen("❯ ")}
		client := &HubClient{nodeCloseSettle: time.Millisecond, nodeHerdr: fake.run}
		if _, code := nodeRun(t, client, nodeCommandPaneClose, args); code != "jobs_root_unset" || fake.called("pane", "close") != 0 {
			t.Fatalf("code=%q", code)
		}
	})

	t.Run("question_job travels beside allow_unnamed", func(t *testing.T) {
		unnamed := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "done", "cwd": cwd, "allow_unnamed": true, "question_job": "job-a"}
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "", "done", cwd))}, read: nodeReapScreen("❯ ")}
		client := &HubClient{jobsInboxRoot: inbox, nodeCloseSettle: time.Millisecond, nodeHerdr: fake.run}
		if _, code := nodeRun(t, client, nodeCommandPaneClose, unnamed); code != "" {
			t.Fatalf("combined optional fields refused: %q", code)
		}
	})

	t.Run("an invalid question_job is refused", func(t *testing.T) {
		bad := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "done", "cwd": cwd, "question_job": "../job-a"}
		client, fake := client()
		if _, code := nodeRun(t, client, nodeCommandPaneClose, bad); code != "invalid_args" || len(fake.calls) != 0 {
			t.Fatalf("code=%q calls=%v", code, fake.calls)
		}
	})
}
