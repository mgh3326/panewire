package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// TestT8EmitConflictingOutboxKeyFailsLoudly is the C1 RED guard: a same-key
// record with differing non-key metadata must not be mistaken for a successful
// notification. An identical wrk file followed by emit is covered separately:
// it is the normal file-first path and must still push.
func TestT8EmitConflictingOutboxKeyFailsLoudly(t *testing.T) {
	inbox := t.TempDir()
	args := []string{"--kind", "job.escalate", "--job", "t8-emit-duplicate", "--epoch", "1", "--owner-lane", "lane-source", "--reason", "needs review", "--question", "what changed?", "--report-last-line", "first state", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("first emit code=%d, want ExitOK", code)
	}
	if files := r20EventFiles(t, inbox, "t8-emit-duplicate"); len(files) != 1 {
		t.Fatalf("first emit files=%v, want one", files)
	}
	conflicting := append(append([]string{}, args[:len(args)-2]...), "--report-last-line", "changed state", "--inbox-root", inbox)
	var stderr bytes.Buffer
	if code := runEmitCLI(conflicting, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code == ExitOK {
		t.Fatal("conflicting outbox key exited 0: the caller cannot distinguish a discarded event")
	} else if code != ExitDeliveryFailure {
		t.Fatalf("duplicate code=%d, want ExitDeliveryFailure", code)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("duplicate outbox key")) {
		t.Fatalf("conflict stderr=%q, want duplicate outbox key", stderr.String())
	}
	if files := r20EventFiles(t, inbox, "t8-emit-duplicate"); len(files) != 1 {
		t.Fatalf("conflicting emit wrote event files=%v, want the original one only", files)
	}
}

// TestT8EmitWrkEscalationsRelayTwice writes the same flat records wrk writes
// before invoking emit. Fixed epoch plus distinct questions must produce two
// immediate pushes and two downstream relay keys without changing the five
// field job.* key.
func TestT8EmitWrkEscalationsRelayTwice(t *testing.T) {
	inbox := t.TempDir()
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	base := []string{"--kind", "job.escalate", "--job", "t8-question-events", "--epoch", "1", "--owner-lane", "lane-source", "--label", "lane-source", "--host", "host-a", "--reason", "needs review", "--inbox-root", inbox}
	for index, question := range []string{"first question", "second question"} {
		contents, err := json.Marshal(map[string]any{
			"kind": "job.escalate", "job_id": "t8-question-events", "epoch": 1,
			"owner_lane": "lane-source", "label": "lane-source", "host": "host-a",
			"report_path": "", "report_last_line": "", "reason": "needs review", "question": question,
		})
		if err != nil {
			t.Fatal(err)
		}
		r20WriteEvent(t, inbox, "t8-question-events", "0000"+string(rune('1'+index))+"-job.escalate.json", string(contents), time.Time{})
		args := append(append([]string{}, base...), "--question", question)
		var stderr bytes.Buffer
		if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
			t.Fatalf("question %q emit code=%d stderr=%q, want quiet ExitOK", question, code, stderr.String())
		}
	}
	if files := r20EventFiles(t, inbox, "t8-question-events"); len(files) != 2 {
		t.Fatalf("question-only difference produced %d event files, want 2: %v", len(files), files)
	}
	pushed := pushes.await(t, 2)
	pushedPaths := map[string]struct{}{}
	for _, record := range pushed {
		pushedPaths[record.ReportPath] = struct{}{}
	}
	if len(pushedPaths) != 2 {
		t.Fatalf("file-first escalations pushed %d paths, want 2: %+v", len(pushedPaths), pushed)
	}

	store := NewMemoryStore(t)
	defer store.Close()
	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 2 {
		t.Fatalf("scanner/outbox offered %d escalation events, want 2", len(events))
	}
	paths := map[string]struct{}{}
	for _, event := range events {
		paths[event.relayKey.ReportPath] = struct{}{}
	}
	if len(paths) != 2 {
		t.Fatalf("scanner gave the two escalations %d relay paths, want 2: %v", len(paths), paths)
	}

	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	lanes := `{"lanes":{"lane-source":{"machine":"host-a","pane":"w1:p1","parent":"lane-destination"},"lane-destination":{"machine":"host-a","pane":"w1:p2"}}}`
	hub, agent := r20t5Hub(t, lanes, client, 4)
	for _, event := range events {
		wire, err := json.Marshal(hubClientWireEvent(event))
		if err != nil {
			t.Fatal(err)
		}
		hub.handleAgentMessage("host-a", "fixture", agent, wire)
		node.commitRelaySent(event)
	}
	if injected := drainRelays(agent); injected != 2 {
		t.Fatalf("question-only difference reached %d relay.inject events, want 2", injected)
	}
	if fake.rowCount() != 2 {
		t.Fatalf("handoffkeep received %d relay rows, want 2", fake.rowCount())
	}
	var outboxRows int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM relay_sent").Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if outboxRows != 2 {
		t.Fatalf("outbox rows=%d, want 2", outboxRows)
	}
}

// TestT8EmitWrkWrittenRelayRecordsPushImmediately keeps the exact production
// ordering: the durable event file exists first, then emit is invoked. Each
// kind must remain quiet, successful, and immediately pushed.
func TestT8EmitWrkWrittenRelayRecordsPushImmediately(t *testing.T) {
	inbox := t.TempDir()
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	type relayCase struct {
		kind, job, report, reason, question, pr, head string
	}
	cases := []relayCase{
		{kind: "job.completed", job: "t8-wrk-done", report: "report.md"},
		{kind: "job.escalate", job: "t8-wrk-escalate", reason: "needs review", question: "what changed?"},
		{kind: "job.joined", job: "t8-wrk-joined", report: "joined.md", reason: "captain joined PR", pr: "https://example.invalid/pr/1", head: "deadbeef"},
	}
	for _, tc := range cases {
		contents, err := json.Marshal(map[string]any{
			"kind": tc.kind, "job_id": tc.job, "epoch": 1,
			"owner_lane": "lane-source", "label": "lane-source", "host": "host-a", "pane_id": "w1:p1",
			"report_path": tc.report, "report_last_line": "last line", "reason": tc.reason,
			"question": tc.question, "pr": tc.pr, "head": tc.head,
		})
		if err != nil {
			t.Fatal(err)
		}
		r20WriteEvent(t, inbox, tc.job, "00001-"+tc.kind+".json", string(contents), time.Time{})
		args := []string{"--kind", tc.kind, "--job", tc.job, "--epoch", "1", "--owner-lane", "lane-source", "--label", "lane-source", "--host", "host-a", "--pane", "w1:p1", "--report", tc.report, "--report-last-line", "last line", "--inbox-root", inbox}
		if tc.reason != "" {
			args = append(args, "--reason", tc.reason)
		}
		if tc.question != "" {
			args = append(args, "--question", tc.question)
		}
		if tc.pr != "" {
			args = append(args, "--pr", tc.pr)
		}
		if tc.head != "" {
			args = append(args, "--head", tc.head)
		}
		var stderr bytes.Buffer
		if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
			t.Fatalf("%s code=%d stderr=%q, want quiet ExitOK", tc.kind, code, stderr.String())
		}
	}
	if got := pushes.await(t, len(cases)); len(got) != len(cases) {
		t.Fatalf("file-first pushes=%d, want %d", len(got), len(cases))
	}
}
