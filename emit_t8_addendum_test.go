package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

// TestT8EmitDuplicateOutboxKeyFailsLoudly is the C1 RED guard: a second
// identical emit must not be mistaken for a successful delivery attempt.
func TestT8EmitDuplicateOutboxKeyFailsLoudly(t *testing.T) {
	inbox := t.TempDir()
	args := []string{"--kind", "job.escalate", "--job", "t8-emit-duplicate", "--epoch", "1", "--owner-lane", "lane-source", "--reason", "needs review", "--question", "what changed?", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("first emit code=%d, want ExitOK", code)
	}
	if files := r20EventFiles(t, inbox, "t8-emit-duplicate"); len(files) != 1 {
		t.Fatalf("first emit files=%v, want one", files)
	}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code == ExitOK {
		t.Fatal("duplicate outbox key exited 0: the caller cannot distinguish a discarded event")
	} else if code != ExitDeliveryFailure {
		t.Fatalf("duplicate code=%d, want ExitDeliveryFailure", code)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("duplicate outbox key")) {
		t.Fatalf("duplicate stderr=%q, want duplicate outbox key", stderr.String())
	}
	if files := r20EventFiles(t, inbox, "t8-emit-duplicate"); len(files) != 1 {
		t.Fatalf("duplicate emit wrote event files=%v, want the original one only", files)
	}
}

// TestT8EmitDifferentEscalationQuestionsRelayTwice keeps wrk's fixed epoch
// shape: only question differs. The scanner's event-file fallback supplies two
// distinct existing 5-field relay keys, without changing that key contract.
func TestT8EmitDifferentEscalationQuestionsRelayTwice(t *testing.T) {
	inbox := t.TempDir()
	base := []string{"--kind", "job.escalate", "--job", "t8-question-events", "--epoch", "1", "--owner-lane", "lane-source", "--label", "lane-source", "--host", "host-a", "--reason", "needs review", "--inbox-root", inbox}
	for _, question := range []string{"first question", "second question"} {
		args := append(append([]string{}, base...), "--question", question)
		if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
			t.Fatalf("question %q emit code=%d, want ExitOK", question, code)
		}
	}
	if files := r20EventFiles(t, inbox, "t8-question-events"); len(files) != 2 {
		t.Fatalf("question-only difference produced %d event files, want 2: %v", len(files), files)
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
