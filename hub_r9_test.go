package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestR9HubEmitUpdatesPresenceOnlyNodeAndBroadcastsWithoutAlert(t *testing.T) {
	clock := time.Date(2026, 9, 1, 3, 40, 0, 0, time.UTC)
	notifier := &r9Notifier{}
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": r6OperatorToken,
			"node-a":   r6NodeAToken,
			"node-b":   r6NodeBToken,
		},
		AlertNodes: map[string]struct{}{"node-a": {}},
		Now:        func() time.Time { return clock },
		Notifier:   notifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	root := t.TempDir()
	nodeEnv := filepath.Join(root, "node.env")
	operatorEnv := filepath.Join(root, "operator.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=node-b\nHUB_TOKEN="+r6NodeBToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operatorEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	events := r6DialEvents(t, server.URL)
	defer events.Close(websocket.StatusNormalClosure, "")
	r6Eventually(t, "event subscriber registration", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return len(hub.subscribers) == 1
	})

	var stdout, stderr bytes.Buffer
	if code := runHubEmitCLI([]string{"--hub-url", r6WSURL(server.URL, ""), "--hub-token-env", nodeEnv, "--text", "completed: evidence recorded"}, &stdout, &stderr, hubCLIDeps{AllowInsecureForTests: true}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("hub-emit code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	readContext, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var event hubEvent
	if err := wsjson.Read(readContext, events, &event); err != nil {
		t.Fatalf("note did not use event broadcast path: %v", err)
	}
	if event.MachineID != "node-b" || event.Kind != "note" || event.Received != clock {
		t.Fatalf("unexpected broadcast event: %+v", event)
	}
	var payload hubNotePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Text != "completed: evidence recorded" {
		t.Fatalf("unexpected note payload: %s err=%v", event.Payload, err)
	}

	nodes := hub.Nodes()
	if len(nodes) != 1 || nodes[0].MachineID != "node-b" || nodes[0].AlertClass != "presence-only" || nodes[0].LastNote == nil || nodes[0].LastNote.Text != payload.Text || nodes[0].LastNote.ReceivedAt != clock {
		t.Fatalf("presence-only last note was not retained: %+v", nodes)
	}
	if got := notifier.Alerts(); len(got) != 0 {
		t.Fatalf("note changed alert/TG state: %+v", got)
	}
	if len(hub.alerts) != 0 {
		t.Fatalf("note created alert state: %+v", hub.alerts)
	}

	stdout.Reset()
	stderr.Reset()
	if code := runHubStatusCLI([]string{"--hub-url", server.URL, "--hub-token-env", operatorEnv}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("hub-status code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "LAST_NOTE") || !strings.Contains(got, `"completed: evidence recorded"`) {
		t.Fatalf("hub-status omitted last note: %q", got)
	}
}

func TestR9HubEmitRejects513ByteText(t *testing.T) {
	root := t.TempDir()
	nodeEnv := filepath.Join(root, "node.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runHubEmitCLI([]string{"--hub-url", "ws://127.0.0.1:1", "--hub-token-env", nodeEnv, "--text", strings.Repeat("a", 513)}, &stdout, &stderr, hubCLIDeps{AllowInsecureForTests: true}); code != ExitConditionInvalid {
		t.Fatalf("513-byte note code=%d stderr=%q, want invalid condition", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "invalid text") {
		t.Fatalf("513-byte note rejection was unclear: %q", stderr.String())
	}
}

func TestR9HubNoteSummaryUsesAtMost60Characters(t *testing.T) {
	summary := hubNoteSummary(strings.Repeat("가", 61))
	if got := len([]rune(summary)); got != 60 {
		t.Fatalf("summary length=%d, want 60", got)
	}
}

type r9Notifier struct {
	mu     sync.Mutex
	alerts []HubAlert
}

func (notifier *r9Notifier) Send(_ context.Context, alert HubAlert) error {
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	notifier.alerts = append(notifier.alerts, alert)
	return nil
}

func (notifier *r9Notifier) Alerts() []HubAlert {
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	return append([]HubAlert(nil), notifier.alerts...)
}
