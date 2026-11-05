package panewire

import (
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLateCompletionEmitScanAndInbox(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "normal"
		if late {
			name = "rearmed"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			args := []string{"--kind", "job.completed", "--job", "late-job", "--owner-lane", "lane-a", "--label", "worker", "--host", "host-a", "--report", "report.md", "--report-last-line", "READY", "--inbox-root", root}
			if late {
				args = append(args, "--late", "--deliver", "inbox")
			}
			if code := runEmitCLI(args, io.Discard, io.Discard, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
				t.Fatal(code)
			}
			events := scanHubRelayEvents(root)
			if len(events) != 1 || events[0].Late != late {
				t.Fatalf("scanned: %+v", events)
			}
			node := &HubClient{completedJobs: map[string]uint64{}}
			wire, ok := node.relayEventForSend(events[0])
			if !ok {
				t.Fatal("no wire event")
			}
			event, ok := decodeHubJobCompletionPayload(wire.Payload)
			if !ok || event.Late != late {
				t.Fatalf("wire=%s decoded=%+v", wire.Payload, event)
			}
			text := relayText(event)
			if strings.HasPrefix(text, "[late completion: re-armed] ") != late {
				t.Fatalf("relay text=%q", text)
			}
			now := time.Now()
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
			result, handled := h.receiveInbox("job.completed", &event, agent, "host-a", false)
			if !late {
				if handled || event.DeliveryClass != "interrupt" {
					t.Fatalf("normal handled=%v class=%s", handled, event.DeliveryClass)
				}
				return
			}
			items, err := h.inbox.store.list("lane-a", true)
			if err != nil || len(items) != 1 || items[0].Class != "inbox" || !handled || !items[0].Event.Late || len(agent.relays) != 0 {
				t.Fatalf("items=%+v err=%v handled=%v result=%+v relays=%d", items, err, handled, result, len(agent.relays))
			}
			if err := h.inbox.store.recoverMustAct(h.inbox.policy); err != nil {
				t.Fatal(err)
			}
			items, err = h.inbox.store.list("lane-a", true)
			if err != nil || len(items) != 1 || items[0].DueAt != 0 {
				t.Fatalf("recovery items=%+v err=%v", items, err)
			}

		})
	}
}

func TestLateCompletionEnvelopeFallback(t *testing.T) {
	root := t.TempDir()
	body := strings.Replace(t2734EnvelopeJSON(t2734SettledAt(), "lane-a", "", ""), `"document":`, `"late":true,"document":`, 1)
	r20t7WriteEvent(t, root, t2734Job, t2734Envelope, body)
	events := t2734Completions(t, root)
	if len(events) != 1 || !events[0].Late {
		t.Fatalf("fallback=%+v", events)
	}
	node := &HubClient{completedJobs: map[string]uint64{}}
	wire, ok := node.relayEventForSend(events[0])
	if !ok {
		t.Fatal("no wire event")
	}
	event, ok := decodeHubJobCompletionPayload(wire.Payload)
	if !ok || !event.Late {
		t.Fatalf("wire=%s decoded=%+v", wire.Payload, event)
	}
}
