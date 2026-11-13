package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLaneInboxStoreContractSupersede(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		cases := []struct {
			name, oldLane, oldCause, oldType, oldHead, kind, newCause, newType, newHead string
			want                                                                        bool
		}{
			{"merged ready", "a", "pr:o/r#1", "ready", "a", "lane.event", "pr:o/r#1", "merged", "", true},
			{"merged blocked", "a", "pr:o/r#1", "blocked", "a", "lane.event", "pr:o/r#1", "merged", "", true},
			{"closed", "a", "pr:o/r#1", "decision", "", "lane.event", "pr:o/r#1", "closed", "", true},
			{"different lane", "b", "pr:o/r#1", "ready", "a", "lane.event", "pr:o/r#1", "merged", "", false},
			{"different cause", "a", "pr:o/r#2", "ready", "a", "lane.event", "pr:o/r#1", "merged", "", false},
			{"new head", "a", "pr:o/r#1", "ready", "a", "lane.event", "pr:o/r#1", "ready", "b", true},
			{"same head", "a", "pr:o/r#1", "ready", "a", "lane.event", "pr:o/r#1", "ready", "a", false},
			{"job end", "a", "job:j", "decision", "", "job.completed", "job:j", "", "", true},
			// An ESC asks a question and the job stays open (hk 3012): a second
			// ESC from the same job must not supersede the first, unanswered one.
			{"second ESC keeps the first", "a", "job:j", "decision", "", "job.escalate", "job:j", "decision", "", false},
			{"no cause", "a", "", "ready", "a", "lane.event", "", "merged", "", false},
		}
		for n, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := open()
				now := time.UnixMilli(1700000000000)
				old := quietEvent(fmt.Sprint("old", n), "old")
				old.Cause, old.NotifyType, old.Head = tc.oldCause, tc.oldType, tc.oldHead
				a, err := s.put(fmt.Sprint("old", n), tc.oldLane, "lane.event", "inbox", old, now, time.Second, 0)
				if err != nil {
					t.Fatal(err)
				}
				next := quietEvent(fmt.Sprint("new", n), "new")
				next.Cause, next.NotifyType, next.Head = tc.newCause, tc.newType, tc.newHead
				b, err := s.put(fmt.Sprint("new", n), "a", tc.kind, "inbox", next, now.Add(time.Second), time.Second, 0)
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.read(a.ID, tc.oldLane, now.Add(2*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if (got.SupersededAt > 0) != tc.want || (tc.want && got.SupersededBy != b.ID) {
					t.Fatalf("old=%+v next=%+v", got, b)
				}
				if tc.want {
					suppressed, err := s.settled(fmt.Sprint("old", n))
					if err != nil || !suppressed {
						t.Fatalf("replay suppression: %v %v", suppressed, err)
					}
					first, err := s.firstUnread(context.Background(), tc.oldLane)
					if err != nil || (first != nil && first.ID == a.ID) {
						t.Fatalf("unread: %+v %v", first, err)
					}
				}
			})
		}
	})
}

func TestInboxCauseValidation(t *testing.T) {
	for _, tc := range []struct {
		cause string
		valid bool
	}{{"", true}, {"pr:owner/repo#12", true}, {"job:abc-1", true}, {"task:3056", true}, {"pr:x/y#0", false}, {"job:a b", false}, {"job:" + strings.Repeat("a", 256), false}, {"task:a/1", false}} {
		if validInboxCause(tc.cause) != tc.valid {
			t.Errorf("cause %q", tc.cause)
		}
	}
}

func TestInboxSupersededExcludesPendingBatches(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1700000000000)
		old := quietEvent("old", "old")
		old.Cause = "pr:o/r#1"
		old.NotifyType = "ready"
		a, err := s.put("old", "a", "lane.event", "inbox", old, now, time.Second, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.makeBatches(now.Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		next := quietEvent("next", "merged")
		next.Cause = old.Cause
		next.NotifyType = "merged"
		if _, err := s.put("next", "a", "lane.event", "interrupt", next, now.Add(3*time.Second), 0, 0); err != nil {
			t.Fatal(err)
		}
		states, err := s.ackStates([]string{"old"}, "")
		if err != nil || !states["old"].Acked || states["old"].AckedAt == "" {
			t.Fatalf("ack state=%+v err=%v", states, err)
		}
		counts, err := s.counts("a")
		if err != nil || counts["a"] != 0 {
			t.Fatalf("counts=%v err=%v", counts, err)
		}
		if batches, err := s.pendingBatches(); err != nil || len(batches) != 0 {
			t.Fatalf("pending=%v err=%v", batches, err)
		}
		if err := s.makeBatches(now.Add(4 * time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := s.read(a.ID, "a", now.Add(4*time.Second))
		if err != nil || got.SupersededAt == 0 {
			t.Fatalf("read=%+v err=%v", got, err)
		}
	})
}

func TestInboxCauseEmitWire(t *testing.T) {
	root := t.TempDir()
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "id", "--text", "hello", "--cause", "pr:o/r#1", "--head", "abc", "--inbox-root", root}
	var errout bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &errout, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent")}); code != ExitOK {
		t.Fatalf("emit %d %s", code, errout.String())
	}
	events := scanHubLaneEventsWithin(root, 0)
	if len(events) != 1 {
		t.Fatalf("scan=%+v", events)
	}
	client := &HubClient{}
	wire, ok := client.relayEventForSend(events[0])
	if !ok {
		t.Fatal("event not sent")
	}
	event, ok := decodeHubLaneEventPayload(wire.Payload)
	if !ok || event.Cause != "pr:o/r#1" || event.Head != "abc" {
		t.Fatalf("wire=%s event=%+v", wire.Payload, event)
	}
	for _, bad := range []string{"job:bad space", "job:" + strings.Repeat("x", 256)} {
		errout.Reset()
		args[9] = bad
		if code := runEmitCLI(args, &bytes.Buffer{}, &errout, CLIConfig{}); code != ExitUsage || !strings.Contains(errout.String(), "--cause") {
			t.Fatalf("invalid cause: %d %s", code, errout.String())
		}
	}
}

func TestInboxLaneEventScanRetainsHistoricalDottedLane(t *testing.T) {
	root := t.TempDir()
	event, err := json.Marshal(hubInboxEvent{Type: "lane.event", OwnerLane: "legacy.parent", EventID: "old-event", Text: "old notification"})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "events-lane", "legacy.parent")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "old-event.json"), event, 0600); err != nil {
		t.Fatal(err)
	}
	scanned := scanHubLaneEventsWithin(root, 0)
	if len(scanned) != 1 || scanned[0].OwnerLane != "legacy.parent" {
		t.Fatalf("historical lane event was skipped: %+v", scanned)
	}
}

func TestInboxSupersededReadCLI(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	old := quietEvent("old", "old")
	old.Cause = "pr:o/r#1"
	old.NotifyType = "ready"
	a, err := h.inbox.store.put("old", "lane-a", "lane.event", "inbox", old, now, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	next := quietEvent("next", "merged")
	next.Cause = old.Cause
	next.NotifyType = "merged"
	b, err := h.inbox.store.put("next", "lane-a", "lane.event", "inbox", next, now.Add(time.Second), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	listed := inboxAckCLI(t, h, "list", "--lane", "lane-a")
	if !strings.Contains(listed, "\tsuperseded\t") {
		t.Fatalf("list=%q", listed)
	}
	read := inboxAckCLI(t, h, "read", fmt.Sprint(a.ID), "--lane", "lane-a")
	if !strings.Contains(read, fmt.Sprintf("superseded by #%d", b.ID)) {
		t.Fatalf("read=%q", read)
	}
}

// A lane event skips the job-field normalization, so emit must refuse a --pr or
// --head the hub would drop, and must do so before any event file is written.
func TestInboxEmitLaneEventRejectsBadPRAndHead(t *testing.T) {
	for _, tc := range []struct {
		flag, value string
	}{
		{"--head", strings.Repeat("a", 257)},
		{"--head", "abc\ndef"},
		{"--head", "abc\x00"},
		{"--pr", strings.Repeat("p", 257)},
		{"--pr", "https://example.test/pull/1\r\n"},
	} {
		root := t.TempDir()
		args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "id", "--text", "hello", tc.flag, tc.value, "--inbox-root", root}
		var errout bytes.Buffer
		if code := runEmitCLI(args, &bytes.Buffer{}, &errout, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent")}); code != ExitUsage || !strings.Contains(errout.String(), tc.flag) {
			t.Fatalf("%s %q: code=%d stderr=%s", tc.flag, tc.value, code, errout.String())
		}
		if events := scanHubLaneEventsWithin(root, 0); len(events) != 0 {
			t.Fatalf("%s %q: event file written: %+v", tc.flag, tc.value, events)
		}
	}
	// The longest accepted value still records and passes the hub decode.
	root := t.TempDir()
	long := strings.Repeat("a", laneEventRefLimit)
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "id", "--text", "hello", "--head", long, "--pr", long, "--inbox-root", root}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent")}); code != ExitOK {
		t.Fatal(code)
	}
	events := scanHubLaneEventsWithin(root, 0)
	if len(events) != 1 {
		t.Fatalf("scan=%+v", events)
	}
	wire, ok := (&HubClient{}).relayEventForSend(events[0])
	if !ok {
		t.Fatal("event not sent")
	}
	if event, ok := decodeHubLaneEventPayload(wire.Payload); !ok || event.Head != long || event.PR != long {
		t.Fatalf("hub dropped an accepted value: %s", wire.Payload)
	}
}

// job.completed carries --head to the hub: its PR supersession rule needs a
// non-empty head, and an empty head leaves the payload unchanged.
func TestInboxJobCompletedRelaySendsHead(t *testing.T) {
	for _, head := range []string{"abc123", ""} {
		root := t.TempDir()
		args := []string{"--kind", "job.completed", "--job", "job-1", "--owner-lane", "lane-a", "--label", "worker", "--host", "host-a", "--report", "report.md", "--head", head, "--inbox-root", root}
		if code := runEmitCLI(args, io.Discard, io.Discard, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
			t.Fatal(code)
		}
		events := scanHubRelayEvents(root)
		if len(events) != 1 {
			t.Fatalf("scan=%+v", events)
		}
		wire, ok := (&HubClient{completedJobs: map[string]uint64{}}).relayEventForSend(events[0])
		if !ok {
			t.Fatal("event not sent")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(wire.Payload, &fields); err != nil {
			t.Fatal(err)
		}
		_, has := fields["head"]
		event, ok := decodeHubJobCompletionPayload(wire.Payload)
		if !ok || event.Head != head || has != (head != "") {
			t.Fatalf("head %q: wire=%s decoded=%+v", head, wire.Payload, event)
		}
	}
}

// A cause is a supersession key: the hub validates it whole and never
// truncates it, since a cut key names a different cause (#235 review).
func TestHubJobCompletionKeepsLongCauseWhole(t *testing.T) {
	cause := "job:" + strings.Repeat("a", 250)
	payload, _ := json.Marshal(map[string]any{"job_id": "j1", "epoch": 1, "cause": cause})
	event, _, ok := decodeHubJobCompletionPayloadDetailed(payload)
	if !ok || event.Cause != cause {
		t.Fatalf("cause = %d bytes ok=%v, want the %d-byte cause unchanged", len(event.Cause), ok, len(cause))
	}
	tooLong, _ := json.Marshal(map[string]any{"job_id": "j1", "epoch": 1, "cause": "job:" + strings.Repeat("a", 260)})
	if _, _, ok := decodeHubJobCompletionPayloadDetailed(tooLong); ok {
		t.Fatal("a cause over the limit was accepted instead of refused")
	}
}
