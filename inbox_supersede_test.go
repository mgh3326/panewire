package panewire

import (
	"bytes"
	"context"
	"fmt"
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
					suppressed, err := s.replaySuppressed(fmt.Sprint("old", n))
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
