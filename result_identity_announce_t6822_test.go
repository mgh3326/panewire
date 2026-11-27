package panewire

// hk 6822, review of #938: a merge never tells the lane less than delivering
// the completion on its own would have.

import (
	"strings"
	"testing"
	"time"
)

// t6822Wakes is the injections that are the wake line.
func t6822Wakes(a *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for _, d := range t3426Injected(a) {
		if strings.HasPrefix(d.Text, "[inbox] ") {
			out = append(out, d)
		}
	}
	return out
}

// A completion of a job with no task and no PR wakes a design lane, whose READY
// asked for no wake. Merged, it still does, once.
// Mutant: announce only a typed READY.
func TestMergeKeepsTheWakeOfAnInboxOnlyLane(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	if wakes := t6822Wakes(agent); len(wakes) != 0 {
		t.Fatalf("fixture READY woke the lane: %+v", wakes)
	}
	now := time.Now()
	h.now = func() time.Time { return now }
	c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	c.JobID = "consult-1" // its hk record names no task
	h.relayJobCompletionFrom("host-a", c)

	want := inboxWakeText(t3426Design, 1, "ready")
	if wakes := t6822Wakes(agent); len(wakes) != 1 || wakes[0].Pane != "w1:design" || wakes[0].Text != want {
		t.Fatalf("wakes=%+v, want the one line %q", wakes, want)
	}
	if items := t6822Items(t, h, t3426Design); len(items) != 1 || len(items[0].Event.MergedCompletions) != 1 {
		t.Fatalf("items=%+v, want the merge", items)
	}
	now = now.Add(h.inbox.policy.Relay.wakeWindow + time.Second)
	h.relayJobCompletionFrom("host-a", c)
	if wakes := t6822Wakes(agent); len(wakes) != 0 {
		t.Fatalf("a repeat woke the lane again: %+v", wakes)
	}
}

// The completion's question stays readable on the READY, and a second one is
// added to it. Mutants: drop the question; keep only the last.
func TestMergeKeepsTheCompletionsQuestion(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	first := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	first.Question = "ship the migration first"
	second := t6822Completion(t3426Design, "ready-1", "00002-job.completed.json")
	second.Question = "and the rollback owner"
	third := t6822Completion(t3426Design, "ready-1", "00003-job.completed.json")
	for _, c := range []hubJobEventPayload{first, second, third, second} {
		h.relayJobCompletionFrom("host-a", c)
	}
	items := t6822Items(t, h, t3426Design)
	if len(items) != 1 || items[0].Event.Question != first.Question+" / "+second.Question {
		t.Fatalf("items=%+v, want both questions once", items)
	}
}

// A typed completion whose READY has not reached the pane, or never will by
// itself, is announced: it would have been typed. Mutant: announce only a typed
// READY.
func TestMergeAnnouncesATypedCompletionWhoseReadyHasNotLanded(t *testing.T) {
	for _, tc := range []struct {
		name, deliver string
		typedReady    int
	}{{"the READY is in flight", "", 1}, {"the READY was filed as inbox", "inbox", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			h, agent, _, _ := t6822Hub(t)
			if tc.deliver == "inbox" {
				// A busy receiver, so the inbox READY is not promoted into the pane.
				for i := range h.nodes["host-a"].sessionSnapshot.sessions {
					h.nodes["host-a"].sessionSnapshot.sessions[i].Status = "working"
				}
			}
			ready := t3426Ready(t6822Director, "ready-1")
			ready.DeliveryClass, ready.Text = tc.deliver, "hk3404 PR #12 head abc1234"
			h.relayLaneEvent(ready, agent)
			if injected := t3426Injected(agent); len(injected) != tc.typedReady {
				t.Fatalf("fixture READY injections=%+v, want %d", injected, tc.typedReady)
			}
			h.relayJobCompletionFrom("host-a", t6822Completion(t6822Director, "ready-1", "00001-job.completed.json"))
			if wakes := t6822Wakes(agent); len(wakes) != 1 || wakes[0].Pane != "w1:director" {
				t.Fatalf("wakes=%+v, want one", wakes)
			}
		})
	}
}

// On a lane that holds while working, a READY typed in a held batch counts as
// typed; a READY still held carries the completion in its own batch, and the
// working receiver is not typed to. Mutants: read delivered_at alone; announce
// every held completion.
func TestMergeAnnouncesAReadyTypedInAHeldBatch(t *testing.T) {
	completion := func(readyID string) hubJobEventPayload {
		c := t6822Completion("lane-a", readyID, "00001-job.completed.json")
		c.ReportPath, c.ReportLastLine = "/jobs/x/held.md", "held report line"
		return c
	}
	working := func(h *HubServer, now time.Time) {
		h.nodes["host-a"].sessionSnapshot.sessions[0].Status = "working"
		h.nodes["host-a"].sessionSnapshot.receivedAt = now
	}
	for _, receiver := range []string{"working", "idle"} {
		t.Run("the READY's held batch was sent, receiver "+receiver, func(t *testing.T) {
			h, agent, now, _, _ := holdHub(t, "", true)
			holdNotice(t, h, agent, "ready-1", "ready", "READY ready for review")
			holdIdle(h, *now)
			h.flushInbox()
			if batch := holdReceive(t, agent); !strings.HasPrefix(batch.Text, "[batch 1: READY 1]") {
				t.Fatalf("batch=%+v", batch)
			}
			item := t6822Item(t, h, "lane-a", relayLaneEventOutboxKey("lane-a", "ready-1"))
			if item.DeliveredAt != 0 || item.DigestBatch == "" || item.AckedAt != 0 {
				t.Fatalf("fixture READY=%+v, want typed through a batch only", item)
			}
			if receiver == "working" {
				working(h, *now)
			}
			h.relayJobCompletionFrom("host-a", completion("ready-1"))
			if wakes := t6822Wakes(agent); len(wakes) != 1 || wakes[0].Pane != "w1:p1" {
				t.Fatalf("wakes=%+v, want one", wakes)
			}
		})
	}
	t.Run("the READY is still held", func(t *testing.T) {
		h, agent, now, _, _ := holdHub(t, "", true)
		holdNotice(t, h, agent, "ready-1", "ready", "READY ready for review")
		h.relayJobCompletionFrom("host-a", completion("ready-1"))
		h.flushInbox()
		if injected := t3426Injected(agent); len(injected) != 0 {
			t.Fatalf("a working receiver was typed to: %+v", injected)
		}
		holdIdle(h, *now)
		h.flushInbox()
		if batch := holdReceive(t, agent); !strings.HasPrefix(batch.Text, "[batch 1: READY 1]") || !strings.Contains(batch.Text, "held report line") {
			t.Fatalf("batch=%+v, want the READY with the completion's report", batch)
		}
	})
	// The READY's text left the item before the merge, so nothing still to
	// come carries the completion.
	t.Run("a held completion whose READY is in flight", func(t *testing.T) {
		h, agent, now, _, _ := holdHub(t, "", true)
		holdIdle(h, *now)
		holdNotice(t, h, agent, "ready-1", "ready", "READY ready for review")
		holdReceive(t, agent)
		working(h, *now)
		h.relayJobCompletionFrom("host-a", completion("ready-1"))
		if wakes := t6822Wakes(agent); len(wakes) != 1 {
			t.Fatalf("wakes=%+v, want one", wakes)
		}
	})
	t.Run("a held completion whose READY's batch is composed and unsent", func(t *testing.T) {
		h, agent, now, _, _ := holdHub(t, "", true)
		holdNotice(t, h, agent, "ready-1", "ready", "READY ready for review")
		holdIdle(h, *now)
		if _, _, err := h.makeHeldBatches(); err != nil {
			t.Fatal(err)
		}
		if item := t6822Item(t, h, "lane-a", relayLaneEventOutboxKey("lane-a", "ready-1")); item.DigestBatch == "" {
			t.Fatalf("fixture READY=%+v, want it claimed by a batch", item)
		}
		working(h, *now)
		h.relayJobCompletionFrom("host-a", completion("ready-1"))
		if wakes := t6822Wakes(agent); len(wakes) != 1 {
			t.Fatalf("wakes=%+v, want one", wakes)
		}
	})
	t.Run("a held completion whose READY is a plain inbox item", func(t *testing.T) {
		h, agent, _, _, _ := holdHub(t, "", true)
		ready := quietEvent("ready-1", "hk 3404 for review")
		ready.NotifyType, ready.DeliveryClass = "ready", "inbox"
		h.relayLaneEvent(ready, agent)
		h.relayJobCompletionFrom("host-a", completion("ready-1"))
		if wakes := t6822Wakes(agent); len(wakes) != 1 {
			t.Fatalf("wakes=%+v, want one", wakes)
		}
	})
}

// On a lane that is not inbox-only, an announcement that falls inside an open
// window is sent when the window ends, as the second completion would have been
// typed. Mutant: drop it with the window.
func TestMergeAnnouncementWaitsForTheWindowOnATypedLane(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822Typed(t, h, agent, "ready-1")
	t6822Typed(t, h, agent, "ready-2")
	now := time.Now()
	h.now = func() time.Time { return now }
	h.relayJobCompletionFrom("host-a", t6822Completion(t6822Director, "ready-1", "00001-job.completed.json"))
	h.relayJobCompletionFrom("host-a", t6822Completion(t6822Director, "ready-2", "00002-job.completed.json"))
	if wakes := t6822Wakes(agent); len(wakes) != 1 {
		t.Fatalf("wakes inside the window=%+v, want the first alone", wakes)
	}
	now = now.Add(h.inbox.policy.Relay.wakeWindow + time.Second)
	h.flushInboxWakes()
	if wakes := t6822Wakes(agent); len(wakes) != 1 || wakes[0].Text != inboxWakeText(t6822Director, 2, "ready") {
		t.Fatalf("wakes at the window's end=%+v, want the owed line", wakes)
	}
	now = now.Add(h.inbox.policy.Relay.wakeWindow + time.Second)
	h.flushInboxWakes()
	if wakes := t6822Wakes(agent); len(wakes) != 0 {
		t.Fatalf("an owed line was sent twice: %+v", wakes)
	}
}

// The completion's own item would have started a new unread set, with its own
// re-wakes. A merge adds no item, so it starts the count again itself.
// Mutant: leave the count spent.
func TestMergeRestartsTheRewakeCount(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	now := time.Now()
	h.now = func() time.Time { return now }
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	step := func() int {
		now = now.Add(h.inbox.policy.Relay.rewakeInterval + time.Minute)
		h.mu.Lock()
		h.nodes["host-a"].sessionSnapshot.receivedAt = now
		h.mu.Unlock()
		h.rewakeInboxLanes()
		return len(t6822Wakes(agent))
	}
	h.rewakeInboxLanes() // the first scan sees the unread set
	for line := 1; line <= h.inbox.policy.Relay.rewakeMax; line++ {
		if sent := step(); sent != 1 {
			t.Fatalf("re-wake %d sent %d lines", line, sent)
		}
	}
	if sent := step(); sent != 0 {
		t.Fatalf("fixture count is not spent: %d lines", sent)
	}
	// A quiet completion: it asks for no wake of its own.
	h.relayJobCompletionFrom("host-a", t6822Completion(t3426Design, "ready-1", "00001-job.completed.json"))
	if wakes := t6822Wakes(agent); len(wakes) != 0 {
		t.Fatalf("a quiet completion woke the lane: %+v", wakes)
	}
	if sent := step(); sent != 1 {
		t.Fatalf("the merge left the re-wake count spent: %d lines", sent)
	}
}
