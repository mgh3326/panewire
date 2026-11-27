package panewire

// hk 6822, review of #938: on an inbox-only lane a merge tells the lane no less
// than delivering the completion on its own would have.

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
