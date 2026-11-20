package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

// hk 5780: an implementer's announced stop (the node's quota-stop manager decided
// a screen stop is a hand-over) goes through the guarded worktree hand-over, not
// the legacy close and not nothing. These tests drive the real job.lost frame or
// the replacement worker with no exhausted-pool reading anywhere.

// sendAnnouncedStop delivers the job.lost frame a node relays for an announced stop.
func sendAnnouncedStop(t *testing.T, f *quotaReplaceFixture, replay bool) {
	t.Helper()
	event := relayedQuotaStop(f.job, "claude")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "owner_lane": "parent", "reason": event.Reason, "pane_id": roleHeldPane, "replay": replay})
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(t.Context(), f.node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
		t.Fatal(err)
	}
}

// answerHandoverCheck plays the holder node for the checks that come before any
// hand-over row: the pane probe and the worktree check. Anything else (a close,
// a capture, a snapshot) fails the test: nothing may touch the pane yet.
func answerHandoverCheck(t *testing.T, f *quotaReplaceFixture, patch map[string]any) {
	t.Helper()
	for answered := false; !answered; {
		command := nodeCommandRead(t, f.node)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
		case `"worktree.handover_check"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": patch})
			answered = true
		default:
			t.Fatalf("unexpected node command %s for an announced implementer stop", command["op"])
		}
	}
}

// The first acceptance test: a screen stop the manager decided to hand over gets
// a successor through the guarded path, on the same branch, with no exhausted-pool
// reading. Before hk 5780 the hub placed nothing and the watcher wrote a plain
// loss after its grace.
func TestAnnouncedImplementerScreenStopIsHandedOverThroughTheGuardedPath_t5780(t *testing.T) {
	const job, next = "pw-screen-stop-5780", "pw-screen-stop-5780-q1"
	f := handoverFixture(t, job, 3, time.Hour)
	sendAnnouncedStop(t, f, false)
	answerHandoverCheck(t, f, handoverReadReply(job))
	eventually(t, 3*time.Second, "the hand-over row", func() bool {
		rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
		return len(rows) == 1
	})
	action := readOnlyPoolAction(t, f.hub)
	handover := action.Data.Handover
	if action.Data.Kind != poolActionKindHandover || handover == nil || handover.Job != next || handover.Branch != "wk/task" || handover.Submitted {
		t.Fatalf("row=%+v", action)
	}
	if action.Data.EndReason != "quota_stop pool=claude replaced_by="+next {
		t.Fatalf("end_reason=%q", action.Data.EndReason)
	}
	assertHandoverSuccessor(t, handover.Successor, job, next)
	if ids := spawnOperations(t, f.hub, next); len(ids) != 0 {
		t.Fatalf("successor submitted before the close: %v", ids)
	}
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" {
			t.Fatalf("the announced stop was filed as a loss for its owner: %+v", relay)
		}
	}
	if record, _ := f.jobs.Get(t.Context(), job); record["state"] != "spawned" {
		t.Fatalf("the old record was written before the close: %v", record)
	}
}

// The second acceptance test: a dirty worktree on a node with no
// handover_snapshot_v1 is refused with exactly one owner decision and the pane is
// not closed. A resend of the same stop (the node had no acknowledgement) adds no
// second decision. The stop is settled each time, so the node stops resending.
func TestAnnouncedImplementerStopOnADirtyWorktreeIsRefusedWithOneDecision_t5780(t *testing.T) {
	const job = "pw-dirty-stop-5780"
	f := handoverFixture(t, job, 3, time.Hour)
	var settled, filed int
	announced := &quotaAnnouncement{relay: func(context.Context) { filed++ }}
	for delivery := 1; delivery <= 2; delivery++ {
		done := make(chan struct{})
		go func() {
			defer close(done)
			f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", relayedQuotaStop(job, "claude"), func() { settled++ }, announced)
		}()
		answerHandoverCheck(t, f, map[string]any{"outcome": handoverDirty})
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("delivery %d did not finish", delivery)
		}
		if settled != delivery || filed != 0 {
			t.Fatalf("delivery %d: settled=%d filed=%d, want the stop settled and no loss filed", delivery, settled, filed)
		}
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 0 {
		t.Fatalf("a refused hand-over left rows: %+v", rows)
	}
	lines := ownerLines(t, f.hub, "pool-exhausted:")
	if len(lines) != 1 || !strings.Contains(lines[0], handoverDirty) || !strings.Contains(lines[0], job) {
		t.Fatalf("owner decisions=%q, want exactly one naming %s", lines, handoverDirty)
	}
	if f.paneClosed {
		t.Fatal("the dirty worktree's pane was closed")
	}
	if record, _ := f.jobs.Get(t.Context(), job); record["state"] != "spawned" {
		t.Fatalf("the old record was written: %v", record)
	}
}

// The third acceptance test: the node's websocket reader does no store I/O for an
// announced stop. The store's only connection is held, so a read there would wait
// for it; the reader must return anyway, and files nothing itself. Before hk 5780
// it read the stored submission for the role (reviewerQuotaAnnouncement).
func TestAnnouncedQuotaStopReaderDoesNoStoreIO_t5780(t *testing.T) {
	f := newQuotaReplaceFixture(t, "implementer", "pw-reader-5780")
	f.hub.mu.Lock()
	agent := f.hub.nodes["machine-a"].agent
	f.hub.mu.Unlock()
	event := relayedQuotaStop(f.job, "claude")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "owner_lane": "parent", "reason": event.Reason, "pane_id": roleHeldPane})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw}))
	if err != nil {
		t.Fatal(err)
	}
	db := f.hub.inbox.store.db
	db.SetMaxOpenConns(1)
	held, cancelHold := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelHold()
	conn, err := db.Conn(held)
	if err != nil {
		t.Fatalf("could not hold the store's connection: %v", err)
	}
	defer conn.Close()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		f.hub.handleAgentMessageContext(t.Context(), "machine-a", "127.0.0.1", agent, payload)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the websocket reader waited on the store")
	}
}
