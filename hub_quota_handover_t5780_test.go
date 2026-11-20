package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5780: an implementer's announced stop (the node's quota-stop manager decided
// a screen stop is a hand-over) goes through the guarded worktree hand-over, not
// the legacy close and not nothing. These tests drive the real job.lost frame or
// the replacement worker with no exhausted-pool reading anywhere.

// announcedFixture is a hand-over fixture whose node can close a pane at a stop's
// own sequence (pane_close_op_stop_seq_v1).
func announcedFixture(t *testing.T, job string) *quotaReplaceFixture {
	t.Helper()
	f := handoverFixture(t, job, 3, time.Hour)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOpStopSeq] = true
	f.hub.mu.Unlock()
	return f
}

// stopEvent is the announcement a node relays, with its producer event id.
func stopEvent(job, id string) hubJobEventPayload {
	event := relayedQuotaStop(job, "claude")
	event.EventID = id
	return event
}

// deliver runs the replacement worker for one announcement and reports how it
// ended: the node's outbox acknowledgement (settled) and whether the stop went to
// the plain loss relay (filed). serve plays the node, or is nil when the worker
// must send it no command.
func deliver(t *testing.T, f *quotaReplaceFixture, event hubJobEventPayload, serve func()) (settled, filed int) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { settled++ }, &quotaAnnouncement{relay: func(context.Context) { filed++ }})
	}()
	if serve != nil {
		serve()
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the worker did not finish")
	}
	return settled, filed
}

// sendAnnouncedStop delivers the job.lost frame a node relays for an announced stop.
func sendAnnouncedStop(t *testing.T, f *quotaReplaceFixture, replay bool) {
	t.Helper()
	event := relayedQuotaStop(f.job, "claude")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "owner_lane": "parent", "reason": event.Reason, "pane_id": roleHeldPane, "replay": replay, "event_id": "ev-1"})
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
	f := announcedFixture(t, job)
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
	// The close is fenced to the pane state the stop was observed in (seq=7).
	if handover.StopSeq == nil || *handover.StopSeq != 7 || handover.StopEvent != "ev-1" {
		t.Fatalf("row does not bind the announcement: seq=%v event=%q", handover.StopSeq, handover.StopEvent)
	}
	assertHandoverSuccessor(t, handover.Successor, job, next)
	if ids := spawnOperations(t, f.hub, next); len(ids) != 0 {
		t.Fatalf("successor submitted before the close: %v", ids)
	}
	closed := closeHandover(t, f, action)
	if args := closed.Data.Args; args == nil || args.StopSeq == nil || *args.StopSeq != 7 {
		t.Fatalf("the node's close is not fenced to the announcement: %+v", closed.Data.Args)
	}
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" {
			t.Fatalf("the announced stop was filed as a loss for its owner: %+v", relay)
		}
	}
}

// The second acceptance test: a dirty worktree on a node with no
// handover_snapshot_v1 is refused with exactly one owner decision and the pane is
// not closed. The stop is settled, so the node stops resending.
func TestAnnouncedImplementerStopOnADirtyWorktreeIsRefusedWithOneDecision_t5780(t *testing.T) {
	const job = "pw-dirty-stop-5780"
	f := announcedFixture(t, job)
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), func() { answerHandoverCheck(t, f, map[string]any{"outcome": handoverDirty}) })
	if settled != 1 || filed != 0 {
		t.Fatalf("settled=%d filed=%d, want the stop settled and no loss filed", settled, filed)
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

// hk 5780 review: a resend of the same announcement finds its own outcome before
// anything is probed. The owner read the refusal and may have committed and pushed
// since; a second answer (a queued close) would contradict the decision they hold.
// A later stop is another announcement and is decided afresh.
func TestAnnouncedStopResendFindsItsOutcomeBeforeProbing_t5780(t *testing.T) {
	const job = "pw-resend-5780"
	f := announcedFixture(t, job)
	deliver(t, f, stopEvent(job, "ev-1"), func() { answerHandoverCheck(t, f, map[string]any{"outcome": handoverDirty}) })
	// The worktree is clean now; the resend sends the worker no node command at all.
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), nil)
	if settled != 1 || filed != 0 {
		t.Fatalf("resend: settled=%d filed=%d, want settled by the first delivery's decision", settled, filed)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 0 {
		t.Fatalf("the resend queued a hand-over after the refusal: %+v", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 1 {
		t.Fatalf("owner decisions=%q, want still one", lines)
	}
	// Another stop (new producer event) is decided afresh, and the clean branch now hands over.
	deliver(t, f, stopEvent(job, "ev-2"), func() { answerHandoverCheck(t, f, handoverReadReply(job)) })
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 {
		t.Fatalf("a later stop was not handed over: %+v", rows)
	}
	// And a resend of that one finds the row that owns the job, with no command and no new row.
	if settled, _ := deliver(t, f, stopEvent(job, "ev-2"), nil); settled != 1 {
		t.Fatalf("a resend while the row owns the job was not acknowledged: settled=%d", settled)
	}
}

// hk 5780 review: the plain-loss fallback is not a control outcome. Its receipt is
// the ordinary relay's, so the worker never acknowledges it as settled: a relay
// that could not persist must not retire the node's outbox row. That covers a job
// the hub never spawned, a role it does not carry on, a node that cannot close at
// the stop's sequence, and a stop that names no sequence.
func TestAnnouncedStopFiledAsALossIsNotAcknowledgedAsSettled_t5780(t *testing.T) {
	const job = "pw-fallback-5780"
	f := announcedFixture(t, job)
	if settled, filed := deliver(t, f, stopEvent("never-spawned", "ev-1"), nil); settled != 0 || filed != 1 {
		t.Fatalf("a job the hub never spawned: settled=%d filed=%d", settled, filed)
	}
	noSeq := stopEvent(job, "ev-2")
	noSeq.Reason = "quota_stop pool=claude reset_at=2026-11-19T05:59:11Z"
	if settled, filed := deliver(t, f, noSeq, nil); settled != 0 || filed != 1 {
		t.Fatalf("a stop that names no sequence: settled=%d filed=%d", settled, filed)
	}
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseOpStopSeq)
	f.hub.mu.Unlock()
	if settled, filed := deliver(t, f, stopEvent(job, "ev-3"), nil); settled != 0 || filed != 1 {
		t.Fatalf("a node that cannot fence the close: settled=%d filed=%d", settled, filed)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 0 {
		t.Fatalf("a hand-over was queued for a stop it could not fence: %+v", rows)
	}
}

// hk 5780 review: hk's answer that the task read failed (a timeout, another
// status, bad JSON) says nothing about the task's binding. Only a confirmed
// rebind is refused; an unreadable one leaves the stop with its node.
func TestAnnouncedStopWithAnUnreadableTaskBindingIsRetried_t5780(t *testing.T) {
	const job = "pw-unread-5780"
	f := announcedFixture(t, job)
	f.hub.handoverInputs = func(context.Context, string) handoverInputs { return handoverInputs{unreadable: true} }
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), nil)
	if settled != 0 || filed != 0 {
		t.Fatalf("settled=%d filed=%d, want the stop left with its node", settled, filed)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 0 {
		t.Fatalf("an unreadable binding was told to the owner as a rebind: %q", lines)
	}
	// A confirmed rebind is still refused, with one decision.
	f.hub.handoverInputs = func(context.Context, string) handoverInputs { return handoverInputs{bound: "another-job"} }
	if settled, _ := deliver(t, f, stopEvent(job, "ev-1"), nil); settled != 1 {
		t.Fatalf("a confirmed rebind was not settled: %d", settled)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 1 || !strings.Contains(lines[0], "task_rebound") {
		t.Fatalf("decisions=%q", lines)
	}
}

// hk 5780 review: a successor lookup that fails is a retryable read, not an
// outcome. prepareHandover answered already_replaced for it, and the worker
// acknowledged that, so a store timeout dropped the only announcement with no
// row, no successor and no decision. An existing successor counts only with its
// own lineage; another job that merely holds the next name is a refusal.
func TestPrepareHandoverTellsAFailedSuccessorLookupFromAnExistingSuccessor_t5780(t *testing.T) {
	const job = "pw-lookup-5780"
	f := announcedFixture(t, job)
	prior, err := fleet.ValidateSubmission(f.holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	record, _, _ := (&fleet.App{JobStore: f.jobs}).ExistingJob(t.Context(), job)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, refusal := f.hub.prepareHandover(canceled, poolExhaustion{Pool: "claude", Cause: poolCauseQuotaStop}, f.holder, prior, record, job+"-q1", "42"); refusal != handoverSuccessorUnavailable {
		t.Fatalf("a failed lookup refused as %q, want %q", refusal, handoverSuccessorUnavailable)
	}
	other, otherJob := parkRequest(401)
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), other, job+"-q1", "someone-else", time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = otherJob
	if _, refusal := f.hub.prepareHandover(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseQuotaStop}, f.holder, prior, record, job+"-q1", "42"); refusal != "successor_name_collision" {
		t.Fatalf("an unrelated job on the next name refused as %q, want successor_name_collision", refusal)
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
