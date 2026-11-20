package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5780: an implementer's announced stop (the node's quota-stop manager decided
// a screen stop is a hand-over) goes through the guarded worktree hand-over, not
// the legacy close and not nothing. These tests drive the real job.lost frame or
// the replacement worker with no exhausted-pool reading anywhere.

// announcedCloseStatus is the node's pane.close_status for a job with no close
// request: the attempt the hub binds an announced stop's close to.
var announcedCloseStatus = map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "00002-job.spawned.json"}

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

// handoverRowsOf is the hand-over rows of job: the refusal rows an announced stop
// leaves are final records, not hand-overs.
func handoverRowsOf(t *testing.T, f *quotaReplaceFixture, job string) []poolAction {
	t.Helper()
	rows, err := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	var out []poolAction
	for _, row := range rows {
		if row.Data.Kind == poolActionKindHandover {
			out = append(out, row)
		}
	}
	return out
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
		case `"pane.close_status"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": announcedCloseStatus})
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
	// An implementer's pointer, as the requester contract and the legacy
	// replacement write it (hk 5780 review).
	if action.Data.EndReason != "quota_stop pool=claude handed_over_to="+next {
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
	if rows := handoverRowsOf(t, f, job); len(rows) != 0 {
		t.Fatalf("a refused hand-over left a hand-over row: %+v", rows)
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
	if rows := handoverRowsOf(t, f, job); len(rows) != 0 {
		t.Fatalf("the resend queued a hand-over after the refusal: %+v", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 1 {
		t.Fatalf("owner decisions=%q, want still one", lines)
	}
	// Another stop (new producer event) is decided afresh, and the clean branch now hands over.
	deliver(t, f, stopEvent(job, "ev-2"), func() { answerHandoverCheck(t, f, handoverReadReply(job)) })
	if rows := handoverRowsOf(t, f, job); len(rows) != 1 {
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
	if _, refusal, _ := f.hub.prepareHandover(canceled, poolExhaustion{Pool: "claude", Cause: poolCauseQuotaStop}, f.holder, prior, record, job+"-q1", "42"); refusal != handoverSuccessorUnavailable {
		t.Fatalf("a failed lookup refused as %q, want %q", refusal, handoverSuccessorUnavailable)
	}
	other, otherJob := parkRequest(401)
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), other, job+"-q1", "someone-else", time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = otherJob
	if _, refusal, _ := f.hub.prepareHandover(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseQuotaStop}, f.holder, prior, record, job+"-q1", "42"); refusal != "successor_name_collision" {
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

// queueAnnouncedRow stores a clean hand-over row for the announcement ev.
func queueAnnouncedRow(t *testing.T, f *quotaReplaceFixture, ev string) {
	t.Helper()
	deliver(t, f, stopEvent(f.job, ev), func() { answerHandoverCheck(t, f, handoverReadReply(f.job)) })
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), f.job)
	if len(rows) != 1 || rows[0].Data.Kind != poolActionKindHandover || rows[0].Data.Handover.StopEvent != ev {
		t.Fatalf("rows=%+v", rows)
	}
}

// hk 5780 review: a hand-over row that is queued owns the job, but only for the
// announcement that opened it. Another stop of the job (the pane resumed and
// stopped again) is not acknowledged on its account: its own outcome does not
// exist yet, and the node's resend is how it is decided once the row settles.
func TestAnotherStopIsNotAcknowledgedOnAQueuedRowsAccount_t5780(t *testing.T) {
	const job = "pw-two-stops-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	if settled, filed := deliver(t, f, stopEvent(job, "ev-2"), nil); settled != 0 || filed != 0 {
		t.Fatalf("another stop: settled=%d filed=%d, want it left with its node", settled, filed)
	}
	if settled, _ := deliver(t, f, stopEvent(job, "ev-1"), nil); settled != 1 {
		t.Fatalf("the row's own announcement was not acknowledged: settled=%d", settled)
	}
}

// hk 5780 review: overlapping deliveries of one announcement (a reconnect starts a
// second worker while the first is still probing) are serialized by the job's
// lock, held through preparation and the refusal. The second finds the first's
// refusal and probes nothing: it could otherwise queue a close once the owner has
// committed and pushed, contradicting the decision they hold.
func TestOverlappingDeliveriesOfOneAnnouncementAreSerialized_t5780(t *testing.T) {
	const job = "pw-overlap-5780"
	f := announcedFixture(t, job)
	var mu sync.Mutex
	settled := 0
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", stopEvent(job, "ev-1"), func() { mu.Lock(); settled++; mu.Unlock() }, &quotaAnnouncement{})
			done <- struct{}{}
		}()
	}
	// One probe and one worktree check for the two deliveries; a second worker
	// that did not wait would send its own and block the node script below.
	answerHandoverCheck(t, f, map[string]any{"outcome": handoverDirty})
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a delivery did not finish: it probed the node on its own")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if settled != 2 {
		t.Fatalf("settled=%d, want both deliveries settled", settled)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Data.Kind != poolActionKindRefusal || rows[0].Data.Handover.StopEvent != "ev-1" || rows[0].Data.Handover.Reason != handoverDirty || rows[0].Phase != poolActionDone {
		t.Fatalf("rows=%+v, want one final refusal row keyed by the announcement", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 1 {
		t.Fatalf("owner decisions=%q, want one", lines)
	}
}

// hk 5780 review: a node that is not connected says nothing about what it can do.
// A capable node's stop that reaches the worker across a disconnect stays with its
// node; it is filed as a loss only for a node connected without the capability.
func TestAnnouncedStopAcrossADisconnectIsNotFiledAsALoss_t5780(t *testing.T) {
	const job = "pw-disconnect-5780"
	f := announcedFixture(t, job)
	f.hub.mu.Lock()
	agent := f.hub.nodes["machine-a"].agent
	f.hub.nodes["machine-a"].agent = nil
	f.hub.mu.Unlock()
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), nil)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].agent = agent
	f.hub.mu.Unlock()
	if settled != 0 || filed != 0 {
		t.Fatalf("across a disconnect: settled=%d filed=%d, want the stop left with its node", settled, filed)
	}
}

// hk 5780 review: a queued, fenced hand-over outlives the capability check made at
// the announcement. A node that reconnects as an older binary still offers close
// ops but rejects stop_seq; a request not begun is released and the owner told,
// not retried against it for ever with the job fenced.
func TestFencedHandoverIsReleasedWhenTheNodeCanNoLongerFenceTheClose_t5780(t *testing.T) {
	const job = "pw-downgrade-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseOpStopSeq)
	f.hub.mu.Unlock()
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(nodeCloseOpNone, false, false), releasedReply); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase != poolActionRefused || rows[0].Data.Handover.Reason != "stop_seq_unsupported" || rows[0].Data.Handover.StopSeq == nil {
		t.Fatalf("rows=%+v, want a refused row with the reason and the stored fence", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 || !strings.Contains(lines[0], "stop_seq_unsupported") {
		t.Fatalf("owner notice=%q", lines)
	}
}

// hk 5780 review: the second read of the task, before the close, can fail
// transiently after the first succeeded and the stop was acknowledged. An
// unreadable binding keeps the row pending; only a confirmed change withdraws it.
func TestQueuedHandoverSurvivesAnUnreadableTaskBeforeTheClose_t5780(t *testing.T) {
	const job = "pw-reread-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	f.hub.handoverInputs = func(context.Context, string) handoverInputs { return handoverInputs{unreadable: true} }
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(nodeCloseOpNone, false, false)); err == nil {
		t.Fatal("an unreadable task binding was taken for a decision")
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase == poolActionRefused || rows[0].Phase == poolActionDone || rows[0].Data.Handover.Reason != "" {
		t.Fatalf("the row was withdrawn on an unreadable read: %+v", rows)
	}
}

// answerCheckThenSnapshot plays the node for a dirty worktree: the pane probe, the
// worktree check, and (when snapshot is set) the snapshot, whose args it hands to
// seen. Any other command fails the test.
func answerCheckThenSnapshot(t *testing.T, f *quotaReplaceFixture, snapshot map[string]any, seen func(nodeHandoverSnapshotArgs)) {
	t.Helper()
	for finished := false; !finished; {
		command := nodeCommandRead(t, f.node)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
		case `"pane.close_status"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": announcedCloseStatus})
		case `"worktree.handover_check"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": handoverDirty}})
			finished = snapshot == nil
		case `"worktree.handover_snapshot"`:
			if snapshot == nil {
				t.Fatal("a snapshot was asked of a node that cannot fence it")
			}
			var args nodeHandoverSnapshotArgs
			if err := json.Unmarshal(command["args"], &args); err != nil {
				t.Fatal(err)
			}
			seen(args)
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": snapshot})
			finished = true
		default:
			t.Fatalf("unexpected node command %s", command["op"])
		}
	}
}

// hk 5780 review: a snapshot publishes the worktree to origin before any close, so
// for a stop it is bound to the pane state itself. The announcement's sequence goes
// to the node with the snapshot command, and a node that cannot check it is not
// asked: the dirty worktree is refused with the owner's one decision instead.
func TestSnapshotOfAScreenStopIsBoundToTheStopSequence_t5780(t *testing.T) {
	t.Run("the sequence travels with the snapshot", func(t *testing.T) {
		const job = "pw-snap-seq-5780"
		f := announcedFixture(t, job)
		f.hub.mu.Lock()
		f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverSnapshot] = true
		f.hub.nodes["machine-a"].capabilities[hubCapabilitySnapshotStopSeq] = true
		f.hub.mu.Unlock()
		var got *int64
		deliver(t, f, stopEvent(job, "ev-1"), func() {
			answerCheckThenSnapshot(t, f, handoverSnapshotReply(job), func(a nodeHandoverSnapshotArgs) { got = a.StopSeq })
		})
		if got == nil || *got != 7 {
			t.Fatalf("snapshot stop_seq=%v, want 7", got)
		}
		if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 || rows[0].Data.Handover.Snapshot == nil {
			t.Fatalf("rows=%+v", rows)
		}
	})
	t.Run("a node that cannot check it is not asked", func(t *testing.T) {
		const job = "pw-snap-old-5780"
		f := announcedFixture(t, job)
		f.hub.mu.Lock()
		f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverSnapshot] = true
		f.hub.mu.Unlock()
		settled, _ := deliver(t, f, stopEvent(job, "ev-1"), func() { answerCheckThenSnapshot(t, f, nil, nil) })
		if settled != 1 {
			t.Fatalf("settled=%d", settled)
		}
		lines := ownerLines(t, f.hub, "pool-exhausted:")
		if len(lines) != 1 || !strings.Contains(lines[0], "snapshot_stop_seq_unsupported") {
			t.Fatalf("owner decisions=%q", lines)
		}
	})
}

// hk 5780 review: the quota manager hands over an implementer sitting on a blocked
// limit dialog. Its row is not held for an idle pane: a pane blocked on the stop's
// own dialog is the stop, closed only because the close is fenced to its sequence.
func TestBlockedImplementerAtTheStopIsClosedNotHeldForIdle_t5780(t *testing.T) {
	const job = "pw-dialog-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "blocked"),
		idlePane(job, "/work/project.checkout", "blocked"),
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}},
	); err != nil {
		t.Fatalf("a blocked pane at the stop was held for idle: %v", err)
	}
	row := readOnlyPoolAction(t, f.hub)
	if args := row.Data.Args; args == nil || args.AgentStatus != "blocked" || args.StopSeq == nil || *args.StopSeq != 7 || args.Handover == nil {
		t.Fatalf("close args=%+v, want the blocked pane, the sequence and the guard", row.Data.Args)
	}
}

// hk 5780 review: an announcement with a saved outcome is answered from it before
// the node's capabilities are consulted. A refusal was stored, its owner notice
// never went out, and the node came back as an older binary: the replay finishes
// the notice and settles. It is not filed as a plain loss for a pane that is still
// open, nor acknowledged by the ordinary relay with the decision unsent.
func TestSavedRefusalIsReplayedBeforeAnyCapabilityFallback_t5780(t *testing.T) {
	const job = "pw-saved-5780"
	f := announcedFixture(t, job)
	row := poolAction{ID: "refusal-row-1", Job: job, Node: "machine-a", Epoch: 1, Phase: poolActionDone, Data: poolActionData{
		Owner: "parent", Kind: poolActionKindRefusal,
		Handover: &poolHandover{Pool: "claude", Cause: poolCauseQuotaStop, StopEvent: "ev-1", Outcome: handoverFailed, Reason: handoverDirty},
	}}
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), row, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseOpStopSeq)
	f.hub.mu.Unlock()
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), nil)
	if settled != 1 || filed != 0 {
		t.Fatalf("settled=%d filed=%d, want the saved refusal answered and no loss filed", settled, filed)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 1 || !strings.Contains(lines[0], handoverDirty) {
		t.Fatalf("the stored refusal's notice was not finished: %q", lines)
	}
}

// hk 5780 review: unavailable evidence is not a refusal. A pane list cut short, or a
// node that does not answer the worktree check (it dropped, or timed out), leaves
// the announcement retryable with no final row and no notice; the same stop then
// hands over once the node answers.
func TestAnnouncedStopTransportFailuresStayRetryable_t5780(t *testing.T) {
	const job = "pw-transport-5780"
	f := announcedFixture(t, job)
	serveUntil := func(stopAt string, reply func(command map[string]json.RawMessage)) func() {
		return func() {
			for {
				command := nodeCommandRead(t, f.node)
				op := strings.Trim(string(command["op"]), `"`)
				switch op {
				case "pane.close_status":
					nodeCommandReplyTo(t, f.node, command, map[string]any{"result": announcedCloseStatus})
				case "agents.list":
					if stopAt == op {
						reply(command)
						return
					}
					roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
				case "worktree.handover_check":
					reply(command)
					return
				default:
					t.Fatalf("unexpected node command %s", op)
				}
			}
		}
	}
	// A pane list cut short (panes_ok false): the pane is not known gone.
	settled, filed := deliver(t, f, stopEvent(job, "ev-1"), serveUntil("agents.list", func(command map[string]json.RawMessage) {
		roleReplyPanes(t, f.node, command, []nodeAgentRow{}, false)
	}))
	if settled != 0 || filed != 0 {
		t.Fatalf("a cut pane list: settled=%d filed=%d, want the stop left with its node", settled, filed)
	}
	// The worktree check that gets no answer.
	settled, filed = deliver(t, f, stopEvent(job, "ev-1"), serveUntil("", func(command map[string]json.RawMessage) {
		nodeCommandReplyTo(t, f.node, command, map[string]any{"error": "herdr_failed"})
	}))
	if settled != 0 || filed != 0 {
		t.Fatalf("an unanswered worktree check: settled=%d filed=%d", settled, filed)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 0 {
		t.Fatalf("unavailable evidence was stored as an outcome: %+v", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-exhausted:"); len(lines) != 0 {
		t.Fatalf("the owner was told of a refusal that was never proved: %q", lines)
	}
	// The same stop, once the node answers, hands over.
	deliver(t, f, stopEvent(job, "ev-1"), func() { answerHandoverCheck(t, f, handoverReadReply(job)) })
	if rows := handoverRowsOf(t, f, job); len(rows) != 1 {
		t.Fatalf("the retried stop was not handed over: %+v", rows)
	}
}

// hk 5780 review: the row's close is bound to the attempt the stop was admitted
// in. A job re-prompted since has another attempt, and its close is released (the
// owner told), not run.
func TestHandoverCloseIsBoundToTheAdmittedAttempt_t5780(t *testing.T) {
	const job = "pw-attempt-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	action := readOnlyPoolAction(t, f.hub)
	if action.Data.TargetAttempt != "00002-job.spawned.json" || action.Data.Handover.Attempt != "00002-job.spawned.json" {
		t.Fatalf("row not bound to the admitted attempt: target=%q", action.Data.TargetAttempt)
	}
	moved := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "00009-job.reprompted.json"}}
	if err := runPoolActionStep(t, f.hub, f.node, action, moved, idlePane(job, "/work/project.checkout", "idle"), releasedReply); err != nil {
		t.Fatal(err)
	}
	if rows := handoverRowsOf(t, f, job); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("the re-prompted job's close was not released: %+v", rows)
	}
}

// hk 5780 review: a close the node journaled as closing, on a node that came back
// without stop-sequence support, is recovered by repeating its request without the
// fence field: the node answers a request it holds from its journal (closing
// becomes unknown) and starts no close for it. The row keeps its stored fence and
// the job stays fenced until that settles.
func TestClosingJournalIsRecoveredAcrossARollback_t5780(t *testing.T) {
	const job = "pw-closing-5780"
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "idle"),
		idlePane(job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}},
	); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseOpStopSeq)
	f.hub.mu.Unlock()
	action = readOnlyPoolAction(t, f.hub)
	if action.Data.Args == nil || action.Data.Args.StopSeq == nil {
		t.Fatalf("the stored close lost its fence: %+v", action.Data.Args)
	}
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), action) }()
	status := nodeCommandRead(t, f.node)
	if string(status["op"]) != `"pane.close_status"` {
		t.Fatalf("first command %s", status["op"])
	}
	nodeCommandReplyTo(t, f.node, status, map[string]any{"result": map[string]any{"state": fleet.CloseOpClosing, "released": false, "settled": false, "attempt": "00002-job.spawned.json"}})
	repeat := nodeCommandRead(t, f.node)
	if string(repeat["op"]) != `"pane.close_op"` || strings.Contains(string(repeat["args"]), "stop_seq") {
		t.Fatalf("the repeat for the closing journal is %s %s, want pane.close_op without stop_seq", repeat["op"], repeat["args"])
	}
	nodeCommandReplyTo(t, f.node, repeat, map[string]any{"result": map[string]any{"outcome": fleet.CloseOpUnknown, "replayed": false}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if after := readOnlyPoolAction(t, f.hub); after.Data.Args == nil || after.Data.Args.StopSeq == nil {
		t.Fatalf("the saved fence was dropped: %+v", after.Data.Args)
	}
}

// hk 5780 review: already_replaced needs the lineage the successor's claim wrote
// (replaced_from with its reason, the owner and --parent-job), not just a name and
// a flag, which an unrelated submission can also carry.
func TestAlreadyReplacedNeedsClaimWrittenLineage_t5780(t *testing.T) {
	prepare := func(t *testing.T, claim func(f *quotaReplaceFixture, row poolAction)) string {
		const job = "pw-lineage-5780"
		f := announcedFixture(t, job)
		queueAnnouncedRow(t, f, "ev-1")
		row := readOnlyPoolAction(t, f.hub)
		prior, err := fleet.ValidateSubmission(f.holder.Request)
		if err != nil {
			t.Fatal(err)
		}
		record, _, _ := (&fleet.App{JobStore: f.jobs}).ExistingJob(t.Context(), job)
		claim(f, row)
		_, refusal, _ := f.hub.prepareHandover(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseQuotaStop}, f.holder, prior, record, row.Data.Handover.Job, "42")
		return refusal
	}
	t.Run("a plain spawn that carries the next name", func(t *testing.T) {
		refusal := prepare(t, func(f *quotaReplaceFixture, row poolAction) {
			lookAlike, _ := parkRequest(402)
			if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), lookAlike, row.Data.Handover.Job, f.holder.Owner, time.Now()); err != nil {
				t.Fatal(err)
			}
		})
		if refusal != "successor_name_collision" {
			t.Fatalf("a look-alike was taken for the hand-over's successor: %q", refusal)
		}
	})
	t.Run("the successor the hand-over claimed", func(t *testing.T) {
		refusal := prepare(t, func(f *quotaReplaceFixture, row poolAction) {
			if _, _, err := f.hub.inbox.store.claimHandoverSuccessor(t.Context(), row.Data.Handover.Successor, row.Data.Handover.Job, row.Data.Owner, row.Data.SpawnRequest, row.Job, time.Now()); err != nil {
				t.Fatal(err)
			}
		})
		if refusal != "already_replaced" {
			t.Fatalf("the hand-over's own successor refused as %q", refusal)
		}
	})
	t.Run("a successor whose lineage the claim did not write", func(t *testing.T) {
		// Same name, owner and --parent-job, but the lineage is only derived from the
		// job's name (the startup backfill), not written by the claim.
		refusal := prepare(t, func(f *quotaReplaceFixture, row poolAction) {
			task, _, err := f.hub.inbox.store.claimHandoverSuccessor(t.Context(), row.Data.Handover.Successor, row.Data.Handover.Job, row.Data.Owner, row.Data.SpawnRequest, row.Job, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), f.hub.inbox.store.q(`UPDATE spawn_operations SET replaced_reason='' WHERE request_id=?`), task.Request.RequestID); err != nil {
				t.Fatal(err)
			}
		})
		if refusal != "successor_name_collision" {
			t.Fatalf("a name-derived lineage was taken for the claim's: %q", refusal)
		}
	})
}
