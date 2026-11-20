package panewire

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// serveCommands answers the next count node commands of a replacement the way a
// holder node does: the pane is there and idle until it is closed.
func (f *quotaReplaceFixture) serveCommands(t *testing.T, count int) []map[string]json.RawMessage {
	t.Helper()
	var commands []map[string]json.RawMessage
	for i := 0; i < count; i++ {
		command := nodeCommandRead(t, f.node)
		commands = append(commands, command)
		switch string(command["op"]) {
		case `"agents.list"`:
			rows := []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: f.paneStatus, Cwd: "/work/project.checkout"}}
			if f.paneClosed {
				rows = []nodeAgentRow{}
			}
			roleReplyPanes(t, f.node, command, rows, true)
		case `"pane.capture"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/" + f.job + "/pane-screen.txt"}})
		case `"pane.close"`:
			f.paneClosed = true
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
		case `"job.mark_lost"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
		default:
			t.Fatalf("unexpected node command %s", command["op"])
		}
	}
	return commands
}

// quotaGiveUpNotices drains the owner lane's injections and returns the give-up
// notices among them.
func quotaGiveUpNotices(destination *hubAgent) []hubRelayInjectEvent {
	var notices []hubRelayInjectEvent
	for _, relay := range spawnFailedInjections(destination) {
		if strings.Contains(relay.Text, "gave up") {
			notices = append(notices, relay)
		}
	}
	return notices
}

// relayedQuotaStop is the job.lost the node relays for an announced stop
// (hub_jobs_client.go): the reason carries the pool and the reset.
func relayedQuotaStop(job, pool string) hubJobEventPayload {
	return hubJobEventPayload{JobID: job, Epoch: 1, Reason: "quota_stop pool=" + pool + " reset_at=2026-11-19T05:59:11Z seq=7", OwnerLane: "parent"}
}

// hk 5690, the root cause of the 2026-11-19 losses: the hub began a replacement
// only for the bare reason "quota_stop", but every relayed announcement carries
// "pool=" and "reset_at=", so no announced stop was ever replaced. The relayed
// form starts the replacement (its first act is to look at the old pane).
func TestRelayedQuotaStopReasonStartsTheReplacement(t *testing.T) {
	for _, reason := range []string{"quota_stop", relayedQuotaStop("x", "devin-free").Reason} {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-5-p1")
		event := relayedQuotaStop(f.job, "devin-free")
		event.Reason = reason
		f.hub.startQuotaReplacement("machine-a", event)
		got := make(chan string, 1)
		go func() {
			got <- strings.Trim(string(nodeCommandRead(t, f.node)["op"]), `"`)
		}()
		select {
		case op := <-got:
			if op != "agents.list" {
				t.Fatalf("reason %q: first node command %q, want the pane probe", reason, op)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("reason %q started no replacement", reason)
		}
	}
	for _, reason := range []string{"", "quotastop", "completed", "quota_stopped pool=x"} {
		if isQuotaStopReason(reason) {
			t.Fatalf("%q is read as a quota stop", reason)
		}
	}
}

// A Devin reviewer at its first prompt shows "Reached free model rate limit"
// (stop pool devin-free). Its announced stop is carried on: the successor is
// placed on another pool (the free band is excluded), the old record ends with
// replaced_by, and the owner hears nothing (hk 5690).
func TestFirstPromptDevinFreeLimitReviewerIsCarriedOn(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-6-p1")
	f.stopPool = "devin-free"
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}
	result, commands := f.run(t, placed, 4)
	if !result.ok || result.job != "mwr-6-p1-q1" {
		t.Fatalf("replacement=%+v, want mwr-6-p1-q1", result)
	}
	args := f.successor(t, "mwr-6-p1-q1").Args
	if i := indexOf(args, "--exclude-pool"); i < 0 || args[i+1] != "devin-free" {
		t.Fatalf("the successor must not take the pool that stopped: %v", args)
	}
	if reason := endReasonOf(t, commands[3]); reason != "quota_stop pool=claude replaced_by=mwr-6-p1-q1" {
		t.Fatalf("end_reason=%q, want the record's pool and replaced_by last", reason)
	}
	time.Sleep(300 * time.Millisecond) // proves nothing more is sent to the requester
	if relays := spawnFailedInjections(f.destination); len(relays) != 0 {
		t.Fatalf("the requester was told of a successful carry-over: %+v", relays)
	}
}

func indexOf(args []string, flag string) int {
	for i, a := range args {
		if a == flag {
			return i
		}
	}
	return -1
}

// A reviewer whose limit leaves no other pool open is given up once: the old
// record ends lost with the plain reason carrying the stop's pool and reset, and
// the owner gets one notice.
func TestGiveUpWritesTheStopsOwnPoolAndTheResetOnce(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-7-p1")
	f.stopPool = "devin-free"
	event := relayedQuotaStop(f.job, "devin-free")
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", event)
		done <- quotaReplaceResult{job, ok}
	}()
	commands := f.serveCommands(t, 3) // probe, capture, close
	var successor hubSpawnTask
	next, _, _ := quotaReplacementJob(f.job)
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
		return err == nil
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 81, Stage: "placement", Reason: "no_eligible_host"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	commands = append(commands, f.serveCommands(t, 2)...) // probe (pane gone), mark_lost
	if got := <-done; got.ok {
		t.Fatalf("a give-up reported a replacement: %+v", got)
	}
	want := fleet.QuotaStopEndReason("devin-free", time.Date(2026, 11, 19, 5, 59, 11, 0, time.UTC), "", "")
	if reason := endReasonOf(t, commands[4]); reason != want || strings.Contains(reason, "replaced_by") {
		t.Fatalf("end_reason=%q, want %q", reason, want)
	}
	eventually(t, 2*time.Second, "the owner's give-up notice", func() bool { return len(quotaGiveUpNotices(f.destination)) == 1 })
}

// A pane that is busy when the hub looks has moved on: the stop is stale, so
// nothing is closed, written or announced to the owner (the watcher decides).
func TestGiveUpLeavesABusyPaneAlone(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-8-p1")
	f.paneStatus = "working"
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.giveUpQuotaStop(t.Context(), f.holder, "parent", relayedQuotaStop(f.job, "claude"), "not_placed")
	}()
	commands := f.serveCommands(t, 1)
	<-done
	if len(commands) != 1 {
		t.Fatalf("commands=%d, want only the probe", len(commands))
	}
	if relays := quotaGiveUpNotices(f.destination); len(relays) != 0 {
		t.Fatalf("a stale stop was given up: %+v", relays)
	}
	if state := f.jobs.records[f.job]["state"]; state != "spawned" {
		t.Fatalf("the busy job's record was changed: %v", state)
	}
}

// hk 5690 review: a reviewer's announced quota stop reaches the hub as a
// job.lost frame from its node. The frame starts the replacement (the real relay
// path, the relayed reason) and is a control event: the owner lane is not handed
// a loss for a review the hub is carrying on. An implementer's announcement is
// relayed to its owner as before and starts no replacement: the legacy close has
// none of the hand-over checks its worktree needs (dirty or unpushed work).
func TestAnnouncedReviewerQuotaStopStartsTheReplacementAndIsNotRelayedToTheOwner(t *testing.T) {
	for role, isReviewer := range map[string]bool{"reviewer": true, "implementer": false} {
		t.Run(role, func(t *testing.T) {
			f := newQuotaReplaceFixture(t, role, "mwr-12-p1")
			event := relayedQuotaStop(f.job, "devin-free")
			raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "owner_lane": "parent", "reason": event.Reason, "pane_id": roleHeldPane})
			if err != nil {
				t.Fatal(err)
			}
			if err := wsjson.Write(t.Context(), f.node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
				t.Fatal(err)
			}
			if isReviewer {
				// The replacement's first act is to look at the old pane.
				if op := strings.Trim(string(nodeCommandRead(t, f.node)["op"]), `"`); op != "agents.list" {
					t.Fatalf("first node command %q, want the pane probe", op)
				}
			} else {
				time.Sleep(1200 * time.Millisecond) // proves nothing is sent to the node for an implementer
				f.hub.mu.Lock()
				sent := len(f.hub.nodeCommands)
				f.hub.mu.Unlock()
				if sent != 0 {
					t.Fatalf("%d node commands for a relayed implementer stop", sent)
				}
			}
			relayed := false
			waitUntil(700*time.Millisecond, func() bool {
				for _, relay := range spawnFailedInjections(f.destination) {
					if relay.Kind == "job.lost" {
						relayed = true
					}
				}
				return relayed
			})
			if relayed == isReviewer {
				t.Fatalf("job.lost relayed to the owner = %v for a %s", relayed, role)
			}
		})
	}
}

// hk 5690 review: the pane state the stop was observed in travels in the relayed
// reason (seq=<n>) and fences the close on the node. The hub passes it in
// pane.close's stop_seq and closes nothing without it being fenced.
func TestReplacementFencesTheCloseToTheAnnouncedPaneState(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-14-p1")
	event := relayedQuotaStop(f.job, "claude")
	go f.hub.replaceQuotaStopped(t.Context(), "machine-a", event)
	commands := f.serveCommands(t, 3) // probe, capture, close
	var args map[string]json.RawMessage
	if err := json.Unmarshal(commands[2]["args"], &args); err != nil {
		t.Fatal(err)
	}
	if string(args["stop_seq"]) != "7" {
		t.Fatalf("pane.close args %s, want stop_seq 7", commands[2]["args"])
	}
	if seq, ok := quotaStopReasonSeq(event.Reason); !ok || seq != 7 {
		t.Fatalf("seq=%d ok=%v", seq, ok)
	}
	if _, ok := quotaStopReasonSeq("quota_stop pool=claude reset_at=2026-11-19T05:59:11Z"); ok {
		t.Fatal("a reason with no seq reads as fenced")
	}
}

// hk 5690 review: a reviewer's job.lost frame is acknowledged to its node only
// once the replacement is durable. While the hub is still looking at the pane
// nothing is acknowledged; a resent (replay) announcement after a hub restart
// starts the replacement again instead of being dropped.
func TestReviewerAnnouncementIsAcknowledgedOnlyOnceTheReplacementIsDurable(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-15-p1")
	acked := make(chan struct{}, 4)
	event := relayedQuotaStop(f.job, "claude")
	f.hub.startQuotaReplacementAck("machine-a", event, func() { acked <- struct{}{} })
	probe := nodeCommandRead(t, f.node)
	roleReplyPanes(t, f.node, probe, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	select {
	case <-acked:
		t.Fatal("acknowledged before the successor was claimed")
	case <-time.After(300 * time.Millisecond): // proves nothing more happens: the hub is still waiting on the close
	}
	f.serveCommands(t, 2) // capture, close
	next, _, _ := quotaReplacementJob(f.job)
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		_, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
		return err == nil
	})
	successor, _ := f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.serveCommands(t, 1) // mark_lost
	select {
	case <-acked:
	case <-time.After(3 * time.Second):
		t.Fatal("never acknowledged after the replacement completed")
	}
}

// A replayed reviewer announcement (the node resent it after a hub restart) is
// not dropped: it reaches the replacement, which is idempotent.
func TestReplayedReviewerAnnouncementStartsTheReplacement(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-16-p1")
	event := relayedQuotaStop(f.job, "claude")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "owner_lane": "parent", "reason": event.Reason, "pane_id": roleHeldPane, "replay": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(t.Context(), f.node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
		t.Fatal(err)
	}
	if op := strings.Trim(string(nodeCommandRead(t, f.node)["op"]), `"`); op != "agents.list" {
		t.Fatalf("first node command %q, want the pane probe of the replay", op)
	}
}

// A node that cannot fence the close (no pane_close_stop_seq_v1) closes nothing
// for a stop that carries a pane state: the watcher decides after its grace.
func TestFencedStopOnANodeWithoutTheFenceClosesNothing(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-17-p1")
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseStopSeq)
	f.hub.mu.Unlock()
	event := relayedQuotaStop(f.job, "claude")
	event.Reason += " seq=7"
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", event)
		done <- quotaReplaceResult{job, ok}
	}()
	commands := f.serveCommands(t, 1) // probe only
	select {
	case got := <-done:
		if got.ok {
			t.Fatalf("a stop that could not be fenced was replaced: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the hub went on to close an unfenceable pane")
	}
	if len(commands) != 1 {
		t.Fatalf("commands=%d, want only the probe", len(commands))
	}
}

// hk 5690 review: the watcher ends an unchanged stopped job itself when its
// 10 min grace runs out. A resent announcement that is too old for a replacement
// to finish before that is left to the watcher: nothing is probed, closed or
// claimed, because a successor placed after the plain loss would have no pointer.
func TestStaleResentAnnouncementIsLeftToTheWatcher(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-18-p1")
	event := relayedQuotaStop(f.job, "claude")
	event.Reason += " at=" + strconv.FormatInt(time.Now().Add(-9*time.Minute).Unix(), 10)
	acked := false
	job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked = true })
	if ok || job != "" {
		t.Fatalf("a stale announcement was replaced: %q", job)
	}
	if !acked {
		t.Fatal("a stale announcement was left unacknowledged: the node would resend it forever")
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-18-p1-q1"); err == nil {
		t.Fatal("a successor was claimed for a stale announcement")
	}
	if at, ok := quotaStopReasonAt(event.Reason); !ok || time.Since(at) < 8*time.Minute {
		t.Fatalf("at=%v ok=%v", at, ok)
	}
}

// hk 5690 review: a replacement starts as soon as the successor is placed, so it
// never inherits the start conditions the stopped job already met: they would
// park the successor in waiting, where it could be admitted after the review was
// given up.
func TestReplacementDropsTheStoppedJobsStartConditions(t *testing.T) {
	old := quotaHolderRequest("reviewer", "mwr-19-p1")
	old.StartConditions = []fleet.StartCondition{{Kind: "task_state", TaskID: "42", State: "merged"}}
	next, err := quotaReplacementRequest(old, "mwr-19-p1", "mwr-19-p1-q1", "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	if len(next.StartConditions) != 0 {
		t.Fatalf("the successor inherited start conditions: %+v", next.StartConditions)
	}
	if len(old.StartConditions) != 1 {
		t.Fatal("the stopped job's own request was changed")
	}
}

// hk 5690 review: a role the hub could not look up (its store failed) is unknown,
// not "not a reviewer": acting on it would relay a loss for a review the hub is
// about to carry on. A job the hub never spawned is known not to be a reviewer.
func TestReviewerRoleLookupFailureIsUnknownNotAnImplementer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-20-p1")
	event := relayedQuotaStop(f.job, "claude")
	if reviewer, known := f.hub.reviewerQuotaAnnouncement(t.Context(), event); !reviewer || !known {
		t.Fatalf("a spawned reviewer: reviewer=%v known=%v", reviewer, known)
	}
	if reviewer, known := f.hub.reviewerQuotaAnnouncement(t.Context(), relayedQuotaStop("never-spawned", "claude")); reviewer || !known {
		t.Fatalf("a job the hub never spawned: reviewer=%v known=%v, want known not-a-reviewer", reviewer, known)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if reviewer, known := f.hub.reviewerQuotaAnnouncement(canceled, event); reviewer || known {
		t.Fatalf("a failed lookup: reviewer=%v known=%v, want unknown", reviewer, known)
	}
}

// hk 5690 review: an hk read that fails (a brief outage) says nothing about the
// job: it may be live and carriable. The replacement decides nothing and the
// announcement is not acknowledged, so the node keeps it; the next delivery, with
// hk back, proceeds.
func TestHkReadFailureLeavesTheAnnouncementUnacknowledged(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-21-p1")
	f.jobs.failGets = 1
	acked := 0
	event := relayedQuotaStop(f.job, "claude")
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ }); ok || job != "" {
		t.Fatalf("a replacement was made with hk unreadable: %q", job)
	}
	if acked != 0 {
		t.Fatalf("the announcement was acknowledged %d times on a transient hk failure", acked)
	}
	// hk is back: the same announcement, delivered again, reaches the pane probe.
	go f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ })
	if op := strings.Trim(string(nodeCommandRead(t, f.node)["op"]), `"`); op != "agents.list" {
		t.Fatalf("first node command %q after hk recovered, want the pane probe", op)
	}
}

// hk 5690 review: a reviewer's announcement has no owner lane, which is the
// generic pool-cooling case the hub acknowledges at once. It is acknowledged only
// from the replacement's settled callback: while the hub is still replacing, the
// node receives no acknowledgement.
func TestReviewerAnnouncementWithNoOwnerLaneIsNotAcknowledgedAtOnce(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-22-p1")
	event := relayedQuotaStop(f.job, "devin-free")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "reason": event.Reason, "pane_id": roleHeldPane})
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(t.Context(), f.node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond) // proves no acknowledgement follows: the hub waits on its pane probe
	defer cancel()
	for {
		_, payload, err := f.node.Read(ctx)
		if err != nil {
			return
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil {
			continue
		}
		if typ := string(fields["type"]); typ != `"node.command"` {
			t.Fatalf("the node received %s while the replacement was still running: %s", typ, payload)
		}
	}
}

// hk 5690 review: an acknowledgement follows durable responsibility. An original
// spawn still settling (reconciling here) can still become the placed holder, so
// the stop stays retryable and is not acknowledged; a terminal holder and a job the
// hub never spawned are final and acknowledged.
func TestAcknowledgementFollowsAnExplicitOutcome(t *testing.T) {
	event := func(job string) hubJobEventPayload { return relayedQuotaStop(job, "claude") }
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-23-p1")
	acked := 0
	ack := func() { acked++ }

	// A job the hub never spawned: no row, a final answer.
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event("never-spawned"), ack); ok || job != "" || acked != 1 {
		t.Fatalf("unknown job: job=%q ok=%v acked=%d, want a final acknowledged skip", job, ok, acked)
	}
	// The original spawn is still settling: retry, no acknowledgement.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET status='reconciling' WHERE job=?`, f.job); err != nil {
		t.Fatal(err)
	}
	acked = 0
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event(f.job), ack); ok || job != "" || acked != 0 {
		t.Fatalf("unsettled holder: job=%q ok=%v acked=%d, want a retry with no acknowledgement", job, ok, acked)
	}
	// The original spawn ended for good: final.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET status='failed' WHERE job=?`, f.job); err != nil {
		t.Fatal(err)
	}
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event(f.job), ack); ok || job != "" || acked != 1 {
		t.Fatalf("terminal holder: job=%q ok=%v acked=%d, want a final acknowledged skip", job, ok, acked)
	}
}

// hk 5690 review: a reviewer's announcement whose role the hub could not look up
// (its store failed) is neither relayed nor acknowledged by the generic pool-cooling
// branch: the node keeps the row and sends it again.
func TestUnknownRoleAnnouncementIsNotAcknowledgedByTheCoolingBranch(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-24-p1")
	event := relayedQuotaStop(f.job, "devin-free")
	raw, err := json.Marshal(map[string]any{"job_id": f.job, "epoch": 1, "reason": event.Reason, "pane_id": roleHeldPane})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.inbox.store.db.Close(); err != nil { // every store read now fails: the role is unknown
		t.Fatal(err)
	}
	if err := wsjson.Write(t.Context(), f.node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond) // proves nothing is sent back: no acknowledgement for an unknown role
	defer cancel()
	for {
		_, payload, err := f.node.Read(ctx)
		if err != nil {
			return
		}
		t.Fatalf("the node received %s for an announcement whose role was unknown", payload)
	}
}

// hk 5690 review: a reviewer's close needs a verified stop sequence. An
// announcement with none (a row reserved before the upgrade) never closes a pane:
// a resumed reviewer is left alone, the announcement stays with its node, and the
// watcher's grace ends it as before. The give-up close takes the same rule.
func TestSeqlessAnnouncementNeverClosesAReviewersPane(t *testing.T) {
	for name, job := range map[string]string{"replacement": "mwr-25-p1", "give-up": "mwr-1-p1-q1-q2"} {
		t.Run(name, func(t *testing.T) {
			f := newQuotaReplaceFixture(t, "reviewer", job)
			event := hubJobEventPayload{JobID: job, Epoch: 1, Reason: "quota_stop pool=claude reset_at=2026-11-19T05:59:11Z", OwnerLane: "parent"}
			acked := 0
			done := make(chan quotaReplaceResult, 1)
			go func() {
				j, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ })
				done <- quotaReplaceResult{j, ok}
			}()
			commands := f.serveCommands(t, 1) // the pane probe
			select {
			case got := <-done:
				if got.ok {
					t.Fatalf("a seq-less stop was replaced: %+v", got)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the hub went on to close a pane for a stop with no verified sequence")
			}
			if len(commands) != 1 {
				t.Fatalf("commands=%d, want only the probe", len(commands))
			}
			if acked != 0 {
				t.Fatal("a seq-less announcement was acknowledged: the node would not send it again")
			}
			if state := f.jobs.records[job]["state"]; state != "spawned" {
				t.Fatalf("the job's record was changed: %v", state)
			}
		})
	}
}

// hk 5690 review: a reviewer's pane in herdr's blocked state at exactly the
// announced sequence is the stop's own dialog, and a fenced close counts it as
// stopped: the review is replaced and the close names the blocked status.
func TestBlockedReviewerAtTheAnnouncedSequenceIsReplaced(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-26-p1")
	f.paneStatus = "blocked"
	done := make(chan quotaReplaceResult, 1)
	go func() {
		j, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", relayedQuotaStop(f.job, "claude"))
		done <- quotaReplaceResult{j, ok}
	}()
	commands := f.serveCommands(t, 3) // probe, capture, close
	var args map[string]json.RawMessage
	if err := json.Unmarshal(commands[2]["args"], &args); err != nil {
		t.Fatal(err)
	}
	if string(args["agent_status"]) != `"blocked"` || string(args["stop_seq"]) != "7" {
		t.Fatalf("pane.close args %s, want the blocked status fenced to stop_seq 7", commands[2]["args"])
	}
	next, _, _ := quotaReplacementJob(f.job)
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		_, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
		return err == nil
	})
}

// hk 5690 review: a transient hk read inside the give-up is not a final skip. The
// announcement stays with its node; the next delivery, with hk back, gives up.
func TestGiveUpRetriesATransientHkRead(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-1-p1-q1-q2")
	f.jobs.failGets = 1
	event := quotaStopEvent(f.job, "claude")
	acked := 0
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ }); ok || job != "" || acked != 0 {
		t.Fatalf("a give-up on a failed hk read: job=%q ok=%v acked=%d, want a retry with no acknowledgement", job, ok, acked)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ })
	}()
	f.serveCommands(t, 4) // probe, capture, close, mark_lost: the give-up goes through
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the give-up did not finish")
	}
	if acked != 1 {
		t.Fatalf("acked=%d after the give-up completed, want 1", acked)
	}
}

// hk 5690 review: a replay after a hub restart finds the successor already claimed
// and finishes the replaced_by write, even when the announcement is too old for a
// fresh replacement (the age gate comes after the claimed-successor check).
func TestReplayAfterRestartFinishesAClaimedSuccessor(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-27-p1")
	next, _, _ := quotaReplacementJob(f.job)
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := f.hub.inbox.store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.paneClosed = true // the old pane was closed before the restart
	event := quotaStopEvent(f.job, "claude")
	event.Reason += " at=" + strconv.FormatInt(time.Now().Add(-9*time.Minute).Unix(), 10) // far past the age gate
	acked := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", event, func() { acked++ })
	}()
	commands := f.serveCommands(t, 2) // probe (pane gone), mark_lost
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the replay did not finish")
	}
	if reason := endReasonOf(t, commands[1]); reason != "quota_stop pool=claude replaced_by="+next {
		t.Fatalf("end_reason=%q, want the replaced_by pointer to the claimed successor", reason)
	}
	if acked != 1 {
		t.Fatalf("acked=%d, want the replayed announcement acknowledged once", acked)
	}
}

// hk 5690 review: the claimed-successor shortcut matches by lineage, not by name. A
// job that merely carries the next name in the chain (no replaced_from) is a name
// collision: it is refused, the record is kept, and the predecessor's pane is never
// closed on its account.
func TestSuccessorNameCollisionIsRefusedWithoutClosingTheTarget(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-28-p1")
	next, _, _ := quotaReplacementJob(f.job)
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	// Claimed under the replacement's name but not as replacing the job.
	collision, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, next, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), collision, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	acked := 0
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked++ }); ok || job != "" {
		t.Fatalf("a name collision was taken for a replacement: %q", job)
	}
	f.hub.mu.Lock()
	sent := len(f.hub.nodeCommands)
	f.hub.mu.Unlock()
	if sent != 0 {
		t.Fatalf("%d node commands sent on a name collision: the target's pane would be closed", sent)
	}
	if state := f.jobs.records[f.job]["state"]; state != "spawned" {
		t.Fatalf("the colliding name changed the job's record: %v", state)
	}
	if acked != 1 {
		t.Fatalf("acked=%d, want the refusal final", acked)
	}
}

// hk 5690 review: an acknowledgement follows finishQuotaReplacement's result. When the
// replaced_by write could not be made (hk unreadable here), the claimed successor's
// stop is retried with no acknowledgement; once hk is back the same delivery finishes.
func TestClaimedSuccessorIsAcknowledgedOnlyOnceItsPointerIsWritten(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-29-p1")
	next, _, _ := quotaReplacementJob(f.job)
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := f.hub.inbox.store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.paneClosed = true
	f.jobs.failGets = 1 // the pointer write cannot read the old record
	acked := 0
	if job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked++ }); ok || job != "" || acked != 0 {
		t.Fatalf("finish failed: job=%q ok=%v acked=%d, want a retry with no acknowledgement", job, ok, acked)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked++ })
	}()
	f.serveCommands(t, 2) // probe (pane gone), mark_lost
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery did not finish")
	}
	if acked != 1 {
		t.Fatalf("acked=%d once the pointer was written, want 1", acked)
	}
}

// hk 5690 review: a spawn operation that cannot be read (a store error) is unknown,
// not "not running": the wait keeps the last status it read and says it is not
// known, which the replacement turns into a retry and never a give-up.
func TestSpawnOutcomeThatCannotBeReadIsUnknown(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-30-p1")
	if _, _, known := f.hub.awaitSpawnOutcomeKnown(t.Context(), f.holder.Request.RequestID, 50*time.Millisecond); !known {
		t.Fatal("a readable operation reported as unknown")
	}
	if err := f.hub.inbox.store.db.Close(); err != nil {
		t.Fatal(err)
	}
	task, settled, known := f.hub.awaitSpawnOutcomeKnown(t.Context(), f.holder.Request.RequestID, 50*time.Millisecond)
	if known || settled {
		t.Fatalf("an unreadable operation: settled=%v known=%v, want unknown", settled, known)
	}
	_ = task
}
