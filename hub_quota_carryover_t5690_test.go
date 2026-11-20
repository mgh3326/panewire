package panewire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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
	return hubJobEventPayload{JobID: job, Epoch: 1, Reason: "quota_stop pool=" + pool + " reset_at=2026-11-19T05:59:11Z", OwnerLane: "parent"}
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
