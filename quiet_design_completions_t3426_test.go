package panewire

// hk 3426: a design lane is not interrupted by a worker's routine result. The
// completion of a job that has a task or a PR, and a lane.event declared
// --type ready, are kept in the lane's inbox without a pane injection;
// mergewake picks the PR up from there. Decisions, escalations and failures
// still interrupt, and non-design lanes are unchanged.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const t3426Lanes = `{"lanes":{
 "handoffkeep-design":{"machine":"host-a","pane":"w1:design"},
 "operator-desk":{"machine":"host-a","pane":"w1:desk"},
 "director-1":{"machine":"host-a","pane":"w1:director"},
 "worker-3404":{"machine":"host-a","pane":"w1:worker","parent":"handoffkeep-design"}
}}`

const t3426Design = "handoffkeep-design"

// t3426Jobs is the hk job-record store the hub reads to learn a job's task.
type t3426Jobs struct {
	records map[string]map[string]any
	err     error
}

func (t3426Jobs) Claim(context.Context, string, string, string) error { return nil }
func (t3426Jobs) Put(context.Context, string, map[string]any) error   { return nil }
func (j t3426Jobs) Get(_ context.Context, id string) (map[string]any, error) {
	if j.err != nil {
		return nil, j.err
	}
	if r, ok := j.records[id]; ok {
		return r, nil
	}
	return nil, fleet.ErrJobNotFound
}

var t3426Records = t3426Jobs{records: map[string]map[string]any{
	"hk3404-review3-fix": {"state": "completed", "task_id": float64(3404), "label": "worker-3404"},
	"hk3420-fix":         {"state": "completed", "task_id": float64(3420), "label": "worker-3420"},
	"consult-1":          {"state": "completed", "label": "consult-1"},
}}

// t3426Hub runs the shipped default policy over three lanes on one connected
// node. Every pane is an idle claude session, the case in which an inbox-class
// owner completion would otherwise be promoted straight into the pane.
func t3426Hub(t *testing.T, path string, hk *handoffkeepRelayClient, jobs fleet.JobStore) (*HubServer, *hubAgent) {
	t.Helper()
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, t3426Lanes),
		DeliveryPolicyPath: "default",
		InboxStorePath:     path,
		handoffkeep:        hk,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 32), persisted: make(chan hubRelayPersistedEvent, 32)}
	var sessions []HubSession
	for _, pane := range []string{"w1:design", "w1:desk", "w1:director"} {
		sessions = append(sessions, HubSession{PaneID: pane, Status: "idle", Harness: "claude"})
	}
	h.mu.Lock()
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: agent, sessionSnapshot: &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: time.Now()}}
	h.mu.Unlock()
	return h, agent
}

func t3426Completion(job, lane, pr string) hubJobEventPayload {
	return hubJobEventPayload{JobID: job, Epoch: 1, OwnerLane: lane, AgentLabel: "worker-" + job, Label: "worker-" + job, Host: "host-b", ReportPath: "/jobs/" + job + "/report.md", ReportLastLine: "READY PR green, report written", EventID: "00001-job.completed.json", PR: pr}
}

func t3426Ready(lane, id string) hubJobEventPayload {
	return hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, NotifyType: "ready", Text: "READY hk3404 PR #12 head abc1234 report /jobs/hk3404/report.md"}
}

// t3426Injected drains the node's relay channel and returns the injections.
func t3426Injected(a *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case d := <-a.relays:
			out = append(out, d)
		default:
			return out
		}
	}
}

func t3426Drain(a *hubAgent) {
	t3426Injected(a)
	for {
		select {
		case <-a.persisted:
		default:
			return
		}
	}
}

// t3426Item returns the lane's inbox item for one producer event, failing when
// it is absent.
func t3426Item(t *testing.T, h *HubServer, lane, kind, jobID string) laneInboxItem {
	t.Helper()
	items, err := h.inbox.store.list(lane, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Kind == kind && item.Event.JobID == jobID {
			return item
		}
	}
	t.Fatalf("%s has no %s item for %s: %+v", lane, kind, jobID, items)
	return laneInboxItem{}
}

// t3426RelayRows counts the hk relay rows the hub wrote for one job id.
func t3426RelayRows(f *fakeHandoffkeep, jobID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := 0
	for _, call := range f.calls {
		if call.Method == http.MethodPost && call.Path == "/v1/relay/events" && call.Body["job_id"] == jobID {
			rows++
		}
	}
	return rows
}

// (1) a design lane is not interrupted by the completion of a job with a task
// or a PR. The completion is in its inbox as an unread inbox item, the node's
// outbox is acknowledged, no hk relay row is written (as for any inbox item),
// and neither a restart replay nor the digest puts it in the pane later.
func TestQuietDesignCompletionOfTrackedJobIsNotInjected(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent := t3426Hub(t, path, hk, t3426Records)

	for _, tc := range []struct{ name, job, pr string }{
		{"task", "hk3404-review3-fix", ""},
		{"pr", "pr-only-job", "https://github.com/example/repo/pull/12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.relayJobCompletionFrom("host-a", t3426Completion(tc.job, t3426Design, tc.pr))
			if got := t3426Injected(agent); len(got) != 0 {
				t.Fatalf("design lane was injected: %+v", got)
			}
			item := t3426Item(t, h, t3426Design, "job.completed", tc.job)
			if item.Class != "inbox" || item.ReadAt != 0 || item.DueAt != 0 || item.DigestAt != 0 {
				t.Fatalf("completion stored as %+v, want an unread inbox item with no deadline", item)
			}
			select {
			case p := <-agent.persisted:
				if p.JobID != tc.job || p.EventID != item.ID {
					t.Fatalf("persisted ack=%+v, want job %s inbox id %d", p, tc.job, item.ID)
				}
			default:
				t.Fatal("the node's outbox row was not acknowledged")
			}
			if rows := t3426RelayRows(fake, tc.job); rows != 0 {
				t.Fatalf("hk relay rows for an inbox item=%d, want 0", rows)
			}
		})
	}

	// A completion that an older hub had persisted and never delivered is
	// still owed by hk at the restart.
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 501, Kind: "job.completed", JobID: "hk3420-fix", Epoch: 1, OwnerLane: t3426Design, ReportPath: "/jobs/hk3420-fix/report.md", ReportLastLine: "READY PR green", EventID: "00001-job.completed.json"})
	h.Close()
	h2, agent2 := t3426Hub(t, path, hk, t3426Records)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())
	h2.flushInbox()
	if err := h2.inbox.store.makeBatches(time.Now().Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	h2.flushInbox()
	if got := t3426Injected(agent2); len(got) != 0 {
		t.Fatalf("restart replay or digest injected the design lane: %+v", got)
	}
	batches, err := h2.inbox.store.pendingBatches()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range batches {
		if b.Lane == t3426Design {
			t.Fatalf("design lane digest batch: %+v", b)
		}
	}
	if item := t3426Item(t, h2, t3426Design, "job.completed", "hk3420-fix"); item.Class != "inbox" {
		t.Fatalf("replayed completion stored as %+v", item)
	}
	if got := fake.deliveredToFor(501); got == "" {
		t.Fatal("the replayed hk row was not retired")
	}
}

// (2) a worker's lane.event --type ready to a design lane is kept in the inbox,
// even when the sender asked for an interrupt, and a restart does not put it
// in the pane.
func TestQuietDesignReadyIsNotInjected(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent := t3426Hub(t, path, hk, t3426Records)

	plain := t3426Ready(t3426Design, "ready-hk3404")
	explicit := t3426Ready(t3426Design, "ready-hk3420")
	explicit.DeliveryClass = "interrupt"
	for _, e := range []hubJobEventPayload{plain, explicit} {
		if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed || !r.Routed || r.Machine != "inbox" {
			t.Fatalf("%s relay=%+v, want an inbox item", e.EventID, r)
		}
		if got := t3426Injected(agent); len(got) != 0 {
			t.Fatalf("%s injected the design lane: %+v", e.EventID, got)
		}
		item := t3426Item(t, h, t3426Design, "lane.event", e.JobID)
		if item.Class != "inbox" || !item.MustAck || item.AckedAt != 0 || item.DueAt != 0 {
			t.Fatalf("%s stored as %+v, want an unacked inbox item with no deadline", e.EventID, item)
		}
		if rows := t3426RelayRows(fake, e.JobID); rows != 0 {
			t.Fatalf("%s hk relay rows=%d, want 0", e.EventID, rows)
		}
	}

	h.Close()
	h2, agent2 := t3426Hub(t, path, hk, t3426Records)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())
	if err := h2.inbox.store.makeBatches(time.Now().Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	h2.flushInbox()
	if got := t3426Injected(agent2); len(got) != 0 {
		t.Fatalf("restart replay or digest injected the design lane: %+v", got)
	}
	// A retry of the same event after the restart is still not injected.
	if r := h2.relayLaneEvent(plain, agent2); r.ID == 0 || r.PersistFailed {
		t.Fatalf("retry relay=%+v", r)
	}
	if got := t3426Injected(agent2); len(got) != 0 {
		t.Fatalf("a retried READY injected the design lane: %+v", got)
	}
}

// (3) a completion of a job with neither a task nor a PR -- a consult answer
// the design session waits for -- is not quieted, and neither is one whose
// job record cannot be read. Since hk 3602 it is filed and wakes the lane.
func TestQuietDesignCompletionWithoutTaskOrPRWakes(t *testing.T) {
	for _, tc := range []struct {
		name string
		jobs t3426Jobs
		job  string
	}{
		{"no task", t3426Records, "consult-1"},
		{"no job record", t3426Records, "unrecorded-job"},
		{"job record unreadable", t3426Jobs{err: context.DeadlineExceeded}, "hk3404-review3-fix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			t.Cleanup(closeServer)
			h, agent := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, tc.jobs)
			h.relayJobCompletionFrom("host-a", t3426Completion(tc.job, t3426Design, ""))
			t3602AssertWoken(t, h, t3426Injected(agent), t3426Design, "w1:design", "job.completed", tc.job)
		})
	}
}

// (4) the notices a design lane must act on still reach it: an escalation of
// a job with a task, and lane events declared decision, blocked, findings and
// automation-failed (SPAWN_FAILED, TASK_LINK_FAILED). Since hk 3602 each is
// filed and wakes the lane.
func TestQuietDesignLaneStillGetsEscalationsAndFailures(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, agent := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)

	// An escalation travels from the worker's lane to the lane it reports to.
	escalation := t3426Completion("hk3404-review3-fix", "worker-3404", "")
	escalation.EventID = "00002-job.escalate.json"
	escalation.Question = "ESC which migration order do we ship?"
	escalation.Reason = "needs_decision"
	h.relayJobEventFrom("host-a", "job.escalate", escalation)
	t3602AssertWoken(t, h, t3426Injected(agent), t3426Design, "w1:design", "job.escalate", escalation.JobID)
	for _, tc := range []struct{ id, notifyType, text string }{
		{"decision", "decision", "ESC which release do we ship?"},
		{"blocked", "blocked", "BLOCKED review needs an operator decision"},
		{"findings", "findings", "FINDINGS 2 on PR #12"},
		{"spawn-failed", "automation-failed", "[panewire] SPAWN_FAILED job-1 request=r1 host=none profile=none rc=70 stage=launch replay=no reason=spawn_failed fix=Check the checkout."},
		{"task-link-failed", "automation-failed", "[panewire] TASK_LINK_FAILED job=job-2 task=7 reason=task claim exited with status 1; link the task manually"},
	} {
		e := hubJobEventPayload{JobID: laneEventTransportID(t3426Design, tc.id), Epoch: 1, OwnerLane: t3426Design, EventID: tc.id, NotifyType: tc.notifyType, Text: tc.text}
		if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed {
			t.Fatalf("%s relay=%+v", tc.id, r)
		}
		t3602AssertWoken(t, h, t3426Injected(agent), t3426Design, "w1:design", "lane.event", e.JobID)
	}
}

// (5) the same completion and READY reach operator-desk and a non-design lane
// exactly as before: the desk keeps both in its inbox for its scheduler, and
// director-1 is interrupted by both.
func TestQuietDesignNonDesignLanesUnchanged(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, agent := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)

	for _, tc := range []struct {
		lane    string
		injects bool
	}{{"operator-desk", false}, {"director-1", true}} {
		t.Run(tc.lane, func(t *testing.T) {
			t3426Drain(agent)
			completion := t3426Completion("hk3404-review3-fix", tc.lane, "")
			completion.EventID = "00001-" + tc.lane + "-job.completed.json"
			h.relayJobCompletionFrom("host-a", completion)
			got := t3426Injected(agent)
			if tc.injects && (len(got) != 1 || got[0].JobID != completion.JobID) || !tc.injects && len(got) != 0 {
				t.Fatalf("completion injections=%+v, want injected=%t", got, tc.injects)
			}
			ready := t3426Ready(tc.lane, "ready-"+tc.lane)
			if r := h.relayLaneEvent(ready, agent); r.ID == 0 || r.PersistFailed {
				t.Fatalf("ready relay=%+v", r)
			}
			got = t3426Injected(agent)
			if tc.injects && (len(got) != 1 || got[0].JobID != ready.JobID) || !tc.injects && len(got) != 0 {
				t.Fatalf("ready injections=%+v, want injected=%t", got, tc.injects)
			}
		})
	}
}
