package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

func poolActionFixture(t *testing.T) (*quotaReplaceFixture, poolAction) {
	t.Helper()
	f := newQuotaReplaceFixture(t, "reviewer", "action-job")
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, Epoch: 1}, Node: "machine-a", Orphaned: true}
	f.hub.mu.Unlock()
	args := &nodePaneCloseOpArgs{RequestID: "action-1", Job: f.job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project", EndReason: "quota_stop pool=claude"}
	action := poolAction{ID: args.RequestID, Job: f.job, Node: "machine-a", Epoch: 1, Phase: poolActionPending,
		Data: poolActionData{Owner: "parent", Text: "The job ended.", SpawnRequest: f.holder.Request.RequestID, Pane: args.PaneID, Label: args.Name, EndReason: args.EndReason, Args: args}}
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), action, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return f, action
}

func TestPoolActionSweepQueuesReviewerAndConsult(t *testing.T) {
	for _, tc := range []struct{ role, job, owner string }{
		{"reviewer", "mwr-42-p1", "mergewake"},
		{"consult", "consult-job", "parent"},
	} {
		t.Run(tc.role, func(t *testing.T) {
			f := newQuotaReplaceFixture(t, tc.role, tc.job)
			f.hub.mu.Lock()
			f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
			f.hub.jobs[tc.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: tc.job, Epoch: 2}, Node: "machine-a"}
			f.hub.mu.Unlock()
			result := f.hub.sweepPoolJob(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder)
			if result.action != poolJobQueued {
				t.Fatalf("sweep=%+v", result)
			}
			action := readOnlyPoolAction(t, f.hub)
			if action.Job != tc.job || action.Node != "machine-a" || action.Epoch != 2 || action.Data.Owner != tc.owner {
				t.Fatalf("wrong binding or owner: %+v", action)
			}
			if events := spawnFailedInjections(f.destination); len(events) != 0 {
				t.Fatalf("notice preceded close: %+v", events)
			}
		})
	}
}

type poolActionReply struct {
	op     string
	result map[string]any
}

func runPoolActionStep(t *testing.T, h *HubServer, node *websocket.Conn, action poolAction, replies ...poolActionReply) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.reconcilePoolAction(t.Context(), action) }()
	for _, reply := range replies {
		command := nodeCommandRead(t, node)
		var op string
		if json.Unmarshal(command["op"], &op) != nil || op != reply.op {
			t.Fatalf("command=%s, want %s", command["op"], reply.op)
		}
		nodeCommandReplyTo(t, node, command, map[string]any{"result": reply.result})
	}
	return <-done
}

func poolStatus(state string, released, settled bool) poolActionReply {
	return poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": state, "released": released, "settled": settled, "attempt": "00002-job.spawned.json"}}
}

func assertPoolActionFence(t *testing.T, f *quotaReplaceFixture, action poolAction, final bool) {
	t.Helper()
	_, err := f.hub.reassignJob(action.Job, "machine-b")
	if final {
		if err != nil {
			t.Fatalf("final action blocked reassignment: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), action.Job) || !strings.Contains(err.Error(), action.ID) {
		t.Fatalf("pending action did not name job and action: %v", err)
	}
}

func TestPoolActionFinalAnswersReleaseReassignment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response poolActionReply
	}{
		{"refused", poolStatus(fleet.CloseOpRefused, false, false)},
		{"preempted", poolStatus(fleet.CloseOpPreempted, false, false)},
		{"cancelled_released", poolStatus(fleet.CloseOpCancelled, true, false)},
		{"settled", poolStatus(fleet.CloseOpClosed, false, true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, action := poolActionFixture(t)
			assertPoolActionFence(t, f, action, false)
			if err := runPoolActionStep(t, f.hub, f.node, action, tc.response); err != nil {
				t.Fatal(err)
			}
			assertPoolActionFence(t, f, action, true)
		})
	}
}

func TestPoolActionUnknownAndRecordedWithoutSettlementStayPending(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
	}{{"unknown", fleet.CloseOpUnknown}, {"recorded", fleet.CloseOpClosed}} {
		t.Run(tc.name, func(t *testing.T) {
			f, action := poolActionFixture(t)
			if tc.name == "recorded" {
				f.jobs.mu.Lock()
				f.jobs.records[action.Job]["state"] = "lost"
				f.jobs.records[action.Job]["end_reason"] = action.Data.EndReason
				f.jobs.mu.Unlock()
			}
			replies := []poolActionReply{poolStatus(tc.state, false, false)}
			if tc.state == fleet.CloseOpUnknown {
				replies = append(replies, poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}})
			}
			if err := runPoolActionStep(t, f.hub, f.node, action, replies...); err != nil {
				t.Fatal(err)
			}
			assertPoolActionFence(t, f, action, false)
		})
	}
}

func TestPoolActionUnknownRetriesUnderNewIDAndKeepsBothFenced(t *testing.T) {
	f, action := poolActionFixture(t)
	err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(fleet.CloseOpUnknown, false, false),
		poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project"}}, "panes_ok": true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("retry actions=%+v err=%v", actions, err)
	}
	var original, retry poolAction
	for _, row := range actions {
		if row.ID == action.ID {
			original = row
		} else {
			retry = row
		}
	}
	if retry.ID == "" || retry.ID == action.ID || retry.Node != action.Node || retry.Epoch != action.Epoch {
		t.Fatalf("retry lost binding: original=%+v retry=%+v", original, retry)
	}
	if err := runPoolActionStep(t, f.hub, f.node, original, poolStatus(fleet.CloseOpUnknown, false, false)); err != nil {
		t.Fatal(err)
	}
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 2 {
		t.Fatalf("unknown spawned a duplicate retry: %+v %v", actions, err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err == nil || !strings.Contains(err.Error(), action.Job) || !strings.Contains(err.Error(), "action") {
		t.Fatalf("unknown requests did not fence reassignment: %v", err)
	}
}

func TestPoolActionTimeoutKeepsFence(t *testing.T) {
	f, action := poolActionFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(ctx, action) }()
	_ = nodeCommandRead(t, f.node) // The unanswered status times out.
	if err := <-done; err == nil {
		t.Fatal("unanswered status did not stay pending")
	}
	assertPoolActionFence(t, f, action, false)
}

func TestPoolActionFenceSurvivesHubRestart(t *testing.T) {
	f, action := poolActionFixture(t)
	restartPoolActionHub(t, f)
	f.hub.mu.Lock()
	f.hub.jobs[action.Job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: action.Job, Epoch: 1}, Node: action.Node, Orphaned: true}
	f.hub.mu.Unlock()
	assertPoolActionFence(t, f, action, false)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpPreempted, false, false)); err != nil {
		t.Fatal(err)
	}
	assertPoolActionFence(t, f, action, true)
}

func TestPoolActionReassignWithoutStoreWarnsOnce(t *testing.T) {
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	logs := captureHubLog(h)
	useLegacyOwnerStore(h)
	for _, id := range []string{"job-a", "job-b"} {
		h.jobs[id] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: id, Epoch: 1}, Node: "machine-a", Orphaned: true}
		if _, err := h.reassignJob(id, "machine-b"); err != nil {
			t.Fatalf("inbox-less reassignment: %v", err)
		}
	}
	if n := strings.Count(logs.String(), "close action finality unchecked without inbox store"); n != 1 {
		t.Fatalf("got %d unavailable-store warnings, want one: %s", n, logs.String())
	}
}

func TestPoolActionReconcilerStartsWithoutHeartbeatOrLivePane(t *testing.T) {
	f, action := poolActionFixture(t)
	f.hub.mu.Lock()
	delete(f.hub.jobs, action.Job)
	f.hub.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.runPoolActions(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	command := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": poolStatus(fleet.CloseOpClosed, false, true).result})
	eventually(t, 2*time.Second, "close settlement persisted", func() bool {
		actions, err := f.hub.inbox.store.activePoolActions(t.Context())
		return err == nil && len(actions) == 1 && actions[0].Phase == poolActionSettled
	})
}

func TestPoolActionFencesConcurrentQuotaReplacement(t *testing.T) {
	f, action := poolActionFixture(t)
	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(action.Job, "claude")); ok || job != "" {
		t.Fatalf("replacement bypassed close fence: job=%q ok=%v", job, ok)
	}
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if f.hub.reconcileQuotaReplacement(t.Context(), app, f.holder, action.Job) {
		t.Fatal("quota reconciliation bypassed close fence")
	}
	record, err := f.jobs.Get(t.Context(), action.Job)
	if err != nil || record["state"] != "spawned" {
		t.Fatalf("quota replacement changed job: %v %v", record, err)
	}
}

func TestPoolActionNeverSendsCloseToWrongOwnerOrEpoch(t *testing.T) {
	for _, kind := range []string{"node", "job_epoch", "command_epoch"} {
		t.Run(kind, func(t *testing.T) {
			f, action := poolActionFixture(t)
			f.hub.mu.Lock()
			switch kind {
			case "node":
				f.hub.jobs[action.Job].Node = "machine-b"
			case "job_epoch":
				f.hub.jobs[action.Job].Epoch = 2
			case "command_epoch":
				action.Data.Args.Epoch = 2
			}
			f.hub.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- f.hub.reconcilePoolAction(ctx, action) }()
			command := nodeCommandRead(t, f.node)
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": poolStatus(nodeCloseOpNone, false, false).result})
			extra := make(chan string, 1)
			go func() {
				for {
					_, raw, err := f.node.Read(ctx)
					if err != nil {
						extra <- ""
						return
					}
					var message struct{ Type, Op string }
					if json.Unmarshal(raw, &message) == nil && message.Type == "node.command" {
						extra <- message.Op
						return
					}
				}
			}()
			select {
			case op := <-extra:
				t.Fatalf("sent unexpected command %q", op)
			case err := <-done:
				if err == nil {
					t.Fatal("mismatched owner or epoch was accepted")
				}
			}
			cancel()
			<-extra
		})
	}
}

func TestPoolActionMismatchNeedsTombstone(t *testing.T) {
	for _, outcome := range []string{nodeCloseOpMismatch, nodeCaptureNotAJob} {
		for _, tombstone := range []bool{false, true} {
			t.Run(outcome+"/"+map[bool]string{false: "without", true: "with"}[tombstone], func(t *testing.T) {
				f, action := poolActionFixture(t)
				state := nodeCloseOpNone
				if tombstone {
					state = fleet.CloseOpCancelled
				}
				err := runPoolActionStep(t, f.hub, f.node, action,
					poolStatus(nodeCloseOpNone, false, false),
					poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": outcome, "replayed": false}},
					poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": state, "released": tombstone, "settled": false, "attempt": "00002-job.spawned.json"}},
				)
				if tombstone && err != nil || !tombstone && err == nil {
					t.Fatalf("tombstone=%v err=%v", tombstone, err)
				}
				assertPoolActionFence(t, f, action, tombstone)
			})
		}
	}
}

func TestPoolActionRefusedHoldsNoticeAndRole(t *testing.T) {
	f, action := poolActionFixture(t)
	app, err := f.hub.newSpawnApp(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if released, probe := f.hub.releaseFinishedSpawnRole(t.Context(), app, f.holder, map[string]any{"state": "spawned"}, "role"); released || probe.reason != "pool_action_pending" {
		t.Fatalf("role release: %v %+v", released, probe)
	}
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpRefused, false, false)); err != nil {
		t.Fatal(err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("refused close notified owner: %+v", events)
	}
}

func TestPoolActionWithdrawnNodeReleasesUnstartedRequest(t *testing.T) {
	f, action := poolActionFixture(t)
	f.hub.mu.Lock()
	delete(f.hub.nodes[action.Node].capabilities, hubCapabilityCloseOps)
	f.hub.mu.Unlock()
	err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPoolActionFence(t, f, action, true)
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("withdrawn close notified owner: %+v", events)
	}
}

func TestPoolActionNoticeWaitsForEveryPendingRequest(t *testing.T) {
	f, pending := poolActionFixture(t)
	// Old hubs could have reserved a reviewer role; it must stay held through
	// the final pending action and be released before the notice.
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_reservations SET task_role='legacy-reviewer' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	settled := pending
	settled.ID, settled.Phase = "settled-retry", poolActionSettled
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), settled, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = settled.Data.EndReason
	f.jobs.mu.Unlock()
	if err := f.hub.finishPoolAction(t.Context(), settled); err == nil {
		t.Fatal("notice advanced while an older request was pending")
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("pending request did not hold notice: %+v", events)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "legacy-reviewer"); err != nil {
		t.Fatalf("pending action released the role: %v", err)
	}
	if err := runPoolActionStep(t, f.hub, f.node, pending, poolStatus(fleet.CloseOpPreempted, false, false)); err != nil {
		t.Fatal(err)
	}
	if err := f.hub.finishPoolAction(t.Context(), settled); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "legacy-reviewer"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("settled action did not release the role: %v", err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 1 {
		t.Fatalf("settled requests did not produce one notice: %+v", events)
	}
}

func restartPoolActionHub(t *testing.T, f *quotaReplaceFixture) {
	t.Helper()
	old := f.hub
	path := filepath.Join(filepath.Dir(old.inbox.deferredPath), "inbox.sqlite3")
	routes, relay, newApp := old.reportRelayPath, old.handoffkeep, old.spawnNewApp
	_ = f.node.CloseNow()
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		InboxStorePath: path, ReportRelayPath: routes, handoffkeep: relay, SupersedeClose: old.supersedeClose,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.spawnNewApp = newApp
	hub.handoverInputs = old.handoverInputs
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	hub.mu.Unlock()
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	f.node = t14Node(t, hub, server, true)
	hub.mu.Lock()
	hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	hub.nodes["machine-a"].capabilities[hubCapabilityHandoverCheck] = true
	hub.mu.Unlock()
	f.hub, f.destination = hub, destination
}

func readOnlyPoolAction(t *testing.T, h *HubServer) poolAction {
	t.Helper()
	actions, err := h.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 1 {
		t.Fatalf("actions=%+v err=%v", actions, err)
	}
	return actions[0]
}

func TestPoolActionRestartAfterCloseAndBeforeNotice(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "restart-job")
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	exhaustion := poolExhaustion{Pool: "claude", Cause: poolCauseRetired}
	if result := f.hub.sweepPoolJob(t.Context(), exhaustion, f.holder); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project"}}, "panes_ok": true}},
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}},
	); err != nil {
		t.Fatal(err)
	}
	// Hub restart after the close, before the node has settled its hk write.
	restartPoolActionHub(t, f)
	action = readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpClosed, false, false)); err != nil {
		t.Fatal(err)
	}
	if id, err := f.hub.inbox.store.pendingPoolAction(t.Context(), f.job); err != nil || id != action.ID {
		t.Fatalf("restart lost close fence: %q %v", id, err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = action.Data.EndReason
	f.jobs.mu.Unlock()
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("notice preceded settlement: %+v", events)
	}
	// A second restart is after terminal settlement and before the notice.
	restartPoolActionHub(t, f)
	f.hub.reconcilePoolActions(t.Context())
	items, err := f.hub.inbox.store.list("parent", false)
	if err != nil || len(items) != 1 || items[0].Event.EventID != "pool-closed:"+f.job {
		t.Fatalf("notice identity: %+v, %v", items, err)
	}
	events := spawnFailedInjections(f.destination)
	if len(events) != 1 {
		t.Fatalf("notices after restart: %+v", events)
	}
	// The fixture destination accepted the relay; record its delivery as a
	// real receiving node does before testing a later hub restart.
	if err := f.hub.handoffkeep.markDelivered(t.Context(), events[0].EventID, "machine-b", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	posts := f.relay.count("POST", "/v1/relay/events")
	// Simulate a crash after the relay accepted the notice but before the
	// action's done update was committed.
	action.Phase = poolActionSettled
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	f.hub.reconcilePoolActions(t.Context())
	if after := f.relay.count("POST", "/v1/relay/events"); after != posts {
		t.Fatalf("notice was published again after restart: posts=%d, want %d", after, posts)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("notice repeated after restart: %+v", events)
	}
}

func TestPoolActionCompletionRaceKeepsCompletedRecord(t *testing.T) {
	f, action := poolActionFixture(t)
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.records[f.job]["end_reason"] = "completed independently"
	f.jobs.mu.Unlock()
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	record, err := f.jobs.Get(t.Context(), f.job)
	if err != nil || record["state"] != "completed" || record["end_reason"] != "completed independently" {
		t.Fatalf("completion overwritten: %v, %v", record, err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("completed job reported lost: %+v", events)
	}
	if actions, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 0 {
		t.Fatalf("completion not reconciled: %+v, %v", actions, err)
	}
}

func TestPoolActionCompletionBeforeCloseReleasesUnsentIntent(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Data.Args = nil
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}},
		poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPoolActionFence(t, f, action, true)
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("completed job got a close notice: %+v", events)
	}
}

func TestWriteJobLostReturnsActualTerminalRecord(t *testing.T) {
	jobs := &quotaJobs{records: map[string]map[string]any{
		"complete":  {"state": "completed", "end_reason": "already done"},
		"escalated": {"state": "escalated", "end_reason": "independent escalation"},
		"lost":      {"state": "lost", "end_reason": "prior loss"},
		"live":      {"state": "spawned"},
	}}
	app := &fleet.App{JobStore: jobs, Now: time.Now}
	for _, tc := range []struct{ job, state, reason string }{
		{"complete", "completed", "already done"}, {"escalated", "escalated", "independent escalation"},
		{"lost", "lost", "prior loss"}, {"live", "lost", "new loss"},
	} {
		t.Run(tc.job, func(t *testing.T) {
			state, reason := (&HubServer{}).writeJobLost(t.Context(), app, tc.job, "new loss")
			if state != tc.state || reason != tc.reason {
				t.Fatalf("got %q/%q, want %q/%q", state, reason, tc.state, tc.reason)
			}
		})
	}
}
