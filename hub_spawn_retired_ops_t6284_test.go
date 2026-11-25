package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6284 (operator decision 494, the gate for change 3): what every reader of
// a spawn operation does for a job whose operations change 3 would have
// deleted. Change 3 is not built; retireSpawnOps writes the end state it would
// leave. This file covers hand-over, pool action, quota replace, escalate and
// the staged ladder step; the other readers by job id are in
// hub_spawn_retired_by_job_t6284_test.go and the readers by request id in
// hub_spawn_retired_by_request_t6284_test.go.
//
// A test skipped with "decision 494 adverse finding" asserts the acceptable
// outcome that its reader does not give today. Remove the skip to see the
// failure.

// retireSpawnOps leaves the store as change 3 would for each request: the
// operation, its notifications, its placement snapshots and their queue rows
// are gone, and the reservation row stays. Like change 3 it takes only a
// terminal operation whose reservation holds no task role.
func retireSpawnOps(t *testing.T, store *laneInboxStore, requestIDs ...string) {
	t.Helper()
	count := func(query string, args ...any) (n int) {
		t.Helper()
		if err := store.db.QueryRowContext(t.Context(), store.q(query), args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, id := range requestIDs {
		task, err := store.spawnTask(t.Context(), id)
		if err != nil {
			t.Fatalf("retire %s: %v", id, err)
		}
		if !spawnTerminalStatus(task.Status) {
			t.Fatalf("retire %s: status %q is not terminal; change 3 keeps it", id, task.Status)
		}
		if count(`SELECT count(*) FROM spawn_reservations WHERE request_id=? AND task_role IS NOT NULL`, id) != 0 {
			t.Fatalf("retire %s: its reservation holds a task role; change 3 keeps it", id)
		}
		reservations := count(`SELECT count(*) FROM spawn_reservations`)
		for _, query := range []string{
			`DELETE FROM spawn_notice_outbox WHERE event_id IN (SELECT event_id FROM spawn_notifications WHERE request_id=?)`,
			`DELETE FROM spawn_notifications WHERE request_id=?`,
			`DELETE FROM placement_record_outbox WHERE request_id=?`,
			`DELETE FROM spawn_placement_snapshots WHERE request_id=?`,
			`DELETE FROM spawn_attempt_grants WHERE node_id IN (SELECT node_id FROM spawn_operations WHERE request_id=?)`,
			`DELETE FROM spawn_attempt_start_by WHERE node_id IN (SELECT node_id FROM spawn_operations WHERE request_id=?)`,
			`DELETE FROM spawn_queue WHERE request_id=?`,
			`DELETE FROM task_fail_notice_outbox WHERE request_id=?`,
			`DELETE FROM spawn_operations WHERE request_id=?`,
		} {
			if _, err := store.db.ExecContext(t.Context(), store.q(query), id); err != nil {
				t.Fatalf("retire %s: %s: %v", id, query, err)
			}
		}
		if _, err := store.spawnTask(t.Context(), id); err == nil {
			t.Fatalf("retire %s: the operation is still readable", id)
		}
		if got := count(`SELECT count(*) FROM spawn_reservations`); got != reservations {
			t.Fatalf("retire %s: reservations %d -> %d; the row must stay", id, reservations, got)
		}
	}
}

// retireEndedJob releases the role of each of job's operations, as a proven end
// would before change 3 runs, and retires them all.
func retireEndedJob(t *testing.T, store *laneInboxStore, job string) []string {
	t.Helper()
	ids := jobOperationIDs(t, store, job)
	if len(ids) == 0 {
		t.Fatalf("job %s has no operation to retire", job)
	}
	for _, id := range ids {
		if err := store.releaseCompletedSpawnRoleOf(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	retireSpawnOps(t, store, ids...)
	return ids
}

func jobOperationIDs(t *testing.T, store *laneInboxStore, job string) (ids []string) {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), store.q(`SELECT request_id FROM spawn_operations WHERE job=? ORDER BY created_ms,request_id`), job)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// reservationRows reads the whole reservation table as "job request role", so a
// test can assert that a reader released, restored or claimed nothing.
func reservationRows(t *testing.T, store *laneInboxStore) (out []string) {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), `SELECT job,request_id,coalesce(task_role,'-') FROM spawn_reservations ORDER BY job`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var job, id, role string
		if err := rows.Scan(&job, &id, &role); err != nil {
			t.Fatal(err)
		}
		out = append(out, job+" "+id+" "+role)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func operationCount(t *testing.T, store *laneInboxStore) (n int) {
	t.Helper()
	if err := store.db.QueryRowContext(t.Context(), `SELECT count(*) FROM spawn_operations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// retiredNameHolder is a job under the given name, spawned for task 43 and so
// unrelated to the fixture's task, that ran, completed and was retired. Only its
// reservation row is left.
func retiredNameHolder(t *testing.T, store *laneInboxStore, jobs *quotaJobs, job string) {
	t.Helper()
	request := quotaHolderRequest("implementer", job)
	request.RequestID = t14RequestID(628499)
	for i, arg := range request.Args {
		if arg == "--task" {
			request.Args[i+1] = "43"
		}
	}
	task, _, err := store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-b", Pane: "w9:p9"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	jobs.mu.Lock()
	jobs.records[job] = map[string]any{"state": "completed", "pool": "claude"}
	jobs.mu.Unlock()
	retireEndedJob(t, store, job)
}

func setJobRecord(jobs *quotaJobs, job, state, endReason string) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.records[job] == nil {
		jobs.records[job] = map[string]any{"pool": "claude"}
	}
	jobs.records[job]["state"] = state
	if endReason != "" {
		jobs.records[job]["end_reason"] = endReason
	}
}

func jobRecordOf(jobs *quotaJobs, job string) (state, endReason string) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	state, _ = jobs.records[job]["state"].(string)
	endReason, _ = jobs.records[job]["end_reason"].(string)
	return state, endReason
}

// retiredNode plays machine-a for a whole test: it answers every node command
// with the held pane's usual replies, keeps the commands it was sent, and writes
// the hk record lost where a real node would.
type retiredNode struct {
	mu     sync.Mutex
	ops    []string
	closed bool
}

func (n *retiredNode) sent() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.ops)
}

// acted are the commands that close a pane or end a job record.
func (n *retiredNode) acted() (out []string) {
	for _, op := range n.sent() {
		switch op {
		case "pane.close", nodeCommandPaneCloseOp, nodeCommandLadderClose, "job.mark_lost":
			out = append(out, op)
		}
	}
	return out
}

func serveRetiredNode(t *testing.T, f *quotaReplaceFixture) *retiredNode {
	t.Helper()
	n := &retiredNode{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, payload, err := f.node.Read(ctx)
			if err != nil {
				return
			}
			var command map[string]json.RawMessage
			if json.Unmarshal(payload, &command) != nil || string(command["type"]) != `"node.command"` {
				continue
			}
			var requestID, op string
			_ = json.Unmarshal(command["request_id"], &requestID)
			_ = json.Unmarshal(command["op"], &op)
			n.mu.Lock()
			n.ops = append(n.ops, op)
			closed := n.closed
			var result any
			switch op {
			case nodeCommandAgentsList:
				panes := []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}
				if closed {
					panes = []nodeAgentRow{}
				}
				result = map[string]any{"agents": []nodeAgentRow{}, "panes": panes, "panes_ok": true}
			case "pane.capture":
				result = map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/" + f.job + "/pane-screen.txt"}
			case "pane.close":
				n.closed = true
				result = map[string]any{"outcome": nodePaneClosed}
			case "job.mark_lost":
				var args nodeJobMarkLostArgs
				_ = json.Unmarshal(command["args"], &args)
				setJobRecord(f.jobs, f.job, "lost", args.EndReason)
				result = map[string]any{"outcome": nodeJobMarkLostLost}
			case "job.result":
				result = map[string]any{"found": false, "quota_resumes": 0}
			case "worktree.handover_check":
				result = handoverReadReply(f.job)
			case nodeCommandPaneCloseStatus:
				state := nodeCloseOpNone
				if closed {
					state = fleet.CloseOpClosed
				}
				result = map[string]any{"state": state, "released": false, "settled": closed, "attempt": "00002-job.spawned.json"}
			case nodeCommandPaneCloseOp:
				var args nodePaneCloseOpArgs
				_ = json.Unmarshal(command["args"], &args)
				n.closed = true
				setJobRecord(f.jobs, f.job, "lost", args.EndReason)
				result = map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}
			}
			n.mu.Unlock()
			message := map[string]any{"type": "node.command.result", "request_id": requestID, "op": op, "result": result}
			if result == nil {
				message = map[string]any{"type": "node.command.result", "request_id": requestID, "op": op, "error": "invalid_args"}
			}
			if wsjson.Write(ctx, f.node, message) != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return n
}

// drivePoolActions reconciles every active pool action up to passes times and
// returns the last error of a reconcile.
func drivePoolActions(t *testing.T, h *HubServer, passes int) (last error) {
	t.Helper()
	for range passes {
		actions, err := h.inbox.store.activePoolActions(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(actions) == 0 {
			return nil
		}
		last = h.reconcilePoolAction(t.Context(), actions[0])
	}
	return last
}

func poolActionSummary(t *testing.T, h *HubServer, job string) string {
	t.Helper()
	rows, err := h.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, row := range rows {
		line := "phase=" + string(row.Phase) + " kind=" + row.Data.Kind
		if handover := row.Data.Handover; handover != nil {
			line += " successor=" + handover.Job + " outcome=" + handover.Outcome + " reason=" + handover.Reason
		}
		out = append(out, line)
	}
	return strings.Join(out, "; ")
}

// Reader hub_pool_handover.go:442 (prepareHandover, through the pool sweep): is
// the successor's job name free?
func TestRetiredOpHandoverOverARetiredSuccessorName_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: a hand-over is queued over a retired successor name; the live pane is closed and the successor claim is then refused")
	f := handoverFixture(t, "pw-6284-ho", 1, time.Hour)
	node := serveRetiredNode(t, f)
	next := f.job + "-q1"
	retiredNameHolder(t, f.hub.inbox.store, f.jobs, next)

	result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder)
	if result.action != poolJobQueued {
		if acted := node.acted(); len(acted) != 0 {
			t.Fatalf("a refused hand-over still acted on the node: %v", acted)
		}
		return
	}
	_ = drivePoolActions(t, f.hub, 6)
	state, endReason := jobRecordOf(f.jobs, f.job)
	t.Fatalf("a hand-over was queued over the retired job name %s: node actions %v; the live job's record is %s (%q); row %s", next, node.acted(), state, endReason, poolActionSummary(t, f.hub, f.job))
}

// Reader hub_pool_action.go:93 (createPoolAction, the admission of a hand-over
// row): `!errors.Is(err, sql.ErrNoRows)` is its only proof that no successor
// exists.
func TestRetiredOpPoolActionAdmissionOverARetiredSuccessor_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: admission stores a second hand-over row for a successor name whose spawn ran and was retired")
	f := handoverFixture(t, "pw-6284-pa", 1, time.Hour)
	serveRetiredNode(t, f)
	prior, err := fleet.ValidateSubmission(f.holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	record, exists, err := app.ExistingJob(t.Context(), f.job)
	if err != nil || !exists {
		t.Fatal(exists, err)
	}
	// The hand-over was prepared while the name was free; the successor of that
	// name then ran and was retired before the row is admitted.
	next := f.job + "-q1"
	exhaustion := window5h()
	handover, refusal, _ := f.hub.prepareHandover(t.Context(), exhaustion, f.holder, prior, record, next, "42")
	if refusal != "" {
		t.Fatalf("prepare refused: %s", refusal)
	}
	retiredNameHolder(t, f.hub.inbox.store, f.jobs, next)

	if err := f.hub.createPoolAction(t.Context(), exhaustion, f.holder, app, handover); err == nil {
		t.Fatalf("admission accepted a hand-over over the retired successor: %s", poolActionSummary(t, f.hub, f.job))
	}
}

// Reader hub_pool_handover.go:442 on the ordinary path: the predecessor was
// handed over, and its successor ran, ended and was retired. A later exhaustion
// sweep must not hand the predecessor over again.
// Mutant: sweepPoolJob drops its `state != "claimed" && state != "spawned"` test.
func TestRetiredOpPoolSweepLeavesAHandedOverJobAlone_t6284(t *testing.T) {
	f := handoverFixture(t, "pw-6284-done", 1, time.Hour)
	node := serveRetiredNode(t, f)
	store := f.hub.inbox.store
	next := f.job + "-q1"
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "implementer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := store.claimHandoverSuccessor(t.Context(), request, next, "parent", f.holder.Request.RequestID, f.job, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-b", Pane: "w9:p1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	setJobRecord(f.jobs, f.job, "lost", "quota_stop pool=claude handed_over_to="+next)
	setJobRecord(f.jobs, next, "completed", "")
	retireEndedJob(t, store, next)
	reservations, operations := reservationRows(t, store), operationCount(t, store)

	result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder)
	if result.action == poolJobQueued || result.why != "job_record" {
		t.Fatalf("sweep=%+v", result)
	}
	if got := poolActionSummary(t, f.hub, f.job); got != "" || len(node.sent()) != 0 {
		t.Fatalf("pool actions %q, node commands %v", got, node.sent())
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != operations {
		t.Fatalf("the store changed: %v -> %v", reservations, got)
	}
}

// Reader hub_quota_replace.go:341 (the holder of a quota-stopped job). A late or
// resent stop of a retired job is filed once as a plain loss, and nothing is
// placed.
// Mutant: the sql.ErrNoRows branch returns retryLater("holder_unavailable")
// instead of filing the loss, so the stop is resent for ever.
func TestRetiredOpQuotaStopOfARetiredJobIsFiledAsALoss_t6284(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-6284-p1")
	node := serveRetiredNode(t, f)
	store := f.hub.inbox.store
	setJobRecord(f.jobs, f.job, "completed", "")
	retireEndedJob(t, store, f.job)
	reservations := reservationRows(t, store)

	filed, acked := 0, 0
	job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked++ }, &quotaAnnouncement{relay: func(context.Context) { filed++ }})
	if ok || job != "" || filed != 1 || acked != 0 {
		t.Fatalf("job=%q ok=%v filed=%d acked=%d; want one filed loss whose relay owns the receipt", job, ok, filed, acked)
	}
	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job, "claude")); ok || job != "" {
		t.Fatalf("replaced a retired job: %q", job)
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != 0 || len(node.sent()) != 0 {
		t.Fatalf("reservations %v, operations %d, node commands %v", got, operationCount(t, store), node.sent())
	}
}

// Reader hub_quota_replace.go:373 (the successor lookup) on the ordinary path:
// the stop is delivered again after the replacement ran, ended and was retired.
// Mutant: replaceQuotaStoppedAck drops its `state != "claimed" && state !=
// "spawned"` test (skip "job_record").
func TestRetiredOpQuotaStopResentAfterTheReplacementRetired_t6284(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-6284-p2")
	node := serveRetiredNode(t, f)
	store := f.hub.inbox.store
	next := f.job + "-q1"
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	ended := "quota_stop pool=claude replaced_by=" + next
	setJobRecord(f.jobs, f.job, "lost", ended)
	setJobRecord(f.jobs, next, "completed", "")
	retireEndedJob(t, store, next)
	reservations, operations := reservationRows(t, store), operationCount(t, store)

	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job, "claude")); ok || job != "" {
		t.Fatalf("a second replacement: %q", job)
	}
	if _, endReason := jobRecordOf(f.jobs, f.job); endReason != ended || len(node.sent()) != 0 {
		t.Fatalf("end_reason=%q node commands %v", endReason, node.sent())
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != operations {
		t.Fatalf("the store changed: %v -> %v", reservations, got)
	}
}

// Reader hub_quota_replace.go:373 when the predecessor is still live and the
// successor's name belongs to a retired job. With the operation present the stop
// is refused as successor_name_collision and the live job is left alone.
func TestRetiredOpQuotaStopOverARetiredSuccessorName_t6284(t *testing.T) {
	for _, role := range []string{"reviewer", "implementer"} {
		t.Run(role, func(t *testing.T) {
			t.Skip("decision 494 adverse finding: a quota stop over a retired successor name closes the live pane and ends its record; the claim is then refused")
			f := newQuotaReplaceFixture(t, role, "pw-6284-q-"+role)
			node := serveRetiredNode(t, f)
			next := f.job + "-q1"
			retiredNameHolder(t, f.hub.inbox.store, f.jobs, next)

			f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"))
			state, endReason := jobRecordOf(f.jobs, f.job)
			if acted := node.acted(); len(acted) != 0 || state != "spawned" {
				t.Fatalf("node actions %v; the live job's record is %s (%q); successor operations %d; owner notices %d", acted, state, endReason, len(jobOperationIDs(t, f.hub.inbox.store, next)), len(spawnFailedInjections(f.destination)))
			}
		})
	}
}

// Reader hub_quota_replace.go:988 (finishQuotaReplacement, through the periodic
// reconcile): the successor is still listed and the predecessor was retired.
// Mutant: the holder read treats sql.ErrNoRows as already finished (return true).
func TestRetiredOpQuotaReconcileWithARetiredPredecessor_t6284(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-6284-p3")
	node := serveRetiredNode(t, f)
	store := f.hub.inbox.store
	next := f.job + "-q1"
	request, err := quotaReplacementRequest(f.holder.Request, f.job, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	setJobRecord(f.jobs, f.job, "lost", "quota_stop pool=claude")
	retireEndedJob(t, store, f.job)
	reservations := reservationRows(t, store)

	for range 2 {
		if wrote := f.hub.reconcileQuotaReplacements(t.Context()); wrote != 0 {
			t.Fatalf("wrote %d pointers for a retired predecessor", wrote)
		}
	}
	if _, endReason := jobRecordOf(f.jobs, f.job); endReason != "quota_stop pool=claude" || len(node.sent()) != 0 {
		t.Fatalf("end_reason=%q node commands %v", endReason, node.sent())
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != 1 {
		t.Fatalf("the store changed: %v -> %v", reservations, got)
	}
}

// postEscalate sends the fixture's escalate-author request to the real route.
func postEscalate(t *testing.T, ctx context.Context, f *escalateFixture) (int, hubEscalateAuthorResult) {
	t.Helper()
	raw, err := json.Marshal(f.request())
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/jobs/escalate-author", strings.NewReader(string(raw)))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	f.hub.Handler().ServeHTTP(w, r)
	var result hubEscalateAuthorResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("escalate-author answered %d %q", w.Code, w.Body)
	}
	return w.Code, result
}

// Reader hub_escalate_author.go:165 (the author of a ladder step). A retired
// author is refused job_unknown, whether it completed or a step already ran, and
// no pane is touched and no successor is claimed.
// Mutant: the read treats sql.ErrNoRows as pending (answer "pending", retry).
func TestRetiredOpEscalateOfARetiredAuthorIsRefused_t6284(t *testing.T) {
	refused := func(t *testing.T, f *escalateFixture) {
		t.Helper()
		store := f.hub.inbox.store
		reservations, operations := reservationRows(t, store), operationCount(t, store)
		f.take()
		for range 2 {
			code, result := postEscalate(t, t.Context(), f)
			if code != http.StatusOK || result.Code != "job_unknown" || result.AuthorEnded || result.Job != "" {
				t.Fatalf("status=%d result=%+v", code, result)
			}
		}
		if ops := commandOps(f.take()); len(ops) != 0 {
			t.Fatalf("node commands %v", ops)
		}
		if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != operations {
			t.Fatalf("the store changed: %v -> %v", reservations, got)
		}
	}
	t.Run("completed author", func(t *testing.T) {
		// The fixture node still lists the author's pane: a completed record is
		// not a closed pane (hk 5521).
		f := newEscalateFixture(t, "pw-6284-esc-completed")
		setJobRecord(f.jobs, f.job, "completed", "")
		retireEndedJob(t, f.hub.inbox.store, f.job)
		refused(t, f)
	})
	t.Run("replay of a step that ran, both jobs retired", func(t *testing.T) {
		f := newEscalateFixture(t, "pw-6284-esc-replay")
		placed := placedAnywhere
		if result, _ := f.escalate(t, &placed); result.Job != f.job+"-l1" {
			t.Fatalf("setup: %+v", result)
		}
		setJobRecord(f.jobs, f.job+"-l1", "completed", "")
		f.endHolder(fleet.LadderStepEndReason(f.request().Round, f.job+"-l1"))
		retireEndedJob(t, f.hub.inbox.store, f.job+"-l1")
		retireEndedJob(t, f.hub.inbox.store, f.job)
		refused(t, f)
	})
}

// Reader hub_escalate_author.go:210 (does the successor already exist?). A
// retired successor reads as absent; the kept reservation row is what refuses a
// second spawn of its job id.
// Mutant: the read treats sql.ErrNoRows as a found, staged successor
// (`if errors.Is(priorErr, sql.ErrNoRows) { priorErr = nil }`).
func TestRetiredOpEscalateWithARetiredSuccessorStartsNoSecondOne_t6284(t *testing.T) {
	t.Run("completed author, pane present", func(t *testing.T) {
		f := newEscalateFixture(t, "pw-6284-esc-name")
		store := f.hub.inbox.store
		next := f.job + "-l1"
		setJobRecord(f.jobs, f.job, "completed", "")
		retiredNameHolder(t, store, f.jobs, next)
		f.take()
		code, result := postEscalate(t, t.Context(), f)
		if code != http.StatusOK || result.Code != "claim_refused" || result.AuthorEnded {
			t.Fatalf("status=%d result=%+v", code, result)
		}
		ops := commandOps(f.take())
		if slices.Contains(ops, "pane.close") || slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, "job.mark_lost") {
			t.Fatalf("the refused step acted on the author: %v", ops)
		}
		if got := jobOperationIDs(t, store, next); len(got) != 0 {
			t.Fatalf("a second %s was claimed: %v", next, got)
		}
	})
	t.Run("author ended by this step, successor retired", func(t *testing.T) {
		// The answer is a refusal, but its reason is no longer true: the
		// successor ran. The report lists it under RISKS.
		f := newEscalateFixture(t, "pw-6284-esc-ended")
		store := f.hub.inbox.store
		next := f.job + "-l1"
		placed := placedAnywhere
		if result, _ := f.escalate(t, &placed); result.Job != next {
			t.Fatalf("setup: %+v", result)
		}
		f.endHolder(fleet.LadderStepEndReason(f.request().Round, next))
		setJobRecord(f.jobs, next, "completed", "")
		retireEndedJob(t, store, next)
		reservations, operations := reservationRows(t, store), operationCount(t, store)
		f.take()
		code, result := postEscalate(t, t.Context(), f)
		if code != http.StatusOK || result.Code != "job_not_marked" || !result.AuthorEnded || result.Job != "" {
			t.Fatalf("status=%d result=%+v", code, result)
		}
		if ops := commandOps(f.take()); len(ops) != 0 {
			t.Fatalf("node commands %v", ops)
		}
		if got := reservationRows(t, store); !slices.Equal(got, reservations) || operationCount(t, store) != operations {
			t.Fatalf("the store changed: %v -> %v", reservations, got)
		}
	})
}

// retiredAuthorStage is a ladder step stopped half way: the successor is staged
// and holds the task role, the author's record has ended with the step's end
// reason, and the author's operation, whose role moved to the stage, is retired.
func retiredAuthorStage(t *testing.T, job string) (f *ladderCloseFixture, stageID string) {
	t.Helper()
	f = newLadderCloseFixture(t, job, true)
	stageID = f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	f.endHolder(fleet.LadderStepEndReason(f.request().Round, job+"-l1"))
	retireSpawnOps(t, f.hub.inbox.store, jobOperationIDs(t, f.hub.inbox.store, job)...)
	return f, stageID
}

func stageState(t *testing.T, h *HubServer, stageID string) string {
	t.Helper()
	task, err := h.inbox.store.spawnTask(t.Context(), stageID)
	if err != nil {
		rows, reservations := stagedRowCount(t, h, stageID)
		if rows == 0 && reservations == 0 {
			return "released"
		}
		t.Fatal(err)
	}
	return task.Status
}

// Reader hub_escalate_author.go:715 (expireStagedLadderSteps, the loop that
// finishes or releases an expired stage).
func TestRetiredOpLadderExpiryWithARetiredAuthor_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: an expired ladder stage whose author was retired is skipped on every pass and keeps the task role for ever")
	f, stageID := retiredAuthorStage(t, "pw-6284-expiry")
	for range 3 {
		f.hub.expireStagedLadderSteps(t.Context())
	}
	if state := stageState(t, f.hub, stageID); state == "staged" {
		t.Fatalf("after three passes the stage is %s and holds role %+v; node commands %v; owner notices %q", state, reservationTaskRole(t, f.hub, stageID), commandOps(f.take()), f.infoTexts())
	}
}

// Reader hub_ladder_release.go:124 (the operator's release of an unresolved
// stage, POST /v1/ladder/stages/{request_id}/release).
func TestRetiredOpLadderReleaseWithARetiredAuthor_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: the operator's release of a stage whose author was retired answers 503 store_unavailable for ever")
	f, stageID := retiredAuthorStage(t, "pw-6284-release")
	recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "author pane verified gone")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s; the stage is %s and holds role %+v", recorder.Code, strings.TrimSpace(recorder.Body.String()), stageState(t, f.hub, stageID), reservationTaskRole(t, f.hub, stageID))
	}
}

// Reader hub_escalate_author.go:165 on the replay that finishes a step stopped
// between the author's lost record and the successor's activation.
func TestRetiredOpEscalateReplayOfAStoppedStepWithARetiredAuthor_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: the replay that should activate a stopped ladder step answers job_unknown and leaves the successor staged")
	f, stageID := retiredAuthorStage(t, "pw-6284-resume")
	// The bound ends the wait for a placement that nothing here performs.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	result, err := f.hub.escalateAuthor(ctx, f.request())
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if state := stageState(t, f.hub, stageID); result.Code == "job_unknown" || state == "staged" {
		t.Fatalf("replay answered %+v; the stage is %s", result, state)
	}
}
