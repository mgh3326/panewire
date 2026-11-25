package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6284: the readers that reach a spawn operation by its request id, taken
// from a reservation, a notification, a placement snapshot, a supersede
// landing, a consult, a pool action or a caller. See
// hub_spawn_retired_ops_t6284_test.go for the helpers.

// deleteOperationOnly removes one operation and keeps every row that names it.
// Change 3 as planned leaves no such child row; a test uses it to show which
// row the retire query must therefore wait for.
func deleteOperationOnly(t *testing.T, store *laneInboxStore, requestID string) {
	t.Helper()
	result, err := store.db.ExecContext(t.Context(), store.q(`DELETE FROM spawn_operations WHERE request_id=?`), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		t.Fatalf("delete %s removed %d rows", requestID, n)
	}
}

func namedSpawnRequest(job string, id int) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(id)
	for i, arg := range request.Args {
		if arg == "test-job" {
			request.Args[i] = job
		}
	}
	return request
}

// Reader hub_spawn_task_store.go:480 behind hub_spawn_async.go:178 (a replay of
// a retired request id). The kept reservation row refuses it: no operation is
// written again.
// Mutant: claimSpawnReplacing answers a fresh claim when that read finds no row.
func TestRetiredOpReplayOfARetiredRequestSpawnsNothing_t6284(t *testing.T) {
	for name, state := range map[string]string{"hk record completed": "completed", "no hk record": ""} {
		t.Run(name, func(t *testing.T) {
			h, _, request := retiredSubmitHub(t, state)
			reservations := reservationRows(t, h.inbox.store)
			for range 2 {
				w := postSpawn(t, h, "/v1/spawns", request)
				if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "job_already_claimed") {
					t.Fatalf("replay = %d %s", w.Code, w.Body)
				}
			}
			if got := reservationRows(t, h.inbox.store); !slices.Equal(got, reservations) || operationCount(t, h.inbox.store) != 0 {
				t.Fatalf("a second spawn of the job: operations %d reservations %v", operationCount(t, h.inbox.store), got)
			}
		})
	}
}

// Reader hub_spawn_role_release.go:453 reached from the same replay. With the
// operation present a replay answers the stored operation and reads no role.
// Without it the replay is a colliding new claim, and reclaim releases the role
// of the job that holds the task now when its hk record is terminal. A completed
// record does not close the pane (hk 5521).
func TestRetiredOpReplayLeavesTheCurrentRoleHolder_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: a replay of a retired request releases the task role of the other job that holds it now")
	h, jobs, request := retiredSubmitHub(t, "")
	holder := namedSpawnRequest("pw-6284-holder", 628410)
	if w := postSpawn(t, h, "/v1/spawns", holder); w.Code != http.StatusAccepted {
		t.Fatalf("holder submit = %d %s", w.Code, w.Body)
	}
	endSpawn(t, h.inbox.store, holder.RequestID, landedOn("machine-a", "w1:p7"))
	setJobRecord(jobs, "pw-6284-holder", "completed", "")
	reservations := reservationRows(t, h.inbox.store)

	w := postSpawn(t, h, "/v1/spawns", request)
	if got := reservationRows(t, h.inbox.store); !slices.Equal(got, reservations) {
		t.Fatalf("the replay (%d %s) changed the reservations: %v -> %v", w.Code, strings.TrimSpace(w.Body.String()), reservations, got)
	}
}

// Reader hub_spawn_task_store.go:474 (the role claim of a new submission): the
// role of a retired job is free, and a new job for the task is accepted.
// Mutant: claimSpawnReplacing refuses while any reservation has no operation.
func TestRetiredOpRoleOfARetiredJobIsFreeForANewJob_t6284(t *testing.T) {
	h, _, _ := retiredSubmitHub(t, "completed")
	next := namedSpawnRequest("pw-6284-next", 628411)
	w := postSpawn(t, h, "/v1/spawns", next)
	if w.Code != http.StatusAccepted {
		t.Fatalf("submit = %d %s", w.Code, w.Body)
	}
	if role := reservationTaskRole(t, h, next.RequestID); role.String != "42:implementer" {
		t.Fatalf("role %+v", role)
	}
}

// Reader hub_spawn_task_store.go:1441 (spawnRoleOwner, the join of a
// reservation to its operation), on a state change 3 must never leave: a held
// role whose operation is gone. The task is refused and nothing releases the
// hold, so the retire query keeps task_role IS NULL.
// Mutant: reclaimSpawnRole clears the role when spawnRoleOwner finds no row.
func TestRetiredOpRoleLeftOnARetiredOperationStaysHeld_t6284(t *testing.T) {
	h := asyncSpawnHub(t)
	jobs := &quotaJobs{records: map[string]map[string]any{}}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	held := durableSpawnRequest()
	if w := postSpawn(t, h, "/v1/spawns", held); w.Code != http.StatusAccepted {
		t.Fatalf("setup submit = %d %s", w.Code, w.Body)
	}
	endSpawn(t, h.inbox.store, held.RequestID, landedOn("machine-a", "w1:p4"))
	setJobRecord(jobs, "test-job", "completed", "")
	deleteOperationOnly(t, h.inbox.store, held.RequestID)

	for range 2 {
		w := postSpawn(t, h, "/v1/spawns", namedSpawnRequest("pw-6284-blocked", 628412))
		if w.Code != http.StatusConflict {
			t.Fatalf("submit = %d %s", w.Code, w.Body)
		}
	}
	if role := reservationTaskRole(t, h, held.RequestID); role.String != "42:implementer" {
		t.Fatalf("role %+v", role)
	}
}

// Reader hub_spawn_task_store.go:1396 (resumeCleanedSpawn) with a partial
// retirement: an older retryable operation of the job survives the newer one, and
// latestSpawnForJob then answers the older one. The submit route lets both
// requests through to the claim, where the kept reservation refuses them.
// Mutant: resumeCleanedSpawn ignores a refused reservation insert.
func TestRetiredOpOlderRetryableOperationCannotRespawnTheJob_t6284(t *testing.T) {
	h := asyncSpawnHub(t)
	jobs := &quotaJobs{records: map[string]map[string]any{}}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	store := h.inbox.store
	older := durableSpawnRequest()
	if w := postSpawn(t, h, "/v1/spawns", older); w.Code != http.StatusAccepted {
		t.Fatalf("setup submit = %d %s", w.Code, w.Body)
	}
	// What a cleaned-up attempt leaves (completeSpawn): retryable, unqueued and
	// without a reservation.
	for _, query := range []string{`UPDATE spawn_operations SET status='retryable',created_ms=created_ms-1000 WHERE request_id=?`, `DELETE FROM spawn_reservations WHERE request_id=?`, `DELETE FROM spawn_queue WHERE request_id=?`} {
		if _, err := store.db.ExecContext(t.Context(), store.q(query), older.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	newer := durableSpawnRequest()
	newer.RequestID = t14RequestID(628413)
	task, _, err := store.claimSpawn(t.Context(), newer, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endSpawn(t, store, task.Request.RequestID, landedOn("machine-a", "w1:p4"))
	if err := store.releaseCompletedSpawnRoleOf(t.Context(), newer.RequestID); err != nil {
		t.Fatal(err)
	}
	retireSpawnOps(t, store, newer.RequestID)
	setJobRecord(jobs, "test-job", "claimed", "")
	reservations := reservationRows(t, store)

	third := durableSpawnRequest()
	third.RequestID = t14RequestID(628414)
	for name, request := range map[string]fleet.Submission{"replay of the older request": older, "new request": third} {
		w := postSpawn(t, h, "/v1/spawns", request)
		if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "job_already_claimed") {
			t.Fatalf("%s = %d %s", name, w.Code, w.Body)
		}
	}
	left, err := store.spawnTask(t.Context(), older.RequestID)
	if err != nil || left.Status != "retryable" || operationCount(t, store) != 1 {
		t.Fatalf("older=%+v err=%v operations=%d", left.Status, err, operationCount(t, store))
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) {
		t.Fatalf("reservations changed: %v -> %v", reservations, got)
	}
}

// Readers hub_spawn_role_eager.go:60 (hk 6282's eager release lists held roles
// by joining each reservation to its operation) and :120 (it then reads that
// operation by the reservation's request id). A retired job holds no role and is
// not looked at. A role left on a retired operation, a state change 3 must never
// make, is not looked at either and stays held. With the operation present the
// same record and pane release the role.
// Mutant: the list LEFT JOINs the operation and the release clears the role when
// the read finds no row.
func TestRetiredOpEagerRoleReleaseLeavesARetiredJobAlone_t6284(t *testing.T) {
	for _, tc := range []struct {
		name     string
		retire   func(t *testing.T, store *laneInboxStore, holder hubSpawnTask)
		released bool
	}{
		{"operation present", func(*testing.T, *laneInboxStore, hubSpawnTask) {}, true},
		{"retired job", func(t *testing.T, store *laneInboxStore, holder hubSpawnTask) {
			retireEndedJob(t, store, holder.Job)
		}, false},
		{"role left on a retired operation", func(t *testing.T, store *laneInboxStore, holder hubSpawnTask) {
			deleteOperationOnly(t, store, holder.Request.RequestID)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQuotaReplaceFixture(t, "implementer", "pw-6284-eager")
			node := serveRetiredNode(t, f)
			// The pane is gone and the record ended: the job is proven over.
			node.mu.Lock()
			node.closed = true
			node.mu.Unlock()
			store := f.hub.inbox.store
			setJobRecord(f.jobs, f.job, "completed", "")
			tc.retire(t, store, f.holder)
			before := reservationRows(t, store)

			if next := f.hub.releaseProvenOverRoles(t.Context(), ""); next != "" {
				t.Fatalf("cursor %q, want the list exhausted", next)
			}
			if tc.released {
				if role := reservationTaskRole(t, f.hub, f.holder.Request.RequestID); role.Valid {
					t.Fatalf("setup: the pass did not release %+v", role)
				}
				return
			}
			if got := reservationRows(t, store); !slices.Equal(got, before) || len(node.sent()) != 0 {
				t.Fatalf("reservations %v -> %v; node commands %v", before, got, node.sent())
			}
		})
	}
}

// settledHandover drives an implementer hand-over to the point where the old
// pane is closed and the stored successor is next to be claimed.
func settledHandover(t *testing.T, job string) (*quotaReplaceFixture, *retiredNode, poolAction) {
	t.Helper()
	f := handoverFixture(t, job, 1, time.Hour)
	node := serveRetiredNode(t, f)
	if result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder); result.action != poolJobQueued {
		t.Fatalf("setup sweep=%+v", result)
	}
	for range 3 {
		action := readOnlyPoolAction(t, f.hub)
		if action.Phase == poolActionSettled {
			return f, node, action
		}
		if err := f.hub.reconcilePoolAction(t.Context(), action); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("setup: %s", poolActionSummary(t, f.hub, job))
	return nil, nil, poolAction{}
}

// claimedSuccessorRetired claims a settled hand-over's successor as a hub that
// stopped before it saved the row would, then lets it run, end and be retired.
func claimedSuccessorRetired(t *testing.T, f *quotaReplaceFixture, action poolAction) string {
	t.Helper()
	store := f.hub.inbox.store
	next := action.Data.Handover.Job
	if _, _, err := store.claimHandoverSuccessor(t.Context(), action.Data.Handover.Successor, next, action.Data.Owner, action.Data.SpawnRequest, action.Job, time.Now()); err != nil {
		t.Fatal(err)
	}
	placeSuccessor(t, f, next)
	setJobRecord(f.jobs, next, "completed", "")
	retireEndedJob(t, store, next)
	return next
}

// Reader hub_pool_handover.go:736 (finishHandover, the successor's outcome): the
// row saved its submission, and the successor was retired before a pass saw it
// end.
func TestRetiredOpHandoverRowWithARetiredSuccessorEnds_t6284(t *testing.T) {
	t.Skip("decision 494 adverse finding: a submitted hand-over row whose successor was retired errors on every pass and never ends")
	f, _, _ := settledHandover(t, "pw-6284-row")
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
	action := readOnlyPoolAction(t, f.hub)
	if !action.Data.Handover.Submitted {
		t.Fatalf("setup: %s", poolActionSummary(t, f.hub, f.job))
	}
	next := action.Data.Handover.Job
	placeSuccessor(t, f, next)
	setJobRecord(f.jobs, next, "completed", "")
	retireEndedJob(t, f.hub.inbox.store, next)

	if err := drivePoolActions(t, f.hub, 4); err != nil {
		t.Fatalf("four passes, last error %v; row %s", err, poolActionSummary(t, f.hub, f.job))
	}
}

// Readers hub_pool_action_store.go:392 and hub_pool_handover.go:720 (the replay
// of a successor claim after a stop between the claim and the saved flag): the
// retired successor is not claimed again, and the row ends claim_refused.
// Mutant: finishHandover takes every refused claim for a sibling row's claim
// (drops the `known != nil` branch), so the row never ends.
func TestRetiredOpHandoverClaimReplayAfterTheSuccessorRetired_t6284(t *testing.T) {
	f, _, action := settledHandover(t, "pw-6284-replay")
	next := claimedSuccessorRetired(t, f, action)
	reservations, operations := reservationRows(t, f.hub.inbox.store), operationCount(t, f.hub.inbox.store)

	if err := drivePoolActions(t, f.hub, 3); err != nil {
		t.Fatal(err)
	}
	want := "outcome=" + handoverFailed + " reason=claim_refused"
	if got := poolActionSummary(t, f.hub, f.job); !strings.Contains(got, "phase="+string(poolActionDone)) || !strings.Contains(got, want) {
		t.Fatalf("row %s", got)
	}
	if got := jobOperationIDs(t, f.hub.inbox.store, next); len(got) != 0 || operationCount(t, f.hub.inbox.store) != operations {
		t.Fatalf("a second successor: %v", got)
	}
	if got := reservationRows(t, f.hub.inbox.store); !slices.Equal(got, reservations) {
		t.Fatalf("reservations changed: %v -> %v", reservations, got)
	}
	if lines := ownerLines(t, f.hub, "pool-handover"); len(lines) > 1 {
		t.Fatalf("owner notices %q", lines)
	}
}

// Reader hub_pool_action.go:782 (finishPoolAction on a moved job): the old job
// moved to another node while the unsaved row's successor ran and was retired.
// Mutant: finishPoolAction returns the read error for sql.ErrNoRows too.
func TestRetiredOpMovedHandoverRowWithARetiredSuccessorEnds_t6284(t *testing.T) {
	f, _, action := settledHandover(t, "pw-6284-moved")
	next := claimedSuccessorRetired(t, f, action)
	f.hub.mu.Lock()
	f.hub.jobs[f.job].Node = "machine-b"
	f.hub.mu.Unlock()

	if err := drivePoolActions(t, f.hub, 3); err != nil {
		t.Fatal(err)
	}
	if got := poolActionSummary(t, f.hub, f.job); !strings.Contains(got, "phase="+string(poolActionDone)) || !strings.Contains(got, "reason=old job moved to another owner") {
		t.Fatalf("row %s", got)
	}
	if got := jobOperationIDs(t, f.hub.inbox.store, next); len(got) != 0 {
		t.Fatalf("a second successor: %v", got)
	}
}

// Reader hub_pool_action.go:839 (releaseCompletedSpawnRoleOf of the closed
// spawn): a close action whose target operation was retired still ends and tells
// its owner once.
// Mutant: releaseCompletedSpawnRoleOf errors when the operation is gone.
func TestRetiredOpCloseActionOfARetiredSpawnFinishes_t6284(t *testing.T) {
	f, action := poolActionFixture(t)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	setJobRecord(f.jobs, f.job, "lost", action.Data.EndReason)
	retireEndedJob(t, f.hub.inbox.store, f.job)
	reservations := reservationRows(t, f.hub.inbox.store)

	if err := drivePoolActions(t, f.hub, 2); err != nil {
		t.Fatal(err)
	}
	if got := poolActionSummary(t, f.hub, f.job); !strings.Contains(got, "phase="+string(poolActionDone)) {
		t.Fatalf("row %s", got)
	}
	if lines := ownerLines(t, f.hub, "pool-closed:"); len(lines) != 1 {
		t.Fatalf("owner notices %q", lines)
	}
	if got := reservationRows(t, f.hub.inbox.store); !slices.Equal(got, reservations) {
		t.Fatalf("reservations changed: %v -> %v", reservations, got)
	}
}

// retiredConsult posts a consult, lets its spawn land and retires it. The row it
// returns still reads as the caller left it.
func retiredConsult(t *testing.T) (*consultFixture, hubConsult) {
	t.Helper()
	f := newConsultFixture(t)
	code, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	id, _ := out["id"].(string)
	if code != http.StatusCreated || id == "" {
		t.Fatalf("consult = %d %v", code, out)
	}
	c, err := f.hub.inbox.store.consultByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	endSpawn(t, f.hub.inbox.store, c.RequestID, landedOn("machine-a", "w1:p5"))
	setJobRecord(f.jobs, c.Job, "completed", "")
	retireEndedJob(t, f.hub.inbox.store, c.Job)
	return f, c
}

// Reader hub_consults.go:563 is not reached for an answered consult: its owed
// event is sent whether or not the operation exists.
// Mutant: reconcileConsults reads the operation first and skips a row without one.
func TestRetiredOpAnsweredConsultIsStillNotified_t6284(t *testing.T) {
	f, c := retiredConsult(t)
	c.State, c.AnswerDoc, c.Notified = consultAnswered, "hk:doc/answer", false
	if err := f.hub.saveConsult(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcileConsults(t.Context())
	got, err := f.hub.inbox.store.consultByID(t.Context(), c.ID)
	if err != nil || got.State != consultAnswered || !got.Notified {
		t.Fatalf("consult %+v err=%v", got, err)
	}
	if view := f.get(t, c.ID); view["state"] != consultAnswered {
		t.Fatalf("view %v", view)
	}
}

// Reader hub_consults.go:790 (cancelConsultJob): a consult past its time limit
// fails once; its retired spawn is neither closed nor replaced.
// Mutant: cancelConsultJob respawns the consult on sql.ErrNoRows.
func TestRetiredOpExpiredConsultFailsWithoutARespawn_t6284(t *testing.T) {
	f, c := retiredConsult(t)
	c.State, c.Deadline = consultRunning, time.Now().Add(-time.Minute)
	if err := f.hub.saveConsult(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	operations := operationCount(t, f.hub.inbox.store)
	for range 2 {
		f.hub.reconcileConsults(t.Context())
	}
	got, err := f.hub.inbox.store.consultByID(t.Context(), c.ID)
	if err != nil || got.State != consultFailed || !strings.HasPrefix(got.Reason, "timeout") {
		t.Fatalf("consult %+v err=%v", got, err)
	}
	if f.calls != 1 || operationCount(t, f.hub.inbox.store) != operations {
		t.Fatalf("plans=%d operations %d -> %d", f.calls, operations, operationCount(t, f.hub.inbox.store))
	}
}

// Reader hub_consults.go:563 for a consult still inside its time limit.
// A queued row goes to recoverConsult and a running one to respawnConsult.
func TestRetiredOpLiveConsultIsNotSpawnedAgain_t6284(t *testing.T) {
	for _, state := range []string{consultQueued, consultRunning} {
		t.Run(state, func(t *testing.T) {
			t.Skip("decision 494 adverse finding: a consult inside its time limit whose operation was retired is spawned a second time")
			f, c := retiredConsult(t)
			c.State, c.Deadline, c.ClaimUntil = state, time.Now().Add(time.Hour), time.Now().Add(-time.Minute)
			if err := f.hub.saveConsult(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			operations := operationCount(t, f.hub.inbox.store)
			f.hub.reconcileConsults(t.Context())
			if got := operationCount(t, f.hub.inbox.store); f.calls != 1 || got != operations {
				after, _ := f.hub.inbox.store.consultByID(t.Context(), c.ID)
				t.Fatalf("plans=%d operations %d -> %d; consult state=%s respawns=%d job %s -> %s", f.calls, operations, got, after.State, after.Respawns, c.Job, after.Job)
			}
		})
	}
}

// Readers hub_spawn_notice_silence.go:166 (a pending notice's first routing) and
// hub_spawn_notice_route.go:266 and :304 (a held one), for a notice that is
// still owed when its operation goes.
func TestRetiredOpOwedSpawnNoticeIsSettled_t6284(t *testing.T) {
	for name, state := range map[string]int{"pending": spawnNoticePending, "held": spawnNoticeHeld} {
		t.Run(name, func(t *testing.T) {
			t.Skip("decision 494 adverse finding: an owed spawn notice whose operation is gone is re-read on every flush and never sent")
			h, _, destination := spawnNotifyHub(t, "")
			request := durableSpawnRequest()
			if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now()); err != nil {
				t.Fatal(err)
			}
			endSpawn(t, h.inbox.store, request.RequestID, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed"})
			if _, err := h.inbox.store.db.Exec(`UPDATE spawn_notifications SET delivered=? WHERE request_id=?`, state, request.RequestID); err != nil {
				t.Fatal(err)
			}
			deleteOperationOnly(t, h.inbox.store, request.RequestID)

			for range 3 {
				h.flushSpawnNotifications(t.Context())
			}
			var delivered int
			if err := h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications WHERE request_id=?`, request.RequestID).Scan(&delivered); err != nil {
				t.Fatal(err)
			}
			if delivered != spawnNoticeDone {
				t.Fatalf("after three flushes delivered=%d, owner injections=%d", delivered, len(spawnFailedInjections(destination)))
			}
		})
	}
}

// Reader hub_spawn_task_fail.go:343 (the hk task-failure comment): an outbox row
// whose operation is gone is dropped. Its comment is never posted, so the retire
// query waits for task_fail_notice_ms.
// Mutant: postSpawnTaskFailNotice returns without dropping the row.
func TestRetiredOpTaskFailRowWithoutAnOperationIsDropped_t6284(t *testing.T) {
	h, _, _ := spawnNotifyHub(t, "")
	request := durableSpawnRequest()
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	endSpawn(t, h.inbox.store, request.RequestID, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed"})
	owed := func() []string {
		ids, err := h.inbox.store.pendingSpawnTaskFailNoticeIDs(t.Context(), time.Now(), 10)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if ids := owed(); len(ids) != 1 {
		t.Fatalf("setup: outbox %v", ids)
	}
	deleteOperationOnly(t, h.inbox.store, request.RequestID)
	h.flushSpawnTaskFailNotices(t.Context())
	if ids := owed(); len(ids) != 0 {
		t.Fatalf("the row is still owed: %v", ids)
	}
}

// Reader hub_placement_record.go:236 (pendingPlacementSnapshots joins the
// operation): an unrecorded snapshot left behind is never delivered and starts
// no retry. The record is lost, so the retire query waits for it.
// Mutant: the join to spawn_operations becomes a LEFT JOIN.
func TestRetiredOpUnrecordedSnapshotWithoutAnOperationIsInert_t6284(t *testing.T) {
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	task := boundSpawnTask(t, h, durableSpawnRequest())
	payload, err := json.Marshal(placementTraceSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.saveSpawnPlacementSnapshot(t.Context(), task.Request.RequestID, task.Attempt, payload, ""); err != nil {
		t.Fatal(err)
	}
	deleteOperationOnly(t, h.inbox.store, task.Request.RequestID)

	for range 2 {
		if result := h.flushPlacementRecords(t.Context()); result.failed || result.retry != 0 {
			t.Fatalf("flush %+v", result)
		}
	}
	if _, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), task.Request.RequestID, task.Attempt); err != nil || recorded {
		t.Fatalf("recorded=%v err=%v", recorded, err)
	}
}

// Reader hub_placement_reconcile.go:25 and saveDeferredPlacementSnapshot: a
// trace save that is still deferred on disk when its operation and snapshot are
// retired. The retire query cannot see that file.
//
// "failed update" drains while the operation exists. "legacy save" (no base) does
// not drain for a recorded snapshot today either.
func TestRetiredOpDeferredPlacementSaveDrains_t6284(t *testing.T) {
	for _, name := range []string{"failed update", "legacy save"} {
		t.Run(name, func(t *testing.T) {
			t.Skip("decision 494 adverse finding: a deferred placement save for a retired operation is retried on every pass and never drains")
			placementOutboxServer(t)
			h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			store := h.inbox.store
			task := boundSpawnTask(t, h, durableSpawnRequest())
			old, err := json.Marshal(placementTraceSnapshot())
			if err != nil {
				t.Fatal(err)
			}
			id := task.Request.RequestID
			if err := store.saveSpawnPlacementSnapshot(t.Context(), id, task.Attempt, old, ""); err != nil {
				t.Fatal(err)
			}
			if err := store.markSpawnPlacementRecorded(t.Context(), id, task.Attempt); err != nil {
				t.Fatal(err)
			}
			endSpawn(t, store, id, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-a", Pane: "w1:p4", NodeResult: true})
			retireEndedJob(t, store, task.Job)
			save := placementDeferredSave{RequestID: id, Attempt: task.Attempt, Payload: []byte(`{"outcome":"landed"}`)}
			if name == "failed update" {
				save.Base = &placementDeferredBase{Exists: true, Payload: old}
			}
			h.placementAppMu.Lock()
			h.placementDeferredSaves, h.placementDeferredLoaded = []placementDeferredSave{save}, true
			err = h.writePlacementDeferredSavesLocked()
			h.placementAppMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}

			var result readResult
			for range 3 {
				result = h.flushPlacementRecords(t.Context())
			}
			if h.hasDeferredTraceSaves() {
				t.Fatalf("after three passes the save is still deferred; next retry in %v", result.retry)
			}
		})
	}
}

// Readers that take the request id from the caller: hub_spawn_async.go:296
// (status), hub_spawn_withdraw.go:122, hub_spawn_orphan.go:258 (abandon) and
// hub_ladder_release.go:87. Each answers 404 not_found.
// Mutant: handleSpawnOperation answers 503 store_unavailable for sql.ErrNoRows.
func TestRetiredOpRequestRoutesAnswerNotFound_t6284(t *testing.T) {
	h, _, request := retiredSubmitHub(t, "completed")
	id := request.RequestID
	for _, route := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/spawns/" + id, ""},
		{http.MethodPost, "/v1/spawns/" + id + "/withdraw", `{"reason":"no longer needed","requester":"parent"}`},
		{http.MethodPost, "/v1/spawns/" + id + "/abandon", `{"reason":"no longer needed"}`},
		{http.MethodPost, "/v1/ladder/stages/" + id + "/release", `{"reason":"no longer needed"}`},
	} {
		r := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), `"not_found"`) {
			t.Fatalf("%s %s = %d %s", route.method, route.path, w.Code, w.Body)
		}
	}
}

// The supersede landing is keyed by the successor's request id and reads no
// operation of it. A landing old enough for its successor to be retired is ended
// by the sweep and closes nothing.
// Mutant: sweepSupersedeLandings no longer ends landings past their window.
func TestRetiredOpStaleSupersedeLandingClosesNothing_t6284(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	landing := supersedeLanding{RequestID: t14RequestID(628415), Parent: f.job, Successor: f.successor, Owner: "parent", Created: time.Now().Add(-supersedeLandingWindow - time.Hour).UnixMilli()}
	if err := f.hub.inbox.store.recordSupersedeLanding(t.Context(), landing); err != nil {
		t.Fatal(err)
	}
	f.hub.sweepSupersedeLandings(t.Context())
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("a close was queued: %+v", rows)
	}
	pending, err := f.hub.inbox.store.pendingSupersedeLandings(t.Context(), supersedeLandingBatch)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending %+v err=%v", pending, err)
	}
	f.noMoreCommands(t)
}
