package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// hk 6284: the readers that look a spawn up by job id, on a retired job. See
// hub_spawn_retired_ops_t6284_test.go for the helpers.

// endSpawn completes the claimed request id with outcome.
func endSpawn(t *testing.T, store *laneInboxStore, requestID string, outcome hubSpawnOutcome) {
	t.Helper()
	task, err := store.spawnTask(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("complete %s: changed=%v err=%v", requestID, changed, err)
	}
}

func landedOn(machine, pane string) hubSpawnOutcome {
	return hubSpawnOutcome{RC: 0, Stage: "done", Host: machine, Pane: pane}
}

// consultAfterRetiredAuthor is the consult fixture with its author job ended and
// retired. When liveJob is set, task 42 is now bound to that live job.
func consultAfterRetiredAuthor(t *testing.T, liveJob string) *consultFixture {
	t.Helper()
	f := newConsultFixture(t)
	store := f.hub.inbox.store
	ids := jobOperationIDs(t, store, "author-job")
	endSpawn(t, store, ids[0], landedOn("machine-a", "w1:p4"))
	setJobRecord(f.jobs, "author-job", "completed", "")
	retireEndedJob(t, store, "author-job")
	if liveJob == "" {
		return f
	}
	request := consultAuthorRequest()
	for i, arg := range request.Args {
		if arg == "author-job" {
			request.Args[i] = liveJob
		}
	}
	request.Repository.Worktree, request.Repository.Checkout.Dir = "live-checkout", "live-checkout"
	if _, _, err := store.claimSpawn(t.Context(), request, liveJob, "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	f.hub.consultTaskBinding = func(context.Context, string) (string, error) { return liveJob, nil }
	return f
}

// Reader hub_consults.go:331 (consultOwnerFor, POST /v1/consults): a retired
// author job names no owner, so the consult belongs to its requester.
// Mutant: consultOwnerFor returns consultToolLane on sql.ErrNoRows.
func TestRetiredOpConsultOwnerFallsBackToTheRequester_t6284(t *testing.T) {
	f := consultAfterRetiredAuthor(t, "live-job")
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	body["requester"] = "panewire-design"
	rowOwner, spawnOwner := consultOwnerOf(t, f, body)
	if rowOwner != "panewire-design" || spawnOwner != "panewire-design" {
		t.Fatalf("row owner %q spawn owner %q, want the requester", rowOwner, spawnOwner)
	}
}

// Reader hub_consults.go:396 (consultRepository, the author job): a retired
// author falls back to the job the task is bound to, and one consult spawns.
// Mutant: the sql.ErrNoRows case returns "author_job_unknown" at once.
func TestRetiredOpConsultTakesTheBoundJobsRepository_t6284(t *testing.T) {
	f := consultAfterRetiredAuthor(t, "live-job")
	operations := operationCount(t, f.hub.inbox.store)
	code, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	if code != http.StatusCreated || out["state"] != consultQueued {
		t.Fatalf("consult = %d %v", code, out)
	}
	if len(f.inputs) != 1 || f.inputs[0].Repository == nil || f.inputs[0].Repository.Worktree != "live-checkout" {
		t.Fatalf("plan inputs %+v", f.inputs)
	}
	if got := operationCount(t, f.hub.inbox.store); got != operations+1 {
		t.Fatalf("operations %d -> %d, want one consult", operations, got)
	}
}

// Reader hub_consults.go:421 (consultRepository, the bound job): a task bound to
// a retired job gives no repository, and the consult is refused with the reason.
// Mutant: the sql.ErrNoRows branch returns an empty repository and no reason.
func TestRetiredOpConsultBoundToARetiredJobIsRefused_t6284(t *testing.T) {
	for name, author := range map[string]string{"author named": "author-job", "author omitted": ""} {
		t.Run(name, func(t *testing.T) {
			f := consultAfterRetiredAuthor(t, "")
			body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
			body["author_job"] = author
			code, out := f.post(t, "", t14OperatorToken, body)
			reason, _ := out["reason"].(string)
			if code != http.StatusCreated || out["state"] != consultRefused || !strings.HasPrefix(reason, "author_job_unknown") {
				t.Fatalf("consult = %d %v", code, out)
			}
			if f.calls != 0 || operationCount(t, f.hub.inbox.store) != 0 {
				t.Fatalf("plans=%d operations=%d, want none", f.calls, operationCount(t, f.hub.inbox.store))
			}
		})
	}
}

// Reader hub_jobs.go:559 (releaseEndedJobRole, a node's final job.lost): a loss
// for a retired job settles the job and releases nothing, although a newer job
// of the same task now holds the role.
// Mutant: on sql.ErrNoRows releaseEndedJobRole clears every held task_role.
func TestRetiredOpFinalLossReleasesNoOtherRole_t6284(t *testing.T) {
	f := newQuotaReplaceFixture(t, "implementer", "pw-6284-lost")
	store := f.hub.inbox.store
	setJobRecord(f.jobs, f.job, "lost", "pane_gone")
	retireEndedJob(t, store, f.job)
	next := quotaHolderRequest("implementer", "pw-6284-lost-next")
	next.RequestID = t14RequestID(628401)
	if _, _, err := store.claimSpawn(t.Context(), next, "pw-6284-lost-next", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	if role := reservationTaskRole(t, f.hub, next.RequestID); role.String != "42:implementer" {
		t.Fatalf("setup: role %+v", role)
	}
	f.hub.mu.Lock()
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	reservations := reservationRows(t, store)

	if !f.hub.observeJobLost("machine-a", hubJobEventPayload{JobID: f.job, Epoch: 1}, time.Now()) {
		t.Fatal("the loss was not accepted")
	}
	if got := reservationRows(t, store); !slices.Equal(got, reservations) {
		t.Fatalf("reservations changed: %v -> %v", reservations, got)
	}
}

// retiredSubmitHub is a submit route over one retired job: durableSpawnRequest
// ran, its record is in state, and its operation is gone.
func retiredSubmitHub(t *testing.T, state string) (*HubServer, *quotaJobs, fleet.Submission) {
	t.Helper()
	h := asyncSpawnHub(t)
	jobs := &quotaJobs{records: map[string]map[string]any{}}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	request := durableSpawnRequest()
	if w := postSpawn(t, h, "/v1/spawns", request); w.Code != http.StatusAccepted {
		t.Fatalf("setup submit = %d %s", w.Code, w.Body)
	}
	outcome := landedOn("machine-a", "w1:p4")
	if state == "" {
		// Refused before any pane: the node never claimed the hk record.
		outcome = hubSpawnOutcome{RC: 70, Stage: "checkout", Reason: "checkout_refused"}
	} else {
		setJobRecord(jobs, "test-job", state, "")
	}
	endSpawn(t, h.inbox.store, request.RequestID, outcome)
	retireEndedJob(t, h.inbox.store, "test-job")
	return h, jobs, request
}

func postSpawn(t *testing.T, h *HubServer, path string, request fleet.Submission) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	return w
}

const jobClaimedFix = "inspect the existing job; use a new job id for independent work"

// Reader hub_spawn_async.go:234 (POST /v1/spawns for a job hk already holds): a
// new request for a retired job id is refused on the record, whatever its state.
// Mutant: a retired latest operation reads as retryable
// (`latestErr == nil && latest.Status != "retryable"`).
func TestRetiredOpSubmitForARetiredJobIDIsRefused_t6284(t *testing.T) {
	for _, state := range []string{"completed", "claimed"} {
		t.Run(state, func(t *testing.T) {
			h, _, request := retiredSubmitHub(t, state)
			reservations := reservationRows(t, h.inbox.store)
			request.RequestID = t14RequestID(628402)
			w := postSpawn(t, h, "/v1/spawns", request)
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusConflict || body["error"] != "job_already_claimed" || body["fix"] != jobClaimedFix {
				t.Fatalf("submit = %d %s", w.Code, w.Body)
			}
			if got := reservationRows(t, h.inbox.store); !slices.Equal(got, reservations) || operationCount(t, h.inbox.store) != 0 {
				t.Fatalf("a second spawn of the job: operations %d reservations %v", operationCount(t, h.inbox.store), got)
			}
		})
	}
}

// Reader hub_spawn_plan.go:85 (retryableClaim, POST /v1/spawns/plan).
// Mutant: retryableClaim also answers true on sql.ErrNoRows.
func TestRetiredOpPlanForARetiredJobIDIsRefused_t6284(t *testing.T) {
	h, _, request := retiredSubmitHub(t, "claimed")
	request.RequestID = t14RequestID(628403)
	w := postSpawn(t, h, "/v1/spawns/plan", request)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "job_already_claimed") {
		t.Fatalf("plan = %d %s", w.Code, w.Body)
	}
}

// Reader hub_spawn_notice_silence.go:266 (ownerNotifyModeFor, a node's
// job.completed): a retired spawn names no --owner-notify mode, so the record is
// delivered. With the operation present, "never" would withhold it.
// Mutant: ownerNotifyModeFor returns fleet.OwnerNotifyNever on sql.ErrNoRows.
func TestRetiredOpOwnerNotifyModeWithholdsNothing_t6284(t *testing.T) {
	h, _, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	job := "pw-6284-notify"
	request := t4270Request(job, t3426Design, "", "implementer")
	request.Args = append(request.Args, fleet.OwnerNotifyArg, fleet.OwnerNotifyNever)
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}
	endSpawn(t, h.inbox.store, request.RequestID, landedOn("host-a", "w1:p4"))
	retireEndedJob(t, h.inbox.store, job)

	h.relayJobCompletionFrom("host-a", t3426Completion(job, t3426Design, ""))
	if items := t4270Items(t, h, t3426Design); len(items) != 1 {
		t.Fatalf("owner items=%d, want the completion delivered once", len(items))
	}
}

// Reader inbox_design_filter.go:76 (daemonStartedJob, a node's job.completed): a
// retired daemon reviewer is no longer known to be one, so the design lane gets
// the record that was filtered while the operation existed.
// Mutant: daemonStartedJob returns true on sql.ErrNoRows.
func TestRetiredOpDesignFilterDeliversARetiredReviewersRecord_t6284(t *testing.T) {
	h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := t4270Request(t4270Reviewer, t3426Design, "", "reviewer")
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}
	endSpawn(t, h.inbox.store, request.RequestID, landedOn("host-a", "w1:p4"))
	retireEndedJob(t, h.inbox.store, t4270Reviewer)

	h.relayJobCompletionFrom("host-a", t3426Completion(t4270Reviewer, t3426Design, ""))
	if items := t4270Items(t, h, t3426Design); len(items) != 1 {
		t.Fatalf("design lane items=%d, want the record delivered", len(items))
	}
	_, deliveredTo := t4270Rows(fake, t4270Reviewer)
	if slices.ContainsFunc(deliveredTo, func(to string) bool { return strings.HasPrefix(to, "hub/filtered:") }) {
		t.Fatalf("the record was filtered: %v", deliveredTo)
	}
}

// Reader hub_spawn_supersede.go:309 (createSupersedeAction, the parent of a
// landed successor, through the landing sweep): a parent that still heartbeats
// but whose operation is retired is refused, and the landing is decided.
// Mutant: the sql.ErrNoRows branch returns retry=true, so the landing stays.
func TestRetiredOpSupersedeOfARetiredParentIsDecidedOnce_t6284(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	setJobRecord(f.jobs, f.job, "completed", "")
	retireEndedJob(t, f.hub.inbox.store, f.job)

	f.land(t, f.successorTask("implementer", "parent", true))
	f.hub.sweepSupersedeLandings(t.Context())
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("a close was queued for a retired parent: %+v", rows)
	}
	pending, err := f.hub.inbox.store.pendingSupersedeLandings(t.Context(), supersedeLandingBatch)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending landings %+v err=%v; the landing would be retried", pending, err)
	}
	f.noMoreCommands(t)
}

// Reader lanes_write.go:394 (acceptedSpawnRecord, PUT /v1/lanes with takeover):
// a retired predecessor, or a retired job in the parent chain, proves nothing,
// and no lane row moves.
// Mutants: acceptedSpawnRecord returns (zero record, true) on sql.ErrNoRows and
// laneTakeoverRow skips its placement check for a record with no host
// (ancestor-a); takeoverChainRecord skips an unreadable hop to the takeover job
// (mid-b).
func TestRetiredOpLaneTakeoverThroughARetiredJobIsRefused_t6284(t *testing.T) {
	for _, retired := range []string{"ancestor-a", "mid-b"} {
		t.Run(retired, func(t *testing.T) {
			hub, path := takeoverHub(t,
				`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"ancestor-a","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
				nil)
			seedAcceptedChainJob(t, hub.inbox.store, "ancestor-a", "", "machine-a", "w1:p1")
			seedAcceptedChainJob(t, hub.inbox.store, "mid-b", "ancestor-a", "machine-a", "w1:p2")
			bindChainSuccessor(t, hub.inbox.store, "successor-s", "mid-b", "machine-b")
			retireEndedJob(t, hub.inbox.store, retired)

			writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
				`{"machine":"machine-b","pane":"w2:p5","job":"successor-s","parent":"mergewake","takeover":"ancestor-a"}`)
			if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
				t.Fatalf("takeover = %d %q", writer.Code, writer.Body.String())
			}
			if route := lanesNodeFile(t, path)["task-lane"]; route.Machine != "machine-a" || route.Job != "ancestor-a" {
				t.Fatalf("the refused takeover moved the row: %+v", route)
			}
		})
	}
}

// Reader lanes_write.go:440 (laneSpawnProof, a node writing its own lane): a
// write that names a retired job is accepted as a plain write, binds no job to a
// new row and keeps the binding of an unchanged one.
// Mutant: laneSpawnProof returns a valid proof on sql.ErrNoRows.
func TestRetiredOpLaneWriteNamingARetiredJobBindsNothing_t6284(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"held-lane":{"machine":"machine-b","pane":"w2:p5","job":"old-binding","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	bindChainSuccessor(t, hub.inbox.store, "retired-w", "mergewake-parent", "machine-b")
	ids := jobOperationIDs(t, hub.inbox.store, "retired-w")
	endSpawn(t, hub.inbox.store, ids[0], landedOn("machine-b", "w2:p6"))
	retireEndedJob(t, hub.inbox.store, "retired-w")

	for lane, want := range map[string]struct {
		code      int
		pane, job string
	}{
		"retired-w": {http.StatusCreated, "w2:p6", ""},
		"held-lane": {http.StatusOK, "w2:p5", "old-binding"},
	} {
		writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, lanesNodeBToken, "machine-b",
			`{"machine":"machine-b","pane":"`+want.pane+`","job":"retired-w"}`)
		if writer.Code != want.code {
			t.Fatalf("%s write = %d %q", lane, writer.Code, writer.Body.String())
		}
		if route := lanesNodeFile(t, path)[lane]; route.Pane != want.pane || route.Job != want.job {
			t.Fatalf("%s row %+v, want pane %s job %q", lane, route, want.pane, want.job)
		}
	}
}

// Reader own_jobs.go:100 (fillOwnJobTasks, GET /v1/jobs/mine): a retired job is
// listed without a task.
// Mutant: fillOwnJobTasks writes task "unknown" on sql.ErrNoRows.
func TestRetiredOpJobsMineListsARetiredJobWithoutATask_t6284(t *testing.T) {
	hub, server := ownJobsHub(t)
	inbox, err := newHubInbox("default", filepath.Join(t.TempDir(), "inbox.sqlite3"), "")
	if err != nil {
		t.Fatal(err)
	}
	hub.inbox = inbox
	t.Cleanup(func() { inbox.store.close() })
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "lane-a"}, Node: "node-a"}
	request := quotaHolderRequest("implementer", "job-a")
	if _, _, err := inbox.store.claimSpawn(t.Context(), request, "job-a", "lane-a", hub.now()); err != nil {
		t.Fatal(err)
	}
	endSpawn(t, inbox.store, request.RequestID, landedOn("node-a", "w1:p1"))
	retireEndedJob(t, inbox.store, "job-a")
	if _, err := inbox.store.latestSpawnForJob(t.Context(), "job-a"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("setup: %v", err)
	}

	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs/mine?lane=lane-a", "")
	var result struct {
		Jobs []hubOwnJob `json:"jobs"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil || len(result.Jobs) != 1 || result.Jobs[0].JobID != "job-a" || result.Jobs[0].Task != "" {
		t.Fatalf("status=%d body=%s", status, body)
	}
}
