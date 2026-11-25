package panewire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 6394: the reviewer-end pass lists the window by request id and reads a
// full row (payload included) only for an operation it has not judged. These
// tests pin that the same jobs are ended, that an older orphan is not displaced
// by newer operations, and that the read is smaller.

// reviewerEndOp adds a succeeded operation n for job under role on host, created
// at created, whose hk record is in state ("" for no record). Each operation
// has its own task, so an implementer holds no role another one wants.
func reviewerEndOp(t *testing.T, hub *HubServer, jobs *roleJobs, role, job, host, state string, n int, created time.Time) {
	t.Helper()
	request := reviewerReleaseRequest(role, false)
	request.RequestID = t14RequestID(63940 + n)
	request.Args = replaceSpawnArg(replaceSpawnArg(replaceSpawnArg(request.Args, "--job", job), "-l", job), "--task", fmt.Sprint(6400+n))
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), request, job, "parent", created)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 0, Host: host, Pane: roleHeldPane}, created); err != nil {
		t.Fatal(err)
	}
	if state != "" {
		jobs.mu.Lock()
		jobs.records[job] = map[string]any{"state": state}
		jobs.mu.Unlock()
	}
}

// reviewerEndEndedJobs is the jobs the pass wrote an end for, from its JSON log.
func reviewerEndEndedJobs(logged string) []string {
	var jobs []string
	for _, line := range strings.Split(logged, "\n") {
		var entry struct{ Msg, Job string }
		if json.Unmarshal([]byte(line), &entry) == nil && entry.Msg == "hub ended a reviewer job with a posted result" {
			jobs = append(jobs, entry.Job)
		}
	}
	slices.Sort(jobs)
	return jobs
}

// reviewerEndReferenceJobs is the jobs the pass is owed an end for by the rule
// the pass followed before it listed ids: read every succeeded operation of the
// window in full, keep the reviewers, drop a job whose hk record is terminal or
// whose node is not connected.
func reviewerEndReferenceJobs(t *testing.T, hub *HubServer, jobs *roleJobs) []string {
	t.Helper()
	rows, err := hub.inbox.store.db.Query(hub.inbox.store.q(`SELECT request_id FROM spawn_operations WHERE status='succeeded' AND created_ms>=?`), hub.now().Add(-reviewerEndWindow).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
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
	rows.Close()
	var want []string
	for _, id := range ids {
		holder, err := hub.inbox.store.spawnTask(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !reviewerEndRoleIsReviewer(holder) || !hub.nodeConnected(spawnHolderHost(holder)) {
			continue
		}
		if state := jobs.state(holder.Job); state == "claimed" || state == "spawned" {
			want = append(want, holder.Job)
		}
	}
	slices.Sort(want)
	return want
}

func reviewerEndRoleIsReviewer(holder hubSpawnTask) bool {
	for i, arg := range holder.Request.Args {
		if arg == "--role=reviewer" || arg == "--role" && i+1 < len(holder.Request.Args) && holder.Request.Args[i+1] == "reviewer" {
			return true
		}
	}
	return false
}

// (a) The same reviewer jobs are ended as before the pass listed ids: a mix of
// reviewers, a non-reviewer, a reviewer already ended and a reviewer on a node
// that is not connected. On the first pass no result is posted, so nothing is
// ended and the two live reviewers on the connected node are asked about; once
// the results are posted the next pass ends exactly those two, and a third pass
// asks about nothing.
func TestReviewerEndEndsTheSameJobsAsReadingEveryRow_t6394(t *testing.T) {
	hub, node, jobs, sink := reviewerEndNode(t, reviewerEndNoResult)
	now := time.Now()
	reviewerEndOp(t, hub, jobs, reviewerRole, "ended-reviewer", "machine-a", "completed", 1, now.Add(time.Second))
	reviewerEndOp(t, hub, jobs, "implementer", "an-implementer", "machine-a", "spawned", 2, now.Add(2*time.Second))
	reviewerEndOp(t, hub, jobs, reviewerRole, "reviewer-off-node", "machine-b", "spawned", 3, now.Add(3*time.Second))
	reviewerEndOp(t, hub, jobs, reviewerRole, "second-reviewer", "machine-a", "claimed", 4, now.Add(4*time.Second))

	want := reviewerEndReferenceJobs(t, hub, jobs)
	if !reflect.DeepEqual(want, []string{"second-reviewer", "test-job"}) {
		t.Fatalf("reference jobs %v: the fixture no longer mixes the cases", want)
	}
	hub.endPostedReviewerJobs(t.Context())
	if got := reviewerEndEndedJobs(sink.String()); len(got) != 0 {
		t.Fatalf("ended %v before any result was posted", got)
	}
	node.reply("job.result", map[string]any{"result": reviewerEndPosted})
	hub.endPostedReviewerJobs(t.Context())
	hub.endPostedReviewerJobs(t.Context())

	if got := reviewerEndEndedJobs(sink.String()); !reflect.DeepEqual(got, want) {
		t.Fatalf("ended jobs %v, want %v", got, want)
	}
	wantCommands := []string{"job.result", "job.result", "job.result", "job.mark_lost", "job.result", "job.mark_lost"}
	if got := node.seen(); !reflect.DeepEqual(got, wantCommands) {
		t.Fatalf("node commands %v, want %v", got, wantCommands)
	}
	if state := jobs.state("reviewer-off-node"); state != "spawned" {
		t.Fatalf("hk record of the job on an unconnected node is %v, want it untouched", state)
	}
}

// A reviewer on a node that is not connected is looked at again on every pass,
// not judged once: it is ended on the pass after the node is back.
func TestReviewerEndAnUnconnectedNodesReviewerIsReadAgainOnceItConnects_t6394(t *testing.T) {
	hub, server, jobs, _ := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	sink := captureHubLog(hub)

	hub.endPostedReviewerJobs(t.Context())
	if got := reviewerEndEndedJobs(sink.String()); len(got) != 0 {
		t.Fatalf("ended %v with no node connected", got)
	}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), reviewerEndPosted))
	hub.endPostedReviewerJobs(t.Context())

	if want := []string{"job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want the reviewer ended once its node is connected", node.seen())
	}
	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v", state)
	}
}

// (b) A reviewer that posted and was not ended, older than more than one page
// of newer succeeded operations, is ended by the pass. A pass that kept only
// the newest page of operations would never reach it.
func TestReviewerEndAnOlderOrphanIsNotDisplacedByNewerOperations_t6394(t *testing.T) {
	old := reviewerEndPage
	reviewerEndPage = 3
	t.Cleanup(func() { reviewerEndPage = old })
	hub, node, jobs, sink := reviewerEndNode(t, reviewerEndPosted)
	now := time.Now()
	// Three pages and a bit of newer operations: reviewers already ended, and
	// spawns that are not reviewers.
	for n := 1; n <= 3*reviewerEndPage+1; n++ {
		role, state := "implementer", "spawned"
		if n%2 == 0 {
			role, state = reviewerRole, "completed"
		}
		reviewerEndOp(t, hub, jobs, role, fmt.Sprintf("newer-%d", n), "machine-a", state, n, now.Add(time.Duration(n)*time.Second))
	}

	hub.endPostedReviewerJobs(t.Context())

	if got := reviewerEndEndedJobs(sink.String()); !reflect.DeepEqual(got, []string{"test-job"}) {
		t.Fatalf("ended %v, want the older orphan test-job (commands %v)", got, node.seen())
	}
}

// (c) The read is smaller. The first pass judges every operation, so it reads
// each in full; the next pass lists the same window by id and reads in full
// only the operations that are not judged: the reviewer on the node that is not
// connected. The counts are what the store returned.
func TestReviewerEndReadsOnlyUnjudgedOperationsInFull_t6394(t *testing.T) {
	hub, _, jobs, _ := reviewerEndNode(t, reviewerEndPosted)
	now := time.Now()
	reviewerEndOp(t, hub, jobs, reviewerRole, "ended-reviewer", "machine-a", "completed", 1, now.Add(time.Second))
	reviewerEndOp(t, hub, jobs, reviewerRole, "reviewer-off-node", "machine-b", "spawned", 2, now.Add(2*time.Second))
	for n := 3; n <= 8; n++ {
		reviewerEndOp(t, hub, jobs, "implementer", fmt.Sprintf("implementer-%d", n), "machine-a", "spawned", n, now.Add(time.Duration(n)*time.Second))
	}
	read := func() reviewerEndRead {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.reviewerEndLastRead
	}

	hub.endPostedReviewerJobs(t.Context())
	if got, want := read(), (reviewerEndRead{Listed: 9, Full: 9}); got != want {
		t.Fatalf("first pass read %+v, want %+v", got, want)
	}
	hub.endPostedReviewerJobs(t.Context())
	// test-job's end landed on the first pass, so only the reviewer on the node
	// that is not connected is read again.
	if got, want := read(), (reviewerEndRead{Listed: 9, Full: 1}); got != want {
		t.Fatalf("second pass read %+v, want %+v", got, want)
	}
}

// The store's two reads: ids newest first with no payload, and full rows by id,
// in the order asked, for the operations that are still succeeded.
func TestLaneInboxStoreContractSucceededSpawnReads_t6394(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		store := open()
		if err := store.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		base := time.Now().UTC().Truncate(time.Millisecond)
		var ids []string
		for n := 1; n <= 4; n++ {
			job := fmt.Sprintf("read-job-%d", n)
			request := queueTestRequest(n, job)
			created := base.Add(time.Duration(n) * time.Second)
			task, _, err := store.claimSpawn(t.Context(), request, job, "parent", created)
			if err != nil {
				t.Fatal(err)
			}
			if n != 3 {
				noticeComplete(t, store, task, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, created)
				ids = append(ids, request.RequestID)
			}
		}
		// Newest first; the accepted operation (3) is not listed; the one
		// created before `since` (1) is not either.
		got, err := store.succeededSpawnIDs(t.Context(), base.Add(1500*time.Millisecond))
		if err != nil || !reflect.DeepEqual(got, []string{queueTestRequest(4, "").RequestID, queueTestRequest(2, "").RequestID}) {
			t.Fatalf("listed %v, %v", got, err)
		}
		all, err := store.succeededSpawnIDs(t.Context(), base)
		if err != nil || !reflect.DeepEqual(all, []string{ids[2], ids[1], ids[0]}) {
			t.Fatalf("listed %v, %v", all, err)
		}
		asked := []string{ids[0], queueTestRequest(3, "").RequestID, ids[2], "00000000-0000-4000-8000-999999999999"}
		tasks, err := store.succeededSpawnsByID(t.Context(), asked)
		if err != nil || len(tasks) != 2 || tasks[0].Job != "read-job-1" || tasks[1].Job != "read-job-4" {
			t.Fatalf("read %+v, %v: want read-job-1 then read-job-4, with the accepted and unknown ids left out", tasks, err)
		}
		if tasks[0].Request.RequestID != ids[0] || tasks[0].Request.Brief == "" {
			t.Fatalf("a full row carries its request: %+v", tasks[0].Request)
		}
		if none, err := store.succeededSpawnsByID(t.Context(), nil); err != nil || len(none) != 0 {
			t.Fatalf("no ids read %v, %v", none, err)
		}
	})
}

// A successful id listing must not clear a persistent full-row read failure:
// the two store reads have separate silent-error streaks.
func TestReviewerEndByIDReadFailureLogsOncePerStreak_t6547(t *testing.T) {
	hub, _, _, _ := reviewerEndNode(t, reviewerEndPosted)
	sink := captureHubLog(hub)

	// The id listing uses request_id/status/created_ms, while the full-row read
	// needs payload. Removing that column leaves the listing healthy and keeps
	// each full-row read failing for the rest of this test.
	if _, err := hub.inbox.store.db.ExecContext(t.Context(), `ALTER TABLE spawn_operations RENAME COLUMN payload TO payload_test_only`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		hub.endPostedReviewerJobs(t.Context())
	}

	if got := strings.Count(sink.String(), `hub_spawn_reviewer_end.go:succeededSpawnsByID`); got != 1 {
		t.Fatalf("full-row failure log lines=%d, want 1 across a persistent failure streak; log:\n%s", got, sink.String())
	}
	if got := hub.loopSnapshot()[loopReviewerEnd].SilentErrors; got != 1 {
		t.Fatalf("reviewer_end silent_errors=%d, want 1 for the persistent full-row failure", got)
	}
}
