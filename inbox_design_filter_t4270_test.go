package panewire

// hk 4270 (operator decisions 152 and 153): a design lane's inbox lists what
// the design session reads. The lifecycle of a reviewer a daemon started, and
// a completion that repeats a READY, are kept in hk only, as a delivered relay
// row; a reviewer-spawn failure reaches the requester at once and the design
// lane once per request. Contract: docs/quiet-delivery.md.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const t4270Reviewer = "mwr-617-421e3f3-p1-1794453384"

var t4270Seq int

// t4270Request is a saved spawn request for job, owned by owner. requestedBy
// is the submitting lane ("" names none).
func t4270Request(job, owner, requestedBy, role string) fleet.Submission {
	t4270Seq++
	req := durableSpawnRequest()
	req.RequestID = fmt.Sprintf("d676220b-e763-4af1-aefd-%012d", t4270Seq)
	req.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", fmt.Sprint(4270 + t4270Seq), "--owner", owner, "--role", role, "--host", "auto"}
	if role == "reviewer" {
		req.Args = append(req.Args, "--review-pass", "1")
	}
	if requestedBy != "" {
		req.Args = append(req.Args, "--parent", requestedBy)
	}
	req.Repository.Worktree, req.Repository.Checkout.Dir = job+"-checkout", job+"-checkout"
	return req
}

// t4270Rows returns the hk relay rows stored for job, with their delivered_to.
func t4270Rows(fake *fakeHandoffkeep, job string) (rows []handoffkeepRelayEvent, deliveredTo []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, row := range fake.rows {
		if row.JobID == job {
			rows = append(rows, *row)
			deliveredTo = append(deliveredTo, row.DeliveredTo)
		}
	}
	return rows, deliveredTo
}

func t4270Items(t *testing.T, h *HubServer, lane string) []laneInboxItem {
	t.Helper()
	items, err := h.inbox.store.list(lane, false)
	if err != nil {
		t.Fatal(err)
	}
	return items
}

// A daemon-started reviewer's job.completed and job.revoked never reach the
// design lane's inbox or pane. hk holds each as a delivered row naming the
// filter, and the node's outbox is acknowledged, so a producer retry is
// answered by the same row.
func TestDesignLaneKeepsReviewerLifecycleInHKOnly_t4270(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, t3426Design, "", "reviewer"), t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}

	completed := t3426Completion(t4270Reviewer, t3426Design, "")
	h.relayJobCompletionFrom("host-a", completed)
	revoked := t3426Completion(t4270Reviewer, t3426Design, "")
	revoked.EventID, revoked.Reason = "00002-job.revoked.json", "PR head moved"
	h.relayJobEventFrom("host-a", "job.revoked", revoked)

	if items := t4270Items(t, h, t3426Design); len(items) != 0 {
		t.Fatalf("the design lane's inbox lists reviewer lifecycle records: %+v", items)
	}
	if got := t3426Injected(agent); len(got) != 0 {
		t.Fatalf("the design lane was woken or injected: %+v", got)
	}
	rows, deliveredTo := t4270Rows(fake, t4270Reviewer)
	if len(rows) == 0 {
		t.Fatal("hk holds no row for the reviewer's lifecycle: the records were dropped")
	}
	for i, to := range deliveredTo {
		if to != "hub/filtered:reviewer-lifecycle" {
			t.Fatalf("row %d (%s) delivered_to=%q, want hub/filtered:reviewer-lifecycle", rows[i].ID, rows[i].Kind, to)
		}
	}
	for range 2 {
		select {
		case <-agent.persisted:
		default:
			t.Fatal("a lifecycle record's outbox row was not acknowledged to the node")
		}
	}
	// A producer's retry after an acknowledgement lost on the wire finds the row.
	h.relayJobCompletionFrom("host-a", completed)
	if again, _ := t4270Rows(fake, t4270Reviewer); len(again) != len(rows) {
		t.Fatalf("a retry wrote another hk row: %d -> %d", len(rows), len(again))
	}
}

// SPAWNED for a daemon-started reviewer is kept in hk only; SPAWNED for a
// design session's own worker, and a reviewer a human named like the
// daemon's but started as a worker, are still listed.
func TestDesignLaneKeepsReviewerSpawnedInHKOnly_t4270(t *testing.T) {
	h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	for _, tc := range []struct{ job, role string }{{t4270Reviewer, "reviewer"}, {"mwr-9-aaaaaaa-p1-1", "worker"}, {"hk4270-own-worker", "worker"}} {
		task, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(tc.job, t3426Design, "", tc.role), tc.job, t3426Design, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{Stage: "landing", Host: "host-b", Pane: "w9:p1", Profile: "haiku", Model: "m"}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	h.flushSpawnNotifications(t.Context())

	var listed []string
	for _, item := range t4270Items(t, h, t3426Design) {
		listed = append(listed, item.Event.Text)
	}
	if len(listed) != 2 || strings.Contains(strings.Join(listed, "\n"), t4270Reviewer) {
		t.Fatalf("design lane lists %q, want the two non-reviewer SPAWNED lines only", listed)
	}
	var filtered int
	fake.mu.Lock()
	for _, row := range fake.rows {
		if strings.Contains(row.Text, "SPAWNED "+t4270Reviewer) && row.DeliveredTo == "hub/filtered:reviewer-lifecycle" {
			filtered++
		}
	}
	fake.mu.Unlock()
	if filtered != 1 {
		t.Fatalf("hk rows for the reviewer's SPAWNED filtered by the daemon rule=%d, want 1", filtered)
	}
	var pending int
	if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_notifications WHERE delivered<>1`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("outbox rows left unrouted=%d err=%v", pending, err)
	}
}

// A completion that arrives within the repeat window of the same worker's
// READY into the same lane is the same news: kept in hk only, however often
// it is seen. A completion without a READY (or long after it) is delivered as
// before.
func TestDesignInboxOmitsCompletionThatRepeatsReady_t4270(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Now()
	h.now = func() time.Time { return now }

	ready := t3434Event(t3426Design, t3434WorkerPane, "ready", "READY hk3430 PR #221 head abc1234")
	if r := h.relayLaneEvent(ready, agent); r.PersistFailed || r.Machine != "inbox" {
		t.Fatalf("READY relay=%+v, want an inbox item", r)
	}
	completion := t3426Completion(t3434WorkerJob, t3426Design, "https://github.com/example/repo/pull/221")
	now = now.Add(2 * time.Minute)
	h.relayJobCompletionFrom("host-a", completion)

	for _, item := range t4270Items(t, h, t3426Design) {
		if item.Kind == "job.completed" {
			t.Fatalf("the repeat completion is listed: %+v", item)
		}
	}
	rows, deliveredTo := t4270Rows(fake, t3434WorkerJob)
	if len(rows) != 1 || deliveredTo[0] != "hub/filtered:repeats-ready" {
		t.Fatalf("hk rows for the repeat completion=%d delivered_to=%q, want one hub/filtered:repeats-ready", len(rows), deliveredTo)
	}

	// The note is not spent: a retry after a lost acknowledgement, or a second
	// turn end inside the window, is the same news and is filed again.
	again := t3426Completion(t3434WorkerJob, t3426Design, "https://github.com/example/repo/pull/221")
	again.EventID, again.ReportPath = "00002-job.completed.json", "/jobs/second/report.md"
	h.relayJobCompletionFrom("host-a", again)
	h.relayJobCompletionFrom("host-a", completion)
	for _, item := range t4270Items(t, h, t3426Design) {
		if item.Kind == "job.completed" {
			t.Fatalf("a completion inside the window was listed: %+v", item)
		}
	}

	// A completion long after a READY is not its repeat.
	now = now.Add(readyRepeatWindow + time.Minute)
	after := t3426Completion(t3434WorkerJob, t3426Design, "https://github.com/example/repo/pull/221")
	after.EventID, after.ReportPath = "00003-job.completed.json", "/jobs/third/report.md"
	h.relayJobCompletionFrom("host-a", after)
	var completions int
	for _, item := range t4270Items(t, h, t3426Design) {
		if item.Kind == "job.completed" {
			completions++
		}
	}
	if completions != 1 {
		t.Fatalf("listed completions=%d, want only the one after the window", completions)
	}
}

// With no hk client nothing is filtered: a filter without a durable copy would
// lose the record.
func TestDesignInboxFilterNeedsHK_t4270(t *testing.T) {
	h, _, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, t3426Design, "", "reviewer"), t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.handoffkeep = nil
	h.relayJobCompletionFrom("host-a", t3426Completion(t4270Reviewer, t3426Design, ""))
	if item := t3426Item(t, h, t3426Design, "job.completed", t4270Reviewer); item.Class != "inbox" {
		t.Fatalf("completion stored as %+v", item)
	}
}

// A reviewer's records for a lane that is not a design lane are unchanged.
func TestNonDesignLaneKeepsReviewerLifecycle_t4270(t *testing.T) {
	h, agent, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, "director-1", "", "reviewer"), t4270Reviewer, "director-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	h.relayJobCompletionFrom("host-a", t3426Completion(t4270Reviewer, "director-1", ""))
	if got := t3426Injected(agent); len(got) != 1 {
		t.Fatalf("a non-design lane got %d injections, want 1: %+v", len(got), got)
	}
}

// A reviewer-spawn failure the saved request can be replayed after reaches
// the requester at once and the design lane not at all; the replay's outcome
// is what the design lane is told, once.
func TestReviewerSpawnFailureReachesDesignLaneOncePerRequest_t4270(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "mergewake-design", "mergewake-daemon")
	req := t4270Request(t4270Reviewer, "mergewake-design", "mergewake-daemon", "reviewer")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, t4270Reviewer, "mergewake-design", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	retryable := hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:locked", Fix: "Run panewire again.", Host: "rpi", Profile: "haiku", Retryable: true}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, retryable, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if items := t4270Items(t, h, "mergewake-design"); len(items) != 0 {
		t.Fatalf("the design lane heard a failure its requester can replay: %+v", items)
	}
	toDaemon := spawnFailedInjections(destination)
	if len(toDaemon) != 1 || toDaemon[0].Lane != "mergewake-daemon" || !strings.Contains(toDaemon[0].Text, "SPAWN_FAILED") {
		t.Fatalf("the requester got %+v, want one SPAWN_FAILED at once", toDaemon)
	}

	// The requester replays; the replay fails for good.
	resumed, err := h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	final := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Fix: "Check the checkout.", Host: "rpi"}
	if _, err = h.inbox.store.completeSpawn(t.Context(), resumed, final, time.Now()); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		h.flushSpawnNotifications(t.Context())
	}
	items := t4270Items(t, h, "mergewake-design")
	if len(items) != 1 || !strings.Contains(items[0].Event.Text, "SPAWN_FAILED") || !strings.Contains(items[0].Event.Text, "replay=no") {
		t.Fatalf("the design lane lists %+v, want exactly the last attempt's SPAWN_FAILED", items)
	}
	var open int
	if err = h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_notifications WHERE delivered<>1`).Scan(&open); err != nil || open != 0 {
		t.Fatalf("outbox rows still held or pending=%d err=%v: the superseded failure must be closed, not lost", open, err)
	}
}

// A requester that never replays cannot leave the failure unreported: once the
// hold ends the design lane is told, with the failure's own text.
func TestHeldReviewerSpawnFailureIsDeliveredWhenHoldEnds_t4270(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "mergewake-design", "mergewake-daemon")
	now := time.Now()
	h.now = func() time.Time { return now }
	req := t4270Request(t4270Reviewer, "mergewake-design", "mergewake-daemon", "reviewer")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, t4270Reviewer, "mergewake-design", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:locked", Host: "rpi", Retryable: true}, now); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	now = now.Add(h.quiet.timings.spawnFailureHold - time.Minute)
	h.flushSpawnNotifications(t.Context())
	if items := t4270Items(t, h, "mergewake-design"); len(items) != 0 {
		t.Fatalf("delivered before the hold ended: %+v", items)
	}
	now = now.Add(2 * time.Minute)
	h.flushSpawnNotifications(t.Context())
	items := t4270Items(t, h, "mergewake-design")
	if len(items) != 1 || !strings.Contains(items[0].Event.Text, "checkout_refused:locked") {
		t.Fatalf("design lane items after the hold=%+v, want the held SPAWN_FAILED", items)
	}
}

// A design session's own spawn failure is unchanged: one event per outcome,
// at once.
func TestDesignLaneOwnSpawnFailureIsUnchanged_t4270(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "panewire-design")
	req := t4270Request("hk4270-own-worker", "panewire-design", "", "worker")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "hk4270-own-worker", "panewire-design", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:locked", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if items := t4270Items(t, h, "panewire-design"); len(items) != 1 {
		t.Fatalf("design lane items=%+v, want its own worker's SPAWN_FAILED at once", items)
	}
}

// A completion whose filing failed is sent again by its producer and must be
// filed again: the READY that made it a repeat is spent only by a successful
// filing.
func TestDesignInboxReadyNoteSurvivesFailedFiling_t4270(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	ready := t3434Event(t3426Design, t3434WorkerPane, "ready", "READY hk3430 PR #221 head abc1234")
	h.relayLaneEvent(ready, agent)
	completion := t3426Completion(t3434WorkerJob, t3426Design, "https://github.com/example/repo/pull/221")

	fake.mu.Lock()
	fake.status = 500
	fake.mu.Unlock()
	h.relayJobCompletionFrom("host-a", completion)
	fake.mu.Lock()
	fake.status = 201
	fake.mu.Unlock()
	if items := t4270Items(t, h, t3426Design); len(items) != 1 {
		t.Fatalf("a failed filing listed %d items, want only the READY", len(items))
	}
	h.relayJobCompletionFrom("host-a", completion)
	for _, item := range t4270Items(t, h, t3426Design) {
		if item.Kind == "job.completed" {
			t.Fatalf("the retried completion was delivered instead of filed: %+v", item)
		}
	}
	if _, to := t4270Rows(fake, t3434WorkerJob); len(to) != 1 || to[0] != "hub/filtered:repeats-ready" {
		t.Fatalf("hk rows delivered_to=%q, want one hub/filtered:repeats-ready", to)
	}
}

// A design session's own reviewer spawn (the owner is also the requester, so
// no second copy exists) hears its retryable failure at once: it is the only
// notice it can act on.
func TestReviewerSpawnFailureWithoutDistinctRequesterIsNotHeld_t4270(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "mergewake-design")
	for _, requester := range []string{"", "mergewake-design"} {
		job := fmt.Sprintf("mwr-5-aaaaaaa-p1-%d", len(requester))
		task, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(job, "mergewake-design", requester, "reviewer"), job, "mergewake-design", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:locked", Retryable: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	h.flushSpawnNotifications(t.Context())
	if items := t4270Items(t, h, "mergewake-design"); len(items) != 2 {
		t.Fatalf("design lane items=%d, want both failures at once", len(items))
	}
}
