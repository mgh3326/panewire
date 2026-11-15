package panewire

// hk 4270 (operator decisions 152 and 153): a design lane's inbox lists what
// the design session reads. The lifecycle of a reviewer a daemon started, and
// are kept in hk only, as a delivered relay row; a reviewer-spawn failure reaches the requester at once and the design
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
	for _, tc := range []struct{ job, role string }{{t4270Reviewer, "reviewer"}, {"mwr-9-aaaaaaa-p1-1", "implementer"}, {"hk4270-own-worker", "implementer"}} {
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
	now = now.Add(h.quietTimings.spawnFailureHold - time.Minute)
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
	req := t4270Request("hk4270-own-worker", "panewire-design", "", "implementer")
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

// hk's job.* idempotency key excludes the event id: a second completion that
// reuses the job, epoch, report path and reason gets the first row back. That
// row does not hold the second event, so it is delivered to the inbox and
// both payloads are retained.
func TestDesignInboxKeepsBothCompletionsThatShareAnHKKey_t4270(t *testing.T) {
	h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, t3426Design, "", "reviewer"), t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}
	first := t3426Completion(t4270Reviewer, t3426Design, "")
	second := t3426Completion(t4270Reviewer, t3426Design, "")
	second.EventID, second.ReportLastLine = "00002-job.completed.json", "a different result"
	h.relayJobCompletionFrom("host-a", first)
	h.relayJobCompletionFrom("host-a", second)

	items := t4270Items(t, h, t3426Design)
	if len(items) != 1 || items[0].Event.EventID != second.EventID {
		t.Fatalf("design inbox=%+v, want exactly the second event, whose contents hk does not hold", items)
	}
	rows, deliveredTo := t4270Rows(fake, t4270Reviewer)
	if len(rows) != 1 || rows[0].EventID != first.EventID || deliveredTo[0] != "hub/filtered:reviewer-lifecycle" {
		t.Fatalf("hk rows=%+v delivered_to=%q, want the first event filed", rows, deliveredTo)
	}
	// Retries of either event, as after a lost acknowledgement, decide the same way.
	h.relayJobCompletionFrom("host-a", first)
	h.relayJobCompletionFrom("host-a", second)
	if got := t4270Items(t, h, t3426Design); len(got) != 1 {
		t.Fatalf("retries changed the inbox: %+v", got)
	}
}

// The decision depends only on the record and the saved spawn request, so a
// producer's retry after a restart, however late, is filed again and never
// becomes an inbox item.
func TestDesignInboxFilterSurvivesRestartAndLateRetry_t4270(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, _, fake := t3434Hub(t, path)
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, t3426Design, "", "reviewer"), t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}
	completion := t3426Completion(t4270Reviewer, t3426Design, "")
	h.relayJobCompletionFrom("host-a", completion)
	h.Close()

	h2, _ := t3426Hub(t, path, h.handoffkeep, t3426Records)
	h2.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	h2.relayJobCompletionFrom("host-a", completion)
	if items := t4270Items(t, h2, t3426Design); len(items) != 0 {
		t.Fatalf("a late retry after a restart reached the inbox: %+v", items)
	}
	if rows, _ := t4270Rows(fake, t4270Reviewer); len(rows) != 1 {
		t.Fatalf("hk rows=%d, want the one filed row", len(rows))
	}
}

// A worker's READY is no longer a reason to hide its completion: a second
// turn's result is new information the design lane has not been told.
func TestDesignInboxStillListsCompletionAfterReady_t4270(t *testing.T) {
	h, agent, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	h.relayLaneEvent(t3434Event(t3426Design, t3434WorkerPane, "ready", "READY hk3430 PR #221 head abc1234"), agent)
	h.relayJobCompletionFrom("host-a", t3426Completion(t3434WorkerJob, t3426Design, "https://github.com/example/repo/pull/221"))
	if item := t3426Item(t, h, t3426Design, "job.completed", t3434WorkerJob); item.Class != "inbox" {
		t.Fatalf("completion stored as %+v", item)
	}
}

// An expiry that loses the claim to a replay publishes nothing: the claim is
// one statement conditioned on the request still being at the row's attempt.
func TestHeldSpawnNoticeClaimLosesToReplay_t4270(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "mergewake-design", "mergewake-daemon")
	req := t4270Request(t4270Reviewer, "mergewake-design", "mergewake-daemon", "reviewer")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, t4270Reviewer, "mergewake-design", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:locked", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	held := h.readSpawnNotifications(t.Context(), spawnNoticeHeld)
	if len(held) != 1 {
		t.Fatalf("held rows=%d, want 1", len(held))
	}
	if _, err = h.inbox.store.resumeCleanedSpawn(t.Context(), task, time.Now()); err != nil {
		t.Fatal(err)
	}
	if h.claimHeldSpawnNotification(t.Context(), held[0], spawnNoticeAttempt(held[0].id)) {
		t.Fatal("the expiry claimed a failure whose request was replayed first")
	}
	if items := t4270Items(t, h, "mergewake-design"); len(items) != 0 {
		t.Fatalf("a superseded failure was published: %+v", items)
	}
}
