package panewire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// taskFailHub is a hub whose handoffkeep points at the fake. Each sweep makes
// exactly one hk attempt per due row; tests that need the per-row retry
// delay set spawnTaskFailBackoffBase explicitly.
func taskFailHub(t *testing.T, storePath string, client *handoffkeepRelayClient, logs *bytes.Buffer) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	config := HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"},
		InboxStorePath:  storePath,
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     client,
	}
	if logs != nil {
		config.Logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func taskFailComments(t *testing.T, fake *fakeHandoffkeep) []fakeHandoffkeepComment {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]fakeHandoffkeepComment(nil), fake.comments...)
}

func taskFailPending(t *testing.T, h *HubServer) []string {
	t.Helper()
	ids, err := h.inbox.store.pendingSpawnTaskFailNoticeIDs(t.Context(), time.Now().UTC(), 100)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func taskFailUnjudged(t *testing.T, h *HubServer) int {
	t.Helper()
	var n int
	if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_operations WHERE status IN ('failed','refused') AND task_fail_notice_ms=0`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func taskFailClaim(t *testing.T, h *HubServer, seq int) hubSpawnTask {
	t.Helper()
	return taskFailClaimTask(t, h, seq, "42")
}

func taskFailClaimTask(t *testing.T, h *HubServer, seq int, taskID string) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(51370 + seq)
	job := fmt.Sprintf("task-fail-%d", seq)
	request.Args = replaceSpawnArg(replaceSpawnArg(request.Args, "--job", job), "--task", taskID)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// Each final failure with --task posts exactly one comment with the exact
// wake-skill text, and a second sweep posts nothing more.
func TestSpawnTaskFail5137PostsOncePerFinalStatus(t *testing.T) {
	for _, test := range []struct {
		name    string
		seq     int
		outcome hubSpawnOutcome
		want    string
		refused bool
	}{
		{
			name:    "failed",
			seq:     11,
			outcome: hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"},
			want:    "spawn failed request=%s rc=1 stage=placement reason=spawn_failed: boom",
		},
		{
			name:    "refused",
			seq:     12,
			outcome: hubSpawnOutcome{RC: 78, Stage: "queue", Reason: "task:42=missing"},
			want:    "spawn failed request=%s rc=78 stage=queue reason=task:42=missing",
			refused: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake, client, stop := newFakeHandoffkeep(t)
			t.Cleanup(stop)
			h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
			var task hubSpawnTask
			if test.refused {
				request := durableSpawnRequest()
				request.RequestID = t14RequestID(51379)
				request.Args = replaceSpawnArg(request.Args, "--job", "task-fail-refused")
				request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
				var err error
				task, _, err = h.inbox.store.claimSpawn(t.Context(), request, "task-fail-refused", "parent", time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if task.Status != "waiting" {
					t.Fatalf("conditioned claim status = %q, want waiting", task.Status)
				}
				h.endWaitingSpawn(t.Context(), task, "refused", "task:42=missing", 78)
			} else {
				task = taskFailClaim(t, h, test.seq)
				if _, err := h.inbox.store.completeSpawn(t.Context(), task, test.outcome, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			h.flushSpawnTaskFailNotices(t.Context())
			got := taskFailComments(t, fake)
			if len(got) != 1 {
				t.Fatalf("comments after first sweep = %d, want 1: %+v", len(got), got)
			}
			if want := fmt.Sprintf(test.want, task.Request.RequestID); got[0].Body != want {
				t.Fatalf("comment = %q, want %q", got[0].Body, want)
			}
			if got[0].TaskPath != "/v1/tasks/42/comments" {
				t.Fatalf("comment path = %q, want /v1/tasks/42/comments", got[0].TaskPath)
			}
			h.flushSpawnTaskFailNotices(t.Context())
			if got := taskFailComments(t, fake); len(got) != 1 {
				t.Fatalf("comments after second sweep = %d, want 1", len(got))
			}
			if pending := taskFailPending(t, h); len(pending) != 0 {
				t.Fatalf("pending notices = %v, want none", pending)
			}
		})
	}
}

// deadline_exceeded is not final, so it posts nothing; a late node result
// still lands it. If the row is later finished as failed, the failed post
// fires once; if it lands as succeeded, nothing ever posts.
func TestSpawnTaskFail5137DeadlineExceededPostsOnlyWhenFailed(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Nanosecond
	claimSent := func(t *testing.T, seq int) hubSpawnTask {
		t.Helper()
		task := taskFailClaimTask(t, h, seq, fmt.Sprintf("44%02d", seq))
		node := hubSpawnRequest{RequestID: t14RequestID(51990 + seq), Machine: "machine-a", Args: task.Request.Args, WaitSeconds: 600}
		bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		return sentSpawnFixture(t, h.inbox.store, bound)
	}
	task := claimSent(t, 21)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID); err != nil || saved.Status != "deadline_exceeded" {
		t.Fatalf("row = %+v %v, want deadline_exceeded", saved, err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("comments while deadline_exceeded = %d, want 0", len(got))
	}
	// The stuck path finishes the row as failed: one comment with the failed
	// outcome, not the earlier timeout.
	current, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.completeSpawn(t.Context(), current, hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "orphaned_after_restart", Host: "machine-a", NodeResult: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	got := taskFailComments(t, fake)
	if len(got) != 1 {
		t.Fatalf("comments after the row failed = %d, want 1: %+v", len(got), got)
	}
	if want := "spawn failed request=" + task.Request.RequestID + " rc=75 stage=landing reason=orphaned_after_restart"; got[0].Body != want {
		t.Fatalf("comment = %q, want %q", got[0].Body, want)
	}
	// A deadline_exceeded row a late node result lands posts nothing, ever.
	landed := claimSent(t, 22)
	if _, err := h.inbox.store.completeSpawn(t.Context(), landed, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	late, err := h.inbox.store.spawnTask(t.Context(), landed.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.completeSpawn(t.Context(), late, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments after a late success = %d, want still 1", len(got))
	}
}

// A restart after the post marks nothing pending and posts no second comment:
// the posted-at mark on the operation is the idempotency proof.
func TestSpawnTaskFail5137RestartPostsNothing(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first := taskFailHub(t, storePath, client, nil)
	task := taskFailClaim(t, first, 1)
	if _, err := first.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	first.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments before restart = %d, want 1", len(got))
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := taskFailHub(t, storePath, client, nil)
	second.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments after restart sweep = %d, want 1", len(got))
	}
	if pending := taskFailPending(t, second); len(pending) != 0 {
		t.Fatalf("pending notices after restart = %v, want none", pending)
	}
}

// An hk outage keeps the comment pending and warns once; once hk answers,
// exactly one comment lands however many sweeps ran meanwhile.
func TestSpawnTaskFail5137RetryThenSinglePost(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	var logs bytes.Buffer
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &logs)
	// Shrink the per-row delay to nanoseconds: every sweep re-attempts while
	// hk is down, so the test observes sweep-level retry deterministically.
	h.spawnTaskFailBackoffBase = time.Nanosecond
	task := taskFailClaim(t, h, 1)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.commentStatus = http.StatusServiceUnavailable
	fake.mu.Unlock()
	h.flushSpawnTaskFailNotices(t.Context())
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("comments while hk is down = %d, want 0", len(got))
	}
	if n := strings.Count(logs.String(), "hub spawn task comment not posted"); n != 1 {
		t.Fatalf("failure warns = %d, want 1 (one per streak):\n%s", n, logs.String())
	}
	if pending := taskFailPending(t, h); len(pending) != 1 {
		t.Fatalf("pending notices while hk is down = %v, want the one failure", pending)
	}
	fake.mu.Lock()
	fake.commentStatus = 0
	fake.mu.Unlock()
	h.flushSpawnTaskFailNotices(t.Context())
	h.flushSpawnTaskFailNotices(t.Context())
	got := taskFailComments(t, fake)
	if len(got) != 1 {
		t.Fatalf("comments after recovery = %d, want 1: %+v", len(got), got)
	}
	if want := "spawn failed request=" + task.Request.RequestID + " rc=1 stage=placement reason=spawn_failed: boom"; got[0].Body != want {
		t.Fatalf("comment = %q, want %q", got[0].Body, want)
	}
	if pending := taskFailPending(t, h); len(pending) != 0 {
		t.Fatalf("pending notices after recovery = %v, want none", pending)
	}
}

func TestSpawnTaskFail5137NoPostWithoutTerminalFailure(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fake, client, stop := newFakeHandoffkeep(t)
		t.Cleanup(stop)
		h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
		task := taskFailClaim(t, h, 1)
		if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
		h.flushSpawnTaskFailNotices(t.Context())
		if got := taskFailComments(t, fake); len(got) != 0 {
			t.Fatalf("comments for success = %d, want 0", len(got))
		}
	})

	t.Run("no task", func(t *testing.T) {
		fake, client, stop := newFakeHandoffkeep(t)
		t.Cleanup(stop)
		h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(51380)
		args := replaceSpawnArg(request.Args, "--job", "task-fail-notask")
		// Drop the --task flag and its value; --parent-job keeps the request
		// valid without naming an hk task.
		var kept []string
		skip := false
		for _, arg := range args {
			if skip {
				skip = false
				continue
			}
			if arg == "--task" {
				skip = true
				continue
			}
			kept = append(kept, arg)
		}
		request.Args = append(kept, "--parent-job", "task-fail-parent")
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "task-fail-notask", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		h.flushSpawnTaskFailNotices(t.Context())
		if got := taskFailComments(t, fake); len(got) != 0 {
			t.Fatalf("comments without --task = %d, want 0", len(got))
		}
		if pending := taskFailPending(t, h); len(pending) != 0 {
			t.Fatalf("pending notices without --task = %v, want none (judged)", pending)
		}
	})

	t.Run("retryable then success", func(t *testing.T) {
		fake, client, stop := newFakeHandoffkeep(t)
		t.Cleanup(stop)
		h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
		task := taskFailClaim(t, h, 1)
		// A node refusal the same request can survive is retryable, not
		// final: nothing posts. The attempt must have started; an unstarted
		// timeout renews the attempt instead (hk 4660).
		node := hubSpawnRequest{RequestID: t14RequestID(51991), Machine: "machine-a", Args: task.Request.Args, WaitSeconds: 600}
		bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.completeSpawn(t.Context(), bound, hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:dirty", Host: "machine-a", Retryable: true, NodeResult: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
		h.flushSpawnTaskFailNotices(t.Context())
		if got := taskFailComments(t, fake); len(got) != 0 {
			t.Fatalf("comments for retryable = %d, want 0", len(got))
		}
		current, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := h.inbox.store.resumeCleanedSpawn(t.Context(), current, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.completeSpawn(t.Context(), replayed, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
		h.flushSpawnTaskFailNotices(t.Context())
		if got := taskFailComments(t, fake); len(got) != 0 {
			t.Fatalf("comments after replay to success = %d, want 0", len(got))
		}
	})
}

func taskFailCommentCalls(t *testing.T, fake *fakeHandoffkeep) int {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	n := 0
	for _, c := range fake.calls {
		if c.Method == http.MethodPost && strings.HasSuffix(c.Path, "/comments") {
			n++
		}
	}
	return n
}

// Terminal rows that predate the posted-at mark never owed a comment: the
// one-shot backfill stamps them judged, and only later failures post.
func TestSpawnTaskFail5137BackfillStampsHistory(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Nanosecond
	old := taskFailClaim(t, h, 1)
	if _, err := h.inbox.store.completeSpawn(t.Context(), old, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Pretend old predates the mark column: drop the one-shot guard and run
	// ensure again, as a store from before hk 5137 would on first open.
	if _, err := h.inbox.store.db.Exec(`DELETE FROM spawn_task_fail_backfill`); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("comments for pre-existing failures = %d, want 0", len(got))
	}
	if pending := taskFailPending(t, h); len(pending) != 0 {
		t.Fatalf("pending notices after backfill = %v, want none", pending)
	}
	fresh := taskFailClaimTask(t, h, 2, "4202")
	if _, err := h.inbox.store.completeSpawn(t.Context(), fresh, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments for a new failure after backfill = %d, want 1", len(got))
	}
}

// Two overlapping sweeps post once: the per-request single-flight plus the
// mark CAS own the row, not the sweep count.
func TestSpawnTaskFail5137ConcurrentSweepsPostOnce(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Nanosecond
	task := taskFailClaim(t, h, 1)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.flushSpawnTaskFailNotices(context.Background())
		}()
	}
	wg.Wait()
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments after concurrent sweeps = %d, want 1: %+v", len(got), got)
	}
}

// A mark write lost after hk stored the comment reposts once: exactly-once
// per mark write, at-least-once across that window, as documented.
func TestSpawnTaskFail5137LostMarkRepostsOnce(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Nanosecond
	task := taskFailClaim(t, h, 1)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments after first sweep = %d, want 1", len(got))
	}
	// Simulate the lost mark write: the row is unjudged again with hk
	// already holding the comment.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET task_fail_notice_ms=0 WHERE request_id=?`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	got := taskFailComments(t, fake)
	if len(got) != 2 || got[0].Body != got[1].Body {
		t.Fatalf("comments after lost mark = %d, want the same line twice: %+v", len(got), got)
	}
}

// A --task-shaped value inside another flag's slot must not name the hk
// task: only the real --task pair counts.
func TestSpawnTaskFail5137FlagValueSlotIsNotTask(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Nanosecond
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(51381)
	args := replaceSpawnArg(request.Args, "--job", "task-fail-forge")
	// A --task-shaped value in another flag's slot: the loose reader would
	// take 43 for the task, the strict pair walk skips it for the real 7.
	var forged []string
	for _, arg := range args {
		if arg == "--task" {
			forged = append(forged, "--purpose", "--task=43")
		}
		forged = append(forged, arg)
	}
	forged = replaceSpawnArg(forged, "--task", "7")
	request.Args = forged
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "task-fail-forge", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	got := taskFailComments(t, fake)
	if len(got) != 1 {
		t.Fatalf("comments = %d, want 1: %+v", len(got), got)
	}
	if got[0].TaskPath != "/v1/tasks/7/comments" {
		t.Fatalf("comment path = %q, want the real --task 7", got[0].TaskPath)
	}
}

// A row hk keeps refusing is re-attempted only after its growing delay, so an
// outage costs one bounded sweep and cheap skips instead of stalling the loop.
func TestSpawnTaskFail5137BackoffSkipsUntilDue(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Hour
	task := taskFailClaim(t, h, 1)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.commentStatus = http.StatusServiceUnavailable
	fake.mu.Unlock()
	h.flushSpawnTaskFailNotices(t.Context())
	h.flushSpawnTaskFailNotices(t.Context())
	if n := taskFailCommentCalls(t, fake); n != 1 {
		t.Fatalf("hk comment attempts = %d, want 1 (second sweep skips)", n)
	}
	if n := taskFailUnjudged(t, h); n != 1 {
		t.Fatalf("unjudged failures = %d, want the one failure (backed off, not due)", n)
	}
	// Simulate the delay passing: stamping the row's next attempt in the past
	// makes it due again, and the next sweep re-attempts the post.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET task_fail_retry_ms=1 WHERE request_id=?`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if n := taskFailCommentCalls(t, fake); n != 2 {
		t.Fatalf("hk comment attempts after the delay = %d, want 2", n)
	}
}

// Backed-off rows must not starve newer failures: the due filter runs before
// LIMIT, so a page of 32 backed-off rows still leaves room for what is due.
func TestSpawnTaskFail5137BackedOffRowsDoNotStarveNewer(t *testing.T) {
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	h.spawnTaskFailBackoffBase = time.Hour
	fake.mu.Lock()
	fake.commentStatus = http.StatusServiceUnavailable
	fake.mu.Unlock()
	for seq := 1; seq <= 32; seq++ {
		task := taskFailClaimTask(t, h, seq, fmt.Sprintf("45%02d", seq))
		if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if n := taskFailCommentCalls(t, fake); n != 32 {
		t.Fatalf("hk comment attempts = %d, want 32", n)
	}
	fresh := taskFailClaimTask(t, h, 33, "4533")
	if _, err := h.inbox.store.completeSpawn(t.Context(), fresh, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if n := taskFailCommentCalls(t, fake); n != 33 {
		t.Fatalf("hk comment attempts = %d, want 33 (the newer failure is tried despite 32 backed-off rows)", n)
	}
}

// Bookkeeping writes run detached from the sweep budget: a mark or deferral
// lands even when the caller's context is already spent.
func TestSpawnTaskFail5137BookkeepingSurvivesCancel(t *testing.T) {
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, nil)
	task := taskFailClaimTask(t, h, 1, "4601")
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if marked, err := h.inbox.store.markSpawnTaskFailNotice(canceled, task.Request.RequestID, time.Now()); err != nil || !marked {
		t.Fatalf("mark under canceled context = %v, %v; want it landed", marked, err)
	}
	fresh := taskFailClaimTask(t, h, 2, "4602")
	if _, err := h.inbox.store.completeSpawn(t.Context(), fresh, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(time.Hour)
	if deferred, err := h.inbox.store.deferSpawnTaskFailNotice(canceled, fresh.Request.RequestID, next); err != nil || !deferred {
		t.Fatalf("defer under canceled context = %v, %v; want it landed", deferred, err)
	}
	if pending := taskFailPending(t, h); len(pending) != 0 {
		t.Fatalf("pending notices = %v, want none (one marked, one deferred past now)", pending)
	}
}

// Without an hk client the hub judges its pre-start backlog once at startup:
// enabling hk later posts nothing for rows already terminal then, while
// failures that turn final afterwards stay owed.
func TestSpawnTaskFail5137NoHKJudgesOnlyBacklog(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
	// Seed a terminal row before the hub exists, as an unconfigured window
	// would leave behind.
	seed := openSpawnStore(t, storePath)
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(51390)
	request.Args = replaceSpawnArg(replaceSpawnArg(request.Args, "--job", "task-fail-backlog"), "--task", "4690")
	backlog, _, err := seed.claimSpawn(t.Context(), request, "task-fail-backlog", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.completeSpawn(t.Context(), backlog, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A row submitted before the hub started but turning final after it is
	// not backlog: it stays owed. A created_ms cutoff would judge it without
	// posting and suppress it forever.
	staleRequest := durableSpawnRequest()
	staleRequest.RequestID = t14RequestID(51391)
	staleRequest.Args = replaceSpawnArg(replaceSpawnArg(staleRequest.Args, "--job", "task-fail-stale"), "--task", "4691")
	stale, _, err := seed.claimSpawn(t.Context(), staleRequest, "task-fail-stale", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.db.Exec(`UPDATE spawn_operations SET created_ms=1 WHERE request_id=?`, stale.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	config := HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"},
		InboxStorePath:  storePath,
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	if _, err := h.inbox.store.completeSpawn(t.Context(), stale, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	fresh := taskFailClaimTask(t, h, 2, "4602")
	if _, err := h.inbox.store.completeSpawn(t.Context(), fresh, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if n := taskFailUnjudged(t, h); n != 2 {
		t.Fatalf("unjudged failures = %d, want this lifetime's two rows", n)
	}
	if pending := taskFailPending(t, h); len(pending) != 2 {
		t.Fatalf("pending notices = %v, want this lifetime's two rows", pending)
	}
}
