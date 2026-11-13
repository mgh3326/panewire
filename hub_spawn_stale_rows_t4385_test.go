package panewire

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// A bound request past its deadline stays deadline_exceeded until the node
// proves what happened to it. The coordinator replayed every such row on every
// tick with a 5 s timeout, forever, and a row whose node never answered
// `unknown` never reached the orphan rule (hk 4385). The rows below are the
// stale rows of that incident.

// answerEveryReplay answers each job.spawn the hub sends with failure and
// counts them; it returns the counter.
func answerEveryReplay(t *testing.T, node *websocket.Conn, failure string) *atomic.Int32 {
	t.Helper()
	var replays atomic.Int32
	go func() {
		for {
			_, payload, err := node.Read(t.Context())
			if err != nil {
				return
			}
			message, valid := parseHubOutbound(payload)
			if !valid || message.Type != "job.spawn" {
				continue
			}
			replays.Add(1)
			_ = wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": message.RequestID, "rc": 75, "job_id": "", "pane": "", "stdout_tail": "", "error": failure})
		}
	}()
	return &replays
}

// ageStrandedSpawn moves the stranded request's deadline pastDeadline into the past.
func ageStrandedSpawn(t *testing.T, s *strandedSpawn, pastDeadline time.Duration) {
	t.Helper()
	if _, err := s.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET deadline_ms=? WHERE request_id=?`, time.Now().Add(-pastDeadline).UnixMilli(), s.task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	s.task = s.status(t)
}

func kickSpawnCoordinator(h *HubServer) {
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

func TestStaleSpawnRowIsNotReplayedEveryTick(t *testing.T) {
	s := newStrandedSpawn(t)
	node := t14Node(t, s.hub, s.server, true)
	replays := answerEveryReplay(t, node, "reconciling")
	s.hub.startSpawnCoordinator(t.Context())
	eventually(t, 3*time.Second, "the first replay", func() bool { return replays.Load() >= 1 })
	// Five more coordinator passes, paced so each sees the previous replay
	// finished (fixture pacing), must not replay a row probed a moment ago.
	for range 5 {
		kickSpawnCoordinator(s.hub)
		time.Sleep(100 * time.Millisecond)
	}
	if got := replays.Load(); got != 1 {
		t.Fatalf("a deadline_exceeded row was replayed %d times inside its back-off, want 1", got)
	}
}

func TestStaleSpawnRowIsSettledAfterTheBound(t *testing.T) {
	t.Run("no labelled pane orphans it and releases the role", func(t *testing.T) {
		s := newStrandedSpawn(t)
		ageStrandedSpawn(t, s, spawnStuckAfter+time.Minute)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "reconciling")
		probe := nodeCommandRead(t, node)
		if string(probe["op"]) != `"agents.list"` {
			t.Fatalf("probe op=%s", probe["op"])
		}
		roleReplyPanes(t, node, probe, []nodeAgentRow{{PaneID: "w2:p9", Name: "another-job"}}, true)
		args := roleMarkLostArgs(t, roleReplyMarkLost(t, node, nodeJobMarkLostLost))
		if args["end_reason"] != spawnOrphanedReason {
			t.Fatalf("mark lost args=%v", args)
		}
		wait()
		if got := s.status(t); got.Status != "failed" || got.Outcome.Reason != spawnOrphanedReason {
			t.Fatalf("stale request not settled: %+v", got)
		}
		if err := s.claimNext(t); err != nil {
			t.Fatalf("reservation still held: %v", err)
		}
		pending, err := s.hub.inbox.store.pendingSpawns(t.Context(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range pending {
			if task.Request.RequestID == s.task.Request.RequestID {
				t.Fatalf("settled row still pending: %+v", task)
			}
		}
	})
	t.Run("a labelled pane keeps ownership", func(t *testing.T) {
		s := newStrandedSpawn(t)
		ageStrandedSpawn(t, s, spawnStuckAfter+time.Minute)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "reconciling")
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w9:p1", Name: "test-job"}}, true)
		wait()
		if got := s.status(t); got.Status != "deadline_exceeded" {
			t.Fatalf("status=%s", got.Status)
		}
		if writes := s.jobs.writes(); len(writes) != 0 {
			t.Fatalf("hk written: %v", writes)
		}
	})
	t.Run("a young stale row is not probed for panes yet", func(t *testing.T) {
		s := newStrandedSpawn(t)
		ageStrandedSpawn(t, s, spawnStuckAfter-time.Minute)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "reconciling")
		wait()
		if got := s.status(t); got.Status != "deadline_exceeded" {
			t.Fatalf("status=%s", got.Status)
		}
	})
}

// staleSpawnRow stores request number n bound to machine-a and past its
// deadline (deadline_exceeded), never probed.
func staleSpawnRow(t *testing.T, h *HubServer, n int) {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(n)
	request.Args = append([]string(nil), request.Args...)
	job := fmt.Sprintf("stale-%d", n)
	for i, arg := range request.Args {
		switch arg {
		case "test-job":
			request.Args[i] = job
		case "42":
			request.Args[i] = fmt.Sprint(1000 + n)
		}
	}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now().Add(-3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(5000 + n), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", job}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestStaleSpawnRowsDoNotFillThePageAheadOfFreshOnes(t *testing.T) {
	h := asyncSpawnHub(t)
	for n := 1; n <= 40; n++ {
		staleSpawnRow(t, h, n)
	}
	fresh := durableSpawnRequest()
	fresh.RequestID = t14RequestID(9000)
	fresh.Args = append([]string(nil), fresh.Args...)
	for i, arg := range fresh.Args {
		switch arg {
		case "test-job":
			fresh.Args[i] = "fresh-job"
		case "42":
			fresh.Args[i] = "9000"
		}
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), fresh, "fresh-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	h.startSpawnCoordinator(t.Context())
	// The first coordinator pass runs at once and the next tick is 2 s away.
	eventually(t, time.Second, "the fresh accepted row to be dispatched in the first pass", func() bool {
		var lastRun int64
		if err := h.inbox.store.db.QueryRow(`SELECT last_run_ms FROM spawn_operations WHERE request_id=?`, fresh.RequestID).Scan(&lastRun); err != nil {
			t.Fatal(err)
		}
		return lastRun > 0
	})
}
