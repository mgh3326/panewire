package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// Each request state, served as the only answer, ends the wait the way the
// design says: landed returns, refusals and ended requests exit with their
// documented code, and every state still in flight keeps the wait open (the
// short context proves it did not return).
func TestWaitSpawnRequestStateTable(t *testing.T) {
	landed := map[string]any{"rc": 0, "host": "node-b", "profile": "sonnet", "pane": "w:p9"}
	for _, test := range []struct {
		state  string
		result map[string]any
		code   int  // 0: the wait returns without error
		waits  bool // the wait is still open when the context ends
	}{
		{state: "succeeded", result: landed},
		{state: "landed", result: landed},
		{state: "failed", result: map[string]any{"rc": 81, "reason": "no_host"}, code: 81},
		{state: "retryable", result: map[string]any{"rc": 2, "reason": "cwd_unmapped"}, code: 2},
		{state: "refused", result: map[string]any{"rc": 77, "reason": "brief_missing"}, code: 77},
		{state: "refused", result: map[string]any{"reason": "task_closed"}, code: exitPlacement},
		{state: "given_up", result: map[string]any{"reason": "expired"}, code: exitWaitLost},
		{state: "cancelled", result: map[string]any{"reason": "owner"}, code: exitWaitLost},
		{state: "deadline_exceeded", result: map[string]any{"rc": 124}, code: exitStateConflict},
		{state: "accepted", waits: true},
		{state: "waiting", waits: true},
		{state: "pending", waits: true},
		{state: "parked", waits: true},
		{state: "bound", waits: true},
		{state: "dispatching", waits: true},
		{state: "launching", waits: true},
		{state: "reconciling", waits: true},
	} {
		t.Run(test.state+"/"+firstNonempty(strings.Join(mapKeys(test.result), ","), "bare"), func(t *testing.T) {
			c, _ := spawnStatusSequence(t, map[string]any{"status": test.state, "result": test.result})
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
			defer cancel()
			_, err := c.WaitSpawn(ctx, statusTestOperation, SpawnWaitOptions{})
			switch {
			case test.waits:
				if err == nil || ctx.Err() == nil {
					t.Fatalf("state %s ended the wait early: %v", test.state, err)
				}
			case test.code == 0:
				if err != nil {
					t.Fatalf("state %s: %v", test.state, err)
				}
			default:
				if ErrorCode(err) != test.code {
					t.Fatalf("state %s: code=%d err=%v, want %d", test.state, ErrorCode(err), err, test.code)
				}
			}
		})
	}
}

func mapKeys(m map[string]any) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// A state no client knows keeps the wait going, says so once, and never
// turns into success or failure; a later landed still ends it.
func TestWaitSpawnUnknownStateKeepsWaiting(t *testing.T) {
	c, reads := spawnStatusSequence(t,
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "warming_up"},
		map[string]any{"status": "landed", "result": map[string]any{"host": "node-b", "pane": "w:p9"}})
	var notice bytes.Buffer
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Notice: &notice})
	if err != nil || attempt.Pane != "w:p9" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
	if got, want := notice.String(), "unknown state warming_up: still waiting\n"; got != want {
		t.Fatalf("notice=%q want exactly once %q", got, want)
	}
	if reads.Load() != 4 {
		t.Fatalf("reads=%d", reads.Load())
	}
}

// An unknown state that never resolves ends only at the caller's own bound,
// with the timeout code, not a failure of the request.
func TestWaitSpawnUnknownStateEndsOnlyAtTheWaitBound(t *testing.T) {
	c, _ := spawnStatusSequence(t, map[string]any{"status": "warming_up"})
	_, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond})
	if ErrorCode(err) != exitWaitTimeout {
		t.Fatalf("code=%d err=%v", ErrorCode(err), err)
	}
}

// A parked request prints its reason and next check once per change, and a
// wait that ends while it is still parked has its own exit code.
func TestWaitSpawnParkedPastTheBound(t *testing.T) {
	c, _ := spawnStatusSequence(t,
		map[string]any{"status": "parked", "reason": "quota", "next_check_at": "2026-11-15T10:00:00Z"},
		map[string]any{"status": "parked", "reason": "quota", "next_check_at": "2026-11-15T10:00:00Z"},
		map[string]any{"status": "parked", "reason": "pool_cap", "next_check_at": "2026-11-15T10:05:00Z"})
	var notice bytes.Buffer
	_, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond, Notice: &notice})
	if ErrorCode(err) != exitWaitParked || !strings.Contains(err.Error(), statusTestOperation) {
		t.Fatalf("code=%d err=%v", ErrorCode(err), err)
	}
	want := "wait request=" + statusTestOperation + " state=parked reason=quota next_check_at=2026-11-15T10:00:00Z\n" +
		"wait request=" + statusTestOperation + " state=parked reason=pool_cap next_check_at=2026-11-15T10:05:00Z\n"
	if notice.String() != want {
		t.Fatalf("notice=%q want %q", notice.String(), want)
	}
}

// A request that is not parked ends at the bound with the existing timeout
// code, and a request that lands in time is not cut off by a bound.
func TestWaitSpawnBoundOnlyEndsAnOpenRequest(t *testing.T) {
	c, _ := spawnStatusSequence(t, map[string]any{"status": "pending"})
	if _, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: 40 * time.Millisecond}); ErrorCode(err) != exitWaitTimeout {
		t.Fatalf("pending past the bound: %v", err)
	}
	c, _ = spawnStatusSequence(t, map[string]any{"status": "pending"}, map[string]any{"status": "landed", "result": map[string]any{"host": "node-b"}})
	if _, err := c.WaitSpawn(t.Context(), statusTestOperation, SpawnWaitOptions{Timeout: time.Minute}); err != nil {
		t.Fatalf("landed inside the bound: %v", err)
	}
}

// Only a terminal state with no pane lets a job directory go as "the hub
// operation failed".
func TestSpawnNeverLanded(t *testing.T) {
	for state, want := range map[string]bool{
		"failed": true, "refused": true, "given_up": true, "cancelled": true,
		"succeeded": false, "landed": false, "retryable": false, "deadline_exceeded": false, "reconciling": false,
		"parked": false, "pending": false, "unknown": false,
	} {
		if got := spawnNeverLanded(state); got != want {
			t.Errorf("spawnNeverLanded(%q)=%v want %v", state, got, want)
		}
	}
}
