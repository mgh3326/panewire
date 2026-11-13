package panewire

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

const (
	hubQuotaRefreshConcurrency = 4
	hubQuotaRefreshRetryDelay  = 2 * time.Second
	hubQuotaFailureLogTTL      = time.Minute
)

type hubQuotaRefreshKey struct{ machine, pool string }
type hubQuotaRefreshWork struct {
	key      hubQuotaRefreshKey
	deadline time.Time
}
type hubQuotaRefreshResult struct {
	key     hubQuotaRefreshKey
	failure string
}
type hubQuotaRefreshFailure struct {
	reason string
	seen   time.Time
}

// hubQuotaRefreshScheduler owns one queue entry or in-flight operation per
// (configured machine, native pool), independent of the number of spawns.
// Its active set is bounded, and no node occupies more than one active slot.
type hubQuotaRefreshScheduler struct {
	mu             sync.Mutex
	queue          []hubQuotaRefreshWork
	keys           map[hubQuotaRefreshKey]bool
	completed      map[hubQuotaRefreshKey]time.Time
	failures       map[hubQuotaRefreshKey]hubQuotaRefreshFailure
	wake           chan struct{}
	done           chan struct{}
	closed         bool
	completedCount atomic.Uint64
}

func (h *HubServer) scheduleQuotaRefresh(machine, pool string, timeout time.Duration) {
	q := &h.quotaRefresh
	key := hubQuotaRefreshKey{machine: machine, pool: pool}
	now := time.Now()
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	if q.wake == nil {
		q.keys = make(map[hubQuotaRefreshKey]bool)
		q.completed = make(map[hubQuotaRefreshKey]time.Time)
		q.failures = make(map[hubQuotaRefreshKey]hubQuotaRefreshFailure)
		q.wake = make(chan struct{}, 1)
		q.done = make(chan struct{})
		go func() {
			defer close(q.done)
			h.runQuotaRefreshes(h.closeCtx)
		}()
	}
	for old, until := range q.completed {
		if !now.Before(until) {
			delete(q.completed, old)
		}
	}
	for old, failure := range q.failures {
		if now.Sub(failure.seen) >= hubQuotaFailureLogTTL {
			delete(q.failures, old)
		}
	}
	if !q.keys[key] && !now.Before(q.completed[key]) {
		q.keys[key] = true
		q.queue = append(q.queue, hubQuotaRefreshWork{key: key, deadline: now.Add(timeout)})
	} else if q.keys[key] {
		// An expired queued attempt retains its position, but it cannot send
		// until placement asks for that key again with a fresh budget.
		for i := range q.queue {
			if q.queue[i].key == key && q.queue[i].deadline.IsZero() {
				q.queue[i].deadline = now.Add(timeout)
				break
			}
		}
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// closeQuotaRefreshes prevents new work and joins the scheduler after the
// hub's close context is cancelled. It never waits with the queue lock held.
func (h *HubServer) closeQuotaRefreshes() {
	q := &h.quotaRefresh
	q.mu.Lock()
	q.closed = true
	done := q.done
	q.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (h *HubServer) kickSpawnPlacement() {
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

// runQuotaRefreshes scans queued keys in arrival order, skipping a busy node
// so another node can progress. A queue deadline expires before a write can
// start; the accepted request supplies the key again on a later placement pass.
func (h *HubServer) runQuotaRefreshes(ctx context.Context) {
	q := &h.quotaRefresh
	done := make(chan hubQuotaRefreshResult, hubQuotaRefreshConcurrency)
	active := make(map[string]bool)
	running := 0
	defer func() {
		for running > 0 {
			<-done
			running--
		}
	}()
	for {
		// A queued attempt can expire even when every slot is busy. Retain
		// its place and wake placement, rather than waiting for another node.
		q.mu.Lock()
		now := time.Now()
		expired := false
		for i := range q.queue {
			if !q.queue[i].deadline.IsZero() && !now.Before(q.queue[i].deadline) {
				q.queue[i].deadline = time.Time{}
				expired = true
			}
		}
		q.mu.Unlock()
		if expired {
			q.completedCount.Add(1)
			h.kickSpawnPlacement()
		}
		for running < hubQuotaRefreshConcurrency && ctx.Err() == nil {
			q.mu.Lock()
			index := -1
			for i, work := range q.queue {
				if !work.deadline.IsZero() && !active[work.key.machine] {
					index = i
					break
				}
			}
			if index < 0 {
				q.mu.Unlock()
				break
			}
			work := q.queue[index]
			if !time.Now().Before(work.deadline) {
				q.queue[index].deadline = time.Time{}
				q.mu.Unlock()
				q.completedCount.Add(1)
				h.kickSpawnPlacement()
				continue
			}
			q.queue = append(q.queue[:index], q.queue[index+1:]...)
			q.mu.Unlock()
			active[work.key.machine] = true
			running++
			go func() {
				refreshCtx, cancel := context.WithDeadline(ctx, work.deadline)
				defer cancel()
				failure := h.refreshQuotaFromNode(refreshCtx, work.key)
				done <- hubQuotaRefreshResult{key: work.key, failure: failure}
			}()
		}
		q.mu.Lock()
		var nextDeadline time.Time
		for _, work := range q.queue {
			if !work.deadline.IsZero() && (nextDeadline.IsZero() || work.deadline.Before(nextDeadline)) {
				nextDeadline = work.deadline
			}
		}
		q.mu.Unlock()
		var timer *time.Timer
		var deadline <-chan time.Time
		if !nextDeadline.IsZero() {
			timer = time.NewTimer(time.Until(nextDeadline))
			deadline = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case result := <-done:
			running--
			delete(active, result.key.machine)
			q.mu.Lock()
			delete(q.keys, result.key)
			now := time.Now()
			q.completed[result.key] = now.Add(hubQuotaRefreshRetryDelay)
			previous := q.failures[result.key]
			if result.failure == "" {
				delete(q.failures, result.key)
			} else {
				q.failures[result.key] = hubQuotaRefreshFailure{reason: result.failure, seen: now}
			}
			q.mu.Unlock()
			if result.failure != "" && result.failure != previous.reason {
				h.logger.Warn("hub quota refresh failed", "machine", result.key.machine, "pool", result.key.pool, "reason", result.failure)
			}
			q.completedCount.Add(1)
			h.kickSpawnPlacement()
		case <-q.wake:
		case <-deadline:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (h *HubServer) refreshQuotaFromNode(ctx context.Context, key hubQuotaRefreshKey) string {
	args, _ := json.Marshal(map[string]string{"pool": key.pool})
	pending, id, _, sent := h.sendNodeCommandWithContext(ctx, key.machine, nodeCommandQuotaRefresh, args)
	if !sent {
		if ctx.Err() == context.DeadlineExceeded {
			return "node_timeout"
		}
		if ctx.Err() != nil {
			return ""
		}
		return "node_unavailable"
	}
	defer h.forgetNodeCommand(id)
	select {
	case result := <-pending.done:
		if result.Error == "node_lost" {
			return "node_lost"
		}
		if result.Error != "" {
			return "refresh_failed"
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return "node_timeout"
		}
	}
	return ""
}
