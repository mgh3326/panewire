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
)

type hubQuotaRefreshKey struct{ machine, pool string }
type hubQuotaRefreshWork struct {
	key      hubQuotaRefreshKey
	deadline time.Time
}

// hubQuotaRefreshScheduler owns one queue entry or in-flight operation per
// (configured machine, native pool), independent of the number of spawns.
// Its active set is bounded, and no node occupies more than one active slot.
type hubQuotaRefreshScheduler struct {
	mu             sync.Mutex
	queue          []hubQuotaRefreshWork
	keys           map[hubQuotaRefreshKey]bool
	completed      map[hubQuotaRefreshKey]time.Time
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
	if !q.keys[key] && !now.Before(q.completed[key]) {
		q.keys[key] = true
		q.queue = append(q.queue, hubQuotaRefreshWork{key: key, deadline: now.Add(timeout)})
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
	done := make(chan hubQuotaRefreshKey, hubQuotaRefreshConcurrency)
	active := make(map[string]bool)
	running := 0
	defer func() {
		for running > 0 {
			<-done
			running--
		}
	}()
	for {
		// A queued key can expire even when every slot is busy. Remove it and
		// wake placement now, rather than waiting for an unrelated node.
		q.mu.Lock()
		now := time.Now()
		remaining := q.queue[:0]
		expired := false
		for _, work := range q.queue {
			if !now.Before(work.deadline) {
				delete(q.keys, work.key)
				expired = true
			} else {
				remaining = append(remaining, work)
			}
		}
		clear(q.queue[len(remaining):])
		q.queue = remaining
		q.mu.Unlock()
		if expired {
			q.completedCount.Add(1)
			h.kickSpawnPlacement()
		}
		for running < hubQuotaRefreshConcurrency && ctx.Err() == nil {
			q.mu.Lock()
			index := -1
			for i, work := range q.queue {
				if !active[work.key.machine] {
					index = i
					break
				}
			}
			if index < 0 {
				q.mu.Unlock()
				break
			}
			work := q.queue[index]
			q.queue = append(q.queue[:index], q.queue[index+1:]...)
			if !time.Now().Before(work.deadline) {
				delete(q.keys, work.key)
				q.mu.Unlock()
				q.completedCount.Add(1)
				h.kickSpawnPlacement()
				continue
			}
			q.mu.Unlock()
			active[work.key.machine] = true
			running++
			go func() {
				refreshCtx, cancel := context.WithDeadline(ctx, work.deadline)
				defer cancel()
				h.refreshQuotaFromNode(refreshCtx, work.key)
				done <- work.key
			}()
		}
		q.mu.Lock()
		var nextDeadline time.Time
		for _, work := range q.queue {
			if nextDeadline.IsZero() || work.deadline.Before(nextDeadline) {
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
		case key := <-done:
			running--
			delete(active, key.machine)
			q.mu.Lock()
			delete(q.keys, key)
			q.completed[key] = time.Now().Add(hubQuotaRefreshRetryDelay)
			q.mu.Unlock()
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

func (h *HubServer) refreshQuotaFromNode(ctx context.Context, key hubQuotaRefreshKey) {
	args, _ := json.Marshal(map[string]string{"pool": key.pool})
	pending, id, _, sent := h.sendNodeCommandWithContext(ctx, key.machine, nodeCommandQuotaRefresh, args)
	if !sent {
		return
	}
	defer h.forgetNodeCommand(id)
	select {
	case <-pending.done:
	case <-ctx.Done():
	}
}
