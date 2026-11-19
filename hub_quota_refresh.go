package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	hubQuotaRefreshConcurrency = 4
	hubQuotaRefreshRetryDelay  = 2 * time.Second
	hubQuotaFailureLogTTL      = time.Minute
	// hubQuotaRefreshUseMargin is the part of a request's budget a completed
	// refresh needs to be worth waiting for: an attempt is not admitted when
	// its measurement could only finish at the request's own deadline
	// (hk 5456).
	hubQuotaRefreshUseMargin = 5 * time.Second
	// hubCapabilityQuotaRefreshAccount is advertised by a node whose
	// quota.refresh accepts account_key. Only such a connection is sent the
	// field: an older node's strict decoder rejects it (hk 5456).
	hubCapabilityQuotaRefreshAccount = "quota_refresh_account_v1"
)

// hubQuotaRefreshKey is one logical demand: a pool measured on a machine for
// one account. Two requests that wait on the same reading join one attempt;
// another account on the same machine and pool is a different attempt, so one
// account's completion never answers for another's (hk 5456).
type hubQuotaRefreshKey struct {
	machine, pool, accountKey string
}

type hubQuotaRefreshWork struct {
	// id is the opaque attempt ID this admission was given. It is returned to
	// the request, stored with its wait, and keys the durable result, so a
	// completion is matched to the attempt that asked for it and never to a
	// later or earlier one for the same machine and pool.
	id       string
	key      hubQuotaRefreshKey
	deadline time.Time
}
type hubQuotaRefreshResult struct {
	id      string
	key     hubQuotaRefreshKey
	failure string
}
type hubQuotaRefreshFailure struct {
	reason string
	seen   time.Time
}

// hubQuotaRefreshScheduler owns one queued or in-flight attempt per logical
// demand (machine, pool, account), independent of the number of spawns. A node
// runs one measurement at a time, the active set is bounded, and a completion
// is recorded by attempt ID so no request can be settled by another's result.
type hubQuotaRefreshScheduler struct {
	mu             sync.Mutex
	queue          []hubQuotaRefreshWork
	keys           map[hubQuotaRefreshKey]string
	completed      map[hubQuotaRefreshKey]time.Time
	failures       map[hubQuotaRefreshKey]hubQuotaRefreshFailure
	seq            atomic.Uint64
	wake           chan struct{}
	done           chan struct{}
	closed         bool
	completedCount atomic.Uint64
}

// scheduleQuotaRefresh admits one demand and reports the attempt ID and
// whether the caller should wait for it. Admission is decided here, once, from
// the effective request deadline, the measurement window and the use margin:
// a demand that cannot finish inside the caller's budget is refused, a
// compatible live demand is joined (its ID returned), and a demand inside the
// post-completion cooldown is refused without an attempt. Only an admitted or
// joined demand may leave the caller pending (hk 5456).
func (h *HubServer) scheduleQuotaRefresh(machine, pool, accountKey string, window time.Duration, deadline time.Time) (string, fleet.QuotaRefreshAdmission) {
	q := &h.quotaRefresh
	key := hubQuotaRefreshKey{machine: machine, pool: pool, accountKey: accountKey}
	now := time.Now()
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return "", fleet.QuotaRefreshRefused
	}
	if q.wake == nil {
		q.keys = make(map[hubQuotaRefreshKey]string)
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
	if window <= 0 {
		q.mu.Unlock()
		return "", fleet.QuotaRefreshRefused
	}
	if !deadline.IsZero() && !now.Add(window+hubQuotaRefreshUseMargin).Before(deadline) {
		q.mu.Unlock()
		return "", fleet.QuotaRefreshRefused
	}
	if id, live := q.keys[key]; live {
		// A live entry that an earlier pass admitted stays joinable; one whose
		// queue deadline expired is re-budgeted for this caller.
		for i := range q.queue {
			if q.queue[i].id == id && !now.Before(q.queue[i].deadline) {
				q.queue[i].deadline = now.Add(window)
				q.mu.Unlock()
				select {
				case q.wake <- struct{}{}:
				default:
				}
				return id, fleet.QuotaRefreshAdmitted
			}
		}
		q.mu.Unlock()
		return id, fleet.QuotaRefreshAdmitted
	}
	if now.Before(q.completed[key]) {
		_, failed := q.failures[key]
		q.mu.Unlock()
		if failed {
			// A failed attempt's cooldown: the failure may be transient, so the
			// caller keeps waiting for the retry, which a later pass admits.
			return "", fleet.QuotaRefreshRetry
		}
		return "", fleet.QuotaRefreshRefused
	}
	id := fmt.Sprintf("qr-%d", q.seq.Add(1))
	q.keys[key] = id
	q.queue = append(q.queue, hubQuotaRefreshWork{id: id, key: key, deadline: now.Add(window)})
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return id, fleet.QuotaRefreshAdmitted
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

// runQuotaRefreshes scans queued attempts in arrival order, skipping a busy
// node so another node can progress. An attempt whose deadline passes before
// it is sent ends as a node_timeout result: it was admitted, so the request
// that waits on it must be able to read that it never ran.
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
		// A queued attempt can expire even when every slot is busy. Record it
		// and wake placement rather than waiting for another node.
		q.mu.Lock()
		now := time.Now()
		expired := false
		for i := range q.queue {
			// An expired attempt keeps its place and its ID, but it is sent
			// only when a later pass asks for that demand again with a fresh
			// budget (scheduleQuotaRefresh re-budgets it). Its request must
			// not wait on it in the meantime, so placement is woken.
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
				if work.deadline.IsZero() {
					continue // expired: waits for a fresh budget
				}
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
			q.mu.Unlock()
			active[work.key.machine] = true
			running++
			go func() {
				refreshCtx, cancel := context.WithDeadline(ctx, work.deadline)
				defer cancel()
				failure := h.refreshQuotaFromNode(refreshCtx, work.key)
				done <- hubQuotaRefreshResult{id: work.id, key: work.key, failure: failure}
			}()
		}
		q.mu.Lock()
		var nextDeadline time.Time
		for _, work := range q.queue {
			if work.deadline.IsZero() {
				continue
			}
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
			// The durable copy lands before the log line, so an observed
			// failure is also one a later timeout can read after a restart.
			h.recordQuotaRefreshOutcome(result)
			if result.failure != "" && result.failure != previous.reason {
				h.logger.Warn("hub quota refresh failed", "machine", result.key.machine, "pool", result.key.pool, "account", result.key.accountKey, "attempt", result.id, "reason", result.failure)
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

// recordQuotaRefreshOutcome persists the finished attempt under its ID,
// success included. The request that waits on that attempt reads it back by
// ID (spawnRefreshWaitOutcome, spawnRefreshSettled), so a result can only ever
// answer for the attempt it belongs to.
func (h *HubServer) recordQuotaRefreshOutcome(result hubQuotaRefreshResult) {
	if h.inbox == nil || result.id == "" {
		return
	}
	if err := h.inbox.store.recordQuotaRefreshResult(h.closeCtx, result.id, result.key.machine, result.key.pool, result.key.accountKey, result.failure, h.now()); err != nil {
		h.logger.Warn("hub quota refresh state not persisted", "machine", result.key.machine, "pool", result.key.pool, "attempt", result.id, "error", err)
	}
}

// refreshQuotaFromNode asks one node to measure the pool. The account is sent
// only to a node that advertised hubCapabilityQuotaRefreshAccount: an older
// node's strict argument decoder rejects the field, so it receives the
// pool-only request and its success is an ordinary read of the account by the
// next placement pass.
func (h *HubServer) refreshQuotaFromNode(ctx context.Context, key hubQuotaRefreshKey) string {
	request := map[string]string{"pool": key.pool}
	if key.accountKey != "" && h.nodeCapability(key.machine, hubCapabilityQuotaRefreshAccount) {
		request["account_key"] = key.accountKey
	}
	args, _ := json.Marshal(request)
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
		switch result.Error {
		case "":
		case "node_lost":
			return "node_lost"
		case "account_mismatch":
			// Only an account-aware node answers this, and only for the named
			// account: the measurement cannot supply the reading the request
			// waits on (hk 5456).
			return "account_mismatch"
		default:
			return "refresh_failed"
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return "node_timeout"
		}
	}
	return ""
}

// quotaRefreshActive reports whether an attempt for this machine and pool is
// queued or running, whatever account it measures.
func (h *HubServer) quotaRefreshActive(machine, pool string) bool {
	q := &h.quotaRefresh
	q.mu.Lock()
	defer q.mu.Unlock()
	for key := range q.keys {
		if key.machine == machine && key.pool == pool {
			return true
		}
	}
	return false
}

// nodeCapability reports whether the connected node advertised a feature.
func (h *HubServer) nodeCapability(machine, feature string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	return record != nil && record.capabilities[feature]
}
