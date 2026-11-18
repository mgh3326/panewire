package panewire

import (
	"context"
	"fmt"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The commandcode pool holds several accounts per host and fleet-wide, so a
// provider credit rejection on one job marks that job's own account, never the
// pool (hk 5138; docs/quota-stop.md, "An exhausted pool"). The mark is served
// in the fleet snapshot, where every host's selection reads it, and is kept in
// the hub store so a restart holds it until it lifts:
//
//   - at the first quota reading of that account observed after the mark that
//     reports at least one remaining credit (a fresh omp report), or
//   - after fleet.AccountCreditMarkTTL (24 h), which is the only bound for an
//     account whose readings carry no balance (decision 403) —
//     whichever comes first.
const accountCreditMarkPool = "commandcode"

// loadAccountCreditMarks reads the durable marks once per hub process.
func (h *HubServer) loadAccountCreditMarks() {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	marks, err := h.inbox.store.accountCreditMarks(ctx)
	if err != nil {
		h.logger.Warn("account credit marks could not be read", "error", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.accountCreditMarks == nil {
		h.accountCreditMarks = marks
	}
}

// accountCreditMarkActive reports whether account's mark still holds at now:
// inside the TTL and not lifted by a reading observed after it that reports at
// least one credit. A later reading with no balance says nothing about the
// credits, so the mark holds until the TTL; a reading below one credit keeps it
// too.
func (h *HubServer) accountCreditMarkActive(account string, mark accountCreditMark, now time.Time) bool {
	if mark.Marked.IsZero() || !now.Before(mark.Marked.Add(fleet.AccountCreditMarkTTL)) {
		return false
	}
	store := h.stateStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, message := range store.latest {
		if key.kind != fleetStateKindQuota || key.key != account || message.Quota == nil || message.Quota.AccountKey != account {
			continue
		}
		if !fleetStateObservedAt(message).After(mark.Marked) {
			continue
		}
		if message.Quota.Balance != nil && *message.Quota.Balance >= 1 {
			return false
		}
	}
	return true
}

// markAccountCreditExhausted records the rejection for account. A mark that
// already holds is left as it is, so repeated passes do not extend its TTL; a
// mark that lifted starts a new episode with the new time.
func (h *HubServer) markAccountCreditExhausted(pool, account string, at time.Time) error {
	h.accountCreditOnce.Do(h.loadAccountCreditMarks)
	at = at.UTC()
	h.mu.Lock()
	prior, found := h.accountCreditMarks[account]
	h.mu.Unlock()
	if found && h.accountCreditMarkActive(account, prior, at) {
		return nil
	}
	if h.inbox == nil {
		return errAccountCreditMarkUnstored
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mark := accountCreditMark{Account: account, Pool: pool, Marked: at}
	if err := h.inbox.store.putAccountCreditMark(ctx, account, mark); err != nil {
		return err
	}
	h.mu.Lock()
	if h.accountCreditMarks == nil {
		h.accountCreditMarks = map[string]accountCreditMark{}
	}
	h.accountCreditMarks[account] = mark
	h.mu.Unlock()
	return nil
}

// accountCreditMarkHeld reports whether account is currently marked, for the
// sweep's lift check.
func (h *HubServer) accountCreditMarkHeld(account string, now time.Time) bool {
	if account == "" {
		return false
	}
	h.accountCreditOnce.Do(h.loadAccountCreditMarks)
	h.mu.Lock()
	mark, found := h.accountCreditMarks[account]
	h.mu.Unlock()
	return found && h.accountCreditMarkActive(account, mark, now)
}

// handOffAccountCreditStopped starts the quota-stop successor for an
// implementer whose account ran out of provider credit (hk 5138). It reuses
// the quota-stop flow's replacement, so the old hk record ends with
// handed_over_to and the successor carries the same task and brief. No pool is
// excluded: the account mark keeps the successor off the spent account while
// the pool's other accounts stay placeable.
func (h *HubServer) handOffAccountCreditStopped(ctx context.Context, task hubSpawnTask, exhaustion poolExhaustion) {
	machine := spawnHolderHost(task)
	if machine == "" {
		h.logger.Warn("account credit exhausted job has no holder", "job", task.Job, "account", exhaustion.Account)
		return
	}
	event := hubJobEventPayload{
		JobID:     task.Job,
		Epoch:     1,
		Reason:    fmt.Sprintf("quota_stop pool=%s account=%s", exhaustion.Pool, exhaustion.Account),
		OwnerLane: task.Owner,
		Host:      machine,
	}
	if next, ok := h.replaceQuotaStopped(ctx, machine, event); ok {
		h.logger.Info("account credit exhausted job handed over", "job", task.Job, "replacement", next, "pool", exhaustion.Pool, "account", exhaustion.Account)
		return
	}
	h.logger.Info("account credit exhausted job not handed over", "job", task.Job, "pool", exhaustion.Pool, "account", exhaustion.Account)
}

// snapshotAccountCreditMarks answers the marks still holding at now for the
// fleet snapshot, dropping (and durably forgetting) the lifted ones so a hub
// restart cannot read them as holding again.
func (h *HubServer) snapshotAccountCreditMarks(now time.Time) map[string]time.Time {
	h.accountCreditOnce.Do(h.loadAccountCreditMarks)
	h.mu.Lock()
	var active map[string]time.Time
	var lifted []string
	for account, mark := range h.accountCreditMarks {
		if h.accountCreditMarkActive(account, mark, now) {
			if active == nil {
				active = map[string]time.Time{}
			}
			active[account] = mark.Marked
			continue
		}
		delete(h.accountCreditMarks, account)
		lifted = append(lifted, account)
	}
	h.mu.Unlock()
	if h.inbox == nil || len(lifted) == 0 {
		return active
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, account := range lifted {
		if err := h.inbox.store.deleteAccountCreditMark(ctx, account); err != nil {
			h.logger.Warn("lifted account credit mark could not be deleted", "account", account, "error", err)
		}
	}
	return active
}
