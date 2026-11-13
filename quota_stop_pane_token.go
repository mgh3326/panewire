package panewire

import (
	"context"
	"strconv"
	"time"
)

// A wait decision shows on the pane itself: panewire reports the display-only
// herdr token panewire_quota from its own source, so an operator sees why the
// pane is idle and until when without asking the daemon. The token never feeds
// a decision — the quota_stop_decisions row stays the only authority — and
// herdr restores no token after its own restart, so a token published before a
// panewire restart is left to its TTL.
const (
	paneQuotaMetadataSource = "panewire.quota"
	paneTokenQuota          = "panewire_quota"
)

const (
	// paneQuotaTokenTTL is herdr's maximum token TTL. A reset further out than
	// this needs the token re-reported.
	paneQuotaTokenTTL = 24 * time.Hour
	// paneQuotaTokenRefreshBefore re-reports a capped token this long before
	// it lapses, so a weekly reset stays visible without a report per tick.
	paneQuotaTokenRefreshBefore = 5 * time.Minute
	// paneQuotaTokenRetryAfter backs off a refused report.
	paneQuotaTokenRetryAfter = 30 * time.Second
	// paneQuotaCallTimeout bounds one report; the observation context that
	// triggered it may already be out of budget.
	paneQuotaCallTimeout = 2 * time.Second
)

// quotaWaitText is the token value for a wait: the pool and the reset, in the
// same RFC 3339 UTC form the decision row and the hk note carry.
func quotaWaitText(pool, reset string) string {
	return truncatePaneMetadataValue(pool + " wait until " + reset)
}

// reportQuotaWait is the manager's default reporter: one herdr pane
// report-metadata call that sets panewire_quota, or clears it with an empty
// value. Source panewire.quota keeps the token beside the pw_ job tokens the
// pane metadata publisher owns.
func (q *quotaStopManager) reportQuotaWait(ctx context.Context, pane, value string, ttl time.Duration) error {
	session := ""
	if q.session != nil {
		session = q.session("", pane)
	}
	ctx = withRelaySession(ctx, session)
	args := []string{"pane", "report-metadata", pane, "--source", paneQuotaMetadataSource}
	if value == "" {
		args = append(args, "--clear-token", paneTokenQuota)
	} else {
		if ttl < time.Millisecond {
			ttl = time.Millisecond
		}
		args = append(args, "--token", paneTokenQuota+"="+value, "--ttl-ms", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	_, err := herdrCommandOutput(ctx, pane, args...)
	return err
}

// showQuotaWait reports the pane's wait token for a reset and records when it
// next needs a report. A wait whose reset is inside herdr's maximum TTL needs
// none: the token lapses after the resume has cleared it.
func (q *quotaStopManager) showQuotaWait(ctx context.Context, pane, pool, reset string, now time.Time) {
	if q.show == nil || pane == "" {
		return
	}
	ttl := paneQuotaTokenTTL
	if at, err := time.Parse(time.RFC3339, reset); err == nil {
		if left := at.Sub(now) + paneQuotaTokenRefreshBefore; left < ttl {
			ttl = left
		}
	}
	showCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), paneQuotaCallTimeout)
	err := q.show(showCtx, pane, quotaWaitText(pool, reset), ttl)
	cancel()
	next := time.Time{}
	switch {
	case err != nil:
		// Never log the pane's text or the token value, only the failure.
		q.logger.Debug("quota wait token was not reported", "error", err)
		next = now.Add(paneQuotaTokenRetryAfter)
	case ttl >= paneQuotaTokenTTL:
		next = now.Add(ttl - paneQuotaTokenRefreshBefore)
	}
	q.showMu.Lock()
	q.shown[pane] = next
	q.showMu.Unlock()
}

// waitTokenDue reports whether pane's wait token is due for its first or next
// report. A pane this manager has not reported yet is due, so a restarted
// daemon re-reports the waits it still holds.
func (q *quotaStopManager) waitTokenDue(pane string, now time.Time) bool {
	q.showMu.Lock()
	defer q.showMu.Unlock()
	next, shown := q.shown[pane]
	return !shown || (!next.IsZero() && !now.Before(next))
}

// refreshQuotaWaits re-reports the token of every pending wait that is due.
// tick runs once a second, so a due time is what keeps this to one report per
// token rather than one per tick.
func (q *quotaStopManager) refreshQuotaWaits(ctx context.Context, now time.Time) {
	if q.show == nil {
		return
	}
	type pendingWait struct{ pane, pool, reset, role string }
	q.store.mu.Lock()
	rows, err := q.store.db.QueryContext(ctx, `SELECT pane,pool,reset,role FROM quota_stop_decisions WHERE decision='wait' AND action_recorded=0`)
	var waits []pendingWait
	if err == nil {
		for rows.Next() {
			var w pendingWait
			if rows.Scan(&w.pane, &w.pool, &w.reset, &w.role) == nil {
				waits = append(waits, w)
			}
		}
		_ = rows.Close()
	}
	q.store.mu.Unlock()
	if err != nil {
		return
	}
	for _, w := range waits {
		if w.pane == "" || !q.quotaWaits(w.role, w.pool) || !q.waitTokenDue(w.pane, now) {
			continue
		}
		q.showQuotaWait(ctx, w.pane, w.pool, w.reset, now)
	}
}

// clearQuotaWait removes pane's wait token once its wait has ended. A pane
// this manager never reported is left alone: there is nothing to clear, and
// the token is not ours to guess at.
func (q *quotaStopManager) clearQuotaWait(ctx context.Context, pane string) {
	q.showMu.Lock()
	_, shown := q.shown[pane]
	delete(q.shown, pane)
	q.showMu.Unlock()
	if !shown || q.show == nil {
		return
	}
	clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), paneQuotaCallTimeout)
	err := q.show(clearCtx, pane, "", 0)
	cancel()
	if err != nil {
		q.logger.Debug("quota wait token was not cleared", "error", err)
	}
}
