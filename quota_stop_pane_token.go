package panewire

import (
	"context"
	"sort"
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
	// paneQuotaTokenRetryAfter backs off a refused report or clear.
	paneQuotaTokenRetryAfter = 30 * time.Second
	// paneQuotaCallTimeout bounds one report; the observation context that
	// triggered it may already be out of budget.
	paneQuotaCallTimeout = 2 * time.Second
)

// shownQuotaToken is one pane's report of ours: when herdr needs it again,
// when herdr drops it without another report, and whether herdr took it.
// reported gates a clear: a report that never arrived leaves nothing of ours
// to clear. expiry bounds the retries of a clear herdr refused.
type shownQuotaToken struct {
	next     time.Time // next report due; zero when the token lapses unrefreshed
	expiry   time.Time // herdr drops the key at this point
	reported bool      // herdr took the last report, so a clear is owed
}

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
// none: the token lapses after the resume has cleared it. A report error is
// logged once and retried; it never changes the wait decision.
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
	// The lock is held across the report so a publish and a clear for one
	// pane cannot cross on their way to herdr.
	q.showMu.Lock()
	defer q.showMu.Unlock()
	err := q.show(showCtx, pane, quotaWaitText(pool, reset), ttl)
	cancel()
	if err != nil {
		q.logQuotaTokenFailure(err)
		// Only a report herdr already took leaves a token to clear, and that
		// token keeps the earlier expiry: a failed refresh does not extend it.
		token := shownQuotaToken{next: now.Add(paneQuotaTokenRetryAfter)}
		if prev, ok := q.shown[pane]; ok && prev.reported {
			token.reported = true
			token.expiry = prev.expiry
			if now.Add(ttl).Before(token.expiry) {
				token.expiry = now.Add(ttl)
			}
		}
		q.shown[pane] = token
		return
	}
	q.showErrLogged = false
	// A fresh report supersedes any clear still owed for this pane.
	delete(q.clearing, pane)
	next := time.Time{}
	if ttl >= paneQuotaTokenTTL {
		next = now.Add(ttl - paneQuotaTokenRefreshBefore)
	}
	q.shown[pane] = shownQuotaToken{next: next, expiry: now.Add(ttl), reported: true}
}

// logQuotaTokenFailure reports the first failure of a streak, with the
// failure alone: never the pane's text or the token value. A herdr that keeps
// refusing the call must not print the same line on every retry, and the
// refusal must not change the quota-stop decision. showMu must be held.
func (q *quotaStopManager) logQuotaTokenFailure(err error) {
	if q.showErrLogged {
		return
	}
	q.showErrLogged = true
	q.logger.Warn("quota wait token was not reported", "error", err)
}

// waitTokenDue reports whether pane's wait token is due for its first or next
// report. A pane this manager has not reported yet is due, so a restarted
// daemon re-reports the waits it still holds.
func (q *quotaStopManager) waitTokenDue(pane string, now time.Time) bool {
	q.showMu.Lock()
	defer q.showMu.Unlock()
	token, shown := q.shown[pane]
	return !shown || (!token.next.IsZero() && !now.Before(token.next))
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
func (q *quotaStopManager) clearQuotaWait(ctx context.Context, pane string, now time.Time) {
	if q.show == nil || pane == "" {
		return
	}
	q.showMu.Lock()
	if token, shown := q.shown[pane]; !shown || !token.reported {
		delete(q.clearing, pane)
		q.showMu.Unlock()
		return
	}
	// A clear herdr refuses is retried by retryQuotaClears until the token
	// lapses; the pane keeps advertising the wait until then either way.
	q.clearing[pane] = now
	q.showMu.Unlock()
	q.retryQuotaClears(ctx, now)
}

// retryQuotaClears sends every clear that is due. clearQuotaWait sends one at
// once; tick calls this so a failed clear is tried again. A pane whose token
// lapsed needs no clear: herdr has already dropped it.
func (q *quotaStopManager) retryQuotaClears(ctx context.Context, now time.Time) {
	if q.show == nil {
		return
	}
	q.showMu.Lock()
	var due []string
	for pane, at := range q.clearing {
		token, shown := q.shown[pane]
		if !shown || !token.reported || !now.Before(token.expiry) {
			delete(q.clearing, pane)
			delete(q.shown, pane)
			continue
		}
		if !now.Before(at) {
			due = append(due, pane)
		}
	}
	q.showMu.Unlock()
	sort.Strings(due)
	for _, pane := range due {
		clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), paneQuotaCallTimeout)
		q.showMu.Lock()
		err := q.show(clearCtx, pane, "", 0)
		switch {
		case err == nil:
			q.showErrLogged = false
			delete(q.shown, pane)
			delete(q.clearing, pane)
		default:
			q.logQuotaTokenFailure(err)
			q.clearing[pane] = now.Add(paneQuotaTokenRetryAfter)
		}
		q.showMu.Unlock()
		cancel()
	}
}
