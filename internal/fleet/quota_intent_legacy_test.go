package fleet

import (
	"context"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// recordQuotaIntent writes the close intent a reviewer's stop used to record
// before hk 5690 (a stopped reviewer is announced to the hub now). A watcher
// started on a ledger that holds one still reconciles it (reconcileQuotaIntent),
// so the tests of that recovery build the ledger with this.
func (a *App) recordQuotaIntent(ctx context.Context, j Job, stop quotastop.Stop) error {
	unlock, err := a.lockJobOperation(ctx, j.ID, quotaRecordLockWait)
	if err != nil {
		return err
	}
	defer unlock()
	if fresh, err := a.job(j.ID); err == nil {
		if _, exists := pendingQuotaIntent(fresh); exists {
			return nil
		}
	}
	if ended, _ := a.endedByResultPosted(ctx, j.ID, false); ended {
		return errQuotaStopSuperseded
	}
	payload := map[string]any{"pane_id": j.Pane, "pool": stop.Pool, "end_reason": QuotaStopEndReason(stop.Pool, stop.Reset, "", "")}
	if !stop.Reset.IsZero() {
		payload["reset_at"] = stop.Reset.UTC().Format(time.RFC3339)
	}
	return a.event(ctx, j.ID, quotaIntentKind, payload)
}
