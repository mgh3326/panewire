package fleet

import (
	"context"
	"errors"
)

// recordJobUsage writes usage independently of model, since hk accepts only
// usage updates after a job becomes terminal. Reap reads replace completion
// reads only when they contain figures.
func (a *App) recordJobUsage(ctx context.Context, j Job, atReap bool) {
	session, ok := receiptHarnessSession(j.Receipt)
	if !ok {
		return
	}
	if j.Pane != "" {
		if live := a.harnessSession(ctx, j.Pane, session.Kind); live.Source != "unavailable" {
			session = live
		}
	}
	usage := UsageReader{Root: a.Home}.Read(session, str(j.Receipt, "cwd"))
	if usage.Source == "" || atReap && !usage.HasFigures() {
		return
	}
	if e := a.putJobRecord(ctx, j.ID, map[string]any{"usage": usage.Wire()}); e != nil {
		a.warnUsageWrite("usage", e)
	}
	if atReap || j.Terminal || usage.Model == "" {
		return
	}
	store, e := a.jobStore()
	if e == nil {
		var rec map[string]any
		rec, e = store.Get(ctx, j.ID)
		if e == nil {
			switch str(rec, "state") {
			case "completed", "escalated", "lost":
				return
			}
			e = store.Put(ctx, j.ID, map[string]any{"model": usage.Model})
		}
	}
	if e != nil {
		a.warnUsageWrite("model", e)
	}
}

func (a *App) warnUsageWrite(field string, err error) {
	var backend *BackendError
	if errors.As(err, &backend) {
		if backend.Code != "" {
			a.warn("job %s write failed (hk status %d, code %s)", field, backend.Status, backend.Code)
		} else {
			a.warn("job %s write failed (hk status %d)", field, backend.Status)
		}
	} else {
		a.warn("job %s write failed", field)
	}
}

func receiptHarnessSession(receipt map[string]any) (HarnessSession, bool) {
	raw, ok := receipt["harness_session"].(map[string]any)
	if !ok {
		return HarnessSession{}, false
	}
	session := HarnessSession{Kind: str(raw, "kind"), ID: str(raw, "id"), Source: str(raw, "source")}
	return session, session.Kind != ""
}
