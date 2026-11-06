package fleet

import (
	"context"
	"errors"
)

// recordJobUsage writes usage independently of model, since hk accepts only
// usage updates after a job becomes terminal. Reap reads replace completion
// reads only when they contain figures.
func (a *App) recordJobUsage(ctx context.Context, j Job, atReap bool) {
	usage, ok := a.readJobUsage(ctx, j, atReap)
	if !ok {
		return
	}
	a.writeJobUsage(ctx, j.ID, usage)
	if atReap || j.Terminal || usage.Model == "" {
		return
	}
	store, e := a.jobStore()
	if e == nil {
		var rec map[string]any
		rec, e = store.Get(ctx, j.ID)
		if e != nil {
			a.warnUsageOperation("model state read", e)
			return
		}
		switch str(rec, "state") {
		case "completed", "escalated", "lost":
			return
		}
		e = store.Put(ctx, j.ID, map[string]any{"model": usage.Model})
	}
	if e != nil {
		a.warnUsageOperation("model write", e)
	}
}

// readJobUsage reads the job's token usage from its harness session, taking the
// live session reference from herdr when the pane is listed. A caller that is
// about to close the pane reads before it closes: once herdr stops listing the
// pane only the receipt's session is left, which may name none. ok is false when
// there is nothing to write.
func (a *App) readJobUsage(ctx context.Context, j Job, atReap bool) (TokenUsage, bool) {
	session, ok := receiptHarnessSession(j.Receipt)
	if !ok {
		return TokenUsage{}, false
	}
	if j.Pane != "" {
		if live := a.harnessSession(ctx, j.Pane, session.Kind); live.Source != "unavailable" {
			session = live
		}
	}
	usage := UsageReader{Root: a.Home}.Read(session, str(j.Receipt, "cwd"))
	if usage.Source == "" || atReap && !usage.HasFigures() {
		return TokenUsage{}, false
	}
	return usage, true
}

func (a *App) writeJobUsage(ctx context.Context, id string, usage TokenUsage) {
	if e := a.putJobRecord(ctx, id, map[string]any{"usage": usage.Wire()}); e != nil {
		a.warnUsageOperation("usage write", e)
	}
}

func (a *App) warnUsageOperation(operation string, err error) {
	var backend *BackendError
	if errors.As(err, &backend) {
		if backend.Code != "" {
			a.warn("job %s failed (hk status %d, code %s)", operation, backend.Status, backend.Code)
		} else {
			a.warn("job %s failed (hk status %d)", operation, backend.Status)
		}
	} else {
		a.warn("job %s failed", operation)
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
