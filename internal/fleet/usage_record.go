package fleet

import (
	"context"
	"errors"
)

// recordJobUsage reads the job's harness usage and writes it, with the model
// when known, through PUT /v1/jobs/{job}. The usage object replaces the stored
// one as a whole, so the reap read overwrites the completion read. It is a
// separate write after the terminal record, and a failure only warns: the
// job's completion result never depends on it.
//
// At reap (atReap) a read with no figures is not written, so a record that is
// gone by then cannot erase what the completion read stored.
//
// A job whose receipt has no harness_session (spawned before hk 3004) names no
// record to read and gets no write.
func (a *App) recordJobUsage(ctx context.Context, j Job, atReap bool) {
	session, ok := receiptHarnessSession(j.Receipt)
	if !ok {
		return
	}
	// The session reference can appear after landing; prefer the live one.
	if j.Pane != "" {
		if live := a.harnessSession(ctx, j.Pane, session.Kind); live.Source != "unavailable" {
			session = live
		}
	}
	usage := UsageReader{Root: a.Home}.Read(session, str(j.Receipt, "cwd"))
	if usage.Source == "" || atReap && !usage.HasFigures() {
		return
	}
	fields := map[string]any{"usage": usage.Wire()}
	if usage.Model != "" {
		fields["model"] = usage.Model
	}
	if e := a.putJobRecord(ctx, j.ID, fields); e != nil {
		var backend *BackendError
		if errors.As(e, &backend) {
			a.warn("job usage write failed (hk status %d)", backend.Status)
		} else {
			a.warn("job usage write failed")
		}
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
