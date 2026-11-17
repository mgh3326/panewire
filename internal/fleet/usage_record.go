package fleet

import (
	"context"
	"errors"
)

// jobSession is what one job's harness session proves for its hk record: the
// usage figures and the skills it called. haveUsage and the skills flags say
// which parts may be sent; a reap read with no figures must not erase the
// completion read, and an unreadable skill record must not erase a known list.
type jobSession struct {
	usage     TokenUsage
	haveUsage bool
	skills    SkillUsage
}

// recordJobUsage writes usage and skills independently of model, since hk
// accepts only those fields after a job becomes terminal. Reap reads replace
// completion reads only when they contain figures or a readable skill list.
func (a *App) recordJobUsage(ctx context.Context, j Job, atReap bool) {
	session, ok := a.readJobSession(ctx, j, atReap)
	if !ok {
		return
	}
	a.writeJobSession(ctx, j, session)
	if atReap || j.Terminal || session.usage.Model == "" {
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
		if a.hubReassigned(j) {
			// The model is this node's session; the record is the new owner's.
			return
		}
		e = store.Put(ctx, j.ID, map[string]any{"model": session.usage.Model})
	}
	if e != nil {
		a.warnUsageOperation("model write", e)
	}
}

// readJobSession reads the job's token usage and skill calls from its harness
// session, taking the live session reference from herdr when the pane is
// listed. A caller that is about to close the pane reads before it closes:
// once herdr stops listing the pane only the receipt's session is left, which
// may name none. ok is false when the receipt names no session.
func (a *App) readJobSession(ctx context.Context, j Job, atReap bool) (jobSession, bool) {
	ref, ok := receiptHarnessSession(j.Receipt)
	if !ok {
		return jobSession{}, false
	}
	if j.Pane != "" {
		if live := a.harnessSession(ctx, j.Pane, ref.Kind); live.Source != "unavailable" {
			ref = live
		}
	}
	reader := UsageReader{Root: a.Home}
	cwd := str(j.Receipt, "cwd")
	session := jobSession{
		usage:  reader.Read(ref, cwd),
		skills: reader.ReadSkills(ref, cwd),
	}
	session.haveUsage = session.usage.Source != "" && (!atReap || session.usage.HasFigures())
	return session, true
}

// writeJobSession writes usage and skills in one PUT, the only update hk
// accepts after a job is terminal. A refusal is a warning: job end is never
// failed by a record write.
func (a *App) writeJobSession(ctx context.Context, j Job, session jobSession) {
	if a.hubReassigned(j) {
		// The usage is this node's session; the record is the new owner's.
		return
	}
	id := j.ID
	fields := map[string]any{"skills_supported": session.skills.Supported}
	if session.haveUsage {
		fields["usage"] = session.usage.Wire()
	}
	if session.skills.Known && session.skills.Supported && len(session.skills.Skills) > 0 {
		fields["skills"] = session.skills.Skills
	}
	if e := a.putJobRecord(ctx, id, fields); e != nil {
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
