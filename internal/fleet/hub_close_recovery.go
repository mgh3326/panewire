package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// RecoverLegacyCloseOps makes pre-epoch operations final without starting a
// close. The exclusive admission lock waits for any old command to finish;
// the job end lock then protects the intent and its journal while inspected.
// See docs/node-commands.md, "Hub close protocol".
func RecoverLegacyCloseOps(ctx context.Context, jobs string, now time.Time) error {
	unlock, err := closeOpsLock(ctx, jobs, true)
	if err != nil {
		return err
	}
	defer unlock()
	ids, err := PendingHubCloseJobs(jobs)
	if err != nil {
		return err
	}
	a := &App{Jobs: jobs}
	for _, id := range ids {
		if err := a.recoverLegacyJobClose(ctx, id, now); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) recoverLegacyJobClose(ctx context.Context, id string, now time.Time) error {
	unlock, err := LockJobEndContext(ctx, filepath.Join(a.Jobs, id))
	if err != nil {
		return err
	}
	defer unlock()
	job, err := a.job(id)
	if err != nil {
		return err
	}
	intents, _, _ := openHubCloseIntents(job)
	root, err := os.OpenRoot(filepath.Join(a.Jobs, id))
	if err != nil {
		return err
	}
	defer root.Close()
	for _, in := range intents {
		op, found, err := ReadCloseOp(root, in.RequestID)
		if err != nil {
			return err
		}
		if in.Epoch != 0 {
			if found && op.Epoch == 0 && !op.Final() {
				return errors.New("epoch-bound intent has a legacy close operation")
			}
			continue
		}
		if !found {
			op = CloseOp{RequestID: in.RequestID, PaneID: in.Pane, Attempt: in.Attempt, Created: now}
		} else if op.Final() {
			continue
		} else if op.Epoch != 0 || op.Attempt != in.Attempt || op.PaneID != in.Pane {
			return errors.New("legacy close intent and operation disagree")
		}
		switch op.State {
		case "", CloseOpRegistered:
			op.State, op.Released = CloseOpCancelled, &now
		case CloseOpClosing:
			op.State = CloseOpUnknown
		default:
			return errors.New("unrecognized legacy close operation state")
		}
		op.Updated = now
		if err := WriteCloseOp(root, op); err != nil {
			return err
		}
	}
	return nil
}
