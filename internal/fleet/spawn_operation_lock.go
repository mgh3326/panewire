package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// holdJobOperation takes the job's operation lock for a spawn (the lock `done`
// holds, TryLockJobOperation), waiting for another holder until ctx ends. The
// release is safe to call more than once: a spawn calls it before a --wait and
// defers it for every other way out.
func (a *App) holdJobOperation(ctx context.Context, id string) (func(), error) {
	dir := filepath.Join(a.Jobs, id)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, fail(exitStateConflict, "job operation lock dir: %v", e)
	}
	for {
		unlock, e := TryLockJobOperation(dir)
		if e == nil {
			var once sync.Once
			return func() { once.Do(unlock) }, nil
		}
		if !errors.Is(e, ErrJobOperationBusy) {
			return nil, fail(exitStateConflict, "job operation lock: %v", e)
		}
		select {
		case <-ctx.Done():
			return nil, fail(exitStateConflict, "job %s is busy: another command holds its operation lock", id)
		case <-time.After(jobOperationLockPoll):
		}
	}
}
