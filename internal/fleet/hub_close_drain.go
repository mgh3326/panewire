package fleet

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

const closeOpsDrainingFile = ".close-ops-draining"

var ErrCloseOpsDraining = errors.New("close ops are draining")

// closeOpsLock coordinates every close operation with the operator's
// withdrawal, across processes. A shared holder keeps admission and its
// physical close inside the barrier; withdrawal takes the exclusive lock.
func closeOpsLock(ctx context.Context, jobs string, exclusive bool) (func(), error) {
	if err := os.MkdirAll(jobs, 0o700); err != nil {
		return nil, err
	}
	f, err := secureOpen(filepath.Join(jobs, ".close-ops-admission.lock"), true, nil)
	if err != nil {
		return nil, err
	}
	mode := syscall.LOCK_SH
	if exclusive {
		mode = syscall.LOCK_EX
	}
	for {
		err = syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB)
		if err == nil {
			return func() { f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func closeOpsDraining(jobs string) (bool, error) {
	_, err := os.Lstat(filepath.Join(jobs, closeOpsDrainingFile))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A failed publishing sync may still have made the marker visible.
	// Confirm it is durable before using it to cancel an interrupted close.
	dir, err := os.Open(jobs)
	if err != nil {
		return false, err
	}
	defer dir.Close()
	return true, dir.Sync()
}

// LockCloseOpsAdmission takes the shared barrier before any close operation
// starts. A draining marker refuses even a retry of a registered operation.
func LockCloseOpsAdmission(ctx context.Context, jobs string) (func(), error) {
	unlock, err := closeOpsLock(ctx, jobs, false)
	if err != nil {
		return nil, err
	}
	draining, err := closeOpsDraining(jobs)
	if err != nil || draining {
		unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrCloseOpsDraining
	}
	return unlock, nil
}

// BeginCloseOpsDrain withdraws admission without stopping the node. The
// exclusive barrier waits for every admitted operation, including its close,
// before publishing the durable draining marker and removing readiness.
func BeginCloseOpsDrain(ctx context.Context, jobs string) error {
	info, err := os.Stat(jobs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("close ops jobs root is not a directory")
	}
	unlock, err := closeOpsLock(ctx, jobs, true)
	if err != nil {
		return err
	}
	defer unlock()
	root, err := os.OpenRoot(jobs)
	if err != nil {
		return err
	}
	defer root.Close()
	// The marker is the durable proof on which settlement cancels a request;
	// unlike a best-effort rollout marker, its directory sync must succeed.
	if err := durable.ReplaceFileAtomicallyInRootSynced(root, closeOpsDrainingFile, []byte("draining\n"), 0o600); err != nil {
		return err
	}
	for _, name := range []string{closeOpsAdvertiseFile, closeOpsEnabledFile} {
		if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
