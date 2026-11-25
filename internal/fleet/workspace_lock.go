package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// The herdr workspace lock (hk 4821 W1c, docs/config.md "Host aliases and
// workspaces"). Every panewire writer of one uid takes the same host-fixed
// lock for a (herdr session, label), whatever its HOME or state directory, so
// only one of them at a time decides whether to create that workspace.

// workspaceLockRootUnsafe is the refusal reason of a lock root that is not a
// private directory of this uid.
const workspaceLockRootUnsafe = "workspace_lock_root_unsafe"

// workspaceLockAttempts bounds how often a lock file replaced between its open
// and its flock is opened again.
const workspaceLockAttempts = 3

// defaultWorkspaceLockRoot is uid's host-fixed workspace lock directory. It
// reads no environment variable: writers with a different HOME,
// XDG_STATE_HOME or TMPDIR must reach the same directory.
func defaultWorkspaceLockRoot(uid int) string {
	return "/tmp/panewire-" + strconv.Itoa(uid) + "/herdr-locks"
}

func (a *App) workspaceLockDir() string {
	if a.workspaceLockRoot != "" {
		return a.workspaceLockRoot
	}
	return defaultWorkspaceLockRoot(os.Getuid())
}

// lockWorkspace takes the locks of (herdr session, label) and returns their
// release. It takes the host-fixed lock and then the lock under the state
// directory, always in that order. An older binary takes only the second one,
// so the two exclude each other during a rollout, and no wait cycle exists.
// Each wait ends with ctx.
func (a *App) lockWorkspace(ctx context.Context, label string) (release func(), err error) {
	sum := sha256.Sum256([]byte(a.HerdrSession + "\x00" + label))
	name := hex.EncodeToString(sum[:8]) + ".lock"
	host, err := a.lockHostWorkspace(ctx, name)
	if err != nil {
		return nil, err
	}
	legacy, err := a.lockStateDirWorkspace(ctx, name)
	if err != nil {
		host.Close()
		return nil, err
	}
	a.refreshWorkspaceLockTimes(host.Name())
	return func() {
		legacy.Close()
		host.Close()
	}, nil
}

// lockHostWorkspace returns the held lock file name under the host-fixed root.
// The file it holds is the one at the path: a path replaced or removed between
// the open and the flock is opened again, since a lock on the old file
// excludes nobody.
func (a *App) lockHostWorkspace(ctx context.Context, name string) (*os.File, error) {
	dir := a.workspaceLockDir()
	path := filepath.Join(dir, name)
	for range workspaceLockAttempts {
		// The parent first, so the lock directory is never made through a link.
		for _, level := range []string{filepath.Dir(dir), dir} {
			if err := ensureWorkspaceLockDir(level, os.Getuid()); err != nil {
				return nil, err
			}
		}
		lock, err := secureOpen(path, true, a.warn)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if a.workspaceLockOpened != nil {
			a.workspaceLockOpened(path)
		}
		if err := waitLock(ctx, lock); err != nil {
			lock.Close()
			return nil, err
		}
		if lockStillAtPath(lock, path) {
			return lock, nil
		}
		lock.Close()
	}
	return nil, fail(exitConfig, "workspace lock %s was replaced or removed during each of %d attempts; stop whatever rewrites it, then retry", path, workspaceLockAttempts)
}

// lockStateDirWorkspace holds the lock every binary before hk 6434 takes, at
// its unchanged path under the state directory.
func (a *App) lockStateDirWorkspace(ctx context.Context, name string) (*os.File, error) {
	stateDir := a.stateDir()
	if stateDir == "" {
		return nil, errors.New("workspace lock requires an absolute StateDir or Home")
	}
	dir := filepath.Join(stateDir, "workspace-locks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := secureOpen(filepath.Join(dir, name), true, a.warn)
	if err != nil {
		return nil, err
	}
	if err := waitLock(ctx, lock); err != nil {
		lock.Close()
		return nil, err
	}
	return lock, nil
}

// waitLock polls until lock is held or ctx ends. The poll stays on the wall
// clock: it is a wait, not a recorded time.
func waitLock(ctx context.Context, lock *os.File) error {
	for !tryLock(lock) {
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

// lockStillAtPath reports that path still names the file lock holds open.
func lockStillAtPath(lock *os.File, path string) bool {
	held, err := lock.Stat()
	if err != nil {
		return false
	}
	named, err := os.Lstat(path)
	return err == nil && os.SameFile(held, named)
}

// ensureWorkspaceLockDir makes dir (mode 0700) when missing and refuses one
// that is not a directory (a link included) owned by uid without group or
// other write. Under /tmp another user can create the path first; the spawn
// then fails closed rather than lock where that user can interfere.
func ensureWorkspaceLockDir(dir string, uid int) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fail(exitConfig, "cannot create workspace lock root %s: %v", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fail(exitConfig, "cannot read workspace lock root %s: %v", dir, err)
	}
	owner := -1
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		owner = int(st.Uid)
	}
	if !info.IsDir() || owner != uid || info.Mode().Perm()&0o022 != 0 {
		return fail(exitConfig, "%s: workspace lock root %s is not a private directory of uid %d (owner %d, mode %o, link %t); remove it as its owner or root, then retry",
			workspaceLockRootUnsafe, dir, uid, owner, info.Mode().Perm(), info.Mode()&fs.ModeSymlink != 0)
	}
	return nil
}

// refreshWorkspaceLockTimes sets the access and modification times of the held
// lock file, its directory and the lock root to now, so an age-based /tmp
// cleaner (macOS clean-tmps, systemd-tmpfiles) does not select a held lock.
// They are file metadata on the wall clock, not recorded times. A failure only
// weakens that guard, so it is warned about and the spawn goes on.
func (a *App) refreshWorkspaceLockTimes(path string) {
	now := time.Now()
	dir := filepath.Dir(path)
	for _, target := range []string{path, dir, filepath.Dir(dir)} {
		if err := os.Chtimes(target, now, now); err != nil {
			a.warn("workspace lock times not refreshed on %s: %v", target, err)
		}
	}
}
