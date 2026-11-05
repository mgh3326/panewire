package fleet

import (
	"fmt"
	"os"
	"syscall"
)

func tryLock(f *os.File) bool {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

// secureOpen opens a lock file without following symlinks. warn reports the
// in-place mode repair (App.warn in production; nil discards it).
func secureOpen(path string, create bool, warn func(format string, args ...any)) (*os.File, error) {
	flags := os.O_RDWR | syscall.O_NOFOLLOW
	if create {
		flags |= os.O_CREATE
	}
	f, e := os.OpenFile(path, flags, 0600)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.Close()
		return nil, fail(exitConfig, "insecure lock file %s: cannot read its owner and mode", path)
	}
	self := uint32(os.Getuid())
	perm := info.Mode().Perm()
	// Our own lock may be left group-writable by another tool (the retired
	// spawner, a umask 002 shell). Narrow it through the open fd, never the
	// path, and only for a sole-link regular file we own.
	if lockRepairable(info.Mode(), uint64(st.Nlink), st.Uid, st.Gid, perm, self) {
		if e = f.Chmod(0600); e != nil {
			f.Close()
			return nil, fail(exitConfig, "insecure lock file %s: owner %d:%d mode %04o has group-write and chmod 600 failed: %v", path, st.Uid, st.Gid, perm, e)
		}
		if warn != nil {
			warn("repaired lock file %s: mode %04o -> 0600", path, perm)
		}
		return f, nil
	}
	// A root:root lock is also trusted: a host-wide shared lock under /tmp must
	// be root-owned to be opened by other users under Linux protected_regular,
	// and is shared via group/ACL write. Other-write and symlinks/hard links
	// are always refused.
	if why := lockRefusal(info.Mode(), uint64(st.Nlink), st.Uid, st.Gid, perm, self); why != "" {
		f.Close()
		return nil, fail(exitConfig, "insecure lock file %s: owner %d:%d mode %04o: %s", path, st.Uid, st.Gid, perm, why)
	}
	return f, nil
}

// lockRepairable is true for a sole-link regular file we own, with group-write
// and no other-write. A root:root lock is the shared-lock shape and is never
// chmod'ed, even when we run as root.
func lockRepairable(mode os.FileMode, nlink uint64, uid, gid uint32, perm os.FileMode, self uint32) bool {
	return mode.IsRegular() && nlink == 1 && uid == self && !(uid == 0 && gid == 0) && perm&0002 == 0 && perm&0020 != 0
}

// lockRefusal names the rule an unrepairable lock breaks, with the fix that
// fits the case, or returns "" when the lock is acceptable.
func lockRefusal(mode os.FileMode, nlink uint64, uid, gid uint32, perm os.FileMode, self uint32) string {
	switch {
	case !mode.IsRegular():
		return "not a regular file; remove it while no spawn runs"
	case nlink != 1:
		return fmt.Sprintf("has %d hard links; remove it while no spawn runs", nlink)
	case perm&0002 != 0 && uid == self:
		return "other-write is set; chmod 600 it or remove it while no spawn runs"
	case perm&0002 != 0:
		return "other-write is set; a shared lock must be pre-created root:root without other-write"
	case uid == self:
		return ""
	case lockOwnerOK(uid, gid, perm, self):
		return ""
	case uid == 0:
		return "owned by root but not root:root, or group-write on a foreign file; a shared lock must be pre-created root:root without other-write"
	default:
		return fmt.Sprintf("owned by another user (uid %d, not %d); a shared lock must be pre-created root:root without other-write", uid, self)
	}
}

// lockOwnerOK accepts our own lock without group/other write, or a root:root
// shared lock without other write.
func lockOwnerOK(uid, gid uint32, perm os.FileMode, self uint32) bool {
	if uid == self && perm&0022 == 0 {
		return true
	}
	return uid == 0 && gid == 0 && perm&0002 == 0
}
