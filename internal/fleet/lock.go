package fleet

import (
	"os"
	"syscall"
)

func tryLock(f *os.File) bool {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

func secureOpen(path string, create bool) (*os.File, error) {
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
	// Our own lock must be 0600-style (no group/other write). A root:root lock
	// is also trusted: a host-wide shared lock under /tmp must be root-owned to
	// be opened by other users under Linux protected_regular, and is shared via
	// group/ACL write. Other-write and symlinks/hard links are always refused.
	if !ok || !info.Mode().IsRegular() || st.Nlink != 1 || !lockOwnerOK(st.Uid, st.Gid, info.Mode().Perm(), uint32(os.Getuid())) {
		f.Close()
		return nil, fail(exitConfig, "insecure lock file %s (a lock shared by several users must be pre-created root:root without other-write, e.g. a tmpfiles.d rule)", path)
	}
	return f, nil
}

// lockOwnerOK accepts our own lock without group/other write, or a root:root
// shared lock without other write.
func lockOwnerOK(uid, gid uint32, perm os.FileMode, self uint32) bool {
	if uid == self && perm&0022 == 0 {
		return true
	}
	return uid == 0 && gid == 0 && perm&0002 == 0
}
