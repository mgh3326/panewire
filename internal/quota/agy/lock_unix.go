//go:build unix

package agy

import (
	"os"
	"path/filepath"
	"syscall"
)

// withFileLock runs fn under an advisory kernel lock on path, released even
// after SIGKILL. The lock file is never unlinked, so no two lock generations
// compete. It is pacelever's WithFileLock, so the two coordinate.
func withFileLock(path string, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return fn()
}
