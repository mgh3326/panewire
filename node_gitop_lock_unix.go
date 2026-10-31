//go:build darwin || linux

package panewire

import (
	"errors"
	"os"
	"syscall"
)

// tryNodeGitOpLock takes the clone's git-op lock once. A held lock reports
// errNodeGitOpBusy; the flock releases itself if the process dies.
func tryNodeGitOpLock(path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errNodeGitOpBusy
		}
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
	}, nil
}
