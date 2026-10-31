//go:build !darwin && !linux

package panewire

import (
	"errors"
	"os"
)

// tryNodeGitOpLock has no flock here; an exclusive create stands in, and a
// crashed holder would leave the file behind for an operator to remove.
func tryNodeGitOpLock(path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil, errNodeGitOpBusy
	}
	if err != nil {
		return nil, err
	}
	held, err := file.Stat()
	if err != nil {
		file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return func() {
		// Unlink only our own file: another process may already hold a new
		// lock at the same path. Close first; some platforms cannot remove
		// an open file.
		file.Close()
		if current, err := os.Stat(path); err == nil && os.SameFile(held, current) {
			_ = os.Remove(path)
		}
	}, nil
}
