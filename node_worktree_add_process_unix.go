//go:build darwin || linux

package panewire

import (
	"errors"
	"syscall"
)

// nodeProcessGroupAlive reports whether any process in pgid still runs. known is
// false when the host cannot answer at all; the caller then treats the group as
// alive and refuses the remove.
func nodeProcessGroupAlive(pgid int) (alive, known bool) {
	err := syscall.Kill(-pgid, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return true, true
	case errors.Is(err, syscall.ESRCH):
		return false, true
	}
	return false, false
}
