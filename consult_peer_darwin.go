//go:build darwin

package panewire

import (
	"errors"
	"golang.org/x/sys/unix"
	"net"
)

func localPeerPID(c net.Conn) (int, error) {
	u, ok := c.(*net.UnixConn)
	if !ok {
		return 0, errors.New("not a unix socket")
	}
	raw, err := u.SyscallConn()
	if err != nil {
		return 0, err
	}
	var pid int
	var inner error
	err = raw.Control(func(fd uintptr) { pid, inner = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID) })
	if err != nil {
		return 0, err
	}
	return pid, inner
}
