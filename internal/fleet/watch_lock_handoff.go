package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// A watcher of this binary lends its watch lock to the node for an open hub
// close (hk 4632; #662 consult, design 2). It serves the very open file
// description that holds `.panewire-watch.lock` over a unix socket
// (SCM_RIGHTS), and the node keeps that reference while any intent of the job
// is open. flock is held until every reference to the description closes, so
// the watcher exiting or being killed mid-close cannot free the lock: no
// watcher of any binary can take it before the node releases it. A second
// open of the same path would be a different description and would not do.

// watchLockSocket is the watcher's lending socket for a job directory: inside
// it when the path fits a sockaddr_un (104 bytes on macOS), else a name in
// /tmp derived from the directory.
func watchLockSocket(jobDir string) string {
	if path := filepath.Join(jobDir, ".panewire-watch.sock"); len(path) < 100 {
		return path
	}
	sum := sha256.Sum256([]byte(filepath.Clean(jobDir)))
	return filepath.Join("/tmp", "panewire-watch-"+hex.EncodeToString(sum[:12])+".sock")
}

// serveWatchLock lends lock (the watcher's locked watch-lock file) to every
// connection on the job's socket until the returned stop runs. The socket is
// listening before the watcher takes its capability lock, so a watcher the
// node probes as aware can always lend.
func serveWatchLock(jobDir string, lock *os.File) (stop func(), err error) {
	path := watchLockSocket(jobDir)
	_ = os.Remove(path)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, _, _ = conn.WriteMsgUnix([]byte{1}, syscall.UnixRights(int(lock.Fd())), nil)
			conn.Close()
		}
	}()
	return func() {
		listener.Close()
		<-done
		_ = os.Remove(path)
	}, nil
}

// errNoLend is a watcher that did not lend its lock.
var errNoLend = errors.New("watcher did not lend its watch lock")

// BorrowWatchLock receives the running watcher's locked watch-lock file
// description and returns its release (closing only this reference). It
// verifies the description is the job's watch-lock file.
func BorrowWatchLock(ctx context.Context, jobDir string) (release func(), err error) {
	var dialer net.Dialer
	c, err := dialer.DialContext(ctx, "unix", watchLockSocket(jobDir))
	if err != nil {
		return nil, err
	}
	conn := c.(*net.UnixConn)
	defer conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	buf, oob := make([]byte, 1), make([]byte, syscall.CmsgSpace(4))
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, err
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil || len(msgs) != 1 {
		return nil, errNoLend
	}
	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil || len(fds) != 1 {
		return nil, errNoLend
	}
	f := os.NewFile(uintptr(fds[0]), "borrowed-watch-lock")
	got, err := f.Stat()
	want, err2 := os.Stat(filepath.Join(jobDir, jobWatchLock))
	if err != nil || err2 != nil || !os.SameFile(got, want) {
		f.Close()
		return nil, errNoLend
	}
	return func() { f.Close() }, nil
}
