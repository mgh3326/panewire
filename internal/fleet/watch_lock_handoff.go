package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
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

// watchLockSocket is the watcher's lending socket for a job directory. It
// lives in a directory only this user can enter (watchLockSocketDir), so no
// other local user can connect, even before the socket's own mode is set: the
// directory is the peer check.
func watchLockSocket(jobDir string) (string, error) {
	dir, err := watchLockSocketDir(jobDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "watch.sock"), nil
}

// watchLockBaseDirHook, when non-nil, overrides the base directory for
// long-path lending sockets in tests.
var watchLockBaseDirHook func() string

func watchLockBaseDir() string {
	if watchLockBaseDirHook != nil {
		return watchLockBaseDirHook()
	}
	return filepath.Join("/tmp", "panewire-"+strconv.Itoa(os.Getuid()))
}

// watchLockSocketDir is `<job>/.panewire-watch-sock`, or, when that path is
// too long for a sockaddr_un (104 bytes on macOS),
// `/tmp/panewire-<uid>/<hash of the job dir>`. Each is created 0700 and must
// be a real directory owned by this user with no group or other access.
func watchLockSocketDir(jobDir string) (string, error) {
	dir := filepath.Join(jobDir, ".panewire-watch-sock")
	if len(dir)+len("/watch.sock") >= 100 {
		sum := sha256.Sum256([]byte(filepath.Clean(jobDir)))
		base := watchLockBaseDir()
		if err := privateDir(base); err != nil {
			return "", err
		}
		dir = filepath.Join(base, hex.EncodeToString(sum[:12]))
	}
	return dir, privateDir(dir)
}

// privateDir makes dir (mode 0700) when missing and verifies it is a
// directory, not a symlink, owned by this user, with no group or other bits.
func privateDir(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	return verifyPrivateDir(dir, info, os.Getuid())
}

func verifyPrivateDir(dir string, info fs.FileInfo, uid int) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !ok || int(st.Uid) != uid {
		return fmt.Errorf("watch-lock socket directory %s is not private", dir)
	}
	return nil
}

// serveWatchLock lends lock (the watcher's locked watch-lock file) to every
// connection on the job's socket until the returned stop runs. The socket is
// listening before the watcher takes its capability lock, so a watcher the
// node probes as aware can always lend.
func serveWatchLock(jobDir string, lock *os.File) (stop func(), err error) {
	dir, err := watchLockSocketDir(jobDir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "watch.sock")
	_ = os.Remove(path)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		_ = os.Remove(dir)
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		_ = os.Remove(path)
		_ = os.Remove(dir)
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
		_ = os.Remove(dir)
	}, nil
}

// errNoLend is a watcher that did not lend its lock.
var errNoLend = errors.New("watcher did not lend its watch lock")

// BorrowWatchLock receives the running watcher's locked watch-lock file
// description and returns its release (closing only this reference). It
// verifies the description is the job's watch-lock file.
func BorrowWatchLock(ctx context.Context, jobDir string) (release func(), err error) {
	path, err := watchLockSocket(jobDir)
	if err != nil {
		return nil, err
	}
	var dialer net.Dialer
	c, err := dialer.DialContext(ctx, "unix", path)
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
