//go:build unix

package devin

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const reapWait = 2 * time.Second

// ptyTerminal is the devin TUI on a real pseudo-terminal. pty.Start makes the
// child a session leader, so its pid names its process group.
type ptyTerminal struct {
	*os.File
	cmd    *exec.Cmd
	waited chan struct{}
	once   sync.Once
}

// startPTY runs binary in dir on a 200x50 terminal with the environment the
// TUI needs and no credential added.
func startPTY(_ context.Context, binary, dir string, args []string) (Terminal, error) {
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, errors.New("probe executable unavailable")
	}
	cmd := exec.Command(path, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "TERM=xterm-256color", "NO_COLOR=1")
	file, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 50, Cols: 200})
	if err != nil {
		return nil, err
	}
	t := &ptyTerminal{File: file, cmd: cmd, waited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(t.waited)
	}()
	return t, nil
}

// Close kills the TUI's whole process group, closes the terminal and waits for
// the process to be reaped, at most reapWait, so a probe never leaks a devin.
func (t *ptyTerminal) Close() error {
	t.once.Do(func() {
		select {
		case <-t.waited: // already reaped; its pid may be reused
		default:
			_ = syscall.Kill(-t.cmd.Process.Pid, syscall.SIGTERM)
			_ = syscall.Kill(-t.cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = t.File.Close()
		select {
		case <-t.waited:
		case <-time.After(reapWait):
		}
	})
	return nil
}
