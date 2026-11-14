package panewire

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os/exec"
)

// nodeAddProcess identifies the git child one worktree add ran: its pid and the
// start time the host reported for that pid while the child was alive. The
// kernel reuses pids, so the start time is what makes the pair an identity: a
// stranger later holding the same pid started at another time and never reads
// as this add.
type nodeAddProcess struct {
	PID   int    `json:"pid"`
	Start string `json:"start"`
}

// nodeAddRunner runs one `git worktree add`. The node's own runner starts git as
// a child and hands record that child's identity as soon as it is known, so the
// intent on disk names the process a restart must find gone before it may remove
// the path. A fixture runner starts no child of the node's; it records the
// identity its test states.
type nodeAddRunner func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error

// nodeAddProcessExited reports whether the add child recorded in an intent has
// exited: the one evidence that lets a forced remove touch its path. An identity
// the node never recorded proves nothing, so it never authorizes a remove, and a
// process the host cannot identify counts as not gone.
func nodeAddProcessExited(proc nodeAddProcess) bool {
	if proc.PID <= 0 || proc.Start == "" {
		return false
	}
	current, known := nodeReadAddProcess(proc.PID)
	return known && current.Start != proc.Start
}

// defaultNodeAddRunner runs one worktree add as this node's child and records
// the child's identity while it is alive.
func defaultNodeAddRunner() nodeAddRunner {
	return func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		cmd := nodeGitCommand(ctx, args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		if proc, ok := nodeReadAddProcess(cmd.Process.Pid); ok && proc.Start != "" {
			if err := record(proc); err != nil {
				slog.Warn("node worktree add identity not recorded", "err", err)
			}
		} else {
			// Without an identity a restart cannot tell this add's child from a
			// stranger. Recovery then refuses to reclaim the path, which is safe
			// but leaves it for an operator to remove.
			slog.Warn("node worktree add identity unreadable", "pid", cmd.Process.Pid)
		}
		err := cmd.Wait()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && len(exitErr.Stderr) == 0 {
				// Keep the child's stderr on the error the way exec.Cmd.Output
				// does, so the node's git failure log names the reason.
				exitErr.Stderr = stderr.Bytes()
			}
		}
		if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
			return nil
		}
		return err
	}
}
