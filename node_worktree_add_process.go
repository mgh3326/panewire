package panewire

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os/exec"
)

// nodeAddProcess is the identity of one worktree add: the process group its git
// child leads. git starts checkout hooks as children of that group, so the group
// — not the git pid — is the unit a restart must find gone before it may remove
// the path. A group id the kernel later reuses reads as alive, which only refuses
// a removal; it can never make one happen.
type nodeAddProcess struct {
	Group int `json:"group"`
}

// nodeAddRunner runs one `git worktree add`. The node's own runner starts git as
// a child in a group of its own and hands record that group as soon as it is
// known, so the intent on disk names what a restart must find gone before it may
// remove the path. A fixture runner starts no child of the node's; it records the
// identity its test states.
type nodeAddRunner func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error

// nodeAddProcessExited reports whether everything the add child started has
// exited: the one evidence that lets a forced remove touch its path. An identity
// the node never recorded proves nothing, so it never authorizes a remove, and a
// host that cannot answer counts as not gone.
func nodeAddProcessExited(proc nodeAddProcess) bool {
	if proc.Group <= 0 {
		return false
	}
	alive, known := nodeProcessGroupAlive(proc.Group)
	return known && !alive
}

// defaultNodeAddRunner runs one worktree add as this node's child, in a group of
// its own, and records the group before the child can outlive the node. The group
// is also killed when the add's context ends, so a cancelled add leaves no
// checkout hook writing the worktree behind.
func defaultNodeAddRunner() nodeAddRunner {
	return func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		cmd := nodeGitCommand(ctx, args...)
		nodeSetProcessGroup(cmd)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		if err := record(nodeAddProcess{Group: cmd.Process.Pid}); err != nil {
			slog.Warn("node worktree add identity not recorded", "err", err)
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
