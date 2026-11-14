package panewire

import (
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

// nodeAddStderrLimit bounds the stderr kept from one worktree add. The node's
// git failure log keeps only the tail of stderr, so the tail is what is worth
// keeping.
const nodeAddStderrLimit = 32 << 10

// nodeAddStderr keeps the last nodeAddStderrLimit bytes written and drains the
// rest, so a chatty checkout hook can neither grow the node's memory for the
// whole add nor block on a full pipe.
type nodeAddStderr struct {
	tail []byte
}

func (s *nodeAddStderr) Write(p []byte) (int, error) {
	s.tail = append(s.tail, p...)
	if len(s.tail) > nodeAddStderrLimit {
		copy(s.tail, s.tail[len(s.tail)-nodeAddStderrLimit:])
		s.tail = s.tail[:nodeAddStderrLimit]
	}
	return len(p), nil
}

// Bytes returns the retained tail. The caller must not modify it.
func (s *nodeAddStderr) Bytes() []byte { return s.tail }

// defaultNodeAddRunner runs one worktree add as this node's child, in a group of
// its own, and records the group before the child can outlive the node. The group
// is also killed when the add's context ends, so a cancelled add leaves no
// checkout hook writing the worktree behind.
func defaultNodeAddRunner() nodeAddRunner {
	return func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		cmd := nodeGitCommand(ctx, args...)
		nodeSetProcessGroup(cmd)
		stderr := &nodeAddStderr{}
		cmd.Stderr = stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		if err := record(nodeAddProcess{Group: cmd.Process.Pid}); err != nil {
			slog.Warn("node worktree add identity not recorded", "err", err)
		}
		err := cmd.Wait()
		if ctx.Err() != nil && cmd.Cancel != nil {
			// Wait reads the context result once git exits, so an expiry inside
			// the drain window never cancels the group. Kill it here; a checkout
			// hook the group still holds would keep writing a cancelled add.
			_ = cmd.Cancel()
		}
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
