//go:build darwin || linux

package panewire

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// Finding (mergewake high r4574643477 on #560): git starts checkout hooks as
// children of the add's process group, so the git pid alone is no evidence that
// the add is quiescent. The group is: while any member runs, a replay must leave
// the initializing worktree alone.
func TestNodeAddProcessExitedWaitsForTheGroupNotTheLeader(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "sh", "-c", "sleep 30 &")
	nodeSetProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	group := nodeAddProcess{Group: cmd.Process.Pid}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-group.Group, syscall.SIGKILL) })
	if nodeAddProcessExited(group) {
		t.Fatal("the group still holds a hook, so the add is not quiescent")
	}
	if err := syscall.Kill(-group.Group, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "add group to disappear", func() bool { return nodeAddProcessExited(group) })
}
