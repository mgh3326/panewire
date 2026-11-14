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
	leader := exec.CommandContext(t.Context(), "sleep", "30")
	nodeSetProcessGroup(leader)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	// The member is this test's child in the leader's group, so every process the
	// fixture starts is reaped here: no orphan depends on init.
	member := exec.CommandContext(t.Context(), "sleep", "30")
	member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: leader.Process.Pid}
	if err := member.Start(); err != nil {
		_ = leader.Process.Kill()
		_ = leader.Wait()
		t.Fatal(err)
	}
	group := nodeAddProcess{Group: leader.Process.Pid}
	t.Cleanup(func() {
		_ = syscall.Kill(-group.Group, syscall.SIGKILL)
		_ = leader.Wait()
		_ = member.Wait()
	})
	if err := syscall.Kill(leader.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = leader.Wait()
	if nodeAddProcessExited(group) {
		t.Fatal("the group still holds a member, so the add is not quiescent")
	}
	if err := syscall.Kill(-group.Group, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = member.Wait()
	eventually(t, 5*time.Second, "add group to disappear", func() bool { return nodeAddProcessExited(group) })
}
