//go:build unix

package codex

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the command in its own process group, so killing the
// group takes any child the app-server started with it.
func setProcessGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
