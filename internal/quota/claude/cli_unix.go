//go:build unix

package claude

import (
	"os/exec"
	"syscall"
)

// killGroupOnCancel puts the command in its own process group and kills the
// whole group when the context ends, so a hung child leaves nothing behind.
func killGroupOnCancel(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
