//go:build darwin || linux

package panewire

import (
	"os/exec"
	"syscall"
)

// nodeSetProcessGroup runs cmd in its own process group and kills the whole
// group when its context ends. A git helper or an install.sh child then cannot
// outlive the op's bound, the way a lone exec.CommandContext kill would allow.
func nodeSetProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
}
