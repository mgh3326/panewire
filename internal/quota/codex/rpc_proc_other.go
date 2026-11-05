//go:build !unix

package codex

import "os/exec"

// There are no process groups to use here; the app-server itself is killed.
func setProcessGroup(*exec.Cmd) {}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
