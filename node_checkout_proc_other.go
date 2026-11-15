//go:build !darwin && !linux

package panewire

import "os/exec"

// nodeSetProcessGroup is a no-op where process groups are not used: the
// command is killed alone when its context ends.
func nodeSetProcessGroup(cmd *exec.Cmd) {}
