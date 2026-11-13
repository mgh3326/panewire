//go:build !darwin && !linux

package panewire

import "os/exec"

// nodeCheckoutSetGroup is a no-op where process groups are not used: the
// command is killed alone when its context ends.
func nodeCheckoutSetGroup(cmd *exec.Cmd) {}
