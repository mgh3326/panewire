//go:build !unix

package claude

import "os/exec"

// killGroupOnCancel has no process groups to use here; the context kills the
// child itself.
func killGroupOnCancel(*exec.Cmd) {}
